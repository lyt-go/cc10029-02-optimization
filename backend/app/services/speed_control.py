"""速率整形领域服务：维护传输速率的每秒记录与字节上限、突发窗口与排队整形策略。

本模块控制数据传输的速率上限与排队整形，支持令牌桶算法做限速策略生效验证，
整形结果输出到速率产物文件并联动告警。

失败恢复增强（与传输批次共享同一份发放清单）：
- ``plan_batch_grants`` 按记录数与字节双阈值把批次切成多个分片，逐片做额度发放，
  输出 ``{batch}_grants.jsonl`` 整形发放清单，传输侧同步读取同一份清单发送；
- 每片清单记录分片编号、发放结果（granted/waiting）、生效策略快照（策略编号/版本/
  当时额度）与实际扣减额度；
- ``quota_ledger.jsonl`` 按（批次编号，分片编号）幂等记账，已接受分片重投不再扣额度；
- ``reschedule_chunk`` 在目标端调整额度后重新评估排队分片，清单原地更新并保留
  新生效策略快照。
"""
from __future__ import annotations

import json
import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.transfer_batch import plan_transfer_chunks
from app.store import store


class SpeedControlService(BaseModuleService):
    MODULE = "speed_control"
    ENTITY = "限速策略"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["策略编号", "作用范围", "每秒记录数", "每秒字节数", "突发窗口", "排队上限", "整形算法", "策略版本"]
    REQUIRED_FIELDS = ["策略编号", "作用范围", "每秒记录数"]
    STATUS_ORDER = ["草稿", "待生效", "已生效", "已停用"]
    ACTION_RULES = {"提交生效": "待生效", "确认生效": "已生效", "标记停用": "已停用"}
    NEGATIVE_ACTIONS = ["标记停用"]

    def create_policy(self, scope: str, records_per_sec: int, bytes_per_sec: int = 1048576) -> dict[str, Any]:
        """创建速率整形策略：定义每秒记录数上限、突发窗口和整形算法。"""
        policy_code = f"SPEE-{scope}-{int(time.time())}"
        values = {
            "策略编号": policy_code,
            "作用范围": scope,
            "每秒记录数": str(records_per_sec),
            "每秒字节数": str(bytes_per_sec),
            "突发窗口": "60",
            "排队上限": "10000",
            "整形算法": "token_bucket",
            "策略版本": "v1.0",
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建限速策略失败: {missing}")
        self._write_jsonl(
            self._data_dir("events") / "speed_policy_log.jsonl",
            [{"action": "create_policy", "policy_code": policy_code, "scope": scope,
              "records_per_sec": records_per_sec, "at": time.time()}]
        )
        return entry

    def apply_shaping(self, policy_id: int, batch_code: str, record_count: int) -> dict[str, Any]:
        """应用限速策略到传输批次：按令牌桶算法计算实际发放速率，生成速率整形产物。

        本方法执行令牌桶算法（token_bucket）：
        - 按每秒记录数限速计算令牌消耗
        - 与突发窗口对比判断是否限速生效
        - 写入速率整形产物文件记录排队情况
        """
        policy = self.get_entry(policy_id)
        if policy is None:
            return {"error": f"限速策略 {policy_id} 不存在"}

        records_per_sec = int(policy.get("每秒记录数", 100))
        burst_window = int(policy.get("突发窗口", 60))
        queue_limit = int(policy.get("排队上限", 10000))

        # 令牌桶算法：在突发窗口内计算允许通过的速率
        if record_count > records_per_sec * burst_window / 60:
            effective_rate = records_per_sec
            shaped = True
            queued = record_count - records_per_sec
        else:
            effective_rate = record_count
            shaped = False
            queued = 0

        # 超出排队上限则触发告警
        alert_triggered = queued > queue_limit

        # 写入速率整形产物文件
        shaping_file = self._data_dir("shaping") / f"{batch_code}_shaping.jsonl"
        self._write_jsonl(shaping_file, [{
            "batch_code": batch_code,
            "policy_code": policy["策略编号"],
            "input_records": record_count,
            "records_per_sec_limit": records_per_sec,
            "burst_window": burst_window,
            "effective_rate": effective_rate,
            "shaped": shaped,
            "queued": queued,
            "alert_triggered": alert_triggered,
            "shaped_at": time.time(),
        }])

        return {
            "policy_code": policy["策略编号"],
            "batch_code": batch_code,
            "effective_rate": effective_rate,
            "shaped": shaped,
            "queued": queued,
            "alert_triggered": alert_triggered,
        }

    def get_active_policies(self) -> list[dict[str, Any]]:
        """获取所有已生效的限速策略列表。"""
        return [r for r in store.rows(self.MODULE) if r.get("status") == "已生效"]

    # ------------------------------------------------------------------
    # 失败恢复：整形发放清单 + 额度台账 + 分片重调度
    # ------------------------------------------------------------------

    def _grants_file(self, batch_code: str):
        """返回批次对应的整形发放清单文件路径。"""
        return self._data_dir("shaping") / f"{batch_code}_grants.jsonl"

    def _ledger_file(self):
        """返回全局额度扣减台账文件路径（跨批次共享、幂等追加）。"""
        return self._data_dir("shaping") / "quota_ledger.jsonl"

    def _read_ledger(self) -> list[dict[str, Any]]:
        """读取额度扣减台账全部条目。"""
        return self._read_jsonl(self._ledger_file())

    def _ledger_has(self, batch_code: str, chunk_index: int) -> bool:
        """判断某批次某分片是否已在台账扣过额度（重投幂等判断依据）。"""
        return any(
            line.get("batch_code") == batch_code and int(line.get("chunk_index", -1)) == chunk_index
            for line in self._read_ledger()
        )

    def _append_ledger(self, entry: dict[str, Any]) -> bool:
        """幂等追加额度扣减台账；同一（批次，分片）已存在则跳过，返回是否实际记账。"""
        if self._ledger_has(entry["batch_code"], int(entry["chunk_index"])):
            return False
        self._write_jsonl(self._ledger_file(), [entry])
        return True

    def get_grant_manifest(self, batch_code: str) -> list[dict[str, Any]]:
        """读取批次整形发放清单，传输侧据此核对每片额度与发放结果。"""
        return self._read_jsonl(self._grants_file(batch_code))

    def update_policy_quota(self, policy_id: int, records_per_sec: int,
                            bytes_per_sec: int | None = None) -> dict[str, Any]:
        """模拟目标端调整单次发放额度：更新策略当前生效的记录/字节阈值并留痕。"""
        policy = self.get_entry(policy_id)
        if policy is None:
            return {"error": f"限速策略 {policy_id} 不存在"}
        old_records = int(policy.get("每秒记录数", 0))
        old_bytes = int(policy.get("每秒字节数", 0))
        policy["每秒记录数"] = str(records_per_sec)
        if bytes_per_sec is not None:
            policy["每秒字节数"] = str(bytes_per_sec)
        # 额度变更升一个策略版本，清单中的生效策略快照可区分变更前后
        version = policy.get("策略版本", "v1.0")
        policy["策略版本"] = self._bump_version(version)
        self._write_jsonl(
            self._data_dir("events") / "speed_policy_log.jsonl",
            [{"action": "update_quota", "policy_code": policy["策略编号"],
              "old_records_per_sec": old_records, "new_records_per_sec": records_per_sec,
              "old_bytes_per_sec": old_bytes,
              "new_bytes_per_sec": bytes_per_sec if bytes_per_sec is not None else old_bytes,
              "at": time.time()}]
        )
        return policy

    @staticmethod
    def _bump_version(version: str) -> str:
        """策略小版本号递增，例如 v1.0 -> v1.1。"""
        try:
            prefix, minor = version.rsplit(".", 1)
            return f"{prefix}.{int(minor) + 1}"
        except (ValueError, AttributeError):
            return f"{version}.1"

    def plan_batch_grants(
        self,
        policy_id: int,
        batch_code: str,
        records: list[dict[str, Any]],
        max_records_per_chunk: int = 6,
        max_bytes_per_chunk: int = 1048576,
    ) -> dict[str, Any]:
        """按记录数与字节双阈值切分批次，并逐片做额度发放，输出整形发放清单。

        发放规则：
        - 分片先受单次记录/字节额度约束（单片不得超过单次额度）；
        - 同一发放轮次内剩余额度不足时，该片进入 waiting 队列，实际扣减记 0；
        - 获准分片按（批次编号，分片编号）幂等写入额度台账，重投不重复扣减。

        清单每片包含：分片编号、记录数、字节数、发放结果、生效策略快照、
        实际扣减记录/字节额度，供传输批次侧同步读取并核对同一分片的额度消耗。
        """
        policy = self.get_entry(policy_id)
        if policy is None:
            return {"error": f"限速策略 {policy_id} 不存在"}

        grants_file = self._grants_file(batch_code)
        # 同一批次重复规划时直接复用既有清单，避免重复发放与重复扣额度
        if grants_file.exists():
            existing = self.get_grant_manifest(batch_code)
            return {"batch_code": batch_code, "policy_code": policy["策略编号"],
                    "grants": existing, "reused": True}

        chunks = plan_transfer_chunks(records, max_records_per_chunk, max_bytes_per_chunk)

        record_quota = int(policy["每秒记录数"])
        byte_quota = int(policy["每秒字节数"])
        remaining_records = record_quota
        remaining_bytes = byte_quota

        grant_lines: list[dict[str, Any]] = []
        for chunk in chunks:
            chunk_index = chunk["chunk_index"]
            record_count = int(chunk["record_count"])
            byte_count = int(chunk["byte_count"])

            within_single_request = record_count <= record_quota and byte_count <= byte_quota
            within_round_budget = record_count <= remaining_records and byte_count <= remaining_bytes

            if within_single_request and within_round_budget:
                status = "granted"
                deducted_records = record_count
                deducted_bytes = byte_count
                remaining_records -= record_count
                remaining_bytes -= byte_count
                deny_reason = None
            else:
                status = "waiting"
                deducted_records = 0
                deducted_bytes = 0
                if not within_single_request:
                    deny_reason = "chunk_exceeds_single_request_quota"
                else:
                    deny_reason = "round_budget_exhausted"

            line = {
                "batch_code": batch_code,
                "chunk_index": chunk_index,
                "record_count": record_count,
                "byte_count": byte_count,
                "status": status,
                "policy_code": policy["策略编号"],
                "policy_version": policy.get("策略版本", "v1.0"),
                "record_quota_at_grant": record_quota,
                "byte_quota_at_grant": byte_quota,
                "actual_deducted_records": deducted_records,
                "actual_deducted_bytes": deducted_bytes,
                "deny_reason": deny_reason,
                "granted_at": time.time() if status == "granted" else None,
            }
            grant_lines.append(line)

            if status == "granted":
                # 台账幂等：同一分片只扣一次额度
                self._append_ledger({
                    "batch_code": batch_code,
                    "chunk_index": chunk_index,
                    "policy_code": policy["策略编号"],
                    "policy_version": line["policy_version"],
                    "deducted_records": deducted_records,
                    "deducted_bytes": deducted_bytes,
                    "at": time.time(),
                })

        self._write_jsonl(grants_file, grant_lines)
        return {
            "batch_code": batch_code,
            "policy_code": policy["策略编号"],
            "policy_version": policy.get("策略版本", "v1.0"),
            "record_quota": record_quota,
            "byte_quota": byte_quota,
            "grants": grant_lines,
            "manifest_path": str(grants_file),
            "reused": False,
        }

    def reschedule_chunk(self, batch_code: str, chunk_index: int,
                         policy_id: int | None = None) -> dict[str, Any]:
        """目标端额度调整后重新调度排队分片，按当前生效策略重新评估并更新清单。

        - 已获准分片重调度直接幂等返回，不重复扣额度；
        - 获准后清单原地改写，保留新策略编号/版本/额度快照，并补记额度台账。
        """
        lines = self.get_grant_manifest(batch_code)
        target = next((line for line in lines if int(line.get("chunk_index", -1)) == chunk_index), None)
        if target is None:
            return {"error": f"批次 {batch_code} 分片 {chunk_index} 不在整形发放清单中"}

        if target["status"] == "granted":
            return {"batch_code": batch_code, "chunk_index": chunk_index,
                    "status": "granted", "rescheduled": False,
                    "reason": "already_granted", "grant": target}

        if policy_id is None:
            policy = next(
                (row for row in store.rows(self.MODULE)
                 if row.get("策略编号") == target["policy_code"]),
                None,
            )
        else:
            policy = self.get_entry(policy_id)
        if policy is None:
            return {"error": f"分片 {chunk_index} 的生效策略不存在，无法重新调度"}

        record_quota = int(policy["每秒记录数"])
        byte_quota = int(policy["每秒字节数"])
        record_count = int(target["record_count"])
        byte_count = int(target["byte_count"])

        if record_count > record_quota or byte_count > byte_quota:
            target["deny_reason"] = "chunk_exceeds_single_request_quota"
            self._rewrite_grants(batch_code, lines)
            return {"batch_code": batch_code, "chunk_index": chunk_index,
                    "status": "waiting", "rescheduled": False,
                    "reason": target["deny_reason"], "grant": target}

        target["status"] = "granted"
        target["policy_code"] = policy["策略编号"]
        target["policy_version"] = policy.get("策略版本", "v1.0")
        target["record_quota_at_grant"] = record_quota
        target["byte_quota_at_grant"] = byte_quota
        target["actual_deducted_records"] = record_count
        target["actual_deducted_bytes"] = byte_count
        target["deny_reason"] = None
        target["granted_at"] = time.time()
        self._rewrite_grants(batch_code, lines)

        ledger_written = self._append_ledger({
            "batch_code": batch_code,
            "chunk_index": chunk_index,
            "policy_code": policy["策略编号"],
            "policy_version": target["policy_version"],
            "deducted_records": record_count,
            "deducted_bytes": byte_count,
            "at": time.time(),
        })
        return {"batch_code": batch_code, "chunk_index": chunk_index,
                "status": "granted", "rescheduled": True,
                "ledger_written": ledger_written, "grant": target}

    def _rewrite_grants(self, batch_code: str, lines: list[dict[str, Any]]) -> None:
        """原地重写整形发放清单（重调度后保留全部历史分片行）。"""
        grants_file = self._grants_file(batch_code)
        with open(grants_file, "w", encoding="utf-8") as f:
            for line in lines:
                f.write(json.dumps(line, ensure_ascii=False, default=str) + "\n")
