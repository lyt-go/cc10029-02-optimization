"""速率整形领域服务：维护传输速率的每秒记录与字节上限、突发窗口与排队整形策略。

本模块控制数据传输的速率上限与排队整形，支持令牌桶算法做限速策略生效验证，
整形结果输出到速率产物文件并联动告警。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
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