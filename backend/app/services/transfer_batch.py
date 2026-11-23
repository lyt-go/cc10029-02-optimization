"""传输批次领域服务：维护传输批次的分片组织、记录条数与字节总量登记及批次水位推进。

本模块读取抽取窗口产物文件后创建传输批次，分片写入传输数据文件，
传输完成后自动驱动暂存落地和校验核对，形成窗口抽取→批次传输→落地校验的贯通链路。

失败恢复增强（与速率整形共享同一份发放清单）：
- ``plan_transfer_chunks`` 按记录数与字节双阈值确定性切分批次分片；
- ``start_shaped_transfer`` 同步读取整形发放清单，只发送已获准（granted）分片，
  检查点标明仍处于 waiting 的分片编号，恢复时从实际未发范围继续；
- ``redeliver_chunk`` 按分片编号幂等重投，已发送分片不再重复写批次文件，
  也不会在暂存多写一片数据。
"""
from __future__ import annotations

import json
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


def record_byte_size(record: dict[str, Any]) -> int:
    """按产物文件同一套 JSON 序列化口径计算单条记录的字节数。"""
    return len(json.dumps(record, ensure_ascii=False, default=str).encode("utf-8"))


def plan_transfer_chunks(
    records: list[dict[str, Any]],
    max_records: int,
    max_bytes: int,
) -> list[dict[str, Any]]:
    """按记录数阈值与字节阈值计算传输批次分片。

    逐条累积，加入下一条会突破任一阈值（且当前分片非空）时封口开新片，
    因此同批不同分片的记录数、字节数都可能不同。返回每片的编号、
    记录起止位置、记录数与字节数，供整形侧与传输侧共用同一份分片口径。
    """
    if max_records <= 0 or max_bytes <= 0:
        raise ValueError("分片记录阈值和字节阈值必须为正数")

    chunks: list[dict[str, Any]] = []
    current: list[dict[str, Any]] = []
    current_bytes = 0

    def seal(index: int) -> None:
        nonlocal current, current_bytes
        if not current:
            return
        chunks.append({
            "chunk_index": index,
            "record_count": len(current),
            "byte_count": current_bytes,
            "records": current,
        })
        current = []
        current_bytes = 0

    for record in records:
        size = record_byte_size(record)
        if current and (len(current) >= max_records or current_bytes + size > max_bytes):
            seal(len(chunks))
        current.append(record)
        current_bytes += size
    seal(len(chunks))

    # 回填确定性的记录起止行号，便于恢复时定位实际未发范围
    line_cursor = 0
    for chunk in chunks:
        chunk["start_line"] = line_cursor
        line_cursor += int(chunk["record_count"])
        chunk["end_line"] = line_cursor
    return chunks


class TransferBatchService(BaseModuleService):
    MODULE = "transfer_batch"
    ENTITY = "传输批次"
    OPERATION_TERM = "水位"
    LIST_FIELDS = ["批次编号", "窗口编号", "目标编号", "记录条数", "字节总量", "分片大小", "传输状态", "批次水位"]
    REQUIRED_FIELDS = ["批次编号", "窗口编号", "目标编号"]
    STATUS_ORDER = ["草稿", "待传输", "传输中", "已完成", "已失败"]
    ACTION_RULES = {"启动传输": "传输中", "标记完成": "已完成", "标记失败": "已失败"}
    NEGATIVE_ACTIONS = ["标记失败"]

    def create_batch(self, window_code: str, record_count: int, target_code: str = "TARG-0001") -> dict[str, Any]:
        """基于抽取窗口创建传输批次，传入产物文件的 record_count 并写入批次事件日志。"""
        batch_code = f"TRAN-{window_code}-{int(time.time())}"
        values = {
            "批次编号": batch_code,
            "窗口编号": window_code,
            "目标编号": target_code,
            "记录条数": str(record_count),
            "字节总量": "0",
            "分片大小": "1000",
            "传输状态": "草稿",
            "批次水位": "0",
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建传输批次失败: {missing}")
        # 写入批次创建事件
        self._write_jsonl(
            self._data_dir("events") / "batch_create_log.jsonl",
            [{"action": "create_batch", "batch_code": batch_code, "window_code": window_code,
              "record_count": record_count, "target": target_code, "at": time.time()}]
        )
        return {"batch_code": batch_code, "batch_id": entry["id"], "window_code": window_code,
                "record_count": record_count, "target_code": target_code}

    def start_transfer(self, batch_id: int, chunk_size: int = 3) -> dict[str, Any]:
        """启动传输批次执行：读取抽取窗口产物文件，按分片大小写入传输产物文件并推进水位。

        本方法执行实际的批量数据传输：从抽取产物文件读取数据，切分为分片，
        写入传输产物文件（jsonl），计算字节总量，然后自动触发暂存落地和校验核对。
        """
        batch_entry = self.get_entry(batch_id)
        if batch_entry is None:
            return {"error": f"传输批次 {batch_id} 不存在"}

        entry, msg = self.run_action(batch_id, "启动传输")
        if entry is None:
            return {"error": msg, "batch_id": batch_id}

        batch_code = batch_entry["批次编号"]
        window_code = batch_entry.get("窗口编号", "unknown")

        # 读取上游抽取窗口的产物文件
        artifact_dir = self._data_dir("extracted")
        source_file = artifact_dir / f"{window_code}.jsonl" if window_code != "unknown" else None

        records = []
        if source_file and source_file.exists():
            records = self._read_jsonl(source_file)
        else:
            # 如果上游产物文件不存在，生成模拟数据
            records = self._simulate_records(batch_code, count=int(batch_entry.get("记录条数", 10)),
                                              field_prefix="trans")

        # 分片写入传输产物文件
        output_file = self._data_dir("batches") / f"{batch_code}.jsonl"
        total_bytes = 0
        total_written = 0
        chunks = []
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            for rec in chunk:
                rec["batch_code"] = batch_code
                rec["chunk_index"] = i // chunk_size
                rec["transferred_at"] = time.time()
            self._write_jsonl(output_file, chunk)
            chunk_bytes = sum(len(str(r).encode("utf-8")) for r in chunk)
            chunks.append({"chunk_index": i // chunk_size, "record_count": len(chunk),
                           "bytes": chunk_bytes})
            total_bytes += chunk_bytes
            total_written += len(chunk)

        # 更新批次水位和字节总量
        entry["字节总量"] = str(total_bytes)
        entry["记录条数"] = str(total_written)
        entry["批次水位"] = str(total_written)

        # 写入传输水位断点
        self._write_checkpoint(f"batch_{batch_code}", {
            "batch_code": batch_code,
            "window_code": window_code,
            "records_transferred": total_written,
            "total_bytes": total_bytes,
            "chunk_count": len(chunks),
            "watermark": str(total_written),
            "last_transfer_at": time.time(),
        })

        result = {
            "batch_code": batch_code,
            "window_code": window_code,
            "records_transferred": total_written,
            "total_bytes": total_bytes,
            "chunks": len(chunks),
            "artifact_path": str(output_file),
        }

        # 传输完成后自动触发暂存落地
        try:
            sa_svc = self._safe_import("app.services.staging_area", "StagingAreaService")()
            staging_result = sa_svc.land_batch(batch_code, total_written)
            result["auto_staging"] = staging_result
        except Exception as e:
            result["auto_staging"] = {"error": str(e)}

        # 传输完成后自动创建校验核对任务
        try:
            cv_svc = self._safe_import("app.services.checksum_verify", "ChecksumVerifyService")()
            verify_result = cv_svc.create_verification(batch_code, total_written)
            result["auto_verify"] = verify_result
        except Exception as e:
            result["auto_verify"] = {"error": str(e)}

        # 传输完成后自动创建到达对账单
        try:
            ar_svc = self._safe_import("app.services.arrival_reconcile", "ArrivalReconcileService")()
            reconcile_result = ar_svc.create_reconciliation(batch_code, total_written)
            result["auto_reconcile"] = reconcile_result
        except Exception as e:
            result["auto_reconcile"] = {"error": str(e)}

        return result

    def mark_completed(self, batch_id: int) -> dict[str, Any]:
        """标记传输批次已完成，写入完成事件日志并记录批次汇总统计。"""
        entry, msg = self.run_action(batch_id, "标记完成")
        if entry is None:
            return {"error": msg, "batch_id": batch_id}
        batch_code = entry["批次编号"]
        # 从产物文件验证批次完整性
        artifact_file = self._data_dir("batches") / f"{batch_code}.jsonl"
        records = self._read_jsonl(artifact_file)
        checksum = self._compute_checksum(str(records))
        self._write_jsonl(
            self._data_dir("events") / "batch_complete_log.jsonl",
            [{"action": "complete", "batch_code": batch_code, "record_count": len(records),
              "checksum": checksum, "completed_at": time.time()}]
        )
        return {"batch_code": batch_code, "status": "已完成", "record_count": len(records),
                "checksum": checksum}

    def mark_failed(self, batch_id: int, reason: str = "") -> dict[str, Any]:
        """标记传输批次失败，写入失败事件日志并记录中断原因。"""
        entry, msg = self.run_action(batch_id, "标记失败")
        if entry is None:
            return {"error": msg, "batch_id": batch_id}
        batch_code = entry["批次编号"]
        self._write_jsonl(
            self._data_dir("events") / "batch_fail_log.jsonl",
            [{"action": "fail", "batch_code": batch_code, "reason": reason, "failed_at": time.time()}]
        )
        return {"batch_code": batch_code, "status": "已失败", "reason": reason}

    # ------------------------------------------------------------------
    # 失败恢复：与速率整形发放清单联动的分片发送/检查点/重投
    # ------------------------------------------------------------------

    def _load_batch_records(self, batch_code: str, window_code: str,
                            fallback_count: int) -> list[dict[str, Any]]:
        """加载批次待传记录：优先读抽取窗口产物，缺失时按批次口径生成模拟数据。"""
        artifact_dir = self._data_dir("extracted")
        source_file = artifact_dir / f"{window_code}.jsonl" if window_code != "unknown" else None
        if source_file and source_file.exists():
            return self._read_jsonl(source_file)
        return self._simulate_records(batch_code, count=fallback_count, field_prefix="trans")

    def _shaped_checkpoint_name(self, batch_code: str) -> str:
        """整形联动传输检查点的断点文件名。"""
        return f"shaped_transfer_{batch_code}"

    def _read_shaped_checkpoint(self, batch_code: str) -> dict[str, Any] | None:
        """读取整形联动传输检查点，不存在返回 None。"""
        return self._read_checkpoint(self._shaped_checkpoint_name(batch_code))

    def _write_shaped_checkpoint(self, batch_code: str, state: dict[str, Any]) -> dict[str, Any]:
        """写入整形联动传输检查点，标明已发/等待分片与实际未发范围。"""
        self._write_checkpoint(self._shaped_checkpoint_name(batch_code), state)
        return state

    def start_shaped_transfer(
        self,
        batch_id: int,
        policy_id: int,
        max_records_per_chunk: int = 6,
        max_bytes_per_chunk: int = 1048576,
        records: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """按整形发放清单发送传输批次：同步读取清单，只发送 granted 分片。

        流程：
        - 按记录数/字节双阈值切分并调用速率整形逐片发放，生成发放清单；
        - 仅清单中 status=granted 的分片写入批次产物并落地暂存（逐片幂等）；
        - 检查点标明 sent_chunks、waiting_chunks（仍在排队的分片编号）
          及 actual_unsent_range（实际未发记录范围），恢复时据此继续。
        """
        from app.services.speed_control import SpeedControlService
        from app.services.staging_area import StagingAreaService

        batch_entry = self.get_entry(batch_id)
        if batch_entry is None:
            return {"error": f"传输批次 {batch_id} 不存在"}
        batch_code = batch_entry["批次编号"]
        window_code = batch_entry.get("窗口编号", "unknown")

        if records is None:
            records = self._load_batch_records(
                batch_code, window_code, int(batch_entry.get("记录条数", 10)))

        sc_svc = SpeedControlService()
        plan = sc_svc.plan_batch_grants(
            policy_id, batch_code, records,
            max_records_per_chunk=max_records_per_chunk,
            max_bytes_per_chunk=max_bytes_per_chunk,
        )
        if "error" in plan:
            return plan
        grants = plan["grants"]
        chunks = plan_transfer_chunks(records, max_records_per_chunk, max_bytes_per_chunk)

        output_file = self._data_dir("batches") / f"{batch_code}.jsonl"
        staging_svc = StagingAreaService()

        sent_chunks: list[int] = []
        waiting_chunks: list[int] = []
        sent_records = 0
        sent_bytes = 0

        for grant in grants:
            chunk_index = int(grant["chunk_index"])
            if grant["status"] != "granted":
                waiting_chunks.append(chunk_index)
                continue
            chunk = next(c for c in chunks if c["chunk_index"] == chunk_index)
            written = self._write_chunk_file(batch_code, output_file, chunk)
            staging_svc.land_chunk(batch_code, chunk)
            sent_chunks.append(chunk_index)
            sent_records += written
            sent_bytes += int(chunk["byte_count"])

        # 实际未发范围：从第一个等待分片的起始行号到批次末尾
        if waiting_chunks:
            first_waiting = min(waiting_chunks)
            waiting_chunk = next(c for c in chunks if c["chunk_index"] == first_waiting)
            unsent_range = {"start_line": int(waiting_chunk["start_line"]),
                            "end_line": len(records)}
            transfer_status = "传输中"
        else:
            unsent_range = {"start_line": len(records), "end_line": len(records)}
            transfer_status = "已完成"

        checkpoint = self._write_shaped_checkpoint(batch_code, {
            "batch_code": batch_code,
            "window_code": window_code,
            "policy_code": plan["policy_code"],
            "total_chunks": len(grants),
            "total_records": len(records),
            "sent_chunks": sent_chunks,
            "waiting_chunks": waiting_chunks,
            "actual_unsent_range": unsent_range,
            "records_sent": sent_records,
            "bytes_sent": sent_bytes,
            "manifest_path": str(sc_svc._grants_file(batch_code)),
            "artifact_path": str(output_file),
            "transfer_status": transfer_status,
            "last_transfer_at": time.time(),
        })

        return {
            "batch_code": batch_code,
            "policy_code": plan["policy_code"],
            "grants": grants,
            "sent_chunks": sent_chunks,
            "waiting_chunks": waiting_chunks,
            "actual_unsent_range": unsent_range,
            "records_sent": sent_records,
            "bytes_sent": sent_bytes,
            "total_records": len(records),
            "transfer_status": transfer_status,
            "artifact_path": str(output_file),
            "checkpoint": checkpoint,
        }

    def resume_shaped_transfer(self, batch_id: int) -> dict[str, Any]:
        """失败恢复：重新读取整形发放清单，从实际未发范围继续发送获准分片。

        已发送分片按编号跳过（幂等，不重复写文件/暂存），只补发清单中
        已转为 granted 的等待分片，并刷新检查点的等待列表与未发范围。
        """
        from app.services.speed_control import SpeedControlService
        from app.services.staging_area import StagingAreaService

        batch_entry = self.get_entry(batch_id)
        if batch_entry is None:
            return {"error": f"传输批次 {batch_id} 不存在"}
        batch_code = batch_entry["批次编号"]
        window_code = batch_entry.get("窗口编号", "unknown")

        checkpoint = self._read_shaped_checkpoint(batch_code)
        if checkpoint is None:
            return {"error": f"批次 {batch_code} 缺少整形传输检查点，无法恢复"}

        sc_svc = SpeedControlService()
        grants = sc_svc.get_grant_manifest(batch_code)
        if not grants:
            return {"error": f"批次 {batch_code} 整形发放清单缺失，无法恢复"}

        records = self._load_batch_records(
            batch_code, window_code, int(checkpoint.get("total_records", 0)))
        # 以清单登记的分片边界重建分片，恢复实际未发范围
        chunks = self._rebuild_chunks_from_manifest(records, grants)

        output_file = self._data_dir("batches") / f"{batch_code}.jsonl"
        staging_svc = StagingAreaService()

        previously_sent = [int(i) for i in checkpoint.get("sent_chunks", [])]
        sent_chunks = list(previously_sent)
        waiting_chunks: list[int] = []
        resumed_chunks: list[int] = []
        resumed_records = 0
        resumed_bytes = 0

        for grant in grants:
            chunk_index = int(grant["chunk_index"])
            if grant["status"] != "granted":
                waiting_chunks.append(chunk_index)
                continue
            if chunk_index in sent_chunks:
                # 恢复时跳过已发分片，保证幂等
                continue
            chunk = next(c for c in chunks if c["chunk_index"] == chunk_index)
            self._write_chunk_file(batch_code, output_file, chunk)
            staging_svc.land_chunk(batch_code, chunk)
            sent_chunks.append(chunk_index)
            resumed_chunks.append(chunk_index)
            resumed_records += int(chunk["record_count"])
            resumed_bytes += int(chunk["byte_count"])

        sent_chunks.sort()
        total_sent_records = sum(
            int(g["record_count"]) for g in grants if int(g["chunk_index"]) in sent_chunks)
        total_sent_bytes = sum(
            int(g["byte_count"]) for g in grants if int(g["chunk_index"]) in sent_chunks)

        if waiting_chunks:
            first_waiting = min(waiting_chunks)
            waiting_grant = next(g for g in grants if int(g["chunk_index"]) == first_waiting)
            unsent_range = {"start_line": self._manifest_start_line(grants, first_waiting),
                            "end_line": len(records)}
            transfer_status = "传输中"
        else:
            unsent_range = {"start_line": len(records), "end_line": len(records)}
            transfer_status = "已完成"

        new_checkpoint = self._write_shaped_checkpoint(batch_code, {
            "batch_code": batch_code,
            "window_code": window_code,
            "policy_code": checkpoint.get("policy_code"),
            "total_chunks": len(grants),
            "total_records": len(records),
            "sent_chunks": sent_chunks,
            "waiting_chunks": waiting_chunks,
            "actual_unsent_range": unsent_range,
            "records_sent": total_sent_records,
            "bytes_sent": total_sent_bytes,
            "resumed_chunks": resumed_chunks,
            "resumed_records": resumed_records,
            "manifest_path": str(sc_svc._grants_file(batch_code)),
            "artifact_path": str(output_file),
            "transfer_status": transfer_status,
            "last_transfer_at": time.time(),
        })

        return {
            "batch_code": batch_code,
            "resumed_chunks": resumed_chunks,
            "resumed_records": resumed_records,
            "resumed_bytes": resumed_bytes,
            "sent_chunks": sent_chunks,
            "waiting_chunks": waiting_chunks,
            "actual_unsent_range": unsent_range,
            "transfer_status": transfer_status,
            "checkpoint": new_checkpoint,
        }

    def redeliver_chunk(self, batch_id: int, chunk_index: int) -> dict[str, Any]:
        """按分片编号幂等重投：已接受分片不再扣额度、不重复写批次/暂存数据。

        以整形发放清单为唯一准绳：清单未获准的分片拒绝重投；已发送分片
        直接返回既有结果（idempotent=True），不产生任何重复写入。
        """
        from app.services.speed_control import SpeedControlService
        from app.services.staging_area import StagingAreaService

        batch_entry = self.get_entry(batch_id)
        if batch_entry is None:
            return {"error": f"传输批次 {batch_id} 不存在"}
        batch_code = batch_entry["批次编号"]

        sc_svc = SpeedControlService()
        grants = sc_svc.get_grant_manifest(batch_code)
        grant = next((g for g in grants if int(g.get("chunk_index", -1)) == chunk_index), None)
        if grant is None:
            return {"error": f"批次 {batch_code} 分片 {chunk_index} 不在整形发放清单中"}
        if grant["status"] != "granted":
            return {"error": f"分片 {chunk_index} 尚未获准发放（状态 {grant['status']}）",
                    "chunk_index": chunk_index, "status": grant["status"]}

        checkpoint = self._read_shaped_checkpoint(batch_code)
        already_sent = bool(
            checkpoint and chunk_index in [int(i) for i in checkpoint.get("sent_chunks", [])])
        if already_sent:
            return {
                "batch_code": batch_code,
                "chunk_index": chunk_index,
                "idempotent": True,
                "redelivered": False,
                "reason": "chunk_already_accepted",
                "grant": grant,
                "ledger_duplicate": sc_svc._ledger_has(batch_code, chunk_index),
            }

        # 检查点缺失记录但清单已获准：按清单补发（正常流程不会走到这里）
        window_code = batch_entry.get("窗口编号", "unknown")
        records = self._load_batch_records(
            batch_code, window_code, int((checkpoint or {}).get("total_records", 0)))
        chunks = self._rebuild_chunks_from_manifest(records, grants)
        chunk = next(c for c in chunks if c["chunk_index"] == chunk_index)
        output_file = self._data_dir("batches") / f"{batch_code}.jsonl"
        written = self._write_chunk_file(batch_code, output_file, chunk)
        StagingAreaService().land_chunk(batch_code, chunk)
        return {"batch_code": batch_code, "chunk_index": chunk_index,
                "idempotent": False, "redelivered": True, "records_written": written,
                "grant": grant}

    def _write_chunk_file(self, batch_code: str, output_file: Any, chunk: dict[str, Any]) -> int:
        """把单个分片的记录幂等写入批次产物文件，返回写入条数。"""
        existing = self._read_jsonl(output_file)
        existing_keys = {
            (int(r.get("chunk_index", -1)), int(r.get("line_no", -1))) for r in existing
        }
        payload = []
        for offset, record in enumerate(chunk["records"]):
            line_no = int(chunk["start_line"]) + offset + 1
            key = (int(chunk["chunk_index"]), int(record.get("line_no", line_no)))
            if key in existing_keys:
                continue
            enriched = dict(record)
            enriched["batch_code"] = batch_code
            enriched["chunk_index"] = int(chunk["chunk_index"])
            enriched["transferred_at"] = time.time()
            payload.append(enriched)
        if payload:
            self._write_jsonl(output_file, payload)
        return len(payload)

    @staticmethod
    def _manifest_start_line(grants: list[dict[str, Any]], chunk_index: int) -> int:
        """根据清单中排在前面的分片记录数累加得到指定分片的起始行号。"""
        cursor = 0
        for grant in sorted(grants, key=lambda g: int(g["chunk_index"])):
            idx = int(grant["chunk_index"])
            if idx == chunk_index:
                return cursor
            cursor += int(grant["record_count"])
        return cursor

    def _rebuild_chunks_from_manifest(
        self,
        records: list[dict[str, Any]],
        grants: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """恢复时按清单登记的分片边界，从原始记录重建分片（含记录与字节数核对）。"""
        chunks: list[dict[str, Any]] = []
        cursor = 0
        for grant in sorted(grants, key=lambda g: int(g["chunk_index"])):
            count = int(grant["record_count"])
            part = records[cursor:cursor + count]
            byte_count = sum(record_byte_size(r) for r in part)
            chunks.append({
                "chunk_index": int(grant["chunk_index"]),
                "start_line": cursor,
                "end_line": cursor + len(part),
                "record_count": len(part),
                "byte_count": byte_count,
                "records": part,
            })
            cursor += len(part)
        return chunks
