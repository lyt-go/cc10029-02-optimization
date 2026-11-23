"""失败恢复测试：速率整形发放清单与传输批次分片的对账与断点恢复。

覆盖场景：
1. 按记录数与字节双阈值计算批次分片，同批分片字节数不同；
2. 目标端把单次记录额度从 6 降为 3，排队分片重新调度并采用新生效策略；
3. 传输侧同步读取整形发放清单，只发送获准分片，检查点标明等待分片与未发范围；
4. 已接受分片重投不重复扣额度、不重复写批次文件、不多写暂存数据；
5. 恢复后整形清单、批次文件、暂存记录数三方可核对同一分片的额度消耗。
"""
from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from app.services.base import DATA_ROOT
from app.services.speed_control import SpeedControlService
from app.services.transfer_batch import (
    TransferBatchService,
    plan_transfer_chunks,
    record_byte_size,
)
from app.services.staging_area import StagingAreaService


@pytest.fixture(autouse=True)
def reset_store():
    """每个测试前重置内存仓库，保证模块登记条目隔离。"""
    from app.seed import SEED_ROWS
    from app.store import store
    store._tables = {
        name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
    }
    yield


def _variable_records(prefix: str, count: int) -> list[dict]:
    """构造字节量逐条递增的记录，保证同批不同分片的字节数不同。"""
    return [
        {"line_no": i + 1, "id": f"{prefix}-{i}", "payload": "x" * (20 + i * 12)}
        for i in range(count)
    ]


def _unique_batch(window: str) -> str:
    return f"TRAN-{window}-{uuid.uuid4().hex[:8]}"


class TestDualThresholdChunkPlanning:
    """双阈值分片规划：记录数阈值与字节阈值同时生效。"""

    def test_record_threshold_splits_chunks_with_different_bytes(self):
        records = _variable_records("plan-a", 9)
        chunks = plan_transfer_chunks(records, max_records=6, max_bytes=10_000_000)

        assert [c["chunk_index"] for c in chunks] == [0, 1]
        assert [c["record_count"] for c in chunks] == [6, 3]
        # 同批两片字节数不同，且等于各记录序列化字节之和
        assert chunks[0]["byte_count"] != chunks[1]["byte_count"]
        for chunk in chunks:
            assert chunk["byte_count"] == sum(record_byte_size(r) for r in chunk["records"])
            assert chunk["record_count"] <= 6
            assert chunk["end_line"] - chunk["start_line"] == chunk["record_count"]

    def test_byte_threshold_also_seals_chunk(self):
        records = _variable_records("plan-b", 5)
        small_byte_budget = record_byte_size(records[0]) + record_byte_size(records[1]) + 1
        chunks = plan_transfer_chunks(records, max_records=100, max_bytes=small_byte_budget)

        assert len(chunks) >= 2
        for chunk in chunks:
            assert chunk["record_count"] <= 100
            assert chunk["byte_count"] <= small_byte_budget
        # 分片首尾相接覆盖全部记录
        assert chunks[0]["start_line"] == 0
        assert chunks[-1]["end_line"] == 5


class TestShapedTransferRecovery:
    """额度 6→3 调整下的排队、重调度、恢复与幂等重投。"""

    def test_full_recovery_cycle_with_quota_reduction(self):
        sc_svc = SpeedControlService()
        tb_svc = TransferBatchService()
        sa_svc = StagingAreaService()

        # 目标端初始单次记录额度 6
        policy = sc_svc.create_policy("恢复链路", records_per_sec=6, bytes_per_sec=10_000_000)
        batch = tb_svc.create_batch("EXTR-RECOVERY", 9, "TARG-0001")
        batch_code = _unique_batch("RECOVERY")
        # 用唯一批次编号重写，避免产物文件跨测试追加污染
        tb_svc.get_entry(batch["batch_id"])["批次编号"] = batch_code
        records = _variable_records("rec", 9)

        # 1) 首次整形传输：两片字节数不同，第一片获准发送，第二片排队等待
        first = tb_svc.start_shaped_transfer(
            batch["batch_id"], policy["id"],
            max_records_per_chunk=6, max_bytes_per_chunk=10_000_000,
            records=records,
        )
        assert "error" not in first
        assert first["sent_chunks"] == [0]
        assert first["waiting_chunks"] == [1]
        assert first["transfer_status"] == "传输中"
        # 检查点标明哪个分片还在等待，恢复起点是实际未发范围（第 7 条，行号 6）
        checkpoint = first["checkpoint"]
        assert checkpoint["waiting_chunks"] == [1]
        assert checkpoint["actual_unsent_range"] == {"start_line": 6, "end_line": 9}

        # 2) 发放清单逐片登记：发放结果、生效策略、实际扣减额度与分片编号
        manifest = sc_svc.get_grant_manifest(batch_code)
        assert [line["chunk_index"] for line in manifest] == [0, 1]
        granted_line, waiting_line = manifest
        assert granted_line["status"] == "granted"
        assert granted_line["actual_deducted_records"] == 6
        assert granted_line["actual_deducted_bytes"] == granted_line["byte_count"]
        assert granted_line["policy_code"] == policy["策略编号"]
        assert waiting_line["status"] == "waiting"
        assert waiting_line["actual_deducted_records"] == 0
        assert waiting_line["actual_deducted_bytes"] == 0
        # 同批两片字节数不同
        assert granted_line["byte_count"] != waiting_line["byte_count"]

        # 3) 目标端把单次记录额度从 6 降为 3，重新调度第二片
        updated = sc_svc.update_policy_quota(policy["id"], records_per_sec=3)
        assert int(updated["每秒记录数"]) == 3
        reschedule = sc_svc.reschedule_chunk(batch_code, 1)
        assert reschedule["status"] == "granted"
        assert reschedule["rescheduled"] is True
        # 核对采用的策略：新生效策略版本与新额度快照
        new_line = next(
            line for line in sc_svc.get_grant_manifest(batch_code)
            if line["chunk_index"] == 1
        )
        assert new_line["status"] == "granted"
        assert new_line["policy_version"] == updated["策略版本"]
        assert new_line["record_quota_at_grant"] == 3
        assert new_line["actual_deducted_records"] == 3
        assert new_line["actual_deducted_bytes"] == new_line["byte_count"]
        # 第一片的策略快照保持变更前版本，证明两片可核对不同生效策略
        assert granted_line["policy_version"] != new_line["policy_version"]

        # 4) 恢复传输：从实际未发范围继续，只补发第二片
        resumed = tb_svc.resume_shaped_transfer(batch["batch_id"])
        assert "error" not in resumed
        assert resumed["resumed_chunks"] == [1]
        assert resumed["waiting_chunks"] == []
        assert resumed["transfer_status"] == "已完成"
        assert resumed["actual_unsent_range"] == {"start_line": 9, "end_line": 9}
        assert resumed["sent_chunks"] == [0, 1]

        # 5) 重投第一片：已接受分片不得再扣额度或多写一片暂存数据
        ledger_before = sc_svc._read_ledger()
        ledger_chunk0_before = [
            line for line in ledger_before
            if line["batch_code"] == batch_code and line["chunk_index"] == 0
        ]
        assert len(ledger_chunk0_before) == 1

        redeliver = tb_svc.redeliver_chunk(batch["batch_id"], 0)
        assert redeliver["idempotent"] is True
        assert redeliver["redelivered"] is False
        assert redeliver["ledger_duplicate"] is True

        ledger_after = sc_svc._read_ledger()
        ledger_chunk0_after = [
            line for line in ledger_after
            if line["batch_code"] == batch_code and line["chunk_index"] == 0
        ]
        assert len(ledger_chunk0_after) == 1  # 额度未重复扣减

        # 6) 三方对账：整形清单、批次文件、暂存记录数一致，且重投无重复
        final_manifest = sc_svc.get_grant_manifest(batch_code)
        assert sum(line["record_count"] for line in final_manifest) == 9
        assert all(line["status"] == "granted" for line in final_manifest)
        # 台账每片恰好一条扣减记录
        batch_ledger = [line for line in ledger_after if line["batch_code"] == batch_code]
        assert sorted(line["chunk_index"] for line in batch_ledger) == [0, 1]

        batch_file = Path(first["artifact_path"])
        batch_records = tb_svc._read_jsonl(batch_file)
        assert len(batch_records) == 9  # 重投未多写
        assert sorted({r["chunk_index"] for r in batch_records}) == [0, 1]
        # 每片记录数与清单一致
        for chunk_index, expected in ((0, 6), (1, 3)):
            assert sum(1 for r in batch_records if r["chunk_index"] == chunk_index) == expected

        staging_state = sa_svc.get_chunk_landing_state(batch_code)
        assert staging_state["staging_record_count"] == 9  # 暂存未多写一片
        assert staging_state["landed_chunks"] == [0, 1]
        staging_file = Path(staging_state["artifact_path"])
        staging_lines = sa_svc._read_jsonl(staging_file)
        assert len(staging_lines) == 9

        # 再重投第二片，暂存与批次文件仍保持 9 条
        redeliver_1 = tb_svc.redeliver_chunk(batch["batch_id"], 1)
        assert redeliver_1["idempotent"] is True
        assert len(tb_svc._read_jsonl(batch_file)) == 9
        assert sa_svc.get_chunk_landing_state(batch_code)["staging_record_count"] == 9

    def test_waiting_chunk_cannot_be_redelivered_before_grant(self):
        sc_svc = SpeedControlService()
        tb_svc = TransferBatchService()

        policy = sc_svc.create_policy("未获准重投", records_per_sec=6, bytes_per_sec=10_000_000)
        batch = tb_svc.create_batch("EXTR-WAIT", 9, "TARG-0001")
        batch_code = _unique_batch("WAIT")
        tb_svc.get_entry(batch["batch_id"])["批次编号"] = batch_code

        first = tb_svc.start_shaped_transfer(
            batch["batch_id"], policy["id"],
            max_records_per_chunk=6, max_bytes_per_chunk=10_000_000,
            records=_variable_records("w", 9),
        )
        assert first["waiting_chunks"] == [1]

        result = tb_svc.redeliver_chunk(batch["batch_id"], 1)
        assert "error" in result
        assert result["status"] == "waiting"
        # 被拒重投不产生暂存数据
        staging = StagingAreaService().get_chunk_landing_state(batch_code)
        assert staging["staging_record_count"] == 6

    def test_replan_same_batch_reuses_manifest_and_ledger(self):
        sc_svc = SpeedControlService()
        records = _variable_records("reuse", 7)
        batch_code = _unique_batch("REUSE")
        policy = sc_svc.create_policy("重复规划", records_per_sec=100, bytes_per_sec=10_000_000)

        first = sc_svc.plan_batch_grants(
            policy["id"], batch_code, records,
            max_records_per_chunk=6, max_bytes_per_chunk=10_000_000)
        second = sc_svc.plan_batch_grants(
            policy["id"], batch_code, records,
            max_records_per_chunk=6, max_bytes_per_chunk=10_000_000)
        assert second["reused"] is True
        assert second["grants"] == first["grants"]

        ledger = [line for line in sc_svc._read_ledger() if line["batch_code"] == batch_code]
        assert len(ledger) == len(first["grants"])  # 未重复扣额度
