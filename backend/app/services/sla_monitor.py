"""时效监控领域服务：维护数据交换约定到达时限与实际到达时长的时效统计与迟到率。

本模块统计传输批次从源端发起到目标端到达的实际耗时，计算超时批次和迟到率，
在到达对账发现迟到后更新时效指标，并写入时效监控产物文件。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class SlaMonitorService(BaseModuleService):
    MODULE = "sla_monitor"
    ENTITY = "时效指标"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["指标编号", "源端编号", "约定到达时限", "实际到达时长", "超时批次数", "迟到率", "统计窗口", "指标版本"]
    REQUIRED_FIELDS = ["指标编号", "源端编号", "约定到达时限"]
    STATUS_ORDER = ["草稿", "待统计", "已统计", "已过期"]
    ACTION_RULES = {"启动统计": "待统计", "确认统计": "已统计", "标记过期": "已过期"}
    NEGATIVE_ACTIONS = ["标记过期"]

    def create_sla_metric(self, source_code: str, sla_seconds: int = 3600) -> dict[str, Any]:
        """为源端系统创建时效监控指标，设定约定到达时限。"""
        metric_code = f"SLA_-{source_code}-{int(time.time())}"
        values = {
            "指标编号": metric_code,
            "源端编号": source_code,
            "约定到达时限": str(sla_seconds),
            "实际到达时长": "0",
            "超时批次数": "0",
            "迟到率": "0",
            "统计窗口": "daily",
            "指标版本": "v1.0",
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建时效指标失败: {missing}")
        self._write_jsonl(
            self._data_dir("events") / "sla_metric_log.jsonl",
            [{"action": "create_metric", "metric_code": metric_code, "source_code": source_code,
              "sla_seconds": sla_seconds, "at": time.time()}]
        )
        return entry

    def update_arrival_stats(self, batch_code: str, late_count: int, missing_count: int) -> dict[str, Any]:
        """根据到达对账结果更新时效监控指标：计算实际到达时长、超时批次和迟到率。

        本方法是数据时效监控的核心入口：
        接收到达对账的迟到/缺失条数，更新对应源端的时效统计指标，
        计算超时批次数和迟到率，写入时效产物文件。
        """
        # 查找与该批次关联的时效指标
        metrics = [r for r in store.rows(self.MODULE) if r.get("源端编号", "").startswith(batch_code[:8])
                   or r.get("status") == "已统计"]
        if not metrics:
            # 无关联指标则创建新指标
            metric = self.create_sla_metric(batch_code[:8])
            metrics = [metric]

        updated = []
        for metric in metrics:
            entry = store.find(self.MODULE, metric["id"])
            if entry is None:
                continue

            # 计算时效统计
            current_timeout = int(entry.get("超时批次数", 0))
            current_late_rate = float(entry.get("迟到率", 0))
            total_batches = current_timeout + 1  # 至少1个批次
            new_late_rate = round((current_late_rate * (total_batches - 1) + late_count) / total_batches, 4)

            entry["超时批次数"] = str(current_timeout + (1 if late_count > 0 else 0))
            entry["迟到率"] = str(new_late_rate)
            entry["实际到达时长"] = str(late_count * 10)

            # 写入时效产物文件
            sla_file = self._data_dir("sla") / f"{entry['指标编号']}_stats.jsonl"
            self._write_jsonl(sla_file, [{
                "metric_code": entry["指标编号"],
                "batch_code": batch_code,
                "late_count": late_count,
                "missing_count": missing_count,
                "timeout_batches": entry["超时批次数"],
                "late_rate": entry["迟到率"],
                "updated_at": time.time(),
            }])
            updated.append(entry["指标编号"])

        return {"batch_code": batch_code, "metrics_updated": len(updated), "codes": updated}

    def get_sla_report(self) -> dict[str, Any]:
        """生成时效监控汇总报告，统计所有源端的迟到率与超时批次。"""
        metrics = store.rows(self.MODULE)
        total_batches = len(metrics)
        timeout_batches = sum(1 for m in metrics if int(m.get("超时批次数", 0)) > 0)
        avg_late_rate = sum(float(m.get("迟到率", 0)) for m in metrics) / max(total_batches, 1)
        return {
            "total_metrics": total_batches,
            "timeout_batches": timeout_batches,
            "avg_late_rate": round(avg_late_rate, 4),
            "last_updated": time.time(),
        }