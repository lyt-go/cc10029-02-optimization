"""冲突裁决领域服务：维护同一记录主键在源端与目标端字段取值冲突的登记与裁决依据。

本模块在校验核对发现差异后自动创建冲突记录，支持按裁决方式（源端优先/目标端优先/人工裁决）
处理字段取值冲突，裁决完成后写入裁决日志和裁决产物文件。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ConflictResolveService(BaseModuleService):
    MODULE = "conflict_resolve"
    ENTITY = "冲突记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["冲突编号", "记录主键", "冲突字段", "源端取值", "目标取值", "裁决方式", "裁决依据", "裁决时间"]
    REQUIRED_FIELDS = ["冲突编号", "记录主键", "冲突字段"]
    STATUS_ORDER = ["草稿", "待裁决", "已裁决", "已挂起"]
    ACTION_RULES = {"提交裁决": "待裁决", "确认裁决": "已裁决", "标记挂起": "已挂起"}
    NEGATIVE_ACTIONS = ["标记挂起"]

    def create_conflict(self, batch_code: str, diff_count: int) -> dict[str, Any]:
        """基于校验发现的差异条数创建冲突记录，登记冲突字段与源端目标端取值对。"""
        created = []
        for i in range(min(diff_count, 5)):
            conflict_code = f"CONF-{batch_code}-{i + 1}"
            values = {
                "冲突编号": conflict_code,
                "记录主键": f"PK_{batch_code}_{i}",
                "冲突字段": f"field_{i}",
                "源端取值": f"src_val_{i}",
                "目标取值": f"tgt_val_{i}",
                "裁决方式": "待裁决",
                "裁决依据": "",
                "裁决时间": time.strftime("%Y-%m-%d", time.localtime(time.time())),
            }
            entry, _ = self.create_entry(values)
            if entry:
                created.append(conflict_code)
        self._write_jsonl(
            self._data_dir("events") / "conflict_log.jsonl",
            [{"action": "create_conflicts", "batch_code": batch_code,
              "conflict_count": len(created), "at": time.time()}]
        )
        return {"batch_code": batch_code, "conflicts_created": len(created), "codes": created}

    def resolve_conflict(self, conflict_id: int, strategy: str = "source_first") -> dict[str, Any]:
        """执行冲突裁决：按指定策略裁决字段取值冲突并写入裁决产物文件。

        裁决策略选项：
        - source_first：源端取值优先，取源端值作为最终数据
        - target_first：目标端取值优先，取目标端值作为最终数据
        - manual_override：人工裁决，标记为已挂起等待人工介入
        """
        conflict_entry = self.get_entry(conflict_id)
        if conflict_entry is None:
            return {"error": f"冲突记录 {conflict_id} 不存在"}

        entry, msg = self.run_action(conflict_id, "提交裁决")
        if entry is None:
            return {"error": msg, "conflict_id": conflict_id}

        source_val = conflict_entry.get("源端取值", "")
        target_val = conflict_entry.get("目标取值", "")

        if strategy == "source_first":
            final_val = source_val
            basis = "源端优先：以源端系统数据为准"
            entry["裁决方式"] = "源端优先"
        elif strategy == "target_first":
            final_val = target_val
            basis = "目标端优先：以目标端系统数据为准"
            entry["裁决方式"] = "目标端优先"
        else:
            final_val = "PENDING_MANUAL"
            basis = "挂起待人工裁决"
            entry["裁决方式"] = "人工裁决"
            self.run_action(conflict_id, "标记挂起")

        entry["裁决依据"] = basis
        entry["裁决时间"] = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time()))

        if strategy != "manual_override":
            final_entry, _ = self.run_action(conflict_id, "确认裁决")

        # 写入裁决产物文件
        resolution_file = self._data_dir("resolutions") / f"{conflict_entry['冲突编号']}_resolution.jsonl"
        self._write_jsonl(resolution_file, [{
            "conflict_code": conflict_entry["冲突编号"],
            "strategy": strategy,
            "source_value": source_val,
            "target_value": target_val,
            "final_value": final_val,
            "basis": basis,
            "resolved_at": time.time(),
        }])

        return {
            "conflict_code": conflict_entry["冲突编号"],
            "strategy": strategy,
            "final_value": final_val,
            "basis": basis,
        }

    def batch_resolve(self, batch_code: str, strategy: str = "source_first") -> dict[str, Any]:
        """批量裁决同一批次的全部待裁决冲突记录。"""
        conflicts = [r for r in store.rows(self.MODULE)
                     if r.get("批次编号", "").startswith(batch_code[:8])
                     and r.get("status") == "待裁决"]
        resolved = 0
        for c in conflicts:
            result = self.resolve_conflict(c["id"], strategy)
            if "error" not in result:
                resolved += 1
        return {"batch_code": batch_code, "total_conflicts": len(conflicts), "resolved": resolved}