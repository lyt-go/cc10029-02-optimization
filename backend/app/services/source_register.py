"""源端登记领域服务：管理参与数据交换的源端系统注册、记录格式与增量窗口联动。

本模块负责源端系统的登记生命周期管理，并在确认登记后自动创建配套的
抽取窗口与字段契约，形成源端→窗口→字段的三层数据管道入口。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class SourceRegisterService(BaseModuleService):
    MODULE = "source_register"
    ENTITY = "源端系统"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["源端编号", "系统名称", "数据域", "记录格式", "编码方式", "日增量级", "抽取方式", "登记游标"]
    REQUIRED_FIELDS = ["源端编号", "系统名称", "数据域"]
    STATUS_ORDER = ["草稿", "待校验", "已登记", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "确认登记": "已登记", "标记停用": "已停用"}
    NEGATIVE_ACTIONS = ["标记停用"]

    def register_source(self, values: dict[str, Any]) -> dict[str, Any]:
        """登记新源端系统并写入登记事件日志，初始化源端配置快照。"""
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"缺少必填字段: {missing}")
        source_key = entry["源端编号"]
        # 初始化登记水位
        self._write_checkpoint(f"source_{source_key}", {
            "source_key": source_key,
            "status": "registered",
            "registered_at": time.time(),
            "extract_count": 0,
            "total_records": 0,
        })
        # 写入登记日志，记录源端配置快照
        self._write_jsonl(
            self._data_dir("events") / "source_register_log.jsonl",
            [{"action": "register", "source_key": source_key, "name": entry["系统名称"],
              "domain": entry["数据域"], "format": entry["记录格式"], "at": time.time()}]
        )
        return entry

    def confirm_register(self, source_id: int) -> dict[str, Any]:
        """确认源端登记并自动创建配套抽取窗口与字段契约，形成跨模块数据管道入口。"""
        entry, msg = self.run_action(source_id, "确认登记")
        if entry is None:
            return {"error": msg, "source_id": source_id}
        source_key = entry["源端编号"]
        triggers = {}

        # 自动创建配套增量窗口
        try:
            win_svc = self._safe_import("app.services.extract_window", "ExtractWindowService")()
            win_entry = self._auto_create_seed("extract_window", {
                "窗口编号": f"EXTR-{source_key}",
                "源端编号": source_key,
                "增量字段": "updated_at",
                "窗口起点": time.strftime("%Y-%m-%dT00:00:00", time.localtime(time.time())),
                "窗口终点": time.strftime("%Y-%m-%dT23:59:59", time.localtime(time.time())),
                "抽取条数": "1000",
                "窗口类型": "增量抽取",
                "推进水位": "0",
            })
            if win_entry:
                triggers["extract_window"] = {"created": win_entry["窗口编号"], "status": "草稿"}
        except Exception as e:
            triggers["extract_window"] = {"error": str(e)}

        # 自动创建配套字段契约
        try:
            fc_svc = self._safe_import("app.services.field_contract", "FieldContractService")()
            fc_entry = self._auto_create_seed("field_contract", {
                "契约编号": f"FIEL-{source_key}",
                "源字段": "src_field_001",
                "目标字段": "tgt_field_001",
                "数据类型": "string",
                "转换规则": "直接映射",
                "必填标记": "是",
                "默认值": "",
                "契约版本": "v1.0",
            })
            if fc_entry:
                triggers["field_contract"] = {"created": fc_entry["契约编号"], "status": "已生效"}
        except Exception as e:
            triggers["field_contract"] = {"error": str(e)}

        # 自动注册传输通道配置
        try:
            ch_svc = self._safe_import("app.services.channel_config", "ChannelConfigService")()
            ch_entry = self._auto_create_seed("channel_config", {
                "通道编号": f"CHAN-{source_key}",
                "传输协议": "HTTPS",
                "并发连接数": "4",
                "重试间隔": "30",
                "超时秒数": "300",
                "加密方式": "AES256",
                "通道类型": "源端通道",
                "配置版本": "v1.0",
            })
            if ch_entry:
                triggers["channel_config"] = {"created": ch_entry["通道编号"], "status": "已生效"}
        except Exception as e:
            triggers["channel_config"] = {"error": str(e)}

        return {
            "source_entry": entry,
            "source_key": source_key,
            "confirmed_at": time.time(),
            "auto_triggers": triggers,
        }

    def get_registered_sources(self) -> list[dict[str, Any]]:
        """获取所有已登记的活跃源端系统列表，用于下游窗口抽取调度。"""
        return [r for r in store.rows(self.MODULE) if r.get("status") == "已登记"]