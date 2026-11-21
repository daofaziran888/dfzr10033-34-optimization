"""改写与代价估算批次：把查询、规则、模型版本冻结到同一份批次清单。

一个批次代表“一次查询构建”——查询语句、实际命中的规则记录、规则快照和
模型版本快照共享同一个批次号与同一份模型签名。批次清单还承载改写文本、
扫描预测、成本估算、结算基线和对外公布结果，便于从成本回溯到真正用到的
模型数据。
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from app.services import versioning
from app.services.versioning import query_key

BATCHES: dict[str, "RewriteEstimateBatch"] = {}


@dataclass
class RewriteEstimateBatch:
    batch_id: str
    raw_query: str
    query_key_value: str
    model_ids: list[int]
    models: list[dict[str, Any]]
    model_signature: str
    rules: list[dict[str, Any]]
    rule_signature: str
    sources: list[str]
    query_type: str = "SELECT"
    status: str = "created"
    rewrite: dict[str, Any] | None = None
    prediction: dict[str, Any] | None = None
    estimate: dict[str, Any] | None = None
    settlement: dict[str, Any] | None = None
    cost_entry_id: int | None = None
    cache_id: int | None = None
    plan_id: int | None = None
    conflicts: list[dict[str, Any]] = field(default_factory=list)
    published: dict[str, Any] | None = None
    drift_acknowledged: bool = False
    kept_model_signature: str | None = None
    last_run_id: str | None = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)

    def touch(self) -> None:
        self.updated_at = time.time()

    @property
    def model_version(self) -> dict[str, Any]:
        """写入规则记录、缓存、成本时统一引用的版本描述。"""
        return {
            "signature": self.model_signature,
            "models": self.models,
            "rule_signature": self.rule_signature,
            "batch_id": self.batch_id,
        }

    def manifest(self) -> dict[str, Any]:
        """批次清单：对外公布和执行回写都读这一份。"""
        return {
            "批次编号": self.batch_id,
            "查询语句": self.raw_query,
            "查询摘要键": self.query_key_value,
            "状态": self.status,
            "模型版本": self.model_signature,
            "规则签名": self.rule_signature,
            "模型": [
                {
                    "id": item.get("id"),
                    "模型编号": item.get("模型编号"),
                    "版本序号": item.get("版本序号"),
                    "指纹": item.get("指纹"),
                }
                for item in self.models
            ],
            "规则": [
                {
                    "id": item.get("id"),
                    "规则编号": item.get("规则编号"),
                    "指纹": item.get("指纹"),
                }
                for item in self.rules
            ],
            "命中规则": [
                entry.get("规则编号") for entry in (self.rewrite or {}).get("rule_trace", [])
            ],
            "改写文本": (self.rewrite or {}).get("rewritten"),
            "扫描预测": self.prediction,
            "成本估算": self.estimate,
            "结算": self.settlement,
            "成本记录": self.cost_entry_id,
            "改写缓存": self.cache_id,
            "联邦计划": self.plan_id,
            "冲突": self.conflicts,
            "对外公布": self.published,
            "更新时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        }


def batch_key(raw_query: str, model_signature: str, rule_signature: str) -> str:
    """同一查询 + 同一模型/规则组合对应同一个批次。"""
    digest = versioning.digest([raw_query, model_signature, rule_signature])[:10]
    return f"REB-{digest}"


def begin(
    raw_query: str,
    model_ids: list[int] | None = None,
    sources: list[str] | None = None,
    query_type: str = "SELECT",
) -> tuple[RewriteEstimateBatch, bool]:
    """开始（或复用）一个批次，返回 ``(批次, 是否新建)``。

    批次号由查询和冻结的模型/规则签名推导：完全相同的组合重跑会命中同一
    批次，不会重复记命中；模型或规则变化则得到新的批次，互不覆盖。
    """
    models, model_signature = versioning.model_entries(model_ids)
    rules = versioning.rule_entries()
    rule_signature = versioning.rule_signature(rules)
    key = batch_key(raw_query, model_signature, rule_signature)

    existing = BATCHES.get(key)
    if existing is not None:
        return existing, False

    batch = RewriteEstimateBatch(
        batch_id=key,
        raw_query=raw_query,
        query_key_value=query_key(raw_query),
        model_ids=[int(mid) for mid in (model_ids or [])],
        models=models,
        model_signature=model_signature,
        rules=rules,
        rule_signature=rule_signature,
        sources=list(sources or ["source_registry"]),
        query_type=query_type,
    )
    BATCHES[key] = batch
    return batch, True


def get(batch_id: str | None) -> RewriteEstimateBatch | None:
    if not batch_id:
        return None
    return BATCHES.get(batch_id)


def detect_drift(batch: RewriteEstimateBatch) -> list[dict[str, Any]]:
    """对比批次冻结快照与当前模型/规则，列出漂移项。

    冻结时未显式指定模型，表示该批次覆盖“当时全量模型”；
    这时新上线的模型同样属于漂移。
    """
    conflicts: list[dict[str, Any]] = []

    current_models, current_signature = versioning.model_entries(batch.model_ids or None)
    if not batch.model_ids:
        # 冻结时未指定模型 = 覆盖当时全量模型；这时新上线的模型同样属于漂移。
        all_models, _ = versioning.model_entries(None)
        current_models = all_models
        current_signature = versioning.digest([entry["指纹"] for entry in all_models])
    if current_signature != batch.model_signature:
        frozen = {int(item.get("id", 0)): item for item in batch.models}
        current = {int(item.get("id", 0)): item for item in current_models}
        for model_id, entry in current.items():
            if model_id not in frozen:
                conflicts.append({
                    "type": "model_added",
                    "model_id": model_id,
                    "模型编号": entry.get("模型编号"),
                    "版本序号": entry.get("版本序号"),
                })
            elif frozen[model_id].get("指纹") != entry.get("指纹"):
                changed = [
                    field for field in versioning.MODEL_VERSION_FIELDS
                    if str(frozen[model_id].get(field, "")) != str(entry.get(field, ""))
                ]
                conflicts.append({
                    "type": "model_changed",
                    "model_id": model_id,
                    "模型编号": entry.get("模型编号"),
                    "版本序号": entry.get("版本序号"),
                    "changed_fields": changed,
                })
        for model_id, entry in frozen.items():
            if model_id not in current:
                conflicts.append({
                    "type": "model_removed",
                    "model_id": model_id,
                    "模型编号": entry.get("模型编号"),
                })

    current_rules = versioning.rule_entries()
    if versioning.rule_signature(current_rules) != batch.rule_signature:
        frozen_rules = {int(item.get("id", 0)): item for item in batch.rules}
        current_rule_map = {int(item.get("id", 0)): item for item in current_rules}
        for rule_id, entry in current_rule_map.items():
            if rule_id not in frozen_rules:
                conflicts.append({"type": "rule_added", "rule_id": rule_id,
                                  "规则编号": entry.get("规则编号")})
            elif frozen_rules[rule_id].get("指纹") != entry.get("指纹"):
                conflicts.append({"type": "rule_changed", "rule_id": rule_id,
                                  "规则编号": entry.get("规则编号")})
        for rule_id, entry in frozen_rules.items():
            if rule_id not in current_rule_map:
                conflicts.append({"type": "rule_removed", "rule_id": rule_id,
                                  "规则编号": entry.get("规则编号")})

    return conflicts


def clear() -> None:
    """测试辅助：清空批次注册表。"""
    BATCHES.clear()
