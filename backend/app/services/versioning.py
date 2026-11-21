"""模型/规则版本快照：给改写与代价估算批次提供稳定的版本指纹。

一个批次在开始时冻结一次模型与规则快照，后续改写、扫描预测、成本结算和
缓存都只引用这份快照。只要模型字段（或改写规则）在批次两个阶段之间发生变化，
指纹就会不同，调用方可以据此判定“模型版本已漂移”，而不是把两个版本的数据
拼进同一次估算。
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable

from app.store import store

# 只把真正影响查询翻译结果的字段纳入指纹。status / pending / 命中次数这类
# 运行期可变字段不能进入指纹，否则一次成功改写会把自己判成版本漂移。
MODEL_VERSION_FIELDS = [
    "模型编号",
    "模型名称",
    "源表映射",
    "字段别名",
    "度量定义",
    "维度定义",
    "计算口径",
    "版本序号",
]

RULE_VERSION_FIELDS = [
    "规则编号",
    "原始模式",
    "替换模式",
    "适用模型",
    "改写条件",
    "代价阈值",
]


def _digest(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def digest(value: Any) -> str:
    return _digest(value)


def query_key(query: str) -> str:
    """查询语句的稳定摘要，用于把同一查询的历史批次串联成结算基线。"""
    return hashlib.md5(query.encode("utf-8")).hexdigest()[:16]


def model_entries(model_ids: Iterable[int] | None = None) -> tuple[list[dict[str, Any]], str]:
    """返回批次使用的模型快照与组合签名。

    ``model_ids`` 为空时冻结当前全部语义模型：这样“新上线一套模型”会改变
    签名，流水线能发现改写和估算之间发生了模型版本漂移。
    """
    rows = store.rows("semantic_model")
    wanted = [int(mid) for mid in (model_ids or [])]
    if wanted:
        by_id = {int(row.get("id", 0)): row for row in rows}
        selected = [by_id[mid] for mid in wanted if mid in by_id]
    else:
        selected = sorted(rows, key=lambda row: int(row.get("id", 0)))

    entries: list[dict[str, Any]] = []
    for row in selected:
        version = {field: str(row.get(field, "")) for field in MODEL_VERSION_FIELDS}
        entries.append({
            "id": row.get("id"),
            "模型编号": row.get("模型编号"),
            "模型名称": row.get("模型名称"),
            "版本序号": row.get("版本序号"),
            **version,
            "状态": row.get("status"),
            "指纹": _digest(version),
        })

    signature = _digest([entry["指纹"] for entry in entries]) if entries else ""
    return entries, signature


def current_model_signature(model_ids: Iterable[int] | None = None) -> str:
    return model_entries(model_ids)[1]


def rule_entries() -> list[dict[str, Any]]:
    """冻结当前已生效的改写规则（按 id 排序，排除命中次数等可变字段）。"""
    rows = [
        row for row in store.rows("query_rewrite")
        if row.get("status") == "已生效"
    ]
    rows.sort(key=lambda row: int(row.get("id", 0)))
    entries: list[dict[str, Any]] = []
    for row in rows:
        version = {field: str(row.get(field, "")) for field in RULE_VERSION_FIELDS}
        entries.append({
            "id": row.get("id"),
            **version,
            "指纹": _digest(version),
        })
    return entries


def rule_signature(entries: Iterable[dict[str, Any]] | None = None) -> str:
    frozen = list(entries) if entries is not None else rule_entries()
    fingerprints = [entry.get("指纹", "") for entry in frozen]
    return _digest(fingerprints) if fingerprints else ""


def rule_entry_signature(entry: dict[str, Any]) -> str:
    version = {field: str(entry.get(field, "")) for field in RULE_VERSION_FIELDS}
    return _digest(version)


def model_entry_signature(entry: dict[str, Any]) -> str:
    version = {field: str(entry.get(field, "")) for field in MODEL_VERSION_FIELDS}
    return _digest(version)
