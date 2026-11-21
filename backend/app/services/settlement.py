"""节省量结算：把本次估算与历史批次对账，避免重试重复叠加节省量。

历史数据有两种布局：
- 新批次：``适用角色=rewrite_estimate``，节省量在 ``节省行数`` / ``节省量``；
- 旧实现：``适用角色=query_rewrite``，节省量被写进 ``查询上限``。

读取时两种都兼容，结算基线按查询摘要汇总，当前批次自身不进入基线。
"""
from __future__ import annotations

from typing import Any

from app.services.versioning import query_key
from app.store import store

SETTLEMENT_ROLES = {"rewrite_estimate", "query_rewrite"}


def historical_savings(row: dict[str, Any]) -> int:
    """从一条成本记录里读出它结算过的节省量，兼容旧字段布局。"""
    for field in ("节省行数", "节省量"):
        if field in row and row.get("适用角色") in SETTLEMENT_ROLES:
            try:
                return int(row.get(field) or 0)
            except (ValueError, TypeError):
                return 0
    if row.get("适用角色") == "query_rewrite":
        try:
            return int(row.get("查询上限") or 0)
        except (ValueError, TypeError):
            return 0
    return 0


def resolve_baseline(raw_query: str, batch_id: str | None = None) -> dict[str, Any]:
    """汇总同一查询历史上已结算的节省量，作为本次增量结算的基线。"""
    key = query_key(raw_query)
    baseline_savings = 0
    settled_savings = 0
    batches: list[str] = []
    legacy_seen = False

    for row in store.rows("cost_govern"):
        if row.get("查询摘要键") != key:
            continue
        row_batch = row.get("批次编号")
        if batch_id and row_batch == batch_id:
            continue
        savings = historical_savings(row)
        if savings <= 0:
            continue
        baseline_savings += savings
        batch_key = row_batch or f"legacy:{row.get('id')}"
        if batch_key in batches:
            continue
        batches.append(batch_key)
        settled_savings += savings
        if not row_batch:
            legacy_seen = True

    return {
        "query_key": key,
        "baseline_savings": baseline_savings,
        "settled_savings": settled_savings,
        "settled_batches": len(batches),
        "compatible_legacy": legacy_seen,
    }


def settle(baseline: dict[str, Any] | None, savings: int) -> dict[str, Any]:
    """把本次估算节省量与历史基线对账，得到增量与累计。"""
    baseline = baseline or {}
    baseline_rows = int(baseline.get("baseline_savings", 0) or 0)
    previous = int(baseline.get("settled_savings", 0) or 0)
    incremental = max(0, savings - previous)
    return {
        "baseline_rows": baseline_rows,
        "previous_savings": previous,
        "incremental_rows": incremental,
        "cumulative_savings": previous + incremental,
    }
