"""联邦查询领域服务：跨数据源查询计划、谓词下推、代价评估与执行编排。"""
from __future__ import annotations

import hashlib
import random
import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.services.settlement import settle
from app.services.versioning import query_key
from app.store import store


class QueryFederationService(BaseModuleService):
    MODULE = "query_federation"
    ENTITY = "联邦计划"
    OPERATION_TERM = "查询"
    LIST_FIELDS = ["计划编号", "参与来源", "关联键", "下推策略", "数据量估算", "并行度", "超时上限", "结果合并"]
    REQUIRED_FIELDS = ["计划编号", "参与来源", "关联键"]
    STATUS_ORDER = ["草稿", "执行中", "已完成", "执行异常"]
    ACTION_RULES = {"提交执行": "执行中", "取消计划": "已完成", "标记异常": "执行异常"}
    NEGATIVE_ACTIONS = ["标记异常"]

    def build_federation_plan(
        self, sources: list[str], query_type: str = "SELECT"
    ) -> dict[str, Any]:
        """从可用数据源构建联邦查询计划：自动推导关联键、下推策略和并行度。"""
        registered_sources = store.rows("source_registry")
        available = [s for s in registered_sources if s.get("status") == "已注册"]

        # 谓词下推策略
        pushdown = _infer_pushdown_strategy(sources, available)

        # 代价估算
        cost = _estimate_cost(sources, query_type, pushdown)

        plan = {
            "参与来源": ", ".join(sources),
            "关联键": _infer_join_keys(sources, available),
            "下推策略": pushdown,
            "数据量估算": f"~{cost['estimated_rows']}行",
            "并行度": str(min(len(sources) * 2, 8)),
            "超时上限": f"{max(30, cost['estimated_rows'] // 1000)}s",
            "结果合并": "UNION_ALL" if len(sources) > 1 else "DIRECT",
        }
        return plan

    def execute_federation_query(self, plan_id: int) -> dict[str, Any]:
        """执行联邦查询：读取 source_registry 和 query_rewrite，写入 result_cache 和 query_audit。

        由改写与代价估算批次创建的计划，必须等批次公布（估算已回写清单）
        且模型版本没有被替换后才能进入执行。
        """
        plan = store.find(self.MODULE, plan_id)
        if plan is None:
            return {"ok": False, "message": "查询计划不存在"}

        batch_id = plan.get("批次编号")
        if batch_id and plan.get("对外公布") != batch_id:
            return {
                "ok": False,
                "message": "批次尚未公布或估算未回写清单，计划不能进入执行",
                "batch_id": batch_id,
            }

        sources = [s.strip() for s in plan.get("参与来源", "").split(",") if s.strip()]
        if not sources:
            return {"ok": False, "message": "没有可用的数据源"}

        # 从 source_registry 获取可用数据源
        registry = store.rows("source_registry")
        valid_sources = [
            s for s in registry
            if s.get("源编号", "") in sources and s.get("status") == "已注册"
        ]

        if len(valid_sources) < len(sources):
            missing = set(sources) - {s["源编号"] for s in valid_sources}
            return {
                "ok": False,
                "message": f"部分数据源不可用: {', '.join(missing)}",
            }

        # 应用查询改写规则
        rewrite_rules = _get_applicable_rewrite_rules(plan)
        rewritten_query = _apply_rewrite(plan, rewrite_rules)

        # 执行查询（模拟）
        t0 = time.time()
        result_rows = _simulate_federated_query(sources, plan.get("谓词下推策略", ""))

        # 写入结果缓存（模型版本随计划一起带到缓存上）
        _cache_query_result(plan_id, result_rows, plan)

        # 写入查询审计
        elapsed_ms = (time.time() - t0) * 1000
        _audit_federated_query(plan_id, elapsed_ms, len(result_rows), plan)

        # 更新多维立方数据（如果涉及）
        _update_cube_if_needed(plan_id, result_rows)

        # 更新计划状态
        plan["status"] = "已完成"
        plan["pending"] = False

        return {
            "ok": True,
            "plan_id": plan_id,
            "sources": len(valid_sources),
            "rows_returned": len(result_rows),
            "elapsed_ms": elapsed_ms,
            "results": result_rows[:10],
            "cache_key": _cache_key(plan_id),
            "batch_id": batch_id,
            "model_version": plan.get("模型版本"),
        }

    def estimate_query_cost(
        self,
        sources: list[str],
        query_type: str,
        *,
        prediction: dict[str, Any] | None = None,
        rules: list[dict[str, Any]] | None = None,
        batch_id: str | None = None,
        raw_query: str = "",
        model_version: dict[str, Any] | None = None,
        rule_signature: str = "",
        baseline: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """对联邦查询做代价估算，写入 cost_govern 配额记录。

        ``prediction`` 来自改写环节由实际规则算出的扫描预测；不传时才回退到
        按数据源规模估算。带 ``batch_id`` 时以批次为幂等键：中断后重试复用
        同一条成本记录，不会把节省量重复叠加。
        """
        available = [s for s in store.rows("source_registry") if s.get("status") == "已注册"]
        pushdown = _infer_pushdown_strategy(sources, available)
        cost = _estimate_cost(sources, query_type, pushdown)
        if prediction is not None:
            scan_rows = int(prediction.get("estimated_scan_rows", 0))
            savings = int(prediction.get("estimated_savings_rows", 0))
            settlement = settle(baseline, savings)
        else:
            scan_rows = int(cost["estimated_rows"])
            savings = 0
            settlement = {
                "baseline_rows": 0, "previous_savings": 0,
                "incremental_rows": 0, "cumulative_savings": 0,
            }

        cost_rows = store.rows("cost_govern")
        if batch_id:
            for row in cost_rows:
                if row.get("批次编号") == batch_id and row.get("适用角色") == "rewrite_estimate":
                    return {
                        "ok": True,
                        "cost": cost,
                        "scan_prediction": prediction,
                        "settlement": _row_settlement(row, settlement),                        "cost_entry_id": row["id"],
                        "reused": True,
                        "batch_id": batch_id,
                    }

        cost_id = max((int(r.get("id", 0)) for r in cost_rows), default=0) + 1
        cost_entry = {
            "id": cost_id,
            "策略编号": f"COST-{batch_id or uuid.uuid4().hex[:8]}",
            "适用角色": "rewrite_estimate" if prediction is not None else "federated_query",
            "查询上限": str(scan_rows),
            "并发上限": str(len(sources)),
            "月度配额": str(scan_rows * 30),
            "告警阈值": str(scan_rows * 2),
            "限流动作": "settle_savings" if prediction is not None else "record_only",
            "生效时段": time.strftime("%Y-%m-%d"),
            "status": "已生效",
            "pending": False,
            "abnormal": scan_rows > 100000,
            "批次编号": batch_id,
            "查询摘要键": query_key(raw_query) if raw_query else None,
            "规则编号": [rule.get("规则编号") for rule in (rules or [])],
            "模型版本": (model_version or {}).get("signature"),
            "规则签名": rule_signature or (model_version or {}).get("rule_signature"),
            "扫描预测": scan_rows,
            "扫描行数": str(scan_rows),
            "节省量": savings,
            "节省行数": settlement["incremental_rows"],
            "结算基线": settlement["baseline_rows"],
            "结算累计": settlement["cumulative_savings"],
        }
        cost_rows.append(cost_entry)

        return {
            "ok": True,
            "cost": cost,
            "scan_prediction": prediction,
            "settlement": settlement,
            "cost_entry_id": cost_id,
            "reused": False,
            "batch_id": batch_id,
        }


def _row_settlement(row: dict[str, Any], fallback: dict[str, Any]) -> dict[str, Any]:
    """重试命中已有成本记录时，按已落盘的值回报结算结果。"""
    try:
        return {
            "baseline_rows": int(row.get("结算基线", fallback.get("baseline_rows", 0)) or 0),
            "previous_savings": int(row.get("结算累计", 0) or 0)
            - int(row.get("节省行数", 0) or 0),
            "incremental_rows": int(row.get("节省行数", 0) or 0),
            "cumulative_savings": int(row.get("结算累计", 0) or 0),
        }
    except (ValueError, TypeError):
        return fallback


# ---------- 跨模块辅助函数 ----------

def _infer_pushdown_strategy(sources: list[str], available: list[dict[str, Any]]) -> str:
    """根据数据源类型推导谓词下推策略。"""
    source_types = []
    for s in sources:
        for a in available:
            if a.get("源编号") == s:
                source_types.append(a.get("源类型", "unknown"))
                break

    if any(t in ("mysql", "postgresql") for t in source_types):
        return "FULL_PUSHDOWN"
    elif any(t in ("clickhouse", "hive") for t in source_types):
        return "PARTIAL_PUSHDOWN"
    else:
        return "NO_PUSHDOWN"


def _infer_join_keys(sources: list[str], available: list[dict[str, Any]]) -> str:
    """从不同数据源推导关联键。"""
    keys = []
    for s in sources:
        for a in available:
            if a.get("源编号") == s:
                st = a.get("源类型", "unknown")
                if st in ("mysql", "postgresql"):
                    keys.append("user_id")
                elif st in ("clickhouse", "hive"):
                    keys.append("uid")
                else:
                    keys.append("id")
    return " = ".join(keys[:2]) if len(keys) >= 2 else keys[0] if keys else "id"


def _estimate_cost(sources: list[str], query_type: str, pushdown: str) -> dict[str, Any]:
    """估算查询代价（行数、IO、CPU）。"""
    base_rows = random.randint(1000, 50000)
    multiplier = len(sources)
    if pushdown == "FULL_PUSHDOWN":
        multiplier *= 0.3
    elif pushdown == "PARTIAL_PUSHDOWN":
        multiplier *= 0.6
    else:
        multiplier *= 1.5

    estimated = int(base_rows * multiplier)
    return {
        "estimated_rows": estimated,
        "estimated_io_mb": round(estimated * 0.001, 2),
        "estimated_cpu_ms": estimated // 10,
        "pushdown_strategy": pushdown,
        "query_type": query_type,
    }


def _get_applicable_rewrite_rules(plan: dict[str, Any]) -> list[dict[str, Any]]:
    rows = store.rows("query_rewrite")
    return [r for r in rows if r.get("status") == "已生效"]


def _apply_rewrite(plan: dict[str, Any], rules: list[dict[str, Any]]) -> str:
    if not rules:
        return f"SELECT * FROM federated({plan.get('参与来源', '')})"
    rewrites = [r.get("替换模式", "") for r in rules]
    return f"SELECT /* rewrite: {', '.join(rewrites)} */ * FROM federated({plan.get('参与来源', '')})"


def _simulate_federated_query(sources: list[str], strategy: str) -> list[dict[str, Any]]:
    """模拟联邦查询执行，生成真实的结果行。"""
    rows = []
    for i in range(random.randint(5, 50)):
        source_idx = i % len(sources)
        rows.append({
            "row_id": i + 1,
            "source": sources[source_idx],
            "value": random.randint(1, 10000),
            "category": f"cat_{random.randint(1, 10)}",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "strategy": strategy,
        })
    return rows


def _cache_query_result(
    plan_id: int,
    results: list[dict[str, Any]],
    plan: dict[str, Any] | None = None,
) -> None:
    rows = store.rows("result_cache")
    cache_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    data_str = str(results)
    plan = plan or {}
    cache = {
        "id": cache_id,
        "条目编号": f"RESU-F{plan_id:04d}",
        "查询摘要": f"federated_query_plan_{plan_id}",
        "结果哈希": hashlib.md5(data_str.encode()).hexdigest()[:12],
        "缓存命中": str(len(results)),
        "过期时间": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() + 3600)),
        "存储大小": str(len(data_str)),
        "创建时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        "最后命中": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "有效",
        "pending": True,
        "abnormal": False,
        "批次编号": plan.get("批次编号"),
        "模型版本": plan.get("模型版本"),
        "result_data": results,
    }
    rows.append(cache)


def _audit_federated_query(
    plan_id: int,
    elapsed_ms: float,
    result_count: int,
    plan: dict[str, Any] | None = None,
) -> None:
    rows = store.rows("query_audit")
    audit_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    plan = plan or {}
    audit = {
        "id": audit_id,
        "记录编号": f"AUDIT-{uuid.uuid4().hex[:8]}",
        "查询语句": f"FEDERATED_QUERY plan_id={plan_id}",
        "执行耗时": f"{elapsed_ms:.0f}ms",
        "扫描行数": str(result_count * 3),
        "返回行数": str(result_count),
        "数据来源": "query_federation",
        "操作用户": "federation_engine",
        "查询时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "正常" if elapsed_ms < 5000 else "慢查询",
        "pending": False,
        "abnormal": elapsed_ms > 30000,
        "批次编号": plan.get("批次编号"),
        "模型版本": plan.get("模型版本"),
    }
    rows.append(audit)


def _update_cube_if_needed(plan_id: int, results: list[dict[str, Any]]) -> None:
    """如果查询结果关联到多维立方，则更新立方数据行数指标。"""
    cubes = store.rows("olap_cube")
    for cube in cubes:
        if cube.get("status") == "可用":
            vals = [r.get("value", 0) for r in results]
            if vals and "value" in str(results[0].keys()):
                old_val = cube.get("数据行数", "0")
                try:
                    new_total = int(old_val) + sum(vals) if old_val.isdigit() else sum(vals)
                except (ValueError, TypeError):
                    new_total = sum(vals)
                cube["数据行数"] = str(new_total)


def _cache_key(plan_id: int) -> str:
    return f"federated_query/plan_{plan_id}/{uuid.uuid4().hex[:8]}"