"""查询改写领域服务：SQL改写规则管理，代价评估，与联邦查询和聚合引擎的交互。

改写规则的应用、命中计数、扫描量预测都集中在这里，供改写与代价估算批次复用。
命中计数和扫描预测都必须可重复：同一批次重跑不能再次累加命中，也不能重新
摇一次随机数，否则估算结果无法回溯到同一份规则与模型版本。
"""
from __future__ import annotations

import hashlib
import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.versioning import query_key, rule_entry_signature
from app.store import store

# 批次内已经记过命中的 (批次, 规则) 组合。用于重试时跳过重复的命中累加。
_HIT_LEDGER: set[tuple[str, int]] = set()

# 扫描量预测的基准：固定公式，避免 random 让重跑结果漂移。
BASE_SCAN_ROWS = 12_000
ROW_PER_CHAR = 7


class QueryRewriteService(BaseModuleService):
    MODULE = "query_rewrite"
    ENTITY = "改写规则"
    OPERATION_TERM = "查询"
    LIST_FIELDS = ["规则编号", "原始模式", "替换模式", "适用模型", "改写条件", "代价阈值", "命中次数", "最后命中"]
    REQUIRED_FIELDS = ["规则编号", "原始模式", "替换模式"]
    STATUS_ORDER = ["草稿", "已生效", "待验证", "已废弃"]
    ACTION_RULES = {"启效规则": "已生效", "提交验证": "待验证", "废弃规则": "已废弃"}
    NEGATIVE_ACTIONS = ["废弃规则"]

    def rewrite_query(
        self,
        raw_query: str,
        model_ids: list[int] | None = None,
        *,
        rules: list[dict[str, Any]] | None = None,
        models: list[dict[str, Any]] | None = None,
        batch_id: str | None = None,
        model_version: dict[str, Any] | None = None,
        count_hits: bool = True,
    ) -> dict[str, Any]:
        """应用已生效规则改写查询。

        ``rules`` / ``models`` 由改写与代价估算批次冻结后传入；不传时按当前
        已生效规则计算，保持原有独立调用方式。命中次数按 (批次, 规则) 只记一次，
        重跑同一批次不会重复累加。
        """
        frozen_rules = (
            rules if rules is not None
            else sorted(
                (r for r in store.rows(self.MODULE) if r.get("status") == "已生效"),
                key=lambda r: int(r.get("id", 0)),
            )
        )

        if not frozen_rules:
            prediction = predict_scan(raw_query, raw_query, [])
            cache_rewrite_result(
                raw_query, raw_query,
                batch_id=batch_id, model_version=model_version, rule_trace=[])
            return {
                "ok": True,
                "original": raw_query,
                "rewritten": raw_query,
                "rules_applied": 0,
                "rule_trace": [],
                "scan_prediction": prediction,
                "estimated_savings_rows": 0,
                "savings": 0,
                "batch_id": batch_id,
                "model_version": model_version,
                "reused": False,
            }

        rewritten = raw_query
        trace: list[dict[str, Any]] = []

        for rule in frozen_rules:
            pattern = rule.get("原始模式", "")
            replacement = rule.get("替换模式", "")
            condition = rule.get("改写条件", "")

            if not pattern or pattern not in rewritten:
                continue
            if model_ids:
                matched = _match_models(condition, model_ids, models)
                if not matched:
                    continue

            rewritten = rewritten.replace(pattern, replacement)
            trace.append({
                "rule_id": rule.get("id"),
                "规则编号": rule.get("规则编号"),
                "原始模式": pattern,
                "替换模式": replacement,
                "改写条件": condition,
                "代价阈值": rule.get("代价阈值"),
                "规则签名": rule_entry_signature(rule),
                "模型版本": (model_version or {}).get("signature"),
            })
            if count_hits:
                _record_hit_once(rule, batch_id, model_version)

        prediction = predict_scan(raw_query, rewritten, trace)
        prediction["model_version"] = model_version
        prediction["model_signature"] = (model_version or {}).get("signature")

        cache, created = cache_rewrite_result(
            raw_query, rewritten,
            batch_id=batch_id, model_version=model_version, rule_trace=trace)

        return {
            "ok": True,
            "original": raw_query,
            "rewritten": rewritten,
            "rules_applied": len(trace),
            "rule_trace": trace,
            "scan_prediction": prediction,
            "estimated_savings_rows": prediction["estimated_savings_rows"],
            "savings": prediction["estimated_savings_rows"],
            "cache_id": cache.get("id"),
            "batch_id": batch_id,
            "model_version": model_version,
            "reused": not created,
        }

    def analyze_query_patterns(self) -> dict[str, Any]:
        """分析 query_audit 慢查询模式，生成改写规则建议。"""
        audits = store.rows("query_audit")
        slow_queries = [a for a in audits if a.get("status") == "慢查询"]

        patterns: dict[str, int] = {}
        for sq in slow_queries:
            stmt = sq.get("查询语句", "")
            # 简单模式提取
            if "CUBE_BUILD" in stmt:
                patterns["cube_build"] = patterns.get("cube_build", 0) + 1
            elif "FEDERATED" in stmt:
                patterns["federated_query"] = patterns.get("federated_query", 0) + 1
            elif "EXPORT" in stmt:
                patterns["export"] = patterns.get("export", 0) + 1
            else:
                patterns[stmt[:30]] = patterns.get(stmt[:30], 0) + 1

        recommendations = []
        for pattern, count in patterns.items():
            if count >= 2:
                recommendations.append({
                    "pattern": pattern,
                    "frequency": count,
                    "suggested_action": "CREATE_PUSHDOWN_RULE" if "federated" in pattern else "ADD_CACHE_HINT",
                })

        return {
            "ok": True,
            "slow_queries_count": len(slow_queries),
            "patterns_found": len(patterns),
            "recommendations": recommendations,
        }


def predict_scan(
    original: str,
    rewritten: str,
    rule_trace: list[dict[str, Any]],
) -> dict[str, Any]:
    """由实际命中的改写规则推导扫描量预测与节省行数。

    公式固定、只依赖查询文本和规则自身字段，不引入随机数：同一批次无论
    重跑多少次都得到同一份扫描预测，成本预估才可以回溯到同一次查询构建。
    """
    base_rows = BASE_SCAN_ROWS + ROW_PER_CHAR * len(original)
    current = base_rows
    reductions: list[dict[str, Any]] = []

    for entry in rule_trace:
        try:
            threshold = int(entry.get("代价阈值") or 0)
        except (ValueError, TypeError):
            threshold = 0
        if threshold <= 0:
            threshold = 500
        # 单条规则最多削减 40%，避免多条规则把扫描量压成负数或 0。
        reduction = min(threshold, max(int(current * 0.4), 1))
        current -= reduction
        reductions.append({
            "rule_id": entry.get("rule_id"),
            "规则编号": entry.get("规则编号"),
            "threshold_rows": threshold,
            "reduced_rows": reduction,
        })

    savings = max(0, base_rows - current)
    return {
        "base_scan_rows": base_rows,
        "estimated_scan_rows": current,
        "estimated_savings_rows": savings,
        "rewritten_length": len(rewritten),
        "original_length": len(original),
        "rules_applied": len(rule_trace),
        "reductions": reductions,
    }


def _record_hit_once(
    rule: dict[str, Any],
    batch_id: str | None,
    model_version: dict[str, Any] | None = None,
) -> bool:
    """记录一次规则命中；同一批次内的同一条规则只记一次。

    批次里的 ``rule`` 是冻结快照，命中计数必须写回仓库中的真实规则行。
    """
    if batch_id:
        key = (batch_id, int(rule.get("id", 0)))
        if key in _HIT_LEDGER:
            return False
        _HIT_LEDGER.add(key)

    target = store.find("query_rewrite", int(rule.get("id", 0))) or rule
    try:
        hit_count = int(target.get("命中次数", "0"))
    except (ValueError, TypeError):
        hit_count = 0
    target["命中次数"] = str(hit_count + 1)
    target["最后命中"] = time.strftime("%Y-%m-%d %H:%M:%S")
    target["最后命中批次"] = batch_id
    target["最后命中模型版本"] = (model_version or {}).get("signature")
    return True


def _match_models(
    condition: str,
    model_ids: list[int],
    models: list[dict[str, Any]] | None = None,
) -> bool:
    if not condition:
        return True
    candidates = models if models is not None else store.rows("semantic_model")
    for mid in model_ids:
        for m in candidates:
            if int(m.get("id", 0)) == mid and condition in str(m):
                return True
    return False


def cache_rewrite_result(
    original: str,
    rewritten: str,
    *,
    batch_id: str | None = None,
    model_version: dict[str, Any] | None = None,
    rule_trace: list[dict[str, Any]] | None = None,
) -> tuple[dict[str, Any], bool]:
    """写入改写结果缓存，返回 ``(条目, 是否新建)``。

    带批次时以 ``批次编号`` 做幂等键：重跑只复用原缓存条目，不再新建一条，
    并把模型版本、规则签名一并固化到缓存上。
    """
    rows = store.rows("result_cache")
    if batch_id:
        for row in rows:
            if row.get("批次编号") == batch_id:
                return row, False

    cache_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    signature = (model_version or {}).get("signature", "")
    cache = {
        "id": cache_id,
        "条目编号": f"RESU-RW{uuid_suffix(batch_id or original)}",
        "查询摘要": f"rewrite_{hashlib.md5(original.encode()).hexdigest()[:12]}",
        "结果哈希": hashlib.md5(rewritten.encode()).hexdigest()[:12],
        "缓存命中": "0",
        "过期时间": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() + 3600)),
        "存储大小": str(len(rewritten)),
        "创建时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        "最后命中": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "有效",
        "pending": True,
        "abnormal": False,
        "批次编号": batch_id,
        "查询摘要键": query_key(original),
        "模型版本": signature,
        "规则签名": (model_version or {}).get("rule_signature"),
        "改写规则": [entry.get("规则编号") for entry in (rule_trace or [])],
        "result_data": [{
            "original": original,
            "rewritten": rewritten,
            "batch_id": batch_id,
            "model_version": model_version,
        }],
    }
    rows.append(cache)
    return cache, True


def uuid_suffix(seed: str) -> str:
    return hashlib.md5(seed.encode("utf-8")).hexdigest()[:8]
