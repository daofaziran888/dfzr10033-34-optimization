"""改写与代价估算批次的编排：冻结版本 → 改写 → 估算 → 联邦计划 → 回写清单。

所有阶段共享同一个批次对象，因此改写文本、命中的规则、扫描预测和成本
估算都引用同一个模型版本；估算结果回写到批次清单，由清单决定该计划能
否进入执行。
"""
from __future__ import annotations

import time
from typing import Any

from app.pipeline import PipelineRun, engine
from app.services import batch_store, versioning
from app.services.query_federation import QueryFederationService
from app.services.query_rewrite import QueryRewriteService
from app.services.settlement import resolve_baseline
from app.store import store

rewrite_svc = QueryRewriteService()
federation_svc = QueryFederationService()

PIPELINE_NAME = "rewrite-estimate-execute"
STAGE_FREEZE = "freeze_version"
STAGE_REWRITE = "rewrite"
STAGE_ESTIMATE = "estimate"
STAGE_FEDERATE = "federate"
STAGE_PUBLISH = "publish_decision"


def stage_freezing(batch: batch_store.RewriteEstimateBatch):
    def handler(_upstream: dict[str, Any] | None) -> dict[str, Any]:
        batch.conflicts = batch_store.detect_drift(batch)
        if batch.conflicts:
            batch.status = "version_drift"
        batch.touch()
        return {"batch_id": batch.batch_id, "models": batch.models, "rules": batch.rules,
                "model_version": batch.model_version, "conflicts": batch.conflicts}
    return handler


def stage_rewriting(batch: batch_store.RewriteEstimateBatch):
    def handler(_upstream: dict[str, Any] | None) -> dict[str, Any]:
        result = rewrite_svc.rewrite_query(
            batch.raw_query,
            batch.model_ids or None,
            rules=batch.rules,
            models=batch.models,
            batch_id=batch.batch_id,
            model_version=batch.model_version,
        )
        batch.rewrite = result
        batch.prediction = result["scan_prediction"]
        batch.cache_id = result.get("cache_id")
        if batch.status == "created":
            batch.status = "rewritten"
        batch.touch()
        return {"batch_id": batch.batch_id, "rewrite": result}
    return handler


def stage_estimating(batch: batch_store.RewriteEstimateBatch):
    def handler(_upstream: dict[str, Any] | None) -> dict[str, Any]:
        # 估算前再比对一次模型/规则版本：改写之后上线的新字段会在这里被发现。
        drift = batch_store.detect_drift(batch)
        if drift:
            batch.conflicts = drift
            batch.status = "version_drift"

        if batch.prediction is None:
            raise RuntimeError("改写结果缺失，无法基于同一批规则估算代价")

        baseline = resolve_baseline(batch.raw_query, batch.batch_id)
        estimate = federation_svc.estimate_query_cost(
            batch.sources,
            batch.query_type,
            prediction=batch.prediction,
            rules=batch.rules,
            batch_id=batch.batch_id,
            raw_query=batch.raw_query,
            model_version=batch.model_version,
            rule_signature=batch.rule_signature,
            baseline=baseline,
        )
        batch.estimate = estimate
        batch.settlement = {**estimate.get("settlement", {}), "baseline": baseline}
        batch.cost_entry_id = estimate.get("cost_entry_id")
        if batch.status not in ("version_drift",):
            batch.status = "estimated"
        batch.touch()
        return estimate
    return handler


def stage_federating(batch: batch_store.RewriteEstimateBatch):
    def handler(_upstream: dict[str, Any] | None) -> dict[str, Any]:
        if batch.estimate is None:
            raise RuntimeError("成本估算缺失，不能创建联邦计划")
        existing_plan_id = batch.plan_id
        existing_plan = (
            store.find("query_federation", existing_plan_id)
            if existing_plan_id is not None else None
        )
        if existing_plan is not None:
            return {"plan_id": existing_plan_id, "reused": True, "plan": existing_plan}

        plan_values = federation_svc.build_federation_plan(batch.sources, batch.query_type)
        scan = int(batch.prediction.get("estimated_scan_rows", 0))
        plan_values["计划编号"] = f"QUER-{batch.batch_id.removeprefix('REB-').upper()}"
        plan_values["数据量估算"] = f"~{scan}行"
        plan, missing = federation_svc.create_entry(plan_values)
        if plan is None:
            raise RuntimeError(f"创建联邦计划失败: {missing}")
        # create_entry 只落 LIST_FIELDS，批次溯源字段在创建后补齐。
        plan.update({
            "规则": [rule.get("规则编号") for rule in batch.rules],
            "扫描预测": scan,
            "批次编号": batch.batch_id,
            "模型版本": batch.model_signature,
            "规则签名": batch.rule_signature,
            "改写文本": (batch.rewrite or {}).get("rewritten"),
            "查询语句": batch.raw_query,
        })
        batch.plan_id = plan["id"]
        batch.status = "planned"
        batch.touch()
        return {"plan_id": plan["id"], "plan": plan, "reused": False}
    return handler


def stage_publishing(batch: batch_store.RewriteEstimateBatch):
    def handler(_upstream: dict[str, Any] | None) -> dict[str, Any]:
        if batch.conflicts and not batch.drift_acknowledged:
            batch.status = "version_drift"
            raise RuntimeError(
                "模型/规则版本在改写与估算之间发生变化，批次停在待确认状态")
        if batch.status != "version_drift":
            batch.status = "estimated"
        batch.touch()
        return {"batch_id": batch.batch_id, "status": batch.status,
                "conflicts": batch.conflicts}
    return handler


def _stage_specs(batch: batch_store.RewriteEstimateBatch):
    return [
        (STAGE_FREEZE, "semantic_model", stage_freezing(batch)),
        (STAGE_REWRITE, "query_rewrite", stage_rewriting(batch)),
        (STAGE_ESTIMATE, "query_federation", stage_estimating(batch)),
        (STAGE_FEDERATE, "query_federation", stage_federating(batch)),
        (STAGE_PUBLISH, "query_federation", stage_publishing(batch)),
    ]


STAGE_ORDER = [STAGE_FREEZE, STAGE_REWRITE, STAGE_ESTIMATE, STAGE_FEDERATE, STAGE_PUBLISH]


def parse_stop_before(stop_before: str | None) -> str | None:
    if stop_before is None:
        return None
    if stop_before not in STAGE_ORDER:
        raise ValueError(f"未知阶段：{stop_before}")
    return stop_before


def run(
    raw_query: str,
    model_ids: list[int] | None = None,
    sources: list[str] | None = None,
    query_type: str = "SELECT",
    *,
    stop_before: str | None = None,
    batch_id: str | None = None,
) -> dict[str, Any]:
    """跑一次完整批次；``stop_before`` 用于在任意两步之间中断。"""
    if batch_id:
        batch = batch_store.get(batch_id)
        if batch is None:
            raise ValueError(f"批次不存在：{batch_id}")
        created = False
    else:
        batch, created = batch_store.begin(raw_query, model_ids, sources, query_type)

    run_obj = engine.run_pipeline(
        PIPELINE_NAME,
        _stage_specs(batch),
        context={"batch_id": batch.batch_id},
        stop_before=parse_stop_before(stop_before),
        artifact_scope=batch.batch_id,
    )
    batch.last_run_id = run_obj.run_id
    return {
        "ok": run_obj.status in ("completed", "interrupted"),
        "batch": batch,
        "run": run_obj,
        "created": created,
    }


def acknowledge_drift(batch: batch_store.RewriteEstimateBatch, note: str = "") -> None:
    """确认按冻结的模型/规则版本完成本批次（而不是从新模型重跑）。

    确认后批次可以继续估算与公布，但对外公布的仍是被冻结的那一份规则和模型
    版本；新模型的数据不会被混进本批次。
    """
    batch.drift_acknowledged = True
    batch.conflicts = [
        {**conflict, "处置": "沿用冻结版本", "说明": note}
        for conflict in batch.conflicts
    ]
    batch.kept_model_signature = batch.model_signature
    batch.touch()


def latest_run(batch: batch_store.RewriteEstimateBatch) -> PipelineRun | None:
    run_id = batch.last_run_id
    return engine.get_run(run_id) if run_id else None


def resume_batch(
    batch_id: str,
    run_id: str | None = None,
    *,
    stop_before: str | None = None,
) -> dict[str, Any]:
    """从断点继续批次；不传 ``run_id`` 时续跑该批次最近一次运行。"""
    batch = batch_store.get(batch_id)
    if batch is None:
        raise ValueError(f"批次不存在：{batch_id}")
    run_id = run_id or batch.last_run_id
    if not run_id:
        raise ValueError(f"批次尚无运行记录：{batch_id}")
    run_obj = engine.resume_pipeline(run_id, stop_before=parse_stop_before(stop_before))
    if run_obj is None:
        raise ValueError(f"管道运行不存在：{run_id}")
    batch.last_run_id = run_obj.run_id
    return {"ok": run_obj.status in ("completed", "interrupted"), "batch": batch, "run": run_obj}


def publish(batch: batch_store.RewriteEstimateBatch) -> dict[str, Any]:
    """对外公布批次：只接受“一份完整规则 + 一份模型版本”的批次。"""
    if batch.rewrite is None or batch.estimate is None:
        return {"ok": False, "message": "批次尚未完成改写与估算，不能公布"}
    if batch.conflicts and not batch.drift_acknowledged:
        return {
            "ok": False,
            "message": "模型/规则版本在批次中途发生变化，需先确认沿用冻结版本或从新模型重跑",
            "conflicts": batch.conflicts,
        }

    trace_rules = (batch.rewrite or {}).get("rule_trace", [])
    used_rule_signatures = {entry.get("规则签名") for entry in trace_rules}
    used_rules = [entry.get("规则编号") for entry in trace_rules]
    known_rules = {rule.get("规则编号") for rule in batch.rules}
    if any(rules_no not in known_rules for rules_no in used_rules):
        return {"ok": False, "message": "改写使用了批次外的规则，批次不可公布"}

    published = {
        "批次编号": batch.batch_id,
        "查询语句": batch.raw_query,
        "改写文本": batch.rewrite.get("rewritten"),
        "规则": used_rules,
        "规则签名": versioning.digest(sorted(used_rule_signatures)),
        "模型版本": batch.model_signature,
        "模型": [
            {"模型编号": item.get("模型编号"), "版本序号": item.get("版本序号"),
             "指纹": item.get("指纹")}
            for item in batch.models
        ],
        "扫描预测": batch.prediction.get("estimated_scan_rows"),
        "节省行数": batch.estimate.get("settlement", {}).get("incremental_rows"),
        "结算基线": batch.estimate.get("settlement", {}).get("baseline_rows"),
        "结算累计": batch.estimate.get("settlement", {}).get("cumulative_savings"),
        "成本记录": batch.cost_entry_id,
        "联邦计划": batch.plan_id,
        "公布时间": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    batch.published = published
    batch.status = "published"
    batch.touch()

    if batch.plan_id is not None:
        plan = store.find("query_federation", batch.plan_id)
        if plan is not None:
            plan["对外公布"] = batch.batch_id
            plan["status"] = "执行中"
            plan["pending"] = True

    return {"ok": True, "message": "批次已公布，可进入执行", "published": published}


def plan_executable(batch: batch_store.RewriteEstimateBatch | None, plan_id: int | None = None) -> bool:
    """只有公布过、且估算与改写同一版本的联邦计划才能进入执行。"""
    if batch is None or batch.published is None:
        return False
    if plan_id is None:
        plan_id = batch.plan_id
    plan = store.find("query_federation", plan_id) if plan_id else None
    if plan is None:
        return False
    return (
        plan.get("批次编号") == batch.batch_id
        and plan.get("模型版本") == batch.model_signature
        and plan.get("对外公布") == batch.batch_id
    )


def batch_view(batch: batch_store.RewriteEstimateBatch) -> dict[str, Any]:
    return {
        "manifest": batch.manifest(),
        "model_version": batch.model_version,
        "conflicts": batch.conflicts,
        "drift_acknowledged": batch.drift_acknowledged,
        "executable": plan_executable(batch),
        "status": batch.status,
    }


__all__ = [
    "run", "resume_batch", "publish", "acknowledge_drift", "batch_view",
    "plan_executable",
]
