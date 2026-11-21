"""改写与代价估算批次的集成测试。

覆盖三条主线：
1. 中断 → 续跑：改写和估算落在同一个批次、同一模型版本上，重试不重复记命中、
   不重复叠加节省量、不重复写成本记录；
2. 版本漂移：改写完成后上线新模型，续跑的估算与公布会停在冲突状态，确认沿用
   冻结版本后才可公布；
3. 历史结算基线：旧布局的节省量被计入基线，新批次只记增量。
"""
import copy

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.pipeline import engine
from app.services import batch_store, rewrite_estimate
from app.services.query_rewrite import _HIT_LEDGER, predict_scan
from app.services.settlement import resolve_baseline, settle
from app.services.versioning import query_key
from app.store import store

client = TestClient(app)

QUERY = "SELECT * FROM source_registry JOIN dim_region"

# 会被批次写到的表：每个用例结束后整体还原，避免用例之间互相污染基线。
MUTABLE_MODULES = [
    "query_rewrite", "query_audit", "cost_govern", "result_cache",
    "semantic_model", "query_federation", "source_registry",
]


@pytest.fixture(autouse=True)
def reset_batch_state():
    """每个用例从干净的批次注册表和一份已生效规则开始。"""
    snapshot = {name: copy.deepcopy(store.rows(name)) for name in MUTABLE_MODULES}
    batch_store.clear()
    _HIT_LEDGER.clear()
    engine.runs.clear()
    engine._runs_by_id.clear()
    engine.artifacts.clear()

    rule = next(r for r in store.rows("query_rewrite") if r.get("规则编号") == "QUER-0001")
    rule["status"] = "已生效"
    rule["命中次数"] = "0"
    yield
    for name, rows in snapshot.items():
        store.rows(name)[:] = rows


def _cost_rows(batch_id):
    return [r for r in store.rows("cost_govern") if r.get("批次编号") == batch_id]


def _cache_rows(batch_id):
    return [r for r in store.rows("result_cache") if r.get("批次编号") == batch_id]


def _hit_count():
    rule = next(r for r in store.rows("query_rewrite") if r.get("规则编号") == "QUER-0001")
    return int(rule.get("命中次数", "0"))


def _deploy_model(model_id: int, version: str, fields: str = "id,new_field"):
    store.rows("semantic_model").append({
        "id": model_id,
        "模型编号": f"SEMA-{model_id:04d}",
        "模型名称": f"新上线模型_{version}",
        "源表映射": "source_registry:1",
        "字段别名": fields,
        "度量定义": "COUNT(*)",
        "维度定义": "region",
        "计算口径": "新口径",
        "版本序号": version,
        "status": "已投产",
        "pending": False,
        "abnormal": False,
    })


class TestInterruptedBatch:
    def test_stop_between_rewrite_and_estimate(self):
        outcome = rewrite_estimate.run(QUERY, None, ["source_registry"], stop_before="estimate")
        batch = outcome["batch"]
        run = outcome["run"]

        assert run.status == "interrupted"
        assert [s.stage for s in run.stages] == ["freeze_version", "rewrite"]
        assert batch.rewrite["rewritten"] != QUERY
        assert batch.estimate is None
        # 改写已完成：缓存带模型版本，成本还没写。
        cache = _cache_rows(batch.batch_id)
        assert len(cache) == 1
        assert cache[0]["模型版本"] == batch.model_signature
        assert _cost_rows(batch.batch_id) == []

    def test_resume_shares_one_version_and_is_idempotent(self):
        first = rewrite_estimate.run(QUERY, None, ["source_registry"], stop_before="estimate")
        batch = first["batch"]
        signature = batch.model_signature
        hits_after_rewrite = _hit_count()

        second = rewrite_estimate.resume_batch(batch.batch_id)
        assert second["run"].status == "completed"

        # 只保留一份成本记录、一条缓存，命中数没有二次累加。
        costs = _cost_rows(batch.batch_id)
        caches = _cache_rows(batch.batch_id)
        assert len(costs) == 1
        assert len(caches) == 1
        assert _hit_count() == hits_after_rewrite

        # 改写、规则、扫描预测、成本、缓存、联邦计划都指向同一模型版本。
        assert costs[0]["模型版本"] == signature
        assert caches[0]["模型版本"] == signature
        plan = store.find("query_federation", batch.plan_id)
        assert plan["模型版本"] == signature
        assert plan["批次编号"] == batch.batch_id
        assert plan["扫描预测"] == batch.prediction["estimated_scan_rows"]
        assert batch.rewrite["scan_prediction"]["model_signature"] == signature

    def test_publish_requires_estimate_and_marks_plan_executable(self):
        outcome = rewrite_estimate.run(QUERY, None, ["source_registry"], stop_before="estimate")
        batch = outcome["batch"]
        assert rewrite_estimate.publish(batch)["ok"] is False

        rewrite_estimate.resume_batch(batch.batch_id)
        decision = rewrite_estimate.publish(batch)
        assert decision["ok"] is True
        assert rewrite_estimate.plan_executable(batch) is True
        plan = store.find("query_federation", batch.plan_id)
        assert plan["status"] == "执行中"
        assert plan["对外公布"] == batch.batch_id

    def test_rerun_same_batch_does_not_stamp_again(self):
        first = rewrite_estimate.run(QUERY, None, ["source_registry"])
        batch = first["batch"]
        hits = _hit_count()

        rerun = rewrite_estimate.run(QUERY, None, ["source_registry"])
        assert rerun["created"] is False
        assert rerun["batch"].batch_id == batch.batch_id
        assert _hit_count() == hits
        assert len(_cost_rows(batch.batch_id)) == 1
        assert len(_cache_rows(batch.batch_id)) == 1

        # 同一次运行重写审计行，不会为新重试再追加一条。
        audits = [r for r in store.rows("query_audit")
                  if r.get("记录编号") == f"AUDIT-{rerun['run'].run_id}"]
        assert len(audits) == 1


class TestVersionDrift:
    def test_new_model_between_rewrite_and_estimate_blocks_publish(self):
        first = rewrite_estimate.run(QUERY, None, ["source_registry"], stop_before="estimate")
        batch = first["batch"]
        signature = batch.model_signature

        _deploy_model(999, "v2.0-field-added")

        resumed = rewrite_estimate.resume_batch(batch.batch_id)
        assert resumed["run"].status == "failed"
        assert batch.status == "version_drift"
        kinds = {conflict["type"] for conflict in batch.conflicts}
        assert "model_added" in kinds

        decision = rewrite_estimate.publish(batch)
        assert decision["ok"] is False
        assert rewrite_estimate.plan_executable(batch) is False

        # 估算仍然只引用冻结的模型版本，没有混入新模型。
        costs = _cost_rows(batch.batch_id)
        assert costs and costs[0]["模型版本"] == signature

    def test_acknowledge_keeps_frozen_version(self):
        first = rewrite_estimate.run(QUERY, None, ["source_registry"], stop_before="estimate")
        batch = first["batch"]
        signature = batch.model_signature
        _deploy_model(998, "v2.1-field-added")

        rewrite_estimate.resume_batch(batch.batch_id)
        rewrite_estimate.acknowledge_drift(batch, "沿用冻结版本")

        decision = rewrite_estimate.publish(batch)
        assert decision["ok"] is True
        assert decision["published"]["模型版本"] == signature
        model_versions = {item["版本序号"] for item in decision["published"]["模型"]}
        assert "v2.1-field-added" not in model_versions
        assert rewrite_estimate.plan_executable(batch) is True

    def test_rerun_on_new_model_creates_separate_batch(self):
        first = rewrite_estimate.run(QUERY, None, ["source_registry"])
        _deploy_model(997, "v2.2-field-added")
        second = rewrite_estimate.run(QUERY, None, ["source_registry"])

        assert second["batch"].batch_id != first["batch"].batch_id
        assert second["batch"].model_signature != first["batch"].model_signature


class TestSettlementBaseline:
    def test_legacy_record_is_compatible_and_not_double_counted(self):
        # 旧布局：适用角色 query_rewrite，节省量写在查询上限。
        store.rows("cost_govern").append({
            "id": 9000,
            "策略编号": "COST-LEGACY",
            "适用角色": "query_rewrite",
            "查询上限": "700",
            "并发上限": "1",
            "月度配额": "70000",
            "告警阈值": "1400",
            "限流动作": "optimize",
            "生效时段": "2026-11-01",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
            "查询摘要键": query_key(QUERY),
        })

        baseline = resolve_baseline(QUERY)
        assert baseline["baseline_savings"] == 700
        assert baseline["compatible_legacy"] is True

        outcome = rewrite_estimate.run(QUERY, None, ["source_registry"])
        batch = outcome["batch"]
        cost = _cost_rows(batch.batch_id)[0]
        estimated = batch.prediction["estimated_savings_rows"]

        assert cost["结算基线"] == 700
        assert cost["节省量"] == estimated
        assert cost["节省行数"] == max(0, estimated - 700)
        assert cost["结算累计"] == 700 + max(0, estimated - 700)

    def test_retry_does_not_add_savings_twice(self):
        outcome = rewrite_estimate.run(QUERY, None, ["source_registry"], stop_before="federate")
        batch = outcome["batch"]
        first_cost = _cost_rows(batch.batch_id)[0]
        first_settlement = batch.settlement["cumulative_savings"]

        # 估算已写入，后续阶段中断/重试两次，成本记录始终只有一条。
        rewrite_estimate.resume_batch(batch.batch_id, stop_before="federate")
        rewrite_estimate.resume_batch(batch.batch_id, stop_before="federate")
        costs = _cost_rows(batch.batch_id)
        assert len(costs) == 1
        assert costs[0]["id"] == first_cost["id"]
        assert costs[0]["结算累计"] == first_settlement

        completed = rewrite_estimate.resume_batch(batch.batch_id)
        assert completed["run"].status == "completed"
        costs = _cost_rows(batch.batch_id)
        assert len(costs) == 1
        assert costs[0]["结算累计"] == first_settlement

    def test_settle_only_counts_increment(self):
        result = settle({"baseline_savings": 500, "settled_savings": 500}, 900)
        assert result["incremental_rows"] == 400
        assert result["cumulative_savings"] == 900

        again = settle({"baseline_savings": 900, "settled_savings": 900}, 900)
        assert again["incremental_rows"] == 0
        assert again["cumulative_savings"] == 900

    def test_scan_prediction_is_deterministic(self):
        first = predict_scan(QUERY, "SELECT 1", [{"规则编号": "R1", "代价阈值": "1000"}])
        second = predict_scan(QUERY, "SELECT 1", [{"规则编号": "R1", "代价阈值": "1000"}])
        assert first == second
        assert first["estimated_savings_rows"] > 0


    def test_plan_cannot_execute_before_publish(self):
        from app.services.query_federation import QueryFederationService
        from app.services.source_registry import SourceRegistryService

        entry, _ = SourceRegistryService().register_source({
            "源编号": "SOUR-REB-001", "源类型": "mysql", "连接地址": "db://reb/demo"})
        outcome = rewrite_estimate.run(QUERY, None, [entry["源编号"]])
        batch = outcome["batch"]
        federation = QueryFederationService()

        blocked = federation.execute_federation_query(batch.plan_id)
        assert blocked["ok"] is False
        assert blocked["batch_id"] == batch.batch_id

        assert rewrite_estimate.publish(batch)["ok"] is True
        executed = federation.execute_federation_query(batch.plan_id)
        assert executed["ok"] is True
        assert executed["model_version"] == batch.model_signature

        caches = [c for c in store.rows("result_cache")
                  if c.get("批次编号") == batch.batch_id and c.get("条目编号", "").startswith("RESU-F")]
        assert caches and caches[0]["模型版本"] == batch.model_signature


class TestApiBatchFlow:
    def test_api_stop_deploy_model_resume(self):
        stopped = client.post(
            "/api/pipelines/rewrite-estimate-execute",
            params={"query": QUERY, "stop_before": "estimate"},
        )
        assert stopped.status_code == 200
        body = stopped.json()
        assert body["run_status"] == "interrupted"
        assert body["next_stage"] == "estimate"
        batch_id = body["batch_id"]
        run_id = body["run_id"]

        _deploy_model(996, "v3.0-api-field")

        resumed = client.post(
            "/api/pipelines/rewrite-estimate-execute",
            params={"query": QUERY, "batch_id": batch_id, "run_id": run_id},
        )
        assert resumed.status_code == 200
        data = resumed.json()
        assert data["run_status"] == "failed"
        assert data["conflicts"]
        assert data["executable"] is False
        assert data["manifest"]["模型版本"] == body["manifest"]["模型版本"]

        ack = client.post(
            f"/api/pipelines/rewrite-estimate-execute/{batch_id}/acknowledge-drift")
        assert ack.status_code == 200

        published = client.post(
            "/api/pipelines/rewrite-estimate-execute",
            params={"query": QUERY, "batch_id": batch_id},
        ).json()
        assert published["publish"]["ok"] is True
        assert published["executable"] is True

        detail = client.get(f"/api/pipelines/rewrite-estimate-execute/{batch_id}").json()
        assert detail["manifest"]["批次编号"] == batch_id
        assert detail["model_version"]["signature"] == body["manifest"]["模型版本"]
        assert detail["executable"] is True
