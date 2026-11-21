"""跨模块数据管道引擎：编排多模块协作的端到端数据流。

每个管道是一个有序处理阶段列表，每个阶段消费上游产物并产生下游输入。
管道执行过程写入审计记录，失败时保留中间产物供排查；也可以在指定阶段
前停下（``stop_before``），之后用 ``resume_pipeline`` 从断点继续。
"""
from __future__ import annotations

import hashlib
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable

from app.store import store

StageHandler = Callable[[dict[str, Any] | None], dict[str, Any]]
StageSpec = tuple[str, str, StageHandler]


@dataclass
class StageResult:
    stage: str
    module: str
    ok: bool
    output: dict[str, Any] | None = None
    error: str | None = None
    duration_ms: float = 0.0
    artifact: str | None = None
    attempt: int = 1


@dataclass
class PipelineRun:
    run_id: str
    pipeline_name: str
    stages: list[StageResult] = field(default_factory=list)
    started_at: float = field(default_factory=time.time)
    finished_at: float | None = None
    status: str = "running"
    data: dict[str, Any] | None = None
    stage_specs: list[StageSpec] = field(default_factory=list, repr=False)
    next_stage_index: int = 0
    artifact_scope: str = ""
    context: dict[str, Any] | None = None
    audit_id: int | None = None

    def add_stage(self, result: StageResult) -> None:
        self.stages.append(result)
        if not result.ok:
            self.status = "failed"

    def finish(self) -> None:
        self.finished_at = time.time()
        if self.status == "running":
            self.status = "completed"

    def completed_stages(self) -> list[str]:
        return [stage.stage for stage in self.stages if stage.ok]


class PipelineEngine:
    """管道执行器，维护执行历史和中间产物。"""

    def __init__(self) -> None:
        self.runs: list[PipelineRun] = []
        self.artifacts: dict[str, dict[str, Any]] = {}
        self._runs_by_id: dict[str, PipelineRun] = {}

    def run_pipeline(
        self,
        name: str,
        stages: list[StageSpec],
        context: dict[str, Any] | None = None,
        *,
        stop_before: str | None = None,
        artifact_scope: str = "",
        run_id: str | None = None,
    ) -> PipelineRun:
        """执行管道；``stop_before`` 命中的阶段不会执行，管道停在断点。"""
        run = PipelineRun(
            run_id=run_id or str(uuid.uuid4())[:8],
            pipeline_name=name,
            stage_specs=list(stages),
            artifact_scope=artifact_scope,
            context=context,
        )
        self._runs_by_id[run.run_id] = run
        self._execute(run, 0, context, stop_before)
        return run

    def resume_pipeline(self, run_id: str, *, stop_before: str | None = None) -> PipelineRun | None:
        """从断点继续执行；已经成功的阶段不会重跑。"""
        run = self._runs_by_id.get(run_id)
        if run is None:
            return None
        if run.status not in ("interrupted", "failed"):
            return run
        run.status = "running"
        run.finished_at = None
        self._execute(run, run.next_stage_index, run.data, stop_before)
        return run

    def _execute(
        self,
        run: PipelineRun,
        start_index: int,
        data: dict[str, Any] | None,
        stop_before: str | None,
    ) -> None:
        for index in range(start_index, len(run.stage_specs)):
            stage_name, module, handler = run.stage_specs[index]
            if stop_before and stage_name == stop_before:
                run.next_stage_index = index
                run.status = "interrupted"
                run.finished_at = time.time()
                self._remember(run)
                self._write_audit(run)
                return

            t0 = time.time()
            try:
                output = handler(data)
                result = StageResult(
                    stage=stage_name,
                    module=module,
                    ok=True,
                    output=output,
                    duration_ms=(time.time() - t0) * 1000,
                    artifact=self._artifact_key(run, stage_name),
                )
                self.artifacts[result.artifact] = output
                data = output
            except Exception as exc:  # noqa: BLE001 - 管道需要把失败落成审计
                result = StageResult(
                    stage=stage_name,
                    module=module,
                    ok=False,
                    error=str(exc),
                    duration_ms=(time.time() - t0) * 1000,
                )
                run.add_stage(result)
                run.data = data
                run.next_stage_index = index
                run.status = "failed"
                run.finish()
                self._remember(run)
                self._write_audit(run)
                return
            run.add_stage(result)
            run.data = output
            run.next_stage_index = index + 1

        run.finish()
        self._remember(run)
        self._write_audit(run)

    def _remember(self, run: PipelineRun) -> None:
        if run not in self.runs:
            self.runs.append(run)

    def get_run(self, run_id: str) -> PipelineRun | None:
        return self._runs_by_id.get(run_id)

    def get_artifact(self, pipeline: str, stage: str, scope: str = "") -> dict[str, Any] | None:
        return self.artifacts.get(_artifact_key_values(pipeline, stage, scope))

    def _artifact_key(self, run: PipelineRun, stage: str) -> str:
        return _artifact_key_values(run.pipeline_name, stage, run.artifact_scope)

    def _write_audit(self, run: PipelineRun) -> None:
        rows = store.rows("query_audit")
        audit_entry = {
            "记录编号": f"AUDIT-{run.run_id}",
            "查询语句": f"pipeline:{run.pipeline_name}",
            "执行耗时": f"{sum(s.duration_ms for s in run.stages):.0f}ms",
            "扫描行数": str(len(run.stages)),
            "返回行数": str(1 if run.status == "completed" else 0),
            "数据来源": "跨模块管道",
            "操作用户": "pipeline_engine",
            "查询时间": time.strftime("%Y-%m-%d %H:%M:%S"),
            "status": "正常" if run.status == "completed" else "慢查询",
            "pending": run.status == "interrupted",
            "abnormal": run.status == "failed",
            "管道状态": run.status,
            "断点阶段": run.next_stage_index,
            "批次编号": run.artifact_scope or None,
        }
        # 续跑时更新同一次运行的审计行，而不是再追加一条。
        if run.audit_id is not None:
            existing = store.find("query_audit", run.audit_id)
            if existing is not None:
                existing.update(audit_entry)
                return
        audit_entry["id"] = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        rows.append(audit_entry)
        run.audit_id = audit_entry["id"]


def _artifact_key_values(pipeline: str, stage: str, scope: str) -> str:
    digest = hashlib.md5(f"{pipeline}::{stage}::{scope}".encode()).hexdigest()[:8]
    suffix = f"{scope}/" if scope else ""
    return f"{pipeline}/{suffix}{stage}/{digest}"


# 全局单例
engine = PipelineEngine()
