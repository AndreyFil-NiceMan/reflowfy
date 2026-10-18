"""Executions of the same pipeline running at once must not share a plan.

Found on a cluster: 16 simultaneous `/run`s of one pipeline all got the wrong plan — most
of them zero jobs — and finished `completed` having processed nothing. The registry holds
one instance per pipeline; resolve() stored the plan (``_source``, ``_resolved_params``)
on that shared instance, so the last execution to resolve won.

The tests force the bad interleaving (both executions resolved before either reads its
plan) instead of hoping a race shows up.
"""

import threading
from typing import Any, Dict, List

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from reflowfy.core.abstract_pipeline import AbstractPipeline
from reflowfy.core.runtime_params import RuntimeParams
from reflowfy.execution.job_runner import chunk, job
from reflowfy.reflow_manager.execution import ExecutionManager
from reflowfy.reflow_manager.job_manager import JobManager
from reflowfy.reflow_manager.models import Base, Execution, Job
from reflowfy.reflow_manager.pipeline_runner import PipelineRunner


class _Counting(AbstractPipeline[RuntimeParams]):
    """``n`` records, one job each."""

    name = "concurrent_execution_test"

    def define_jobs(self, runtime_params: Any):
        for i, part in enumerate(chunk(list(range(runtime_params["n"])), 1)):
            yield job(part, job_index=i)

    def define_transformations(self, records: Any, runtime_params: Any):
        return []

    def define_destination(self, records: Any, runtime_params: Any):
        raise AssertionError("not reached")


def test_fork_gives_each_execution_its_own_plan():
    registered = _Counting()
    a, b = registered.fork(), registered.fork()

    a.resolve({"n": 3})
    b.resolve({"n": 7})

    assert [len(list(p.source)) for p in (a, b)] == [3, 7]
    assert a._resolved_params["n"] == 3 and b._resolved_params["n"] == 7
    assert getattr(registered, "_source", None) is None  # the shared instance is never written


@pytest.mark.parametrize("executions", [2, 8])
def test_simultaneous_executions_each_get_their_own_jobs(tmp_path, monkeypatch, executions):
    engine = create_engine(f"sqlite:///{tmp_path}/c.db", connect_args={"timeout": 60})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as db:
        for k in range(1, executions + 1):
            db.add(Execution(execution_id=f"e{k}", pipeline_name=_Counting.name, state="pending",
                             plan_complete=False))
        db.commit()

    # Hold every execution right after its resolve(), so all of them have resolved before
    # any reads its plan back: the interleaving that used to hand them the wrong one.
    barrier = threading.Barrier(executions)
    real_resolve = AbstractPipeline.resolve

    def resolve_then_wait(self: Any, runtime_params: Any) -> Any:
        out = real_resolve(self, runtime_params)
        barrier.wait(timeout=30)
        return out

    monkeypatch.setattr(AbstractPipeline, "resolve", resolve_then_wait)
    monkeypatch.setattr(PipelineRunner, "_drive_execution", lambda *a, **k: (0, 0, 0))
    errors: List[BaseException] = []

    def run(k: int) -> None:
        try:
            with Session() as db:
                runner = PipelineRunner(ExecutionManager(db), JobManager(db), dispatcher=None)  # type: ignore[arg-type]
                runner.run_pipeline_jobs(f"e{k}", _Counting.name, {"n": k})
        except BaseException as exc:  # surfaced below, not lost in the thread
            errors.append(exc)

    threads = [threading.Thread(target=run, args=(k,)) for k in range(1, executions + 1)]
    [t.start() for t in threads]
    [t.join(timeout=60) for t in threads]

    assert not errors, errors
    with Session() as db:
        planned: Dict[int, int] = {
            k: db.query(Job).filter(Job.execution_id == f"e{k}").count()
            for k in range(1, executions + 1)
        }
    assert planned == {k: k for k in range(1, executions + 1)}, f"jobs planned per execution: {planned}"
