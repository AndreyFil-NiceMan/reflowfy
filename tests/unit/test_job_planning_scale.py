"""Planning-path guards: bulk job inserts, and no full `ids` list per job."""

import math
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from reflowfy.core.abstract_pipeline import AbstractPipeline
from reflowfy.core.id_based_pipeline import IdBasedPipeline
from reflowfy.core.registry import pipeline_registry
from reflowfy.reflow_manager.job_manager import JobManager
from reflowfy.reflow_manager.models import Base, Execution, Job
from reflowfy.reflow_manager.pipeline_runner import (
    CHECKPOINT_BATCH_SIZE,
    CHECKPOINT_BATCH_TIMEOUT,
    PipelineRunner,
    _batch_timeout,
)


@pytest.fixture
def session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    db = sessionmaker(bind=engine)()
    db.add(Execution(execution_id="e1", pipeline_name="p", state="running"))
    db.commit()
    return db


def test_create_jobs_inserts_every_row_with_defaults(session: Session) -> None:
    manager = JobManager(session)

    manager.create_jobs(
        [
            {
                "execution_id": "e1",
                "job_id": f"j{i}",
                "job_payload": {"n": i},
                "batch_number": 1,
            }
            for i in range(3)
        ]
    )

    jobs = session.query(Job).order_by(Job.job_id).all()
    assert [j.job_id for j in jobs] == ["j0", "j1", "j2"]
    # state + timestamps are filled in by create_jobs, not by the ORM.
    assert {j.state for j in jobs} == {"pending"}
    assert all(j.created_at is not None and j.updated_at is not None for j in jobs)


def test_create_jobs_is_a_noop_on_empty(session: Session) -> None:
    JobManager(session).create_jobs([])
    assert session.query(Job).count() == 0


class _TinyIdPipeline(IdBasedPipeline):
    """One job per ID, so planning 60 IDs plans 60 jobs."""

    name = "tiny_id_sync_for_planning_test"

    def define_source(self, runtime_params):
        return [{"id": runtime_params["current_ids"][0]}]

    def define_transformations(self, records, runtime_params):
        return []

    def define_destination(self, records, runtime_params):
        return None


def _plan(ids, pipeline_name=_TinyIdPipeline.name, **params):
    """Run Phase 1 of the planner; return the rows it would insert."""
    runner = PipelineRunner(
        execution_manager=MagicMock(), job_manager=MagicMock(), dispatcher=MagicMock()
    )
    assert pipeline_registry.get(pipeline_name) is not None, "pipeline failed to auto-register"

    with patch.object(PipelineRunner, "_dispatch_and_wait_batches", return_value=(0, 0, 0)):
        runner.run_pipeline_jobs(
            execution_id="e1",
            pipeline_name=pipeline_name,
            runtime_params={"ids": list(ids), "env": "prod", **params},
        )

    calls = runner.job_manager.create_jobs.call_args_list
    rows = [row for call in calls for row in call.args[0]]
    return rows, len(calls)


def test_planned_jobs_do_not_carry_the_full_ids_list():
    """A job payload must carry its own IDs, never every ID in the execution.

    Without this, planned bytes grow with (ids x jobs) through both the
    job_payload column and the Kafka message — that is what fails first at
    large ID counts, well before the manager runs out of memory.
    """
    rows, _ = _plan(range(60))

    assert len(rows) == 60
    for row in rows:
        params = row["job_payload"]["metadata"]["runtime_params"]
        assert "ids" not in params
        assert params["env"] == "prod"          # other params still travel
        assert len(row["job_payload"]["metadata"]["current_ids"]) == 1


def test_planning_inserts_in_bulk_not_one_job_at_a_time():
    """60 jobs must not cost 60 inserts — one flush per checkpoint batch."""
    _, insert_calls = _plan(range(60))

    expected = math.ceil(60 / CHECKPOINT_BATCH_SIZE) + 1  # +1 = final partial flush
    assert insert_calls <= expected


class _BatchedIdPipeline(IdBasedPipeline):
    """Two IDs per job, to pin ids_batch_size through the unified planner."""

    name = "batched_id_sync_for_planning_test"
    ids_batch_size = 2

    def define_source(self, runtime_params):
        return [{"id": i} for i in runtime_params["current_ids"]]

    def define_transformations(self, records, runtime_params):
        return []

    def define_destination(self, records, runtime_params):
        return None


class _ExpandingIdPipeline(IdBasedPipeline):
    """One input ID fans out into three jobs — the case the old path couldn't do."""

    name = "expanding_id_sync_for_planning_test"

    def define_jobs(self, runtime_params):
        for parent_id in runtime_params["ids"]:
            for child in range(3):
                yield [{"parent_id": parent_id, "child_id": f"{parent_id}-{child}"}]

    # No define_source at all — define_jobs owns the planning, and the base
    # class's default raises if anything ever calls it.

    def define_transformations(self, records, runtime_params):
        return []

    def define_destination(self, records, runtime_params):
        return None


class TestIdBasedOnUnifiedPlanner:
    """IdBasedPipeline plans through AbstractPipeline's define_jobs now."""

    def test_ids_batch_size_still_groups_ids_per_job(self):
        rows, _ = _plan(range(6), pipeline_name=_BatchedIdPipeline.name)

        assert len(rows) == 3
        current_ids = [row["job_payload"]["metadata"]["current_ids"] for row in rows]
        assert current_ids == [[0, 1], [2, 3], [4, 5]]

    def test_define_jobs_can_expand_one_id_into_many_jobs(self):
        rows, _ = _plan([7, 8], pipeline_name=_ExpandingIdPipeline.name)

        assert len(rows) == 6
        children = [row["job_payload"]["source"]["config"]["records"][0]["child_id"] for row in rows]
        assert children == ["7-0", "7-1", "7-2", "8-0", "8-1", "8-2"]

    def test_define_source_is_optional_when_define_jobs_is_overridden(self):
        """_ExpandingIdPipeline defines no define_source. It must still register:
        an abstract define_source would fail instantiation, and the metaclass
        swallows that — the pipeline would just silently vanish."""
        pipeline = pipeline_registry.get(_ExpandingIdPipeline.name)

        assert pipeline is not None
        with pytest.raises(NotImplementedError, match="define_source|define_jobs"):
            pipeline.define_source({})

    def test_overridden_define_jobs_still_drops_the_full_ids_list(self):
        """An override sets no job_params — the payload must still not carry every ID."""
        rows, _ = _plan([7, 8], pipeline_name=_ExpandingIdPipeline.name)

        for row in rows:
            assert "ids" not in row["job_payload"]["metadata"]["runtime_params"]

    def test_ids_parameter_is_still_injected_and_validated(self):
        pipeline = pipeline_registry.get(_BatchedIdPipeline.name)

        assert "ids" in {p.name for p in pipeline.get_all_parameters()}
        assert pipeline.validate_parameters({"ids": []}) == ["Parameter 'ids' must not be empty"]
        assert "Parameter 'ids' must be a list" in pipeline.validate_parameters({"ids": "1,2"})
        assert pipeline.validate_parameters({}) == ["Missing required parameter: ids"]


class _JobParamsIdPipeline(IdBasedPipeline):
    """One ID fans out into two jobs, each tagged with the ID it belongs to."""

    name = "job_params_id_sync_for_planning_test"

    def define_jobs(self, runtime_params):
        from reflowfy import job

        for parent_id in runtime_params["ids"]:
            for child in range(2):
                yield job(
                    [{"child_id": f"{parent_id}-{child}"}],
                    current_id=parent_id,
                    current_ids=[parent_id],
                )

    def define_transformations(self, records, runtime_params):
        return []

    def define_destination(self, records, runtime_params):
        return None


class TestPerJobParamsFromCustomPlan:
    """A custom plan can give each job its own runtime_params via reflowfy.job()."""

    def test_each_job_carries_the_id_it_belongs_to(self):
        rows, _ = _plan([101, 102], pipeline_name=_JobParamsIdPipeline.name)

        assert len(rows) == 4
        params = [row["job_payload"]["metadata"]["runtime_params"] for row in rows]
        assert [p["current_id"] for p in params] == [101, 101, 102, 102]
        # current_ids also lands in metadata, where the worker reads it from.
        assert [row["job_payload"]["metadata"]["current_ids"] for row in rows] == [
            [101],
            [101],
            [102],
            [102],
        ]

    def test_payload_carries_the_jobs_own_params_for_dedup(self):
        """Content dedup needs what the plan attached, not the merged view:
        the merged one also holds execution params, identical across jobs."""
        rows, _ = _plan([101, 102], pipeline_name=_JobParamsIdPipeline.name)

        own = [row["job_payload"]["job_params"] for row in rows]
        assert own == [
            {"current_id": 101, "current_ids": [101]},
            {"current_id": 101, "current_ids": [101]},
            {"current_id": 102, "current_ids": [102]},
            {"current_id": 102, "current_ids": [102]},
        ]
        assert all("env" not in p for p in own)

    def test_per_job_params_are_merged_not_substituted(self):
        """job() attaches only what identifies the job; the rest must still travel."""
        rows, _ = _plan([101], pipeline_name=_JobParamsIdPipeline.name)

        for row in rows:
            params = row["job_payload"]["metadata"]["runtime_params"]
            assert params["env"] == "prod"   # execution param survives
            assert "ids" not in params       # full ID list still stripped


class _PlainPipeline(AbstractPipeline):
    """No job_params — the abstract path must keep every param it has today."""

    name = "plain_for_planning_test"

    def define_jobs(self, runtime_params):
        return [[{"n": 1}], [{"n": 2}]]

    def define_transformations(self, records, runtime_params):
        return []

    def define_destination(self, records, runtime_params):
        return None


def test_abstract_path_still_carries_all_params():
    rows, _ = _plan([1, 2, 3], pipeline_name=_PlainPipeline.name)

    assert len(rows) == 2
    for row in rows:
        params = row["job_payload"]["metadata"]["runtime_params"]
        assert params["ids"] == [1, 2, 3]        # untouched: no job_params set
        assert params["env"] == "prod"
        assert "current_ids" not in row["job_payload"]["metadata"]


class TestBatchTimeout:
    """The per-batch deadline must outlast the time the rate limiter needs."""

    def test_scales_with_batch_size_and_rate(self):
        # 5000 jobs at 1000/min take 300s just to dispatch — the old fixed
        # 300s deadline would expire while the batch was still healthy.
        assert _batch_timeout(5000, 1000) == pytest.approx(900.0)

    def test_never_below_the_floor(self):
        assert _batch_timeout(10, 1000) == float(CHECKPOINT_BATCH_TIMEOUT)

    def test_unlimited_rate_uses_the_floor(self):
        assert _batch_timeout(1_000_000, None) == float(CHECKPOINT_BATCH_TIMEOUT)
        assert _batch_timeout(1_000_000, 0) == float(CHECKPOINT_BATCH_TIMEOUT)
