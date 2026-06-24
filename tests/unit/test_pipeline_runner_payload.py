from reflowfy.reflow_manager.pipeline_runner import build_job_payload, generate_job_id
from reflowfy.sources.static import StaticSource


def test_job_id_stable_for_same_slice():
    src = {"type": "StaticSource", "config": {"records": [1, 2]}}
    a = generate_job_id("p", source=src, current_ids=[1, 2])
    b = generate_job_id("p", source=src, current_ids=[1, 2])
    assert a == b


def test_job_id_differs_for_different_slice():
    a = generate_job_id("p", source={"type": "S", "config": {"lo": 0}}, current_ids=None)
    b = generate_job_id("p", source={"type": "S", "config": {"lo": 1}}, current_ids=None)
    assert a != b


def test_build_job_payload_v2_shape():
    sub = StaticSource([101, 102])
    payload = build_job_payload(
        execution_id="e1",
        job_id="j1",
        pipeline_name="user_sync",
        sub_source=sub,
        metadata={
            "batch_id": "b1",
            "created_at": "t",
            "batch_number": 1,
            "total_batches": 1,
            "retry_count": 0,
            "is_retry": False,
            "runtime_params": {"env": "prod"},
            "current_ids": [101, 102],
            "source_metadata": None,
        },
    )
    assert payload["schema_version"] == 2
    assert payload["source"] == {"type": "StaticSource", "config": {"records": [101, 102]}}
    assert "records" not in payload
    assert "transformations" not in payload
    assert "destination" not in payload
    assert payload["metadata"]["current_ids"] == [101, 102]
