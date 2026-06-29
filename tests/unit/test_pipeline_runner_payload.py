from reflowfy.reflow_manager.pipeline_runner import build_job_payload
from reflowfy.sources.static import StaticSource


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
    assert payload["dedup_check"] is False


def test_build_job_payload_sets_dedup_check():
    from reflowfy.sources.static import StaticSource

    payload = build_job_payload(
        execution_id="e1",
        job_id="j1",
        pipeline_name="p",
        sub_source=StaticSource([1]),
        metadata={},
        dedup_check=True,
    )
    assert payload["dedup_check"] is True
