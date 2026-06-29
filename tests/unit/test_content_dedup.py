from reflowfy.reflow_manager.models import ProcessedContent


def test_processed_content_table_shape():
    cols = {c.name for c in ProcessedContent.__table__.columns}
    assert {"content_hash", "pipeline_name", "job_id", "execution_id", "created_at"} <= cols
    assert ProcessedContent.__table__.primary_key.columns.keys() == ["content_hash"]


from reflowfy.execution.content_dedup import compute_content_hash


def test_hash_is_stable_for_same_content():
    a = compute_content_hash("p", ["t1", "t2"], [{"id": 1, "v": "x"}])
    b = compute_content_hash("p", ["t2", "t1"], [{"id": 1, "v": "x"}])  # order-insensitive transforms
    assert a == b
    assert len(a) == 64


def test_hash_changes_with_records():
    a = compute_content_hash("p", [], [{"id": 1, "v": "x"}])
    b = compute_content_hash("p", [], [{"id": 1, "v": "y"}])
    assert a != b


def test_hash_changes_with_pipeline_name():
    a = compute_content_hash("p1", [], [{"id": 1}])
    b = compute_content_hash("p2", [], [{"id": 1}])
    assert a != b
