from reflowfy.reflow_manager.models import ProcessedContent


def test_processed_content_table_shape():
    cols = {c.name for c in ProcessedContent.__table__.columns}
    assert {"content_hash", "pipeline_name", "job_id", "execution_id", "created_at"} <= cols
    assert ProcessedContent.__table__.primary_key.columns.keys() == ["content_hash"]
