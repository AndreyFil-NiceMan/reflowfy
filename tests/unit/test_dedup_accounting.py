from reflowfy.reflow_manager.pipeline_runner import _finished_count


def test_finished_includes_deduplicated():
    assert _finished_count(completed=2, failed=0, deduplicated=3) == 5
    assert _finished_count(completed=0, failed=1, deduplicated=0) == 1
