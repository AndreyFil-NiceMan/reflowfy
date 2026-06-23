from reflowfy.reflow_manager.pipeline_runner import generate_job_id


def test_job_id_stable_for_same_slice():
    src = {"type": "StaticSource", "config": {"records": [1, 2]}}
    a = generate_job_id("p", source=src, current_ids=[1, 2])
    b = generate_job_id("p", source=src, current_ids=[1, 2])
    assert a == b


def test_job_id_differs_for_different_slice():
    a = generate_job_id("p", source={"type": "S", "config": {"lo": 0}}, current_ids=None)
    b = generate_job_id("p", source={"type": "S", "config": {"lo": 1}}, current_ids=None)
    assert a != b
