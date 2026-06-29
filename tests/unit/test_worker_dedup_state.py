from reflowfy.worker.executor import JobStats


def test_jobstats_defaults_not_deduplicated():
    s = JobStats()
    assert s.deduplicated is False
