from reflowfy.worker.job_stats import JobStats


def test_jobstats_defaults_not_deduplicated():
    s = JobStats()
    assert s.deduplicated is False
