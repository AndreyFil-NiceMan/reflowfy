"""Prometheus metrics collection."""

from prometheus_client import Counter, Histogram, Gauge

# Job metrics
jobs_processed_total = Counter(
    "reflowfy_jobs_processed_total",
    "Total number of jobs processed",
    ["pipeline", "status"],
)

jobs_failed_total = Counter(
    "reflowfy_jobs_failed_total",
    "Total number of jobs failed",
    ["pipeline", "error_type"],
)

job_processing_duration_seconds = Histogram(
    "reflowfy_job_processing_duration_seconds",
    "Job processing duration in seconds",
    ["pipeline"],
    # Buckets extended well past the default 10s cap so p95/p99 stay accurate
    # for longer-running jobs.
    buckets=(0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600),
)

# Records metrics
records_processed_total = Counter(
    "reflowfy_records_processed_total",
    "Total number of records processed",
    ["pipeline"],
)

# Worker metrics
active_workers = Gauge(
    "reflowfy_active_workers",
    "Number of active workers",
)

# Pipeline execution metrics
pipeline_executions_total = Counter(
    "reflowfy_pipeline_executions_total",
    "Total number of pipeline executions",
    ["pipeline", "mode"],
)

# Observability self-metrics
logs_dropped_total = Counter(
    "reflowfy_logs_dropped_total",
    "Log records dropped because the Elastic ship queue was full",
)

dlq_depth = Gauge(
    "reflowfy_dlq_depth",
    "Number of jobs currently in the dead-letter queue",
)
