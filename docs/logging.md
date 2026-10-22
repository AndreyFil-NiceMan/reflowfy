# Logging map

Every log line is one ECS-shaped JSON object (`LOG_JSON=false` gives text with the same context appended). Configured by `reflowfy/observability/logging.py`.

## How a line gets its fields

- **Always**: `@timestamp` (the record's own time), `log.level`, `logger`, `log.origin.*`, `message`, `service.name`/`.version`/`.environment`, `host.name`, `process.pid`.
- **Bound context** (`log_context(...)` / `@bind_log_context(...)` in `observability/context.py`): `execution_id`, `job_id`, `pipeline_name`, `current_id`, `current_ids_count`, trace ids. Bound once per job (worker) or per execution (manager), so messages don't repeat them.
- **`extra={...}`**: any key is emitted as-is (nothing is dropped). `event` becomes `event.action`.
- **Errors**: `error.type`, `error.message`, `error.stack_trace` when `exc_info` is set.

## Field dictionary

| Field | Meaning |
|---|---|
| `event.action` | Stable snake_case name of what happened; filter on this, not the message. Low cardinality. |
| `execution_id`, `job_id`, `pipeline_name`, `schedule_name`, `dlq_job_id` | Identity |
| `state` | Execution/job state at that moment |
| `jobs_total/_completed/_failed/_dispatched/_pending/_in_flight/_cancelled/_due` | Job counts |
| `records_in`, `records_out` | Record counts |
| `duration_ms`, `destination_write_ms` | Timings |
| `skip_reason`, `reason` | Why a job was dropped / skipped |
| `error_type` | Exception class name |
| `kafka_partition`, `kafka_offset` | Message position (worker) |
| `rate_limit`, `retry_in_s`, `retry_count` | Throttling / retry |

## Rules

- Never log record contents, runtime-param values or credentials (counts only: `param_count`).
- A new terminal state or silent short-circuit needs an `event`, a `JobStats` field and a metrics label (see CLAUDE.md, Observability).
- Threads do not inherit `log_context`: pass `execution_id` in `extra=` there (see `_PlanLease`).

## Events by module

- `api/app.py`: `no_pipelines`, `routes_ready`, `routes_setup`, `service_listen`, `service_start`
- `api/routes.py`: `route_registered`, `run_failed`, `run_finished`, `run_requested`
- `core/abstract_pipeline.py`: `pipeline_register_failed`
- `core/pipeline_discovery.py`: `component_load_failed`, `component_loaded`, `discovery_done`, `discovery_empty`, `discovery_start`
- `core/registry.py`: `pipeline_registered`
- `destinations/decorators.py`: `destination_registered`
- `destinations/kafka.py`: `kafka_lag_check_error`, `kafka_lag_high`, `producer_setting_ignored`
- `execution/distributed_executor.py`: `execution_submit`, `execution_submit_error`, `execution_submit_rejected`, `execution_submitted`
- `execution/job_runner.py`: `skipjob_raised`
- `execution/local_executor.py`: `destination_send`, `execution_empty`, `execution_finished`, `execution_start`, `id_done`, `id_empty`, `id_processing`, `id_transform_done`, `job_dropped`, `source_fetch`, `source_fetched`, `transform_done`
- `reflow_manager/app.py`: `db_init`, `db_ready`, `dispatch_failed`, `driver_leases_released`, `execution_mark_failed_error`, `recovery_check`, `recovery_found`, `recovery_none`, `recovery_resume_scheduled`, `recovery_scheduled`, `resume_failed`, `run_accepted`, `run_failed`, `run_undeclared_params`, `scheduler_init_failed`, `scheduler_ready`, `service_config`, `service_listen`, `service_start`, `service_stop`, `service_stopped`, `stats_error`
- `reflow_manager/content_dedup_scheduler.py`: `dedup_hashes_purged`, `executions_purged`, `sweeper_error`, `sweeper_start`
- `reflow_manager/database.py`: `db_connect_retry`, `migration_skipped`
- `reflow_manager/dispatcher.py`: `job_unsendable`, `kafka_send_error`, `producer_reset`, `rate_limit_wait`
- `reflow_manager/dlq_scheduler.py`: `dlq_archived`, `dlq_dispatch_error`, `dlq_dispatched`, `dlq_due`, `dlq_job_due`, `dlq_poll_error`, `dlq_process`, `dlq_rescheduled`, `scheduler_already_running`, `scheduler_error`, `scheduler_start`, `scheduler_stopped`, `scheduler_stopping`
- `reflow_manager/local_dispatcher.py`: `local_dispatch_error`, `rate_limit_timeout`, `rate_limit_wait`
- `reflow_manager/manager.py`: `manager_init`
- `reflow_manager/pipeline_runner.py`: `backfill_batches_error`, `dispatch_db_error`, `dispatch_skipped`, `dispatch_stalled`, `dispatch_stopped`, `dispatcher_close_error`, `driver_claim_db_error`, `driver_release_error`, `driver_replaced`, `driver_standby`, `execution_drive_end`, `execution_finished`, `execution_mark_failed_error`, `execution_resume`, `execution_start`, `jobs_missing`, `lease_expired`, `orphaned_jobs_requeued`, `plan_abandoned`, `plan_aborted`, `plan_complete`, `plan_heartbeat_error`, `plan_lease_error`, `plan_save`, `plan_split`, `plan_start`, `plan_waiting`, `resume_nothing_to_do`, `resume_pipeline_missing`, `resume_skipped`, `sync_counts_error`
- `reflow_manager/pipeline_scheduler.py`: `schedule_disabled`, `schedule_dispatch_error`, `schedule_dispatched`, `schedule_due`, `schedule_registered`, `schedule_trigger`, `schedule_trigger_error`, `schedule_updated`, `scheduler_already_running`, `scheduler_error`, `scheduler_poll_error`, `scheduler_start`, `scheduler_stopped`, `scheduler_stopping`
- `sources/decorators.py`: `source_registered`
- `sources/elastic.py`: `pit_close_error`, `source_aggs_ignored`, `source_option_ignored`, `source_page`, `source_planned`
- `transformations/registry.py`: `transformation_registered`
- `worker/consumer.py`: `commit_failed_rebalance`, `consumer_connected`, `consumer_start_failed`, `consumer_start_retry`, `job_failed_recorded`, `job_received`, `message_bad_schema`, `message_empty`, `message_error`, `message_invalid`
- `worker/executor.py`: `db_update_error`, `destination_close_error`, `destination_send`, `destination_unhealthy`, `heartbeat_error`, `job_cancelled`, `job_claim_error`, `job_completed`, `job_deduplicated`, `job_dropped`, `job_empty`, `job_failed`, `job_processing`, `job_redelivered`, `job_row_missing`, `job_status_saved`, `transform_applied`
- `worker/main.py`: `service_config`, `service_start`, `service_stop`, `shutdown_drain`, `shutdown_forced`, `worker_crashed`, `worker_ready`
