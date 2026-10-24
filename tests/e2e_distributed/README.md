# Distributed E2E suite

End-to-end tests for **AbstractPipeline** and **IdBasedPipeline** against a Reflowfy deployment in
**distributed mode** — manager, Kafka, a pool of real workers, Postgres. `tests/e2e` only covers local
mode (in-process dispatch, no worker), so the production path
(manager → Kafka → `KafkaJobConsumer` → worker → destination) was covered by unit tests alone.

Delivery is verified **from outside**: pipelines write to a small *sink* that keeps a ledger, and each
test asserts that every expected record arrived exactly once. "The execution completed" is never taken as
proof that the data arrived.

## What is here

| Path | What |
|---|---|
| `pipelines/dist_pipelines.py` | The pipelines under test. Self-contained: they generate their own records (no external data) |
| `sink/server.py` | Stdlib-only HTTP server: delivery ledger, failure injection, and a mock users API |
| `sink/deploy.sh`, `sink/k8s.yaml` | Deploy the sink next to Reflowfy (OpenShift / Kubernetes) |
| `sync_pipelines.sh` | Copy the pipelines into a project so the next image build includes them |
| `conftest.py`, `helpers.py` | Fixtures and clients; the environment variables are documented in `conftest.py` |
| `test_abstract_pipelines.py` | Delivery, job outcomes, retry-failed, dedup, rate limit, pause/resume, concurrency, API behaviour, Elasticsearch, chaos |
| `test_id_based_pipelines.py` | One job per ID, `ids_batch_size`, fan-out via `define_jobs`, odd IDs, the API source with injected failures, chaos |
| `test_destination_concurrency.py` | Per-record `api_destination` (`per_record=True`): one request per record with the id filled into the URL, `key_field`, the `concurrency` cap per job (counted by the sink), a failed record failing its job, retry-failed |
| `test_async_pipelines.py` | Async per-record transformations: delivery, the concurrency cap per job (counted by the sink), 404/5xx per record, retry-failed, a long async step that must not get the worker evicted, chaos |

Pipelines: `dist_abstract`, `dist_abstract_es`, `dist_async`, `dist_per_record`, `dist_ids_b1`, `dist_ids_b7`, `dist_ids_b100`,
`dist_ids_fanout`, `dist_ids_api`, `dist_ids_api_batch`.

## Running it

1. Build the pipelines into the images of a distributed deployment and deploy it:
   ```bash
   tests/e2e_distributed/sync_pipelines.sh /path/to/project      # copies dist_pipelines.py
   reflowfy build ... && reflowfy deploy ...                      # or your own build/deploy
   ```
2. Deploy the sink in the same namespace and reach it, and the manager, from the test runner:
   ```bash
   IMAGE=<any image with python, e.g. the worker image> tests/e2e_distributed/sink/deploy.sh
   oc port-forward svc/e2e-sink 8090:8080 &
   oc port-forward svc/<release>-reflow-manager 8001:8001 &
   ```
3. Run:
   ```bash
   export E2E_DIST_MANAGER_URL=http://localhost:8001 E2E_DIST_SINK_URL=http://localhost:8090
   uv run pytest tests/e2e_distributed -m "not chaos"                 # everything that is safe
   uv run pytest tests/e2e_distributed/test_abstract_pipelines.py &   # the two modules are independent:
   uv run pytest tests/e2e_distributed/test_id_based_pipelines.py     # run them side by side
   ```
   Without a reachable deployment the whole suite **skips**, saying why, so `pytest tests/` stays green.

**Elasticsearch tests** (`needs_es`): `export E2E_DIST_ES=1` with an index `docs` holding documents
`seq = 1..N` (N ≥ 20000) reachable from the workers at `E2E_DIST_ES_INTERNAL_URL`.

**Chaos tests** (`chaos`) kill pods, so run them on their own, not beside other tests. They need to be told how:
```bash
export E2E_DIST_KILL_WORKER_CMD='oc delete pod -l app.kubernetes.io/component=worker --grace-period=0 --force --wait=false | head -1'
export E2E_DIST_RESTART_MANAGER_CMD='oc delete pod -l app.kubernetes.io/component=reflow-manager --grace-period=0 --force'
uv run pytest tests/e2e_distributed -m chaos
```

## How the tests are written

- Each test gets its own **channel** (a unique `run` key), so tests never see each other's records and can
  run in parallel. Delivery goes to `<sink>/ingest/<channel>`; the sink's `GET /ids/<channel>` is the ledger.
- Failure is injected, not hoped for: the sink can answer 500 for the next N requests or lose acknowledgements,
  the mock API can fail chosen IDs (`status_by_id`, `fail_first`, `missing_ids`), and pipeline switches
  (`fail_jobs`, `skip_jobs`, `fail_while_flag`, `slow_s`, `plan_delay_s`) make jobs misbehave on demand.
  `fail_while_flag` is how a test makes a failure that it later "fixes" before `retry-failed`.
- Tests marked `characterization` **pin current behaviour that may deserve a second look**. They say so in
  their docstring; change them deliberately if the behaviour changes.
