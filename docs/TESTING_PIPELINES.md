# Testing your pipelines

`reflowfy.testing` runs a pipeline inside a test, through the same per-job code
the worker runs, with nothing real on either end: no Docker, Kafka, Postgres or
network.

```bash
pip install "reflowfy[test]"     # pytest + pytest-asyncio; the plugin ships with reflowfy
pytest
```

`reflowfy init` creates `tests/` with a working test for the sample pipeline, and
`reflowfy new pipeline <name>` writes `tests/test_<name>.py` beside the new pipeline.

There are two ways to test, and they work well together:

| | You write | Good for |
|---|---|---|
| [Hand-written tests](#hand-written-tests) | input records and assertions | specific cases and edge cases |
| [Recorded fixtures](#recorded-fixtures) | nothing: one CLI command | catching any change in behaviour on real data |

## Hand-written tests

### A whole pipeline

```python
from reflowfy.testing import run_pipeline

def test_orders_are_taxed():
    result = run_pipeline("orders", params={"region": "eu"},
                          records=[{"id": 1, "amount": 10.0}])

    assert result.ok
    assert result.sent == [{"data": {"id": 1, "amount": 10.0}, "tax": 2.0}]
```

What happens:

- **The pipeline** is looked up by name (discovery runs if it is not registered
  yet), or pass the class or an instance. The run uses a private copy, so the
  registered instance is never touched.
- **Params** get their defaults and are validated as `/run` validates them.
  Invalid ones raise `ValueError`.
- **`records=`** replaces the source: `define_source` is not called. Each record
  is wrapped as `{"data": record}`, the shape built-in sources return. Pass
  `wrap=False` to use them as they are.
- **Transformations** run for real: sync, async and per-record, with
  `SkipRecord`, `SkipJob`, `record_type` and `validate_steps` behaving as in a job.
- **`define_destination`** is called, so a broken one fails the test, but the
  records go to an in-memory capture and not to the destination it returned.
- **HTTP is blocked.** A request no mock answers raises `UnmockedCallError`
  naming the step and the URL. See [steps that call out](#steps-that-call-out).

Other inputs, one at a time:

```python
run_pipeline("orders")                                    # the pipeline's own source and job plan
run_pipeline("orders", jobs=[[{"id": 1}], [{"id": 2}]])   # two jobs
run_pipeline("orders", jobs=[job([{"data": 1}], tenant="a")])  # per-job params
run_pipeline("orders", source=my_source)                  # a source of your choice
```

Other options: `send=False` (stop after `define_destination`), `limit=N`,
`destination="real"` (send to what the pipeline defined), `destination=<instance>`,
`allow_network=True`.

### The result

| | |
|---|---|
| `result.ok`, `result.error` | whether it failed, and with what. A failing pipeline does not raise; `result.raise_for_error()` does |
| `result.sent` | every record sent, across jobs |
| `result.records` | every transformed record, sent or not |
| `result.fetched` | how many records the jobs fetched |
| `result.skip_reasons`, `result.records_skipped` | `SkipRecord` reasons (and `invalid …` from `record_type`) with counts |
| `result.jobs_dropped` | the reason of each job dropped with `SkipJob` |
| `result.steps` | one `AppliedStep` per step: name, records in/out, mode, duration |
| `result.destinations` | what `define_destination` returned per job, to assert its class and config |
| `result.jobs` | one `JobResult` per job, with the same details |

Assertions that explain themselves when they fail:

```python
from reflowfy.testing import assert_ok, assert_sent, assert_skipped, assert_job_dropped

assert_ok(result)                                      # fails with the pipeline's error
assert_sent(result, expected, ordered=False, ignore=["processed_at"])
assert_skipped(result, reason="customer not found", count=2)
assert_job_dropped(result, reason="nothing to do")
```

### One transformation

```python
from reflowfy.testing import apply_transformation, make_records

def test_add_tax():
    out, skipped = apply_transformation(add_tax, make_records([{"amount": 10.0}]),
                                        params={"region": "eu"})
    assert out[0]["tax"] == 2.0
```

Pass an instance, or the class a `@transformation` / `@record_transformation`
decorator returns. A failure raises `TransformationError`, as in a job, with the
cause on `.original_error`. `make_params("orders", region="us")` builds the
`runtime_params` a job of that pipeline would see.

### Steps that call out

With pytest, use the `mock_http` and `mock_source` fixtures:

```python
def test_enrich(mock_http):
    mock_http.get("https://crm.internal/customers/7", json={"tier": "gold"})
    mock_http.get("https://crm.internal/customers/8", status=404)

    result = run_pipeline("orders", records=[{"customer_id": 7}, {"customer_id": 8}])

    assert result.sent[0]["tier"] == "gold"
    assert mock_http.calls[0].step == "enrich_customer"

def test_invoices(mock_source):
    mock_source(ElasticSource, records=[{"invoice_id": "A1"}])   # a source a step fetches itself
    ...
```

- A URL is matched exactly, query string included, or by a compiled regular
  expression. `get`, `post`, `put`, `patch`, `delete` or `add(method, url, ...)`,
  answering with `json=`, `text=` or `body=`, plus `status=` and `headers=`.
- `mock_source(SourceClass, records=[...], where=lambda config: ...)` answers any
  `fetch()` a step makes on that kind of source, without building its client.
- Outside pytest: `with mock_network() as net:` gives the same two as `net.http`
  and `net.sources`.

**What is covered.** HTTP sent with `httpx` (sync and async), `requests`,
`urllib3`, the Elasticsearch client and `boto3`, and `fetch()` on any reflowfy
source. **Not covered:** `aiohttp`, raw sockets and database drivers used directly;
those still reach the network.

## Recorded fixtures

Run the pipeline once against the real source and save the run:

```bash
reflowfy test orders -p region=eu --limit 50 \
    --save-fixture tests/fixtures/orders_eu.json \
    --redact customer.email
```

Nothing is sent to the destination. The file now holds the input, what every
outside call answered, and the expected output, so it is a test:

```bash
pytest                                      # tests/fixtures/orders_eu.json::test_fixture[orders_eu]
reflowfy test orders --fixture tests/fixtures/orders_eu.json     # the same check, from the CLI
```

A replay is offline. The source is never built, a request that is not in the file
fails the test, and nothing is sent. It keeps passing after the source has dropped
the data.

When the pipeline's output changes, the test fails with what changed:

```
pipeline 'orders' no longer matches its fixture (tests/fixtures/orders_eu.json):
  job 0, record 3: tax: 2.0 → 2.2
```

If the change is intended, recompute the expected output from the saved input
(no real system involved) and review the file's diff:

```bash
reflowfy test orders --fixture tests/fixtures/orders_eu.json --update-fixture
```

### Flags

| | |
|---|---|
| `--save-fixture FILE` | record this run. Uses `--limit`, `-p` and the other `reflowfy test` options |
| `--fixture FILE` | replay offline and compare; exits 1 on a difference |
| `--update-fixture` | with `--fixture`: rewrite the expected output |
| `--redact a,b.c` | mask fields in the file (see below) |
| `--ignore a,b` | leave fields out of the comparison |
| `--freeze-step NAME` | save a step's output and replay it instead of running the step |
| `--max-body-kb N` | largest HTTP response stored (default 256) |

**`--redact`** replaces a field's value with `"<redacted>"` in the input records,
in recorded JSON responses and source records, and therefore in the expected
output. A field is a key (`email`, masked wherever it appears) or a dotted path
(`customer.email`). The file still contains real data in every other field:
review it before committing.

**`--ignore`** is for a field that differs on every run, such as a timestamp a
step stamps. Recording checks for this: if replaying the new fixture twice gives
different output, it fails and names the fields to ignore. The run's own
`execution_id`, `batch_id` and `created_at` are saved in the fixture and reused on
replay, so stamping those into a record needs no `--ignore`.

**`--freeze-step`** is for a step whose calls cannot be recorded (`aiohttp`, a
database driver, gRPC) or that is random. Its output is saved per job and returned
on replay; the step's own logic is then not exercised, the rest of the pipeline is.

### In a test of your own

```python
from reflowfy.testing import load_fixture, run_fixture, run_pipeline

def test_orders_eu():
    run_fixture("tests/fixtures/orders_eu.json")             # replay + compare

def test_something_else_on_the_same_data():
    result = run_fixture("tests/fixtures/orders_eu.json", check=False)
    assert all(r["tax"] > 0 for r in result.sent)
```

Fixture files in other directories:

```toml
[tool.pytest.ini_options]
reflowfy_fixtures = ["tests/fixtures", "tests/golden"]
```

### The file

```jsonc
{
  "reflowfy_fixture": 1,              // format version
  "pipeline": "users",
  "reflowfy_version": "1.1.2",
  "recorded_at": "2026-11-12T10:15:00+00:00",

  "params": {},                       // the user's params, defaults applied
  "context": {"execution_id": "…", "batch_id": "…", "created_at": "…"},

  // INPUT: the records each job fetched, before any transformation
  "jobs": [[{"data": {"id": 1, "ip": "1.2.3.4"}}]],
  "job_params": [{}],                 // params the plan attached per job (job(...), current_ids)

  // OUTSIDE WORLD: what the steps' calls answered
  "http": [{
    "request":  {"method": "GET", "url": "https://geo.internal/ip/1.2.3.4", "body_sha256": null},
    "response": {"status": 200, "headers": {"content-type": "application/json"},
                 "body": {"country": "IL"}}
  }],
  "source_calls": [{"step": "attach_invoices",
                    "source": {"type": "ElasticSource", "config_hash": "9f2c…"},
                    "records": [{"data": {"invoice_id": "A1"}}]}],
  "step_outputs": {},                 // frozen step -> its output per job
  "frozen_steps": [],

  // EXPECTED: what a replay is compared with (the only part --update-fixture rewrites)
  "expected": {
    "sent": [[{"data": {"id": 1, "ip": "1.2.3.4"}, "country": "IL"}]],   // per job
    "records_skipped": 0,
    "skip_reasons": {},
    "jobs_dropped": [],
    "destinations": [{"type": "KafkaDestination", "config": {"topic": "users"}}]   // per job
  },

  "ignore": [],
  "redacted": []
}
```

- A request is matched by method, URL and a hash of its body, not by order, so a
  per-record step with `concurrency` replays correctly. The same request made
  several times gets its answers in the order they were recorded.
- Request headers are never stored. Response headers are, without `Set-Cookie`.
  A destination is stored as its class and config without credentials.
- A response body is stored as JSON where it is JSON, as text where it is text,
  and base64 otherwise (`"body_encoding"`).
- A source call is matched by the source's class and a hash of its config
  (credentials left out), so a lookup whose query depends on the record has an
  entry of its own.
- The file can be edited by hand: add a record to `jobs` (and its answers under
  `http`), then run `--update-fixture` to fill in the expected output.

### Limits

- A step that builds a request from something that changes on every run (a
  random id, the current time) or from a redacted field cannot be replayed.
  Recording fails and says so; freeze the step.
- A pipeline that fails while recording, or fetches nothing, is not saved.
- A fixture for a pipeline that does one lookup per record stores one answer per
  record. Keep `--limit` small.
- Dedup (`enable_duplicate_jobs = False`), rate limits, retries of failed jobs and
  everything else the manager and the database do are not part of a test run. For
  those, see the E2E suites (`docs/E2E_TESTING.md`).
