"""reflowfy.testing.fixtures: recording a run, replaying it offline, updating it,
and the `reflowfy test --save-fixture / --fixture / --update-fixture` flags."""

import json
import random
import sys
from typing import Any, Dict, Iterator, List

import httpx
import pytest
from typer.testing import CliRunner

from reflowfy import (
    AbstractPipeline,
    BaseTransformation,
    IdBasedPipeline,
    RecordTransformation,
    RuntimeParams,
    SkipJob,
    SkipRecord,
    kafka_destination,
    transformation,
)
from reflowfy.cli.commands import test as test_cmd
from reflowfy.cli.main import app
from reflowfy.cli.utils import console
from reflowfy.sources.mock import MockSource
from reflowfy.testing import (
    FixtureError,
    PipelineFixture,
    UnmockedCallError,
    assert_matches_fixture,
    diff_fixture,
    load_fixture,
    record_fixture,
    run_fixture,
    update_fixture,
)
from reflowfy.testing.diff import REDACTED, diff_values, redact, summarize, without_fields
from reflowfy.testing.fixtures import replay
from testing_support import LocalServer, isolated_registry, local_server

runner = CliRunner()


@pytest.fixture(autouse=True)
def registry() -> Iterator[None]:
    with isolated_registry():
        yield


@pytest.fixture
def server() -> Iterator[LocalServer]:
    with local_server() as running:
        yield running


class Lookup(MockSource):
    pass


RATE = {"value": 0.2}
USERS = [{"id": 1, "email": "one@x.io"}, {"id": 2, "email": "two@x.io"}, {"id": 3}]


def define_users(server: LocalServer, name: str = "fx_users") -> Any:
    """A pipeline whose steps call an HTTP API and fetch from a source of their own."""

    # Subclasses named after the fact: a class-body `name` registers the step, and
    # this helper runs once per test.
    class Geo(RecordTransformation):
        concurrency = 4

        async def apply_record(self, rec: Any, params: Dict[str, Any]) -> Any:
            async with httpx.AsyncClient() as client:
                path = "missing" if rec["data"]["id"] == 3 else f"json/{rec['data']['id']}"
                response = await client.get(f"{server.url}/{path}")
            if response.status_code == 404:
                raise SkipRecord("no geo")
            rec["geo"] = response.json()
            return rec

    class Invoices(BaseTransformation):
        def apply(self, records: List[Any], params: Dict[str, Any]) -> List[Any]:
            for rec in records:
                rec["invoices"] = Lookup(
                    data=[{"for": rec["data"]["id"], "email": "inv@x.io"}]
                ).fetch(params)
                rec["tax"] = RATE["value"]
            return records

    Geo.name, Invoices.name = f"{name}_geo", f"{name}_invoices"
    geo, invoices = Geo, Invoices

    class Users(AbstractPipeline[RuntimeParams]):
        def define_source(self, runtime_params):
            return MockSource(data=list(USERS), batch_size=2)

        def define_transformations(self, records, runtime_params):
            return [geo(), invoices()]

        def define_destination(self, records, runtime_params):
            return kafka_destination(topic="users", bootstrap_servers="kafka:9092")

    Users.name = name
    return type(name, (Users,), {"name": name})


class TestRecordAndReplay:
    def test_a_recorded_run_replays_offline(self, server, tmp_path):
        define_users(server)
        fixture = record_fixture("fx_users")
        path = fixture.save(tmp_path / "nested" / "users.json")
        asked = len(server.hits)
        server.close()

        result = run_fixture(path)

        assert len(server.hits) == asked
        assert result.ok and len(result.sent) == 2 and result.skip_reasons == {"no geo": 1}
        stored = json.loads(path.read_text())
        assert list(stored)[:5] == [
            "reflowfy_fixture", "pipeline", "reflowfy_version", "recorded_at", "params",
        ]
        assert stored["jobs"] == [
            [{"data": USERS[0]}, {"data": USERS[1]}],
            [{"data": USERS[2]}],
        ]
        assert len(stored["http"]) == 3 and len(stored["source_calls"]) == 2
        assert stored["source_calls"][0]["step"] == "fx_users_invoices"
        expected = stored["expected"]
        assert [len(sent) for sent in expected["sent"]] == [2, 0]
        assert expected["sent"][0][0]["geo"]["path"] == "/json/1"
        assert expected["records_skipped"] == 1 and expected["jobs_dropped"] == []
        assert expected["destinations"][0]["type"] == "KafkaDestination"
        assert expected["destinations"][0]["config"]["topic"] == "users"
        assert expected["destinations"][1]["type"] == "KafkaDestination"

    def test_redaction_reaches_input_answers_and_expectation(self, server):
        define_users(server)
        fixture = record_fixture("fx_users", redact_fields=["email"])
        text = json.dumps(fixture.to_dict())
        assert "x.io" not in text and "a@b.c" not in text
        assert fixture.jobs[0][0]["data"]["email"] == REDACTED
        assert fixture.expected["sent"][0][0]["geo"]["email"] == REDACTED
        assert fixture.expected["sent"][0][0]["invoices"][0]["data"]["email"] == REDACTED
        assert fixture.redacted == ["email"]
        assert diff_fixture(replay(fixture), fixture) == []

    def test_a_changed_pipeline_is_reported_field_by_field(self, server, tmp_path, monkeypatch):
        define_users(server)
        path = record_fixture("fx_users").save(tmp_path / "users.json")
        monkeypatch.setitem(RATE, "value", 0.25)

        with pytest.raises(AssertionError) as excinfo:
            run_fixture(path)
        message = str(excinfo.value)
        assert "job 0, record 0: tax: 0.2 → 0.25" in message
        assert "job 0, record 1: tax: 0.2 → 0.25" in message
        assert f"--fixture {path} --update-fixture" in message
        assert run_fixture(path, check=False).ok

        updated, changes = update_fixture(path)
        assert len(changes) == 2 and updated.expected["sent"][0][0]["tax"] == 0.25
        run_fixture(path)
        assert update_fixture(load_fixture(path))[1] == []

    def test_ignored_fields_are_left_out_of_the_comparison(self, server):
        define_users(server)
        fixture = record_fixture("fx_users", ignore=["tax"])
        RATE["value"] = 0.9
        try:
            assert_matches_fixture(replay(fixture), fixture)
            fixture.ignore = []
            assert len(diff_fixture(replay(fixture), fixture)) == 2
        finally:
            RATE["value"] = 0.2

    def test_a_request_the_fixture_does_not_hold_fails_the_replay(self, server):
        define_users(server)
        fixture = record_fixture("fx_users")
        fixture.http = fixture.http[1:]
        with pytest.raises(UnmockedCallError, match="step 'fx_users_geo' made GET"):
            replay(fixture)
        fixture.source_calls = []
        fixture.http = record_fixture("fx_users").http
        with pytest.raises(UnmockedCallError, match="a fetch on Lookup"):
            replay(fixture)

    def test_structural_differences(self, server):
        define_users(server)
        fixture = record_fixture("fx_users")
        result = replay(fixture)
        fixture.expected["sent"] = [fixture.expected["sent"][0][:1]]
        fixture.expected["jobs_dropped"] = ["gone"]
        lines = diff_fixture(result, fixture)
        assert "jobs: 1 → 2" in lines and "job 0: 1 record(s) sent → 2" in lines
        assert any(line.startswith("jobs_dropped") for line in lines)

    def test_a_failing_pipeline_is_a_difference_and_cannot_be_updated(self, server, tmp_path):
        define_users(server)
        path = record_fixture("fx_users").save(tmp_path / "users.json")
        fixture = load_fixture(path)
        fixture.jobs[0][0] = {"data": {}}
        assert diff_fixture(replay(fixture), fixture)[0].startswith("the pipeline failed:")
        with pytest.raises(FixtureError, match="fails on this fixture"):
            update_fixture(fixture)


class TestWhatCannotBeRecorded:
    def test_a_frozen_step_is_replayed_from_its_saved_output(self, server):
        @transformation("fx_random")
        def random_step(records, params):
            return [{**r, "token": random.random()} for r in records]

        class Random(AbstractPipeline[RuntimeParams]):
            name = "fx_random_pipeline"

            def define_source(self, runtime_params):
                return MockSource(data=[{"id": 1}, {"id": 2}], batch_size=1)

            def define_transformations(self, records, runtime_params):
                return [random_step()]

            def define_destination(self, records, runtime_params):
                return kafka_destination(topic="t", bootstrap_servers="kafka:9092")

        fixture = record_fixture("fx_random_pipeline", freeze_steps=["fx_random"])
        assert fixture.frozen_steps == ["fx_random"]
        tokens = [out[0]["token"] for out in fixture.step_outputs["fx_random"]]
        assert [sent[0]["token"] for sent in fixture.expected["sent"]] == tokens
        assert diff_fixture(replay(fixture), fixture) == []

        # Unfrozen, the output differs on every run: recording says so, and how to fix it.
        with pytest.raises(FixtureError) as excinfo:
            record_fixture("fx_random_pipeline")
        assert "changes from one run to the next" in str(excinfo.value)
        assert "--ignore token" in str(excinfo.value) and "--freeze-step" in str(excinfo.value)

        unfrozen = record_fixture("fx_random_pipeline", ignore=["token"])
        assert diff_fixture(replay(unfrozen), unfrozen) == []
        with pytest.raises(FixtureError, match="holds no output for step\\(s\\) fx_random"):
            replay(unfrozen, freeze_steps=["fx_random"])
        assert diff_fixture(replay(fixture, freeze_steps=["fx_random"]), fixture) == []

        fixture.step_outputs["fx_random"] = fixture.step_outputs["fx_random"][:1]
        with pytest.raises(FixtureError, match="frozen but the fixture holds no output"):
            replay(fixture)

    def test_freezing_a_step_that_never_ran(self, server):
        define_users(server)
        with pytest.raises(FixtureError, match="--freeze-step typo: no step of that name ran"):
            record_fixture("fx_users", freeze_steps=["typo"])

    def test_a_request_built_from_something_that_changes_cannot_be_replayed(self, server):
        @transformation("fx_nonce")
        def nonce(records, params):
            httpx.get(f"{server.url}/json/{random.random()}")
            return records

        class Nonce(AbstractPipeline[RuntimeParams]):
            name = "fx_nonce_pipeline"

            def define_source(self, runtime_params):
                return MockSource(data=[{"id": 1}])

            def define_transformations(self, records, runtime_params):
                return [nonce()]

            def define_destination(self, records, runtime_params):
                return kafka_destination(topic="t", bootstrap_servers="kafka:9092")

        with pytest.raises(FixtureError, match="does not replay.*--freeze-step"):
            record_fixture("fx_nonce_pipeline")

    def test_a_pipeline_that_fails_fetches_nothing_or_gets_bad_params(self, server):
        class Broken(AbstractPipeline[RuntimeParams]):
            name = "fx_broken"
            data: List[Any] = [{"id": 1}]

            def define_source(self, runtime_params):
                return MockSource(data=self.data)

            def define_transformations(self, records, runtime_params):
                raise RuntimeError("boom")

            def define_destination(self, records, runtime_params):
                return kafka_destination(topic="t", bootstrap_servers="kafka:9092")

        with pytest.raises(FixtureError, match="failed while recording.*boom"):
            record_fixture("fx_broken")
        Broken.data = []
        with pytest.raises(FixtureError, match="fetched no records"):
            record_fixture("fx_broken")

        class Ids(IdBasedPipeline[RuntimeParams]):
            name = "fx_needs_ids"

            def define_source(self, runtime_params):
                return []

            def define_transformations(self, records, runtime_params):
                return []

            def define_destination(self, records, runtime_params):
                return kafka_destination(topic="t", bootstrap_servers="kafka:9092")

        with pytest.raises(ValueError, match="input_ids"):
            record_fixture("fx_needs_ids")

    def test_a_pipeline_that_only_fails_on_replay(self, server):
        calls = {"n": 0}

        @transformation("fx_second_time")
        def second_time(records, params):
            calls["n"] += 1
            if calls["n"] > 1:
                raise RuntimeError("not twice")
            return records

        class Once(AbstractPipeline[RuntimeParams]):
            name = "fx_once"

            def define_source(self, runtime_params):
                return MockSource(data=[{"id": 1}])

            def define_transformations(self, records, runtime_params):
                return [second_time()]

            def define_destination(self, records, runtime_params):
                return kafka_destination(topic="t", bootstrap_servers="kafka:9092")

        with pytest.raises(FixtureError, match="fails when the fixture is replayed.*not twice"):
            record_fixture("fx_once")


class TestRedactingAnswers:
    def test_only_json_bodies_are_masked(self, server):
        @transformation("fx_text_call")
        def text_call(records, params):
            return [{**r, "words": httpx.get(f"{server.url}/text").text} for r in records]

        class Text(AbstractPipeline[RuntimeParams]):
            name = "fx_text"

            def define_source(self, runtime_params):
                return MockSource(data=[{"id": 1, "email": "a@x.io"}])

            def define_transformations(self, records, runtime_params):
                return [text_call()]

            def define_destination(self, records, runtime_params):
                return kafka_destination(topic="t", bootstrap_servers="kafka:9092")

        fixture = record_fixture("fx_text", redact_fields=["email"])
        assert fixture.http[0]["response"]["body"] == "plain words"
        assert fixture.expected["sent"][0][0] == {
            "data": {"id": 1, "email": REDACTED}, "words": "plain words",
        }


class TestRunContext:
    def test_a_stamped_execution_id_is_the_same_on_every_replay(self):
        @transformation("fx_stamp_context")
        def stamp_context(records, params):
            return [{**r, "run": params["execution_id"], "at": params["created_at"]} for r in records]

        class Stamps(AbstractPipeline[RuntimeParams]):
            name = "fx_stamps"

            def define_source(self, runtime_params):
                return MockSource(data=[{"id": 1}])

            def define_transformations(self, records, runtime_params):
                return [stamp_context()]

            def define_destination(self, records, runtime_params):
                return kafka_destination(topic="t", bootstrap_servers="kafka:9092")

        fixture = record_fixture("fx_stamps")
        assert set(fixture.context) == {"execution_id", "batch_id", "created_at"}
        sent = fixture.expected["sent"][0][0]
        assert sent["run"] == fixture.context["execution_id"]
        assert sent["at"] == fixture.context["created_at"]
        assert replay(fixture).params["execution_id"] == fixture.context["execution_id"]
        assert diff_fixture(replay(fixture), fixture) == []


class TestIdBasedAndDroppedJobs:
    def test_per_job_params_and_a_dropped_job_survive_the_round_trip(self):
        @transformation("fx_stamp")
        def stamp(records, params):
            if params["current_id"] == 2:
                raise SkipJob("id 2 is closed")
            return [{**r, "for": params["current_id"], "mode": params["mode"]} for r in records]

        class ById(IdBasedPipeline[RuntimeParams]):
            name = "fx_by_id"

            def define_source(self, runtime_params):
                return [{"id": i} for i in runtime_params["current_ids"]]

            def define_transformations(self, records, runtime_params):
                return [stamp()]

            def define_destination(self, records, runtime_params):
                return kafka_destination(topic="t", bootstrap_servers="kafka:9092")

        fixture = record_fixture("fx_by_id", {"input_ids": [1, 2, 3], "mode": "full"}, limit=3)
        assert fixture.params == {"input_ids": [1, 2, 3], "mode": "full"}
        assert fixture.job_params == [
            {"current_ids": [1], "current_id": 1},
            {"current_ids": [2], "current_id": 2},
            {"current_ids": [3], "current_id": 3},
        ]
        assert fixture.jobs[1] == [{"id": 2}]
        assert fixture.expected["jobs_dropped"] == ["id 2 is closed"]
        assert fixture.expected["sent"] == [
            [{"id": 1, "for": 1, "mode": "full"}], [], [{"id": 3, "for": 3, "mode": "full"}],
        ]
        assert fixture.expected["destinations"][1] is None
        assert diff_fixture(replay(fixture), fixture) == []


class TestFiles:
    def test_unreadable_files(self, tmp_path):
        with pytest.raises(FixtureError, match="cannot read fixture"):
            load_fixture(tmp_path / "absent.json")
        bad = tmp_path / "bad.json"
        bad.write_text("{nope")
        with pytest.raises(FixtureError, match="not valid JSON"):
            load_fixture(bad)
        bad.write_text('{"some": "json"}')
        with pytest.raises(FixtureError, match="not a reflowfy fixture"):
            load_fixture(bad)
        bad.write_text('{"reflowfy_fixture": 99, "pipeline": "p"}')
        with pytest.raises(FixtureError, match="fixture format 99"):
            load_fixture(bad)

    def test_a_minimal_file_and_saving(self, tmp_path):
        path = tmp_path / "min.json"
        path.write_text('{"reflowfy_fixture": 1, "pipeline": "p", "jobs": [[1], [2]]}')
        loaded = load_fixture(path)
        assert loaded.job_params == [{}, {}] and loaded.path == path
        loaded.params = {"a": 1}
        assert load_fixture(loaded.save()).params == {"a": 1}
        with pytest.raises(FixtureError, match="no path yet"):
            PipelineFixture(pipeline="p").save()
        unsaved, _ = update_fixture_without_path()
        assert unsaved.path is None


def update_fixture_without_path() -> Any:
    class Tiny(AbstractPipeline[RuntimeParams]):
        name = "fx_tiny"

        def define_source(self, runtime_params):
            return []

        def define_transformations(self, records, runtime_params):
            return []

        def define_destination(self, records, runtime_params):
            return kafka_destination(topic="t", bootstrap_servers="kafka:9092")

    return update_fixture(PipelineFixture(pipeline="fx_tiny", jobs=[[{"id": 1}]], job_params=[{}]))


class TestDiffHelpers:
    def test_redact_and_without_fields(self):
        value = {
            "email": "a",
            "customer": {"email": "b", "name": "n"},
            "items": [{"price": 1, "meta": {"email": "c"}}],
        }
        assert redact(value, ["customer.email"]) == {**value, "customer": {"email": REDACTED, "name": "n"}}
        masked = redact(value, ["email"])
        assert masked["email"] == REDACTED and masked["items"][0]["meta"]["email"] == REDACTED
        assert redact(value, ["items.price"])["items"][0]["price"] == REDACTED
        assert redact(value, []) is value and redact(value, ["."]) is value
        assert without_fields(value, ["email", "items"]) == {"customer": {"name": "n"}}
        assert without_fields(value, []) is value
        assert value["email"] == "a"

    def test_diff_values(self):
        assert diff_values({"a": 1, "b": [1, 2]}, {"a": 1.0, "b": [1, 2]}) == []
        assert diff_values({"a": 1}, {"a": 2}) == ["a: 1 → 2"]
        assert diff_values({"a": {"b": 1}}, {"a": {}}) == ["a.b: missing (expected 1)"]
        assert diff_values({}, {"c": "x"}) == ["c: unexpected 'x'"]
        assert diff_values([1], [1, 2]) == ["list: 1 item(s) → 2"]
        assert diff_values({"l": [{"x": 1}]}, {"l": [{"x": 2}]}) == ["l[0].x: 1 → 2"]
        assert diff_values(1, "1") == ["value: 1 → '1'"]

    def test_summarize(self):
        assert summarize(["a", "b"]) == "  a\n  b"
        assert summarize([str(n) for n in range(25)], limit=3).endswith("… 22 more")


# ---------------------------------------------------------------------------
# `reflowfy test` flags
# ---------------------------------------------------------------------------


@pytest.fixture
def cli(monkeypatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "path", list(sys.path))
    monkeypatch.setattr(test_cmd, "setup_logging", lambda service_name="cli": None)
    monkeypatch.setattr(
        "reflowfy.core.pipeline_discovery.discover_and_load_pipelines", lambda *a, **k: None
    )
    monkeypatch.setattr(console, "_width", 250)
    monkeypatch.setattr(console, "quiet", False)


def invoke(*args: str) -> Any:
    return runner.invoke(app, ["test", *args])


@pytest.mark.usefixtures("cli")
class TestCli:
    def test_save_then_replay_then_update(self, server, tmp_path, monkeypatch):
        define_users(server)
        path = tmp_path / "fixtures" / "users.json"

        saved = invoke("fx_users", "--no-input", "--save-fixture", str(path), "--redact", "email,geo.method")
        assert saved.exit_code == 0, saved.output
        assert "Fetched 3 records in 2 job(s)" in saved.output
        assert "Recorded 3 HTTP call(s)" in saved.output and "Recorded 2 source call(s)" in saved.output
        assert "2 would be sent, 1 skipped ('no geo' x1)" in saved.output
        assert "contains real data" in saved.output
        assert load_fixture(path).redacted == ["email", "geo.method"]

        asked = len(server.hits)
        matched = invoke("fx_users", "--fixture", str(path))
        assert matched.exit_code == 0 and "Matches" in matched.output
        assert len(server.hits) == asked

        monkeypatch.setitem(RATE, "value", 0.5)
        differs = invoke("fx_users", "--fixture", str(path))
        assert differs.exit_code == 1 and "2 difference(s)" in differs.output
        assert "job 0, record 0: tax: 0.2 → 0.5" in differs.output

        updated = invoke("fx_users", "--fixture", str(path), "--update-fixture")
        assert updated.exit_code == 0 and "Expected output updated (2 change(s))" in updated.output
        again = invoke("fx_users", "--fixture", str(path), "--update-fixture")
        assert "Nothing changed" in again.output
        assert invoke("fx_users", "--fixture", str(path)).exit_code == 0

    def test_a_pipeline_with_no_calls_and_no_skips(self, tmp_path):
        class Quiet(AbstractPipeline[RuntimeParams]):
            name = "fx_quiet"

            def define_source(self, runtime_params):
                return MockSource(data=[{"id": 1}])

            def define_transformations(self, records, runtime_params):
                return []

            def define_destination(self, records, runtime_params):
                return kafka_destination(topic="t", bootstrap_servers="kafka:9092")

        saved = invoke("fx_quiet", "--no-input", "--save-fixture", str(tmp_path / "quiet.json"))
        assert saved.exit_code == 0, saved.output
        assert "1 would be sent" in saved.output
        assert "HTTP call" not in saved.output and "skipped" not in saved.output

    def test_freeze_step_is_reported(self, server, tmp_path):
        define_users(server)
        path = tmp_path / "users.json"
        saved = invoke(
            "fx_users", "--no-input", "--save-fixture", str(path), "--freeze-step", "fx_users_invoices"
        )
        assert saved.exit_code == 0 and "Frozen step(s): fx_users_invoices" in saved.output
        assert "source call" not in saved.output

    def test_recording_problems_exit_1(self, server, tmp_path):
        define_users(server)
        failed = invoke("fx_users", "--no-input", "--save-fixture", str(tmp_path / "x.json"), "--freeze-step", "nope")
        assert failed.exit_code == 1 and "no step of that name ran" in failed.output
        assert not (tmp_path / "x.json").exists()

    def test_replay_problems_exit_1(self, server, tmp_path):
        define_users(server)
        define_users(server, name="fx_other")
        path = record_fixture("fx_users").save(tmp_path / "users.json")
        wrong = invoke("fx_other", "--fixture", str(path))
        assert wrong.exit_code == 1 and "recorded for pipeline 'fx_users', not 'fx_other'" in wrong.output
        missing = invoke("fx_users", "--fixture", str(tmp_path / "absent.json"))
        assert missing.exit_code == 1 and "cannot read fixture" in missing.output

    def test_update_fixture_needs_a_fixture(self, server):
        define_users(server)
        alone = invoke("fx_users", "--update-fixture")
        assert alone.exit_code == 2 and "--update-fixture needs --fixture" in alone.output

    def test_split_fields(self):
        assert test_cmd.split_fields(["a, b", "c", " ", ""]) == ["a", "b", "c"]
