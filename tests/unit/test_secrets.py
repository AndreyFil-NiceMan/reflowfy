"""secret("ENV") references, the literal-secret warning and redaction of job payloads."""

import copy
import json
import logging
import pickle
from typing import Any, Dict, Iterator

import pytest

from reflowfy.core.secrets import (
    REDACTED,
    SecretError,
    SecretRef,
    decode_secret_refs,
    encode_secret_refs,
    is_sensitive_key,
    literal_secret_fields,
    mask_url_password,
    redact_config,
    secret,
)
from reflowfy.factories.source_factory import SourceFactory
from reflowfy.reflow_manager.pipeline_runner import build_job_payload
from reflowfy.reflow_manager.redaction import redact_job_payload, redact_row
from reflowfy.sources.api import IDBasedAPISource
from reflowfy.sources.elastic import ElasticSource
from reflowfy.sources.s3 import S3Source
from reflowfy.sources.sql import SqlSource

LOGGER = "reflowfy.factories.source_factory"


@pytest.fixture(autouse=True)
def capture_reflowfy_logs(caplog: pytest.LogCaptureFixture) -> Iterator[None]:
    """setup_logging() in other tests stops "reflowfy" propagating to caplog's root handler."""
    logger = logging.getLogger("reflowfy")
    attach = not logger.propagate
    if attach:
        logger.addHandler(caplog.handler)
    yield
    if attach:
        logger.removeHandler(caplog.handler)


@pytest.fixture(autouse=True)
def fresh_warnings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(SourceFactory, "_warned_literal", set())


class TestSecretRef:
    def test_reads_the_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DB_PW", "hunter2")
        ref = secret("DB_PW")
        assert ref == "hunter2" and isinstance(ref, str)
        assert ref.env_name == "DB_PW"
        assert repr(ref) == "secret('DB_PW')"
        assert "hunter2" not in repr({"password": ref})

    def test_missing_variable_fails_naming_it(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("NOPE_PW", raising=False)
        with pytest.raises(SecretError, match="environment variable NOPE_PW is not set"):
            secret("NOPE_PW")
        assert "NOPE_PW is not set" in str(SecretError("NOPE_PW is not set"))

    def test_copy_and_pickle_keep_the_name(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DB_PW", "hunter2")
        ref = secret("DB_PW")
        for clone in (copy.deepcopy(ref), pickle.loads(pickle.dumps(ref))):
            assert isinstance(clone, SecretRef)
            assert clone == "hunter2" and clone.env_name == "DB_PW"

    def test_encode_and_decode(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ES_PW", "pw")
        config = {"auth": ("user", secret("ES_PW")), "nested": {"k": [secret("ES_PW")]}, "n": 1}
        wire = encode_secret_refs(config)
        assert wire == {
            "auth": ["user", {"$secret": "ES_PW"}],
            "nested": {"k": [{"$secret": "ES_PW"}]},
            "n": 1,
        }
        assert "pw" not in json.dumps(wire)
        monkeypatch.setenv("ES_PW", "worker-pw")  # the worker's own environment
        back = decode_secret_refs(wire)
        assert back["auth"] == ["user", "worker-pw"]
        assert isinstance(back["nested"]["k"][0], SecretRef)


class TestSourceRoundTrip:
    def test_only_the_name_is_serialized_and_the_worker_resolves_it(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("ORDERS_DB_URL", "sqlite:///manager.db")
        src = SqlSource(connection_url=secret("ORDERS_DB_URL"), query="SELECT 1")
        payload = build_job_payload("e", "j", "orders", src, metadata={})
        assert payload["source"]["config"]["connection_url"] == {"$secret": "ORDERS_DB_URL"}

        monkeypatch.setenv("ORDERS_DB_URL", "sqlite:///worker.db")
        rebuilt = SourceFactory.create(payload["source"]["type"], payload["source"]["config"])
        assert rebuilt.config["connection_url"] == "sqlite:///worker.db"

    def test_missing_on_the_worker_fails_the_rebuild(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("GONE_URL", raising=False)
        with pytest.raises(SecretError, match="GONE_URL"):
            SourceFactory.create(
                "SqlSource", {"connection_url": {"$secret": "GONE_URL"}, "query": "SELECT 1"}
            )

    def test_split_keeps_the_reference(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("API_TOKEN_X", "tok")
        src = IDBasedAPISource(
            base_url="http://h",
            endpoint_template="/u/{id}",
            ids=[1, 2, 3],
            batch_size=2,
            auth_type="bearer",
            auth_token=secret("API_TOKEN_X"),
        )
        for sub in src.split({}):
            wire = SourceFactory.serialize(sub)
            assert wire["config"]["auth_token"] == {"$secret": "API_TOKEN_X"}


class TestLiteralWarning:
    def _warnings(self, caplog: pytest.LogCaptureFixture) -> list[Dict[str, Any]]:
        return [
            r.__dict__
            for r in caplog.records
            if r.__dict__.get("event") == "literal_secret_serialized"
        ]

    def test_warns_once_per_pipeline_and_field(self, caplog: pytest.LogCaptureFixture) -> None:
        src = SqlSource(connection_url="postgresql://u:pw@h/db", query="SELECT 1")
        with caplog.at_level(logging.WARNING, logger=LOGGER):
            for _ in range(3):
                SourceFactory.serialize(src, pipeline_name="a")
            SourceFactory.serialize(src, pipeline_name="b")
        warned = self._warnings(caplog)
        assert [(w["pipeline_name"], w["field"]) for w in warned] == [
            ("a", "connection_url"),
            ("b", "connection_url"),
        ]
        assert all("pw" not in w["msg"] % w["args"] for w in warned)

    def test_no_warning_for_references_or_urls_without_a_password(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        monkeypatch.setenv("DB_URL", "postgresql://u:pw@h/db")
        with caplog.at_level(logging.WARNING, logger=LOGGER):
            SourceFactory.serialize(SqlSource(connection_url=secret("DB_URL"), query="SELECT 1"))
            SourceFactory.serialize(SqlSource(connection_url="sqlite://", query="SELECT 1"))
        assert self._warnings(caplog) == []

    def test_credential_named_and_declared_fields(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING, logger=LOGGER):
            SourceFactory.serialize(
                IDBasedAPISource(
                    base_url="http://h",
                    endpoint_template="/x",
                    headers={"Authorization": "Bearer abc", "Accept": "json"},
                    auth_token="tok",
                ),
                pipeline_name="p",
            )
        assert sorted(w["field"] for w in self._warnings(caplog)) == ["auth_token", "headers"]


class TestRedaction:
    def test_url_password(self) -> None:
        assert mask_url_password("postgresql://u:pw@h:5432/db?x=1") == (
            f"postgresql://u:{REDACTED}@h:5432/db?x=1"
        )
        assert mask_url_password("http://h/path") == "http://h/path"
        assert mask_url_password("plain") == "plain"

    @pytest.mark.parametrize(
        "key",
        [
            "password",
            "sasl_password",
            "aws_secret_access_key",
            "auth_token",
            "X-Api-Key",
            "Authorization",
            "auth",
            "private_key",
            "Cookie",
        ],
    )
    def test_sensitive_keys(self, key: str) -> None:
        assert is_sensitive_key(key)

    @pytest.mark.parametrize("key", ["auth_type", "author", "id", "batch_size", 3])
    def test_ordinary_keys(self, key: Any) -> None:
        assert not is_sensitive_key(key)

    def test_each_builtin_source(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("S3_SECRET", "s3pw")
        cases = [
            (
                SqlSource(connection_url="postgresql://u:pw@h/db", query="SELECT 1"),
                {"connection_url": f"postgresql://u:{REDACTED}@h/db"},
            ),
            (
                ElasticSource(
                    url="https://u:pw@es:9200", index="i", base_query={}, auth=("e", "pw")
                ),
                {"url": f"https://u:{REDACTED}@es:9200", "auth": ["e", REDACTED]},
            ),
            (
                S3Source(bucket="b", aws_access_key_id="AK", aws_secret_access_key="sk"),
                {"aws_secret_access_key": REDACTED, "aws_access_key_id": "AK"},
            ),
            (
                S3Source(bucket="b", aws_secret_access_key=secret("S3_SECRET")),
                {"aws_secret_access_key": {"$secret": "S3_SECRET"}},
            ),
            (
                IDBasedAPISource(
                    base_url="http://h",
                    endpoint_template="/x",
                    headers={"Authorization": "Bearer abc", "Accept": "json"},
                    auth_token="tok",
                    params={"api_key": "k", "page": 1},
                ),
                {
                    "auth_token": REDACTED,
                    "headers": {"Authorization": REDACTED, "Accept": "json"},
                    "params": {"api_key": REDACTED, "page": 1},
                },
            ),
        ]
        for src, expected in cases:
            redacted = SourceFactory.redact(SourceFactory.serialize(src))
            for key, value in expected.items():
                assert redacted["config"][key] == value, (type(src).__name__, key)

    def test_declared_field_shapes(self) -> None:
        fields = ("cred",)
        assert redact_config({"cred": None}, fields) == {"cred": None}
        assert redact_config({"cred": ""}, fields) == {"cred": ""}
        assert redact_config({"cred": 42}, fields) == {"cred": REDACTED}
        assert redact_config({"cred": ["a", "b", "c"]}, fields) == {
            "cred": [REDACTED, REDACTED, REDACTED]
        }
        assert redact_config({"cred": "http://plain/url"}, fields) == {"cred": "http://plain/url"}
        assert redact_config([{"password": "x"}], ()) == [{"password": REDACTED}]
        # a reference is only a variable name, wherever it sits
        assert redact_config({"opts": {"$secret": "E"}}, ()) == {"opts": {"$secret": "E"}}

    def test_literal_secret_fields(self) -> None:
        config = {"password": "x", "url": "http://u:p@h", "ref": {"$secret": "E"}, "n": 1}
        assert literal_secret_fields(config, ("ref",)) == ["password", "url"]

    def test_job_payload(self) -> None:
        payload = {
            "schema_version": 2,
            "source": {
                "type": "SqlSource",
                "config": {"connection_url": "postgresql://u:pw@h/db", "query": "SELECT 1"},
            },
            "metadata": {"runtime_params": {"api_token": "t", "day": "2026-01-01"}},
        }
        redacted = redact_job_payload(payload)
        assert list(redacted) == list(payload)
        assert redacted["source"]["config"]["connection_url"].endswith(f"u:{REDACTED}@h/db")
        assert redacted["metadata"]["runtime_params"] == {
            "api_token": REDACTED,
            "day": "2026-01-01",
        }
        assert payload["metadata"]["runtime_params"]["api_token"] == "t"  # not mutated

    def test_unknown_source_type_and_odd_payloads(self) -> None:
        payload = {"source": {"type": "Gone", "config": {"password": "x", "k": "v"}}}
        assert redact_job_payload(payload)["source"]["config"] == {"password": REDACTED, "k": "v"}
        assert redact_job_payload({"source": "text"}) == {"source": "text"}
        assert redact_job_payload(None) is None
        assert redact_job_payload(["http://u:p@h"]) == [f"http://u:{REDACTED}@h"]

    def test_row(self) -> None:
        row = {"id": 1, "job_payload": {"password": "x"}, "runtime_params": {"token": "y"}}
        assert redact_row(row) == {
            "id": 1,
            "job_payload": {"password": REDACTED},
            "runtime_params": {"token": REDACTED},
        }
        assert redact_row({"id": 1}) == {"id": 1}
