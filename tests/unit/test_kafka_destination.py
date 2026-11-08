"""KafkaDestination: what each message holds (value_field), its key (key=), and the
deprecated key_field alias. No real Kafka: a fake producer records every send."""

import asyncio
import warnings
from typing import Any, Dict, List
from unittest.mock import patch

import pytest

from reflowfy.destinations.kafka import kafka_destination
from reflowfy.execution.content_dedup import destination_identity


class FakeProducer:
    """aiokafka's send(): hands back a future that resolves when the broker acks."""

    def __init__(self) -> None:
        self.sent: List[Dict[str, Any]] = []

    async def send(self, topic: str, value: bytes, key: Any = None, headers: Any = None):
        self.sent.append({"topic": topic, "value": value, "key": key, "headers": headers})
        future: "asyncio.Future[None]" = asyncio.get_running_loop().create_future()
        future.set_result(None)
        return future


async def _send(dest: Any, records: List[Any]) -> List[Dict[str, Any]]:
    producer = FakeProducer()
    with patch.object(dest, "_get_producer", return_value=producer):
        await dest.send(records)
    return producer.sent


class TestValueField:
    async def test_present_sends_only_that_field(self):
        dest = kafka_destination(bootstrap_servers="k:9092", topic="t", value_field="payload")
        sent = await _send(dest, [{"payload": {"user_id": "42"}, "trace_id": "ignored"}])
        assert sent[0]["value"] == b'{"user_id": "42"}'

    async def test_not_configured_sends_whole_record(self):
        dest = kafka_destination(bootstrap_servers="k:9092", topic="t")
        sent = await _send(dest, [{"user_id": "42"}])
        assert sent[0]["value"] == b'{"user_id": "42"}'

    async def test_missing_on_record_sends_whole_record(self):
        dest = kafka_destination(bootstrap_servers="k:9092", topic="t", value_field="payload")
        sent = await _send(dest, [{"name": "alice"}])
        assert sent[0]["value"] == b'{"name": "alice"}'


class TestKeyFieldAlias:
    async def test_key_field_still_selects_the_value_and_warns(self):
        with pytest.warns(DeprecationWarning, match="Use value_field="):
            dest = kafka_destination(bootstrap_servers="k:9092", topic="t", key_field="payload")
        sent = await _send(dest, [{"payload": {"a": 1}}])
        assert sent[0]["value"] == b'{"a": 1}'
        assert sent[0]["key"] is None  # it never set the key
        assert dest.config["value_field"] == "payload"

    def test_both_names_rejected(self):
        with pytest.raises(ValueError, match="pass value_field= only"):
            kafka_destination(bootstrap_servers="k:9092", topic="t", value_field="a", key_field="a")

    def test_the_rename_keeps_the_dedup_hash(self):
        """Hashed as key_field, its name until 1.1.4, so upgrading re-sends nothing."""
        new = kafka_destination(bootstrap_servers="k:9092", topic="t", value_field="payload")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            old = kafka_destination(bootstrap_servers="k:9092", topic="t", key_field="payload")
        assert destination_identity(new) == destination_identity(old)
        assert new.identity_config()["key_field"] == "payload"
        assert "value_field" not in new.identity_config()

    def test_unset_still_hashes_key_field_none(self):
        identity = kafka_destination(bootstrap_servers="k:9092", topic="t").identity_config()
        assert identity["key_field"] is None


class TestMessageKey:
    async def test_template_key_per_record(self):
        dest = kafka_destination(bootstrap_servers="k:9092", topic="t", key="{customer}")
        sent = await _send(dest, [{"customer": "acme"}, {"customer": 7}])
        assert [m["key"] for m in sent] == [b"acme", b"7"]

    async def test_dotted_template_key(self):
        dest = kafka_destination(bootstrap_servers="k:9092", topic="t", key="{data.region}-x")
        sent = await _send(dest, [{"data": {"region": "eu"}}])
        assert sent[0]["key"] == b"eu-x"

    async def test_constant_key(self):
        dest = kafka_destination(bootstrap_servers="k:9092", topic="t", key="all")
        sent = await _send(dest, [{"a": 1}, {"a": 2}])
        assert [m["key"] for m in sent] == [b"all", b"all"]

    async def test_function_key_str_bytes_none(self):
        dest = kafka_destination(
            bootstrap_servers="k:9092",
            topic="t",
            key=lambda r: {"s": "text", "b": b"\x01", "n": None}[r["k"]],
        )
        sent = await _send(dest, [{"k": "s"}, {"k": "b"}, {"k": "n"}])
        assert [m["key"] for m in sent] == [b"text", b"\x01", None]

    async def test_missing_field_fails_before_any_send(self):
        dest = kafka_destination(bootstrap_servers="k:9092", topic="t", key="{customer}")
        producer = FakeProducer()
        with patch.object(dest, "_get_producer", return_value=producer):
            with pytest.raises(ValueError) as excinfo:
                await dest.send([{"customer": "a"}, {"other": "secret-value"}])
        assert str(excinfo.value) == (
            "kafka_destination: key placeholder {customer} has no matching field on "
            "record 2 of 2"
        )
        assert producer.sent == []

    async def test_function_that_raises_fails_naming_the_record(self):
        dest = kafka_destination(bootstrap_servers="k:9092", topic="t", key=lambda r: r["x"])
        with pytest.raises(ValueError, match="key function failed on record 1 of 1: KeyError"):
            await _send(dest, [{"a": 1}])

    async def test_function_bad_type_fails(self):
        dest = kafka_destination(bootstrap_servers="k:9092", topic="t", key=lambda r: 5)
        with pytest.raises(ValueError, match="key for record 1 of 1 is int"):
            await _send(dest, [{"a": 1}])

    def test_bad_key_shape_rejected(self):
        with pytest.raises(TypeError, match="key must be a"):
            kafka_destination(bootstrap_servers="k:9092", topic="t", key=5)  # type: ignore[arg-type]

    def test_template_key_is_config_but_not_identity(self):
        dest = kafka_destination(bootstrap_servers="k:9092", topic="t", key="{c}")
        assert dest.config["key"] == "{c}"
        assert "key" not in dest.identity_config()
        plain = kafka_destination(bootstrap_servers="k:9092", topic="t")
        assert destination_identity(dest) == destination_identity(plain)

    def test_function_key_is_kept_out_of_config(self):
        dest = kafka_destination(bootstrap_servers="k:9092", topic="t", key=lambda r: "k")
        assert "key" not in dest.config

    def test_key_is_not_a_producer_setting(self):
        dest = kafka_destination(bootstrap_servers="k:9092", topic="t", key="{c}")
        assert "key" not in dest._producer_kwargs()
        assert "value_field" not in dest._producer_kwargs()


def test_concurrency_was_removed():
    with pytest.raises(TypeError, match="concurrency= was removed in 1.1.4"):
        kafka_destination(bootstrap_servers="k:9092", topic="t", concurrency=8)
