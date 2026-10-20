"""Unit tests for reflowfy.kafka_config (env-driven aiokafka connection settings)."""

import pytest

from reflowfy.kafka_config import get_kafka_config, get_kafka_env_vars

_KAFKA_ENV = (
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_TOPIC",
    "KAFKA_GROUP_ID",
    "KAFKA_SECURITY_PROTOCOL",
    "KAFKA_SASL_MECHANISM",
    "KAFKA_SASL_USERNAME",
    "KAFKA_SASL_PASSWORD",
)


@pytest.fixture(autouse=True)
def _clean_kafka_env(monkeypatch):
    for name in _KAFKA_ENV:
        monkeypatch.delenv(name, raising=False)


class TestGetKafkaConfig:
    def test_no_env_and_no_args_is_plaintext_localhost(self):
        assert get_kafka_config() == {"bootstrap_servers": "localhost:9092"}

    def test_single_server_string_is_kept_as_string(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        assert get_kafka_config()["bootstrap_servers"] == "kafka:9092"

    def test_comma_separated_servers_become_a_trimmed_list(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", " a:9092, b:9092 ,,c:9092")
        assert get_kafka_config()["bootstrap_servers"] == ["a:9092", "b:9092", "c:9092"]

    def test_list_argument_is_passed_through(self):
        assert get_kafka_config(bootstrap_servers=["x:1", "y:2"])["bootstrap_servers"] == [
            "x:1",
            "y:2",
        ]

    def test_sasl_from_env_uses_defaults_and_username_as_client_id(self, monkeypatch):
        monkeypatch.setenv("KAFKA_SASL_USERNAME", "svc")
        monkeypatch.setenv("KAFKA_SASL_PASSWORD", "secret")

        config = get_kafka_config()

        assert config == {
            "bootstrap_servers": "localhost:9092",
            # PLAINTEXT is the env default even with credentials set.
            "security_protocol": "PLAINTEXT",
            "sasl_mechanism": "SCRAM-SHA-256",
            "sasl_plain_username": "svc",
            "sasl_plain_password": "secret",
            "client_id": "svc",
        }

    @pytest.mark.parametrize(
        "protocol,mechanism",
        [
            ("SASL_PLAINTEXT", "SCRAM-SHA-256"),
            ("SASL_SSL", "SCRAM-SHA-512"),
            ("SASL_SSL", "PLAIN"),
        ],
    )
    def test_every_sasl_protocol_from_env(self, monkeypatch, protocol, mechanism):
        monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", protocol)
        monkeypatch.setenv("KAFKA_SASL_MECHANISM", mechanism)
        monkeypatch.setenv("KAFKA_SASL_USERNAME", "u")
        monkeypatch.setenv("KAFKA_SASL_PASSWORD", "p")

        config = get_kafka_config()

        assert config["security_protocol"] == protocol
        assert config["sasl_mechanism"] == mechanism

    def test_arguments_override_env(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "env:9092")
        monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
        monkeypatch.setenv("KAFKA_SASL_USERNAME", "env-user")
        monkeypatch.setenv("KAFKA_SASL_PASSWORD", "env-pass")

        config = get_kafka_config(
            bootstrap_servers="arg:9092",
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-512",
            sasl_username="arg-user",
            sasl_password="arg-pass",
        )

        assert config["bootstrap_servers"] == "arg:9092"
        assert config["security_protocol"] == "SASL_SSL"
        assert config["sasl_mechanism"] == "SCRAM-SHA-512"
        assert config["sasl_plain_username"] == "arg-user"
        assert config["sasl_plain_password"] == "arg-pass"
        assert config["client_id"] == "arg-user"

    @pytest.mark.parametrize("missing", ["KAFKA_SASL_USERNAME", "KAFKA_SASL_PASSWORD"])
    def test_half_configured_credentials_do_not_enable_sasl(self, monkeypatch, missing):
        monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
        for name in ("KAFKA_SASL_USERNAME", "KAFKA_SASL_PASSWORD"):
            if name != missing:
                monkeypatch.setenv(name, "x")

        config = get_kafka_config()

        assert config == {"bootstrap_servers": "localhost:9092"}


class TestGetKafkaEnvVars:
    def test_defaults_when_env_is_empty(self):
        assert get_kafka_env_vars() == {
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "KAFKA_TOPIC": "reflow.jobs",
            "KAFKA_GROUP_ID": "reflowfy-workers",
            "KAFKA_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
            "KAFKA_SASL_MECHANISM": "SCRAM-SHA-256",
            "KAFKA_SASL_USERNAME": "reflowfy",
            "KAFKA_SASL_PASSWORD": "reflowfy",
        }

    def test_env_values_win(self, monkeypatch):
        values = {name: f"value-of-{name}" for name in _KAFKA_ENV}
        for name, value in values.items():
            monkeypatch.setenv(name, value)

        assert get_kafka_env_vars() == values
