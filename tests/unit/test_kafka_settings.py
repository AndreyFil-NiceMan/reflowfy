"""core/kafka_settings: the one place Kafka connection arguments are built."""

import ssl

import pytest

from reflowfy.core.kafka_settings import KafkaSettings, connection_kwargs


def test_plaintext_by_default() -> None:
    assert connection_kwargs("k:9092") == {"bootstrap_servers": "k:9092"}


def test_sasl_defaults() -> None:
    assert connection_kwargs("k:9092", sasl_username="u", sasl_password="p") == {
        "bootstrap_servers": "k:9092",
        "security_protocol": "SASL_PLAINTEXT",
        "sasl_mechanism": "SCRAM-SHA-256",
        "sasl_plain_username": "u",
        "sasl_plain_password": "p",
    }


def test_a_username_without_a_password_is_not_sasl() -> None:
    assert connection_kwargs("k:9092", sasl_username="u") == {"bootstrap_servers": "k:9092"}


@pytest.mark.parametrize("protocol", ["SSL", "SASL_SSL"])
def test_tls_gets_the_system_cas_unless_given_a_context(protocol: str) -> None:
    kwargs = connection_kwargs(
        "k", security_protocol=protocol, sasl_username="u", sasl_password="p"
    )
    assert isinstance(kwargs["ssl_context"], ssl.SSLContext)
    mine = ssl.create_default_context()
    assert (
        connection_kwargs("k", security_protocol=protocol, ssl_context=mine)["ssl_context"] is mine
    )


def test_settings_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in {
        "KAFKA_BOOTSTRAP_SERVERS": "a:1,b:2",
        "KAFKA_TOPIC": "t",
        "KAFKA_GROUP_ID": "g",
        "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
        "KAFKA_SASL_MECHANISM": "SCRAM-SHA-512",
        "KAFKA_SASL_USERNAME": "svc",
        "KAFKA_SASL_PASSWORD": "pw",
    }.items():
        monkeypatch.setenv(name, value)
    settings = KafkaSettings.from_env()
    assert (settings.bootstrap_servers, settings.topic, settings.group_id) == ("a:1,b:2", "t", "g")
    assert settings.describe_auth() == "SASL_SSL / SCRAM-SHA-512"
    assert "pw" not in repr(settings)


def test_settings_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "KAFKA_BOOTSTRAP_SERVERS",
        "KAFKA_TOPIC",
        "KAFKA_GROUP_ID",
        "KAFKA_SECURITY_PROTOCOL",
        "KAFKA_SASL_USERNAME",
        "KAFKA_SASL_PASSWORD",
    ):
        monkeypatch.delenv(name, raising=False)
    settings = KafkaSettings.from_env()
    assert (settings.bootstrap_servers, settings.topic, settings.group_id) == (
        "localhost:9092",
        "reflow.jobs",
        "reflowfy-workers",
    )
    assert settings.describe_auth() == "PLAINTEXT"
    assert not settings.uses_sasl
