"""How every Reflowfy Kafka client connects: the job topic's consumer and producer, and
``kafka_destination``.

One function builds the aiokafka connection arguments, so the worker, the manager's
dispatcher and user destinations cannot disagree about TLS or SASL again: the
dispatcher and the worker used to drop ``security_protocol`` on a TLS-only cluster
(connecting in plaintext) and passed no ``ssl_context`` for ``SASL_SSL``, which
aiokafka rejects, while the destination handled both.
"""

import os
import ssl
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_JOB_TOPIC = "reflow.jobs"
DEFAULT_WORKER_GROUP = "reflowfy-workers"
DEFAULT_SASL_PROTOCOL = "SASL_PLAINTEXT"
DEFAULT_SASL_MECHANISM = "SCRAM-SHA-256"
TLS_PROTOCOLS = ("SSL", "SASL_SSL")


def connection_kwargs(
    bootstrap_servers: Any,
    security_protocol: Optional[str] = None,
    sasl_mechanism: Optional[str] = None,
    sasl_username: Optional[str] = None,
    sasl_password: Optional[str] = None,
    ssl_context: Optional[ssl.SSLContext] = None,
) -> Dict[str, Any]:
    """The aiokafka arguments for reaching a cluster (producer, consumer or admin client).

    - SASL credentials: ``security_protocol`` (default ``SASL_PLAINTEXT``) and
      ``sasl_mechanism`` (default ``SCRAM-SHA-256``) with them.
    - No credentials but a protocol (TLS without SASL): the protocol is kept; dropping
      it would silently connect in plaintext.
    - ``SSL`` / ``SASL_SSL``: aiokafka requires an explicit context, so the system CAs
      are used unless ``ssl_context`` is given.
    """
    kwargs: Dict[str, Any] = {"bootstrap_servers": bootstrap_servers}
    if sasl_username and sasl_password:
        kwargs.update(
            security_protocol=security_protocol or DEFAULT_SASL_PROTOCOL,
            sasl_mechanism=sasl_mechanism or DEFAULT_SASL_MECHANISM,
            sasl_plain_username=sasl_username,
            sasl_plain_password=sasl_password,
        )
    elif security_protocol:
        kwargs["security_protocol"] = security_protocol
    if kwargs.get("security_protocol") in TLS_PROTOCOLS:
        kwargs["ssl_context"] = ssl_context or ssl.create_default_context()
    return kwargs


@dataclass(frozen=True)
class KafkaSettings:
    """The services' own Kafka connection (the job topic), read from the environment."""

    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS
    topic: str = DEFAULT_JOB_TOPIC
    group_id: str = DEFAULT_WORKER_GROUP
    security_protocol: Optional[str] = None
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = field(default=None, repr=False)

    @classmethod
    def from_env(cls) -> "KafkaSettings":
        """``KAFKA_BOOTSTRAP_SERVERS``, ``KAFKA_TOPIC``, ``KAFKA_GROUP_ID`` and the SASL
        variables ``KAFKA_SECURITY_PROTOCOL`` / ``_SASL_MECHANISM`` / ``_SASL_USERNAME`` /
        ``_SASL_PASSWORD`` (set by the Helm chart)."""
        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS),
            topic=os.getenv("KAFKA_TOPIC", DEFAULT_JOB_TOPIC),
            group_id=os.getenv("KAFKA_GROUP_ID", DEFAULT_WORKER_GROUP),
            security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL"),
            sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM"),
            sasl_username=os.getenv("KAFKA_SASL_USERNAME"),
            sasl_password=os.getenv("KAFKA_SASL_PASSWORD"),
        )

    @property
    def uses_sasl(self) -> bool:
        return bool(self.sasl_username and self.sasl_password)

    def describe_auth(self) -> str:
        """``SASL_SSL / SCRAM-SHA-512``, ``SSL`` or ``PLAINTEXT``: for a startup log line."""
        if self.uses_sasl:
            protocol = self.security_protocol or DEFAULT_SASL_PROTOCOL
            return f"{protocol} / {self.sasl_mechanism or DEFAULT_SASL_MECHANISM}"
        return self.security_protocol or "PLAINTEXT"
