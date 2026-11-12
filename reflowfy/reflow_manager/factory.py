"""Build a ReflowManager from the service's environment.

Shared by the FastAPI app and :func:`reflowfy.reflow_manager.lifecycle.run_pipeline`
(which starts executions from a lifecycle hook, outside any request).
"""

import os
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from reflowfy.core.kafka_settings import KafkaSettings
from reflowfy.reflow_manager.manager import ReflowManager


def manager_config_from_env() -> Dict[str, Any]:
    """Kafka/execution configuration from environment variables (including SASL).

    ``MAX_JOBS_PER_MINUTE`` is no longer read: a pipeline without a rate limit is
    unlimited (see ``pipeline_runner.resolve_rate_limit``).
    """
    kafka = KafkaSettings.from_env()
    return {
        "kafka_bootstrap_servers": kafka.bootstrap_servers,
        "kafka_topic": kafka.topic,
        # Execution mode: "distributed" (Kafka) or "local" (in-process)
        "execution_mode": os.getenv("EXECUTION_MODE", "distributed"),
        "kafka_security_protocol": kafka.security_protocol,
        "kafka_sasl_mechanism": kafka.sasl_mechanism,
        "kafka_sasl_username": kafka.sasl_username,
        "kafka_sasl_password": kafka.sasl_password,
    }


def make_manager(db: Session, mode: Optional[str] = None) -> ReflowManager:
    """A ReflowManager on ``db`` from environment config, optionally overriding the mode."""
    config = manager_config_from_env()
    if mode:
        config["execution_mode"] = mode
    return ReflowManager(db_session=db, **config)
