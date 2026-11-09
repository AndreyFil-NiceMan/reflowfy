"""Build a ReflowManager from the service's environment.

Shared by the FastAPI app and :func:`reflowfy.reflow_manager.lifecycle.run_pipeline`
(which starts executions from a lifecycle hook, outside any request).
"""

import os
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from reflowfy.reflow_manager.manager import ReflowManager


def manager_config_from_env() -> Dict[str, Any]:
    """Kafka/execution configuration from environment variables (including SASL).

    ``MAX_JOBS_PER_MINUTE`` is no longer read: a pipeline without a rate limit is
    unlimited (see ``pipeline_runner.resolve_rate_limit``).
    """
    return {
        "kafka_bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "kafka_topic": os.getenv("KAFKA_TOPIC", "reflow.jobs"),
        # Execution mode: "distributed" (Kafka) or "local" (in-process)
        "execution_mode": os.getenv("EXECUTION_MODE", "distributed"),
        # SASL Authentication (set by Helm chart)
        "kafka_security_protocol": os.getenv("KAFKA_SECURITY_PROTOCOL"),
        "kafka_sasl_mechanism": os.getenv("KAFKA_SASL_MECHANISM"),
        "kafka_sasl_username": os.getenv("KAFKA_SASL_USERNAME"),
        "kafka_sasl_password": os.getenv("KAFKA_SASL_PASSWORD"),
    }


def make_manager(db: Session, mode: Optional[str] = None) -> ReflowManager:
    """A ReflowManager on ``db`` from environment config, optionally overriding the mode."""
    config = manager_config_from_env()
    if mode:
        config["execution_mode"] = mode
    return ReflowManager(db_session=db, **config)
