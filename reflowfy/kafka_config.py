"""Centralized Kafka configuration with SASL support."""

import os
from typing import Dict, Any, Optional


def get_kafka_config(
    bootstrap_servers: Optional[str] = None,
    security_protocol: Optional[str] = None,
    sasl_mechanism: Optional[str] = None,
    sasl_username: Optional[str] = None,
    sasl_password: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Get Kafka configuration for aiokafka.
    
    Reads from environment variables with optional overrides.
    Sets client_id to SASL username.
    
    Returns:
        Dict with aiokafka configuration
    """
    username = sasl_username or os.getenv("KAFKA_SASL_USERNAME", "")
    password = sasl_password or os.getenv("KAFKA_SASL_PASSWORD", "")
    protocol = security_protocol or os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    mechanism = sasl_mechanism or os.getenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256")
    servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    
    config: Dict[str, Any] = {
        "bootstrap_servers": servers,
    }
    
    # Add SASL config if credentials provided
    if username and password:
        config.update({
            "security_protocol": protocol,
            "sasl_mechanism": mechanism,
            "sasl_plain_username": username,
            "sasl_plain_password": password,
            "client_id": username,  # client_id = username as requested
        })
    
    return config


def get_kafka_env_vars() -> Dict[str, str]:
    """Get all Kafka-related environment variables."""
    return {
        "KAFKA_BOOTSTRAP_SERVERS": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "KAFKA_TOPIC": os.getenv("KAFKA_TOPIC", "reflow.jobs"),
        "KAFKA_GROUP_ID": os.getenv("KAFKA_GROUP_ID", "reflowfy-workers"),
        "KAFKA_SECURITY_PROTOCOL": os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
        "KAFKA_SASL_MECHANISM": os.getenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256"),
        "KAFKA_SASL_USERNAME": os.getenv("KAFKA_SASL_USERNAME", "reflowfy"),
        "KAFKA_SASL_PASSWORD": os.getenv("KAFKA_SASL_PASSWORD", "reflowfy"),
    }
