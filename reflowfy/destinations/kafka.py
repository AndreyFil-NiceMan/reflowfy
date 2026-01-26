"""Kafka destination connector using aiokafka."""

import os
import json
from typing import Any, Dict, List, Optional
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from reflowfy.destinations.base import BaseDestination, DestinationError, RetryConfig


class KafkaDestination(BaseDestination):
    """
    Kafka destination connector using aiokafka.
    
    Supports:
    - SASL authentication (SCRAM-SHA-256)
    - Configurable serialization (JSON by default)
    - Compression
    - Error handling
    """
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        compression_type: str = "gzip",
        retry_config: Optional[RetryConfig] = None,
        # SASL Authentication
        security_protocol: Optional[str] = None,
        sasl_mechanism: Optional[str] = None,
        sasl_username: Optional[str] = None,
        sasl_password: Optional[str] = None,
        **producer_config,
    ):
        """
        Initialize Kafka destination.
        
        Args:
            bootstrap_servers: Kafka broker addresses (comma-separated)
            topic: Target topic
            compression_type: Compression algorithm (gzip, snappy, lz4)
            retry_config: Optional retry configuration
            security_protocol: Security protocol (e.g., SASL_PLAINTEXT)
            sasl_mechanism: SASL mechanism (e.g., SCRAM-SHA-256)
            sasl_username: SASL username
            sasl_password: SASL password
            **producer_config: Additional producer configuration
        """
        config = {
            "bootstrap_servers": bootstrap_servers,
            "topic": topic,
            "compression_type": compression_type,
            "security_protocol": security_protocol,
            "sasl_mechanism": sasl_mechanism,
            "sasl_username": sasl_username,
            "sasl_password": sasl_password,
            **producer_config,
        }
        super().__init__(config, retry_config)
        self._producer: Optional[AIOKafkaProducer] = None
        self._started = False
    
    async def _get_producer(self) -> AIOKafkaProducer:
        """Get or create async Kafka producer."""
        if self._producer is None or not self._started:
            # Build producer kwargs
            producer_kwargs = {
                "bootstrap_servers": self.config["bootstrap_servers"],
                "compression_type": self.config.get("compression_type", "gzip"),
            }
            
            # Add SASL config if credentials provided
            username = self.config.get("sasl_username")
            password = self.config.get("sasl_password")
            if username and password:
                producer_kwargs.update({
                    "security_protocol": self.config.get("security_protocol") or "SASL_PLAINTEXT",
                    "sasl_mechanism": self.config.get("sasl_mechanism") or "SCRAM-SHA-256",
                    "sasl_plain_username": username,
                    "sasl_plain_password": password,
                    "client_id": username,  # client_id = username
                })
            
            self._producer = AIOKafkaProducer(**producer_kwargs)
            await self._producer.start()
            self._started = True
        
        return self._producer
    
    async def send(self, records: List[Any], metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Send records to Kafka topic.
        
        Args:
            records: List of records to send
            metadata: Optional metadata to include in message headers
        
        Raises:
            DestinationError: If send fails
        """
        producer = await self._get_producer()
        topic = self.config["topic"]
        
        try:
            for record in records:
                # Serialize record to JSON
                value = json.dumps(record).encode("utf-8")
                
                # Prepare headers
                headers = []
                if metadata:
                    for k, v in metadata.items():
                        if isinstance(v, str):
                            headers.append((k, v.encode("utf-8")))
                
                # Send message
                await producer.send_and_wait(
                    topic=topic,
                    value=value,
                    headers=headers if headers else None,
                )
        
        except KafkaError as e:
            raise DestinationError("kafka", f"Failed to send to topic '{topic}': {e}", e)
        except Exception as e:
            raise DestinationError("kafka", f"Unexpected error: {e}", e)
    
    async def health_check(self) -> bool:
        """Check Kafka cluster connectivity."""
        try:
            producer = await self._get_producer()
            # If we can get the producer, the connection is healthy
            return True
        except Exception:
            return False
    
    async def close(self):
        """Close producer connection."""
        if self._producer and self._started:
            await self._producer.stop()
            self._producer = None
            self._started = False


def kafka_destination(
    bootstrap_servers: str,
    topic: str,
    compression_type: str = "gzip",
    retry_config: Optional[RetryConfig] = None,
    security_protocol: Optional[str] = None,
    sasl_mechanism: Optional[str] = None,
    sasl_username: Optional[str] = None,
    sasl_password: Optional[str] = None,
    **producer_config,
) -> KafkaDestination:
    """
    Factory function for Kafka destination.
    
    Example:
        >>> destination = kafka_destination(
        ...     bootstrap_servers="kafka:9092",
        ...     topic="processed-logs",
        ...     compression_type="gzip",
        ...     sasl_username="reflowfy",
        ...     sasl_password="reflowfy"
        ... )
    """
    return KafkaDestination(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        compression_type=compression_type,
        retry_config=retry_config,
        security_protocol=security_protocol,
        sasl_mechanism=sasl_mechanism,
        sasl_username=sasl_username,
        sasl_password=sasl_password,
        **producer_config,
    )
