"""Kafka destination connector."""

import json
from typing import Any, Dict, List, Optional
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient
from reflowfy.destinations.base import BaseDestination, DestinationError, RetryConfig


class KafkaDestination(BaseDestination):
    """
    Kafka destination connector.
    
    Supports:
    - Configurable serialization (JSON by default)
    - Batching and compression
    - Connection pooling
    - Error handling
    """
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        compression_type: str = "gzip",
        batch_size: int = 16384,
        linger_ms: int = 10,
        retry_config: Optional[RetryConfig] = None,
        **producer_config,
    ):
        """
        Initialize Kafka destination.
        
        Args:
            bootstrap_servers: Kafka broker addresses (comma-separated)
            topic: Target topic
            compression_type: Compression algorithm (gzip, snappy, lz4, zstd)
            batch_size: Batch size in bytes
            linger_ms: Time to wait before sending batch
            retry_config: Optional retry configuration
            **producer_config: Additional producer configuration
        """
        config = {
            "bootstrap_servers": bootstrap_servers,
            "topic": topic,
            "compression_type": compression_type,
            "batch_size": batch_size,
            "linger_ms": linger_ms,
            **producer_config,
        }
        super().__init__(config, retry_config)
        self._producer: Optional[Producer] = None
    
    def _get_producer(self) -> Producer:
        """Get or create Kafka producer."""
        if self._producer is None:
            producer_config = {
                "bootstrap.servers": self.config["bootstrap_servers"],
                "compression.type": self.config["compression_type"],
                "batch.size": self.config["batch_size"],
                "linger.ms": self.config["linger_ms"],
            }
            
            # Add any additional producer configs
            for key, value in self.config.items():
                if key not in ["bootstrap_servers", "topic", "compression_type", "batch_size", "linger_ms"]:
                    producer_config[key] = value
            
            self._producer = Producer(producer_config)
        
        return self._producer
    
    def send(self, records: List[Any], metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Send records to Kafka topic.
        
        Args:
            records: List of records to send
            metadata: Optional metadata to include in message headers
        
        Raises:
            DestinationError: If send fails
        """
        producer = self._get_producer()
        topic = self.config["topic"]
        
        try:
            for record in records:
                # Serialize record to JSON
                value = json.dumps(record).encode("utf-8")
                
                # Prepare headers
                headers = {}
                if metadata:
                    for k, v in metadata.items():
                        if isinstance(v, str):
                            headers[k] = v.encode("utf-8")
                
                # Produce message
                producer.produce(
                    topic=topic,
                    value=value,
                    headers=headers if headers else None,
                    callback=self._delivery_callback,
                )
            
            # Flush to ensure all messages are sent
            producer.flush(timeout=30.0)
        
        except KafkaException as e:
            raise DestinationError("kafka", f"Failed to send to topic '{topic}': {e}", e)
        except Exception as e:
            raise DestinationError("kafka", f"Unexpected error: {e}", e)
    
    def _delivery_callback(self, err, msg):
        """Callback for message delivery reports."""
        if err:
            print(f"❌ Message delivery failed: {err}")
        # Success logging is too verbose for production
    
    def health_check(self) -> bool:
        """Check Kafka cluster connectivity."""
        try:
            admin_client = AdminClient(
                {"bootstrap.servers": self.config["bootstrap_servers"]}
            )
            
            # Get cluster metadata
            metadata = admin_client.list_topics(timeout=5.0)
            
            # Check if topic exists
            topic = self.config["topic"]
            if topic in metadata.topics:
                return True
            
            # Topic doesn't exist but cluster is reachable
            print(f"⚠️  Topic '{topic}' does not exist but cluster is healthy")
            return True
        
        except Exception:
            return False
    
    def close(self):
        """Close producer connection."""
        if self._producer:
            self._producer.flush()
            self._producer = None


def kafka_destination(
    bootstrap_servers: str,
    topic: str,
    compression_type: str = "gzip",
    batch_size: int = 16384,
    linger_ms: int = 10,
    retry_config: Optional[RetryConfig] = None,
    **producer_config,
) -> KafkaDestination:
    """
    Factory function for Kafka destination.
    
    Example:
        >>> destination = kafka_destination(
        ...     bootstrap_servers="kafka:9092",
        ...     topic="processed-logs",
        ...     compression_type="gzip"
        ... )
    """
    return KafkaDestination(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        compression_type=compression_type,
        batch_size=batch_size,
        linger_ms=linger_ms,
        retry_config=retry_config,
        **producer_config,
    )
