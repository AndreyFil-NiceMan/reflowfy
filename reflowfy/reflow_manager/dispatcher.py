"""Job dispatchers for ReflowManager."""

import json
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from confluent_kafka import Producer, KafkaException

from reflowfy.reflow_manager.rate_limiter import RateLimiter


class BaseDispatcher(ABC):
    """Abstract base class for job dispatchers."""
    
    def __init__(self, rate_limiter: RateLimiter):
        self.rate_limiter = rate_limiter

    @abstractmethod
    def dispatch_job(
        self,
        job_payload: Dict[str, Any],
        pipeline_name: str,
        rate_limit: Optional[float] = None,
    ) -> bool:
        """Dispatch a single job."""
        pass

    @abstractmethod
    def dispatch_jobs_batch(
        self,
        jobs: List[Dict[str, Any]],
        pipeline_name: str,
        rate_limit: Optional[float] = None,
    ) -> int:
        """Dispatch a batch of jobs."""
        pass

    @abstractmethod
    def close(self):
        """Close resources."""
        pass


class KafkaDispatcher(BaseDispatcher):
    """
    Dispatches jobs to Kafka with rate limiting.
    """
    
    def __init__(
        self,
        kafka_bootstrap_servers: str,
        kafka_topic: str,
        rate_limiter: RateLimiter,
        producer_config: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(rate_limiter)
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.producer_config = producer_config or {}
        self._producer: Optional[Producer] = None
    
    def _get_producer(self) -> Producer:
        """Get or create Kafka producer."""
        if self._producer is None:
            config = {
                "bootstrap.servers": self.kafka_bootstrap_servers,
                "compression.type": "gzip",
                **self.producer_config,
            }
            self._producer = Producer(config)
        return self._producer
    
    def _delivery_callback(self, err, msg):
        """Kafka delivery callback."""
        if err:
            print(f"❌ Message delivery failed: {err}")
    
    def dispatch_job(
        self,
        job_payload: Dict[str, Any],
        pipeline_name: str,
        rate_limit: Optional[float] = None,
    ) -> bool:
        # Check and consume tokens
        if not self.rate_limiter.consume_tokens(pipeline_name, 1, rate_limit):
            return False
        
        # Send to Kafka
        producer = self._get_producer()
        
        try:
            producer.produce(
                topic=self.kafka_topic,
                value=json.dumps(job_payload).encode("utf-8"),
                callback=self._delivery_callback,
            )
            producer.poll(0)  # Trigger callbacks
            return True
        
        except KafkaException as e:
            print(f"❌ Kafka error: {e}")
            return False
    
    def dispatch_jobs_batch(
        self,
        jobs: List[Dict[str, Any]],
        pipeline_name: str,
        rate_limit: Optional[float] = None,
    ) -> int:
        dispatched = 0
        producer = self._get_producer()
        
        for job in jobs:
            # Atomic token acquisition with rate limiting
            if not self.rate_limiter.acquire_token(pipeline_name, rate_limit, max_wait=60.0):
                print(f"⚠️ Rate limit timeout after 60s, stopping dispatch after {dispatched} jobs")
                break
            
            # Dispatch
            try:
                producer.produce(
                    topic=self.kafka_topic,
                    value=json.dumps(job).encode("utf-8"),
                    callback=self._delivery_callback,
                )
                dispatched += 1
                
                # Poll periodically
                if dispatched % 100 == 0:
                    producer.poll(0)
            
            except KafkaException as e:
                print(f"❌ Kafka error: {e}")
                break
        
        # Final flush
        producer.flush(timeout=10.0)
        
        return dispatched
    
    def close(self):
        if self._producer:
            self._producer.flush()
            self._producer = None

# Backward compatibility alias (if needed temporarily, but we will update usages)
JobDispatcher = KafkaDispatcher
