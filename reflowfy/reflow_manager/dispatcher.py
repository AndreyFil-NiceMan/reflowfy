"""Job dispatchers for ReflowManager using aiokafka."""

import os
import json
import asyncio
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from reflowfy.reflow_manager.rate_limiter import RateLimiter


class BaseDispatcher(ABC):
    """Abstract base class for job dispatchers."""
    
    def __init__(self, rate_limiter: RateLimiter):
        self.rate_limiter = rate_limiter

    @abstractmethod
    async def dispatch_job(
        self,
        job_payload: Dict[str, Any],
        pipeline_name: str,
        rate_limit: Optional[float] = None,
    ) -> bool:
        """Dispatch a single job."""
        pass

    @abstractmethod
    async def dispatch_jobs_batch(
        self,
        jobs: List[Dict[str, Any]],
        pipeline_name: str,
        rate_limit: Optional[float] = None,
    ) -> int:
        """Dispatch a batch of jobs."""
        pass

    @abstractmethod
    async def close(self):
        """Close resources."""
        pass


class KafkaDispatcher(BaseDispatcher):
    """
    Dispatches jobs to Kafka with rate limiting using aiokafka.
    """
    
    def __init__(
        self,
        kafka_bootstrap_servers: str,
        kafka_topic: str,
        rate_limiter: RateLimiter,
        # SASL Authentication
        security_protocol: Optional[str] = None,
        sasl_mechanism: Optional[str] = None,
        sasl_username: Optional[str] = None,
        sasl_password: Optional[str] = None,
    ):
        super().__init__(rate_limiter)
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        
        # SASL config
        self.security_protocol = security_protocol
        self.sasl_mechanism = sasl_mechanism
        self.sasl_username = sasl_username
        self.sasl_password = sasl_password
        
        self._producer: Optional[AIOKafkaProducer] = None
        self._producer_loop: Optional[asyncio.AbstractEventLoop] = None
        self._started = False
    
    async def _get_producer(self) -> AIOKafkaProducer:
        """Get or create Kafka producer."""
        loop = asyncio.get_running_loop()
        
        # Check if existing producer is bound to a different or closed loop
        if self._producer and (self._producer_loop is None or self._producer_loop != loop or self._producer_loop.is_closed()):
            print("🔄 Detected event loop change, resetting Kafka producer")
            # We cannot strictly close() the old producer if its loop is closed, just discard it
            self._producer = None
            self._producer_loop = None
            self._started = False

        if self._producer is None or not self._started:
            # Build producer kwargs
            producer_kwargs = {
                "bootstrap_servers": self.kafka_bootstrap_servers,
                "compression_type": "gzip",
            }
            
            # Add SASL config if credentials provided
            if self.sasl_username and self.sasl_password:
                producer_kwargs.update({
                    "security_protocol": self.security_protocol or "SASL_PLAINTEXT",
                    "sasl_mechanism": self.sasl_mechanism or "SCRAM-SHA-256",
                    "sasl_plain_username": self.sasl_username,
                    "sasl_plain_password": self.sasl_password,
                    "client_id": self.sasl_username,  # client_id = username
                })
            
            self._producer = AIOKafkaProducer(**producer_kwargs)
            await self._producer.start()
            self._producer_loop = loop
            self._started = True
        
        return self._producer
    
    async def dispatch_job(
        self,
        job_payload: Dict[str, Any],
        pipeline_name: str,
        rate_limit: Optional[float] = None,
    ) -> bool:
        """Dispatch a single job to Kafka."""
        # Check and consume tokens
        if not self.rate_limiter.consume_tokens(pipeline_name, 1, rate_limit):
            return False
        
        # Send to Kafka
        producer = await self._get_producer()
        
        try:
            await producer.send_and_wait(
                topic=self.kafka_topic,
                value=json.dumps(job_payload).encode("utf-8"),
            )
            return True
        
        except KafkaError as e:
            print(f"❌ Kafka error: {e}")
            return False
    
    async def dispatch_jobs_batch(
        self,
        jobs: List[Dict[str, Any]],
        pipeline_name: str,
        rate_limit: Optional[float] = None,
    ) -> int:
        """Dispatch a batch of jobs to Kafka."""
        dispatched = 0
        producer = await self._get_producer()
        
        for job in jobs:
            # Atomic token acquisition with rate limiting
            if not self.rate_limiter.acquire_token(pipeline_name, rate_limit, max_wait=60.0):
                print(f"⚠️ Rate limit timeout after 60s, stopping dispatch after {dispatched} jobs")
                break
            
            # Dispatch
            try:
                await producer.send_and_wait(
                    topic=self.kafka_topic,
                    value=json.dumps(job).encode("utf-8"),
                )
                dispatched += 1
            
            except KafkaError as e:
                print(f"❌ Kafka error: {e}")
                break
        
        return dispatched
    
    async def close(self):
        """Close the producer connection."""
        if self._producer and self._started:
            await self._producer.stop()
            self._producer = None
            self._started = False


# Backward compatibility alias
JobDispatcher = KafkaDispatcher
