"""Async Kafka consumer for job processing."""

import json
import asyncio
from typing import Optional
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from reflowfy.worker.executor import WorkerExecutor


class KafkaJobConsumer:
    """
    Async Kafka consumer that processes Reflowfy jobs.
    
    Consumes jobs from the reflow.jobs topic and executes them asynchronously.
    """
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str = "reflowfy-workers",
        auto_offset_reset: str = "earliest",
        database_url: Optional[str] = None,
        # SASL Authentication
        security_protocol: Optional[str] = None,
        sasl_mechanism: Optional[str] = None,
        sasl_username: Optional[str] = None,
        sasl_password: Optional[str] = None,
    ):
        """
        Initialize async Kafka consumer.
        
        Args:
            bootstrap_servers: Kafka broker addresses
            topic: Topic to consume from
            group_id: Consumer group ID
            auto_offset_reset: Offset reset strategy
            database_url: PostgreSQL connection URL for job status updates
            security_protocol: Security protocol (e.g., SASL_PLAINTEXT)
            sasl_mechanism: SASL mechanism (e.g., SCRAM-SHA-256)
            sasl_username: SASL username
            sasl_password: SASL password
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        
        # SASL config
        self.security_protocol = security_protocol
        self.sasl_mechanism = sasl_mechanism
        self.sasl_username = sasl_username
        self.sasl_password = sasl_password
        
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.executor = WorkerExecutor(database_url=database_url)
        self._running = False
    
    async def start(self):
        """Start consuming and processing jobs asynchronously."""
        # Build consumer kwargs
        consumer_kwargs = {
            "bootstrap_servers": self.bootstrap_servers,
            "group_id": self.group_id,
            "auto_offset_reset": self.auto_offset_reset,
            "enable_auto_commit": False,  # Manual commit for reliability
            "retry_backoff_ms": 500,
            "metadata_max_age_ms": 30000,
        }
        
        # Add SASL config if credentials provided
        if self.sasl_username and self.sasl_password:
            consumer_kwargs.update({
                "security_protocol": self.security_protocol or "SASL_PLAINTEXT",
                "sasl_mechanism": self.sasl_mechanism or "SCRAM-SHA-256",
                "sasl_plain_username": self.sasl_username,
                "sasl_plain_password": self.sasl_password,
                "client_id": self.sasl_username,  # client_id = username
            })
        
        self.consumer = AIOKafkaConsumer(self.topic, **consumer_kwargs)
        
        # Retry starting consumer with backoff (handle GroupCoordinatorNotAvailableError)
        max_retries = 10
        for attempt in range(max_retries):
            try:
                await self.consumer.start()
                print(f"✓ Kafka consumer connected successfully")
                break
            except KafkaError as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    print(f"⚠️  Failed to start consumer (attempt {attempt + 1}/{max_retries}): {e}")
                    print(f"   Retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    print(f"❌ Failed to start consumer after {max_retries} attempts")
                    raise
        
        self._running = True
        
        try:
            async for msg in self.consumer:
                if not self._running:
                    break
                
                # Process message
                try:
                    job_payload = json.loads(msg.value.decode("utf-8"))
                    
                    print(f"📦 Received job: {job_payload.get('job_id', 'unknown')}")
                    
                    # Execute job asynchronously
                    success = await self.executor.execute_job(job_payload)
                    
                    if success:
                        # Commit offset on success
                        await self.consumer.commit()
                    else:
                        # On failure, don't commit - job will be retried
                        print("⚠️  Job failed, will retry")
                
                except json.JSONDecodeError as e:
                    print(f"❌ Invalid job payload: {e}")
                    # Commit anyway to skip bad message
                    await self.consumer.commit()
                
                except Exception as e:
                    print(f"❌ Job processing error: {e}")
                    # Don't commit - will retry
        
        finally:
            await self.consumer.stop()
            await self.executor.close()
    
    async def stop(self):
        """Stop consuming."""
        self._running = False
