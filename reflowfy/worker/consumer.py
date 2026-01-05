"""Kafka consumer for job processing."""

import json
from typing import Optional
from confluent_kafka import Consumer, KafkaException
from reflowfy.worker.executor import WorkerExecutor


class KafkaJobConsumer:
    """
    Kafka consumer that processes Reflowfy jobs.
    
    Consumes jobs from the reflow.jobs topic and executes them.
    """
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str = "reflowfy-workers",
        auto_offset_reset: str = "earliest",
        database_url: Optional[str] = None,
    ):
        """
        Initialize Kafka consumer.
        
        Args:
            bootstrap_servers: Kafka broker addresses
            topic: Topic to consume from
            group_id: Consumer group ID
            auto_offset_reset: Offset reset strategy
            database_url: PostgreSQL connection URL for job status updates
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        
        # Consumer configuration
        self.config = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": False,  # Manual commit for reliability
        }
        
        self.consumer: Optional[Consumer] = None
        self.executor = WorkerExecutor(database_url=database_url)
        self._running = False
    
    def start(self):
        """Start consuming and processing jobs."""
        self.consumer = Consumer(self.config)
        self.consumer.subscribe([self.topic])
        self._running = True
        
        try:
            while self._running:
                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    raise KafkaException(msg.error())
                
                # Process message
                try:
                    job_payload = json.loads(msg.value().decode("utf-8"))
                    
                    print(f"📦 Received job: {job_payload.get('job_id', 'unknown')}")
                    
                    # Execute job
                    success = self.executor.execute_job(job_payload)
                    
                    if success:
                        # Commit offset on success
                        self.consumer.commit(message=msg)
                    else:
                        # On failure, don't commit - job will be retried
                        print("⚠️  Job failed, will retry")
                
                except json.JSONDecodeError as e:
                    print(f"❌ Invalid job payload: {e}")
                    # Commit anyway to skip bad message
                    self.consumer.commit(message=msg)
                
                except Exception as e:
                    print(f"❌ Job processing error: {e}")
                    # Don't commit - will retry
        
        finally:
            if self.consumer:
                self.consumer.close()
    
    def stop(self):
        """Stop consuming."""
        self._running = False
