"""Worker job executor."""

import time
import httpx
import traceback
from typing import Any, Dict, Optional
from reflowfy.transformations.registry import transformation_registry
from reflowfy.destinations.kafka import KafkaDestination
from reflowfy.destinations.http import HttpDestination
from reflowfy.destinations.console import ConsoleDestination


class JobStats:
    """Statistics for a job execution."""
    
    def __init__(self):
        """Initialize job statistics."""
        self.start_time = time.time()
        self.end_time = None
        self.records_input = 0
        self.records_output = 0
        self.transformation_times = {}
        self.destination_write_time = 0
        self.error = None
        self.success = False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        duration = self.end_time - self.start_time if self.end_time else 0
        
        return {
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": round(duration, 3),
            "records_input": self.records_input,
            "records_output": self.records_output,
            "throughput_records_per_second": round(self.records_output / duration, 2) if duration > 0 else 0,
            "transformation_times": self.transformation_times,
            "destination_write_time": round(self.destination_write_time, 3),
            "error": self.error,
            "success": self.success,
        }



class WorkerExecutor:
    """
    Executes jobs on worker nodes.
    
    Responsibilities:
    1. Load transformations from registry
    2. Apply transformations to records
    3. Check destination health
    4. Send to destination with retries
    5. Rate limiting
    6. Report statistics to ReflowManager
    """
    
    def __init__(self, reflow_manager_url: str = "http://localhost:8001"):
        """
        Initialize worker executor.
        
        Args:
            reflow_manager_url: ReflowManager service URL
        """

        self.reflow_manager_url = reflow_manager_url.rstrip("/")
        self._client: Optional[httpx.Client] = None
    
    def _get_client(self) -> httpx.Client:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.Client(timeout=10.0)
        return self._client
    
    def execute_job(self, job_payload: Dict[str, Any]) -> bool:
        """
        Execute a single job.
        
        Args:
            job_payload: Job payload from Kafka
        
        Returns:
            True if successful, False otherwise
        """
        # Initialize statistics
        stats = JobStats()
        
        execution_id = job_payload.get("execution_id", "unknown")
        job_id = job_payload.get("job_id", "unknown")
        pipeline_name = job_payload.get("pipeline_name", "unknown")
        
        try:
            # Extract job data
            transformation_names = job_payload.get("transformations", [])
            destination_config = job_payload.get("destination", {})
            records = job_payload.get("records", [])
            metadata = job_payload.get("metadata", {})
            
            # Track input records
            stats.records_input = len(records)
            
            if not records:
                print(f"⚠️  Job {job_id}: No records to process")
                stats.success = True
                stats.records_output = 0
                stats.end_time = time.time()
                self._report_job_completion(execution_id, job_id, stats)
                return True
            
            print(f"🔄 Processing job {job_id}: {len(records)} records")
            
            # Load and apply transformations
            transformed_records = records
            
            for transformation_name in transformation_names:
                print(f"  🔄 Applying: {transformation_name}")
                
                # Track transformation start time
                transform_start = time.time()
                
                # Load transformation from registry
                transformation = transformation_registry.create_instance(transformation_name)
                
                # Apply transformation
                transformed_records = transformation.apply(transformed_records, metadata)
                
                # Track transformation time
                transform_duration = time.time() - transform_start
                stats.transformation_times[transformation_name] = round(transform_duration, 3)
                
                print(f"  ✓ {transformation_name}: {len(transformed_records)} records ({transform_duration:.2f}s)")
            
            # Track output records
            stats.records_output = len(transformed_records)
            
            # Create destination instance
            destination = self._create_destination(destination_config)
            
            # Health check
            if not destination.health_check():
                print(f"❌ Destination health check failed")
                stats.success = False
                stats.error = "Destination health check failed"
                stats.end_time = time.time()
                self._report_job_completion(execution_id, job_id, stats)
                return False
            
            # Send to destination and track time
            print(f"  📤 Sending {len(transformed_records)} records to destination...")
            dest_start = time.time()
            destination.send_with_retry(transformed_records, metadata)
            stats.destination_write_time = time.time() - dest_start
            
            # Mark as successful
            stats.success = True
            stats.end_time = time.time()
            
            print(f"✓ Job {job_id} completed successfully (duration: {stats.end_time - stats.start_time:.2f}s)\n")
            
            # Report to ReflowManager
            self._report_job_completion(execution_id, job_id, stats)
            
            return True
        
        except Exception as e:
            print(f"❌ Job {job_id} failed: {e}")
            traceback.print_exc()
            
            # Mark as failed
            stats.success = False
            stats.error = str(e)
            stats.end_time = time.time()
            
            # Report failure to ReflowManager
            self._report_job_completion(execution_id, job_id, stats)
            
            return False
    
    def _report_job_completion(
        self,
        execution_id: str,
        job_id: str,
        stats: JobStats
    ):
        """
        Report job completion to ReflowManager.
        
        Args:
            execution_id: Execution ID
            job_id: Batch/job ID
            stats: Job statistics
        """
        try:
            client = self._get_client()
            
            response = client.patch(
                f"{self.reflow_manager_url}/checkpoints/{job_id}",
                json={
                    "state": "completed" if stats.success else "failed",
                    "processed_records": stats.records_output,
                    "error_message": stats.error,
                    "stats": stats.to_dict(),
                }
            )
            response.raise_for_status()
            
            print(f"  ✓ Reported stats to ReflowManager")
            
        except Exception as e:
            print(f"  ⚠️  Failed to report to ReflowManager: {e}")
            # Don't fail the job if reporting fails
    

    
    def _create_destination(self, destination_config: Dict[str, Any]) -> Any:
        """
        Create destination instance from config.
        
        Args:
            destination_config: Destination configuration
        
        Returns:
            Destination instance
        """
        dest_type = destination_config.get("type", "")
        config = destination_config.get("config", {})
        
        if dest_type == "KafkaDestination":
            return KafkaDestination(**config)
        elif dest_type == "HttpDestination":
            return HttpDestination(**config)
        elif dest_type == "ConsoleDestination":
            return ConsoleDestination(**config)
        else:
            raise ValueError(f"Unknown destination type: {dest_type}")
    
    def close(self):
        """Close HTTP client."""
        if self._client:
            self._client.close()
            self._client = None
