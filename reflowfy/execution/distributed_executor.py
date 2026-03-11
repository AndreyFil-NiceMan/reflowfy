"""Distributed executor with ReflowManager."""

import uuid
from typing import Any, Dict, Optional
import httpx
from reflowfy.execution.base import BaseExecutor, ExecutionStatus, ExecutionState


class DistributedExecutor(BaseExecutor):
    """
    Distributed executor for /run mode.
    
    Forwards pipeline execution requests to ReflowManager's /run endpoint.
    ReflowManager handles:
    1. Loading the pipeline from registry
    2. Splitting source data into jobs
    3. Creating execution and checkpoints
    4. Dispatching jobs to Kafka with rate limiting
    5. Workers process asynchronously
    """
    
    def __init__(
        self,
        reflow_manager_url: str = "http://localhost:8001",
        timeout: float = 120.0,  # Increased from 30s to handle large rate-limited dispatches
        mode: str = "distributed",
    ):
        """
        Initialize distributed executor.
        
        Args:
            reflow_manager_url: ReflowManager service URL
            timeout: HTTP request timeout
            mode: Execution mode ('local' or 'distributed')
        """
        self.reflow_manager_url = reflow_manager_url.rstrip("/")
        self.timeout = timeout
        self.mode = mode
        self._client: Optional[httpx.Client] = None
    
    def _get_client(self) -> httpx.Client:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.Client(timeout=self.timeout)
        return self._client
    

    def execute(
        self,
        pipeline: Any,
        runtime_params: Dict[str, Any],
        execution_id: Optional[str] = None,
        rate_limit_override: Optional[Dict[str, Any]] = None,
    ) -> ExecutionStatus:
        """
        Execute pipeline in distributed mode via ReflowManager.
        
        This simplified version just forwards the request to ReflowManager's /run endpoint.
        The ReflowManager handles:
        - Loading the pipeline
        - Splitting source data into jobs  
        - Creating execution and checkpoints
        - Dispatching jobs to Kafka with rate limiting
        
        Args:
            pipeline: Pipeline instance
            runtime_params: Runtime parameters
            execution_id: Optional execution ID
            rate_limit_override: Optional rate limit override (e.g., {"jobs_per_second": 10})
        
        Returns:
            ExecutionStatus (initial - jobs dispatched but processing async)
        """
        if execution_id is None:
            execution_id = str(uuid.uuid4())
        
        # Initialize status
        status = ExecutionStatus(
            execution_id=execution_id,
            pipeline_name=pipeline.name,
            state=ExecutionState.RUNNING,
        )
        
        try:
            print(f"🚀 Starting distributed execution: {execution_id}")
            print(f"📊 Pipeline: {pipeline.name}")
            print(f"🔗 ReflowManager: {self.reflow_manager_url}")
            
            client = self._get_client()
            
            # Extract rate limit if provided
            rate_limit = None
            if rate_limit_override:
                rate_limit = rate_limit_override.get("jobs_per_second")
            
            # Call ReflowManager's /run endpoint - it handles everything
            print("📝 Sending run request to ReflowManager...")
            run_response = client.post(
                f"{self.reflow_manager_url}/run",
                json={
                    "pipeline_name": pipeline.name,
                    "runtime_params": runtime_params,
                    "execution_id": execution_id,
                    "rate_limit": rate_limit,
                    "mode": self.mode,
                },
            )
            
            # Handle errors
            if run_response.status_code >= 400:
                print(f"DEBUG: Run failed with status {run_response.status_code}")
                print(f"DEBUG: Response body: {run_response.text}")
                run_response.raise_for_status()
            
            result = run_response.json()
            
            total_jobs = result.get("jobs_dispatched", 0)
            
            print(f"✓ Dispatched {total_jobs} jobs via ReflowManager")
            
            # Update status
            status.total_jobs = total_jobs
            status.metadata["jobs_dispatched"] = total_jobs
            status.metadata["reflow_manager_url"] = self.reflow_manager_url
            
            return status
        
        except httpx.HTTPError as e:
            error_msg = f"ReflowManager HTTP error: {e}"
            print(f"❌ {error_msg}")
            
            status.state = ExecutionState.FAILED
            status.error_message = error_msg
            
            return status
        
        except Exception as e:
            import traceback
            error_msg = f"Failed to run pipeline: {e}"
            print(f"❌ {error_msg}")
            traceback.print_exc()
            
            status.state = ExecutionState.FAILED
            status.error_message = error_msg
            
            return status
    

    def close(self):
        """Close HTTP client."""
        if self._client:
            self._client.close()
            self._client = None


def get_executor(mode: str, **kwargs) -> BaseExecutor:
    """
    Factory function to get appropriate executor.
    
    Args:
        mode: Execution mode ('local' or 'distributed')
        **kwargs: Executor-specific configuration
    
    Returns:
        BaseExecutor instance
    """
    if mode == "local":
        from reflowfy.execution.local_executor import LocalExecutor
        return LocalExecutor(max_records=kwargs.get("max_records", 100))
    
    elif mode == "distributed":
        return DistributedExecutor(
            reflow_manager_url=kwargs.get(
                "reflow_manager_url",
                "http://localhost:8001"
            ),
            timeout=kwargs.get("timeout", 30.0),
            mode=kwargs.get("execution_mode", "distributed"),
        )
    
    else:
        raise ValueError(f"Unknown execution mode: {mode}")
