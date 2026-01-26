"""Local job dispatcher for ReflowManager."""

import asyncio
from typing import Dict, Any, List, Optional

from reflowfy.reflow_manager.dispatcher import BaseDispatcher
from reflowfy.reflow_manager.rate_limiter import RateLimiter




class LocalDispatcher(BaseDispatcher):
    """
    Dispatches jobs locally by running them in-process.
    Uses WorkerExecutor to process jobs immediately.
    """
    
    def __init__(self, rate_limiter: RateLimiter, db_session=None):
        from reflowfy.worker.executor import WorkerExecutor
        super().__init__(rate_limiter)
        # We need the database URL for WorkerExecutor
        # For now, we'll let WorkerExecutor find it from env, or pass it if available
        self.executor = WorkerExecutor()
        self._loop = asyncio.get_event_loop()
        
    def dispatch_job(
        self,
        job_payload: Dict[str, Any],
        pipeline_name: str,
        rate_limit: Optional[float] = None,
    ) -> bool:
        """Dispatch single job locally."""
        # Simple local execution: fire and forget (background task)
        # We don't need rate limiting for local test usually, but we respect the interface
        
        # Bridge to async execution
        try:
             # If we are in a running loop (FastAPI), create a task
            loop = asyncio.get_running_loop()
            loop.create_task(self.executor.execute_job(job_payload))
            return True
        except RuntimeError:
            # If no running loop (e.g. script), run synchronously
            asyncio.run(self.executor.execute_job(job_payload))
            return True

    def dispatch_jobs_batch(
        self,
        jobs: List[Dict[str, Any]],
        pipeline_name: str,
        rate_limit: Optional[float] = None,
    ) -> int:
        """Dispatch batch locally."""
        count = 0
        loop = None
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            pass

        for job in jobs:
            if loop:
                loop.create_task(self.executor.execute_job(job))
            else:
                asyncio.run(self.executor.execute_job(job))
            count += 1
            
        return count
    
    def close(self):
        # Executor close is async, might need handling
        pass
