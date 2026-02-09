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
        
    async def dispatch_job(
        self,
        job_payload: Dict[str, Any],
        pipeline_name: str,
        rate_limit: Optional[float] = None,
    ) -> bool:
        """Dispatch single job locally."""
        # Execute job directly using the async executor
        try:
            await self.executor.execute_job(job_payload)
            return True
        except Exception as e:
            print(f"❌ Local dispatch failed: {e}")
            return False

    async def dispatch_jobs_batch(
        self,
        jobs: List[Dict[str, Any]],
        pipeline_name: str,
        rate_limit: Optional[float] = None,
    ) -> int:
        """Dispatch batch locally."""
        count = 0
        
        for job in jobs:
            try:
                await self.executor.execute_job(job)
                count += 1
            except Exception as e:
                print(f"❌ Local job execution failed: {e}")
            
        return count
    
    async def close(self):
        """Close executor resources."""
        await self.executor.close()

