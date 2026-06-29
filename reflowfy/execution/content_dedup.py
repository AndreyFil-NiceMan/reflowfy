"""Worker-side content deduplication primitive.

`compute_content_hash` reproduces the v1 deterministic hash (pipeline name +
transformation names + record content). The async claim/release helpers run
against PostgreSQL using whatever AsyncSession factory the worker provides.
"""

import hashlib
import json
from typing import Any, List

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import delete

from reflowfy.reflow_manager.models import ProcessedContent


def compute_content_hash(
    pipeline_name: str,
    transformation_names: List[str],
    records: List[Any],
) -> str:
    """Deterministic SHA256 over stable job content (v1 semantics)."""
    stable = {
        "pipeline_name": pipeline_name,
        "transformations": sorted(transformation_names),
        "records": records,
    }
    content = json.dumps(stable, sort_keys=True, default=str)
    return hashlib.sha256(content.encode()).hexdigest()


async def claim_content_hash(
    session_factory: Any,
    content_hash: str,
    pipeline_name: str,
    job_id: str,
    execution_id: str,
) -> bool:
    """Atomically claim a content hash. Returns True iff this caller inserted it."""
    async with session_factory() as db:
        stmt = (
            pg_insert(ProcessedContent)
            .values(
                content_hash=content_hash,
                pipeline_name=pipeline_name,
                job_id=job_id,
                execution_id=execution_id,
            )
            .on_conflict_do_nothing(index_elements=["content_hash"])
        )
        result = await db.execute(stmt)
        await db.commit()
        return (result.rowcount or 0) == 1


async def release_content_hash(session_factory: Any, content_hash: str, job_id: str) -> None:
    """Release this caller's own claim so a retry can reprocess."""
    async with session_factory() as db:
        stmt = delete(ProcessedContent).where(
            ProcessedContent.content_hash == content_hash,
            ProcessedContent.job_id == job_id,
        )
        await db.execute(stmt)
        await db.commit()
