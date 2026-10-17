"""Worker-side content deduplication primitive.

`compute_content_hash` is a deterministic hash of a job's work: pipeline name +
transformation names + record content + where it is written (the destination). The async claim/release helpers run
against PostgreSQL using whatever AsyncSession factory the worker provides.
"""

import hashlib
import json
from typing import Any, Dict, List, Optional, cast

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import delete, select

from reflowfy.reflow_manager.models import ProcessedContent


# Config keys left out of a destination's identity: rotating a credential must not
# make already-written content look new. Matched as substrings, case-insensitive.
_SECRET_MARKERS = ("password", "secret", "token", "credential", "auth", "api_key", "apikey")


def _strip_secrets(value: Any) -> Any:
    if isinstance(value, dict):
        mapping = cast(Dict[Any, Any], value)
        return {
            k: _strip_secrets(v)
            for k, v in mapping.items()
            if not any(m in str(k).lower() for m in _SECRET_MARKERS)
        }
    if isinstance(value, list):
        return [_strip_secrets(v) for v in cast(List[Any], value)]
    return value


def destination_identity(destination: Any) -> Dict[str, Any]:
    """Where a job writes: the destination's class plus its config, minus credentials.

    Part of the content hash so the same records sent somewhere else (another
    topic, another URL) are new work, not a duplicate of the first write.
    """
    config = getattr(destination, "config", None)
    return {
        "type": type(destination).__name__,
        "config": _strip_secrets(config) if isinstance(config, dict) else {},
    }


def compute_content_hash(
    pipeline_name: str,
    transformation_names: List[str],
    records: List[Any],
    job_params: Optional[Dict[str, Any]] = None,
    destination: Optional[Dict[str, Any]] = None,
) -> str:
    """Deterministic SHA256 over stable job content (v1 semantics).

    ``job_params`` is what the plan attached to this one job (see
    :func:`reflowfy.job`). Two jobs can hold identical records and still be
    different work — the same rows for a different tenant, the same children
    under a different parent — so those params are part of a job's identity.
    Leaving them out silently dropped the second job as a duplicate.

    Only included when non-empty, so jobs that carry no per-job params keep the
    hashes they already have.

    ``destination`` is :func:`destination_identity` of the job's destination.
    Without it, re-running the same data to a different topic was dropped as a
    duplicate and nothing was written.
    """
    stable: Dict[str, Any] = {
        "pipeline_name": pipeline_name,
        "transformations": sorted(transformation_names),
        "records": records,
    }
    if job_params:
        stable["job_params"] = job_params
    if destination:
        stable["destination"] = destination
    content = json.dumps(stable, sort_keys=True, default=str)
    return hashlib.sha256(content.encode()).hexdigest()


async def claim_content_hash(
    session_factory: Any,
    content_hash: str,
    pipeline_name: str,
    job_id: str,
    execution_id: str,
) -> bool:
    """Atomically claim a content hash. True if this job holds it.

    True when this call inserted the claim, or when the claim already belongs to
    this same job_id: a job re-run after its worker died (lease re-dispatch, or a
    SIGKILL that skipped the release) must not be marked a duplicate of itself —
    that dropped the unwritten rest of the job.
    """
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
        if (result.rowcount or 0) == 1:
            return True
        owner = await db.execute(
            select(ProcessedContent.job_id).where(ProcessedContent.content_hash == content_hash)
        )
        return bool(owner.scalar_one_or_none() == job_id)


async def release_content_hash(session_factory: Any, content_hash: str, job_id: str) -> None:
    """Release this caller's own claim so a retry can reprocess."""
    async with session_factory() as db:
        stmt = delete(ProcessedContent).where(
            ProcessedContent.content_hash == content_hash,
            ProcessedContent.job_id == job_id,
        )
        await db.execute(stmt)
        await db.commit()
