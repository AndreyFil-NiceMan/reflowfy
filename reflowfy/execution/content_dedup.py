"""Worker-side content deduplication primitive.

`compute_content_hash` is a deterministic hash of a job's work: pipeline name +
transformation names + record content + where it is written (the destination). The async claim/release helpers run
against PostgreSQL using whatever AsyncSession factory the worker provides.
"""

import hashlib
import json
import os
from typing import Any, Callable, Dict, List, Optional, Sequence, cast

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import delete, select

from reflowfy.reflow_manager.models import ProcessedContent

# Config keys left out of a destination's identity: rotating a credential must not
# make already-written content look new. Matched as substrings of the key lowercased
# with "-" and "_" removed, so api_key, apiKey and an X-API-Key header all match.
_SECRET_MARKERS = ("password", "secret", "token", "credential", "auth", "apikey")


# How keys were matched before hyphenated keys (an X-API-Key header) counted as secrets.
_PREVIOUS_SECRET_MARKERS = (
    "password",
    "secret",
    "token",
    "credential",
    "auth",
    "api_key",
    "apikey",
)


def _is_secret_key(key: Any) -> bool:
    normalized = str(key).lower().replace("-", "").replace("_", "")
    return any(m in normalized for m in _SECRET_MARKERS)


def _was_secret_key(key: Any) -> bool:
    return any(m in str(key).lower() for m in _PREVIOUS_SECRET_MARKERS)


def _strip_secrets(value: Any, is_secret: Callable[[Any], bool] = _is_secret_key) -> Any:
    if isinstance(value, dict):
        mapping = cast(Dict[Any, Any], value)
        return {k: _strip_secrets(v, is_secret) for k, v in mapping.items() if not is_secret(k)}
    if isinstance(value, list):
        return [_strip_secrets(v, is_secret) for v in cast(List[Any], value)]
    return value


def previous_destination_identity(destination: Any) -> Dict[str, Any]:
    """destination_identity as the previous release computed it (hyphenated secret
    keys kept). Claims made under it are honored like the legacy hash, so upgrading
    does not rewrite content already written to such a destination."""
    return _identity(destination, _was_secret_key)


def destination_identity(destination: Any) -> Dict[str, Any]:
    """Where a job writes: the destination's class plus its config, minus credentials.

    Part of the content hash so the same records sent somewhere else (another
    topic, another URL) are new work, not a duplicate of the first write.

    The whole config counts, including an ``api_destination``'s request body. That errs
    on the safe side — a body that differs only by a timestamp or request id is treated
    as new work (a duplicate write, never a lost one) — but it also means dedup never
    matches such a destination. Declare what actually identifies the destination with
    ``api_destination(..., dedup_identity=...)`` to opt in.
    """
    return _identity(destination, _is_secret_key)


def _identity(destination: Any, is_secret: Callable[[Any], bool]) -> Dict[str, Any]:
    explicit = getattr(destination, "dedup_identity", None)
    if explicit is not None:
        return {
            "type": type(destination).__name__,
            "identity": _strip_secrets(explicit, is_secret),
        }
    config = getattr(destination, "config", None)
    return {
        "type": type(destination).__name__,
        "config": _strip_secrets(config, is_secret) if isinstance(config, dict) else {},
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


def honor_legacy_hash() -> bool:
    """Whether claims recorded before destinations joined the hash still block.

    Those claims carry no destination, so honoring them can wrongly block the same
    records going somewhere new — but only until the sweeper's retention (24h)
    has expired them, and the alternative is writing everything a second time at
    upgrade. Set DEDUP_HONOR_LEGACY_HASH=false once that window has passed.
    """
    return os.getenv("DEDUP_HONOR_LEGACY_HASH", "true").strip().lower() not in ("0", "false", "no")


async def claim_content_hash(
    session_factory: Any,
    content_hash: str,
    pipeline_name: str,
    job_id: str,
    execution_id: str,
    legacy_hash: Optional[str] = None,
    previous_hashes: Sequence[str] = (),
) -> bool:
    """Atomically claim a content hash. True if this job holds it.

    ``legacy_hash`` is the same content hashed the way it was before destinations
    were included, and ``previous_hashes`` the way earlier releases hashed it with
    a destination. If another job already holds any of them (claimed before the
    upgrade), this is a duplicate and nothing is claimed.

    True when this call inserted the claim, or when the claim already belongs to
    this same job_id: a job re-run after its worker died (lease re-dispatch, or a
    SIGKILL that skipped the release) must not be marked a duplicate of itself —
    that dropped the unwritten rest of the job.
    """
    async with session_factory() as db:
        older = [h for h in (legacy_hash, *previous_hashes) if h and h != content_hash]
        if older and honor_legacy_hash():
            for older_hash in dict.fromkeys(older):
                legacy_owner = (
                    await db.execute(
                        select(ProcessedContent.job_id).where(
                            ProcessedContent.content_hash == older_hash
                        )
                    )
                ).scalar_one_or_none()
                if legacy_owner is not None and legacy_owner != job_id:
                    return False
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
