"""Worker-side content deduplication primitives.

Dedup is per record: `compute_record_hash` identifies one record's write (pipeline +
the record or its `dedup_key` + destination + the job's own params), and the worker
claims those hashes in `processed_records` before sending, dropping the records
another job already claimed. The async claim/release helpers run against
PostgreSQL using whatever AsyncSession factory the worker provides.

`compute_content_hash` is the previous, job-level hash. It is kept only for the
upgrade guard (`held_by_other_job`): claims the previous release made in
`processed_content` still mark a job as already written.
"""

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Union, cast

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import delete, select

from reflowfy.core.serialization import to_json_safe
from reflowfy.reflow_manager.models import ProcessedContent, ProcessedRecord

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

    Part of every record's hash so the same records sent somewhere else (another
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
    identity_config = getattr(destination, "identity_config", None)
    config = (
        identity_config() if callable(identity_config) else getattr(destination, "config", None)
    )
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
    """Deterministic SHA256 over a whole job's content — the previous, job-level hash.

    Only the upgrade guard uses it now (see :func:`held_by_other_job`).

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
    """Whether job-level claims made by earlier releases still block.

    Dedup used to claim one hash per job (and before that, one without the
    destination). Honoring those claims can wrongly block content going somewhere
    new — but only until the sweeper's retention (24h) has expired them, and the
    alternative is writing everything a second time at upgrade. Set
    DEDUP_HONOR_LEGACY_HASH=false once that window has passed.
    """
    return os.getenv("DEDUP_HONOR_LEGACY_HASH", "true").strip().lower() not in ("0", "false", "no")


async def held_by_other_job(session_factory: Any, job_hashes: Sequence[str], job_id: str) -> bool:
    """Whether another job holds one of these job-level hashes in processed_content.

    Transition guard: dedup used to claim one hash per job. A job whose old hash
    another job already holds was written by the previous release, so it is a
    duplicate even though none of its record hashes are claimed yet. Only checked
    while :func:`honor_legacy_hash` is on.
    """
    hashes = list(dict.fromkeys(h for h in job_hashes if h))
    if not hashes:
        return False
    async with session_factory() as db:
        owners = (
            await db.execute(
                select(ProcessedContent.job_id).where(ProcessedContent.content_hash.in_(hashes))
            )
        ).scalars()
        return any(owner != job_id for owner in owners)


# Plan keys that only say which slice a job is. Two jobs can carry the same record
# under different current_ids (overlapping slices), and that must still dedup.
_SLICE_PARAMS = ("current_id", "current_ids", "input_ids")

# Rows per INSERT/DELETE statement when claiming or releasing record hashes.
CLAIM_CHUNK_SIZE = 1000

DedupKey = Union[str, Callable[[Any, Any], Any]]


def record_identity(record: Any, dedup_key: Optional[DedupKey], runtime_params: Any) -> Any:
    """What identifies one record for dedup: the whole record, one field, or a key function.

    A ``str`` key is looked up in the record (``KeyError`` if missing — the job fails
    rather than sending unchecked); a callable gets ``(record, runtime_params)``.
    """
    if dedup_key is None:
        return to_json_safe(record)
    if isinstance(dedup_key, str):
        return to_json_safe(record[dedup_key])
    return to_json_safe(dedup_key(record, runtime_params))


def compute_record_hash(
    pipeline_name: str,
    identity: Any,
    destination: Optional[Dict[str, Any]] = None,
    job_params: Optional[Dict[str, Any]] = None,
) -> bytes:
    """SHA256 digest identifying one record's write.

    Covers the pipeline, the record's identity (see :func:`record_identity`), the
    destination (:func:`destination_identity`) and the params the plan attached to
    the job with :func:`reflowfy.job` — the same rows for another tenant are new
    work. Slice keys (``current_ids`` et al.) are left out so overlapping slices
    still match. Transformation names are left out: the hash is over what is sent.
    """
    stable: Dict[str, Any] = {"pipeline_name": pipeline_name, "record": identity}
    if destination:
        stable["destination"] = destination
    own = {k: v for k, v in (job_params or {}).items() if k not in _SLICE_PARAMS}
    if own:
        stable["job_params"] = own
    content = json.dumps(stable, sort_keys=True, default=str)
    return hashlib.sha256(content.encode()).digest()


def _chunks(items: List[bytes], size: int = CLAIM_CHUNK_SIZE) -> List[List[bytes]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


async def claim_record_hashes(
    session_factory: Any, hashes: Sequence[bytes], job_id: str
) -> Set[bytes]:
    """Atomically claim record hashes. Returns the ones this job holds.

    A hash counts as held when this call inserted it or when this same job_id
    already holds it: a job re-run after its worker died (lease re-dispatch, or a
    SIGKILL that skipped the release) must not be a duplicate of itself.
    """
    unique = list(dict.fromkeys(hashes))
    won: Set[bytes] = set()
    if not unique:
        return won
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    async with session_factory() as db:
        for part in _chunks(unique):
            stmt = (
                pg_insert(ProcessedRecord)
                .values([{"record_hash": h, "job_id": job_id, "created_at": now} for h in part])
                .on_conflict_do_nothing(index_elements=["record_hash"])
                .returning(ProcessedRecord.record_hash)
            )
            inserted = set((await db.execute(stmt)).scalars())
            won |= inserted
            taken = [h for h in part if h not in inserted]
            if taken:
                own = await db.execute(
                    select(ProcessedRecord.record_hash).where(
                        ProcessedRecord.record_hash.in_(taken), ProcessedRecord.job_id == job_id
                    )
                )
                won |= set(own.scalars())
        await db.commit()
    return won


async def release_record_hashes(session_factory: Any, hashes: Sequence[bytes], job_id: str) -> None:
    """Release this job's own record claims so a retry can send them."""
    async with session_factory() as db:
        for part in _chunks(list(dict.fromkeys(hashes))):
            await db.execute(
                delete(ProcessedRecord).where(
                    ProcessedRecord.record_hash.in_(part), ProcessedRecord.job_id == job_id
                )
            )
        await db.commit()
