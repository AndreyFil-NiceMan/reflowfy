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
