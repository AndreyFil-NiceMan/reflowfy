"""Query template loading shared by the pipeline base classes.

Pipelines keep their query templates in the project's ``queries/`` folder and
read them with :meth:`QueryLoaderMixin.load_query`, so no pipeline has to build
paths or open files itself. Mixed into both
:class:`~reflowfy.core.abstract_pipeline.AbstractPipeline` and
:class:`~reflowfy.core.id_based_pipeline.IdBasedPipeline`.
"""

import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional


class QueryLoaderMixin:
    """Reads query templates from the project's ``queries/`` folder."""

    _query_text_cache: Optional[Dict[str, str]] = None

    def load_query(self, filename: str) -> Any:
        """Load a query template from the project's ``queries/`` folder.

        ``.json`` files are parsed to a dict/list; every other extension is
        returned as raw text. The ``queries/`` folder is discovered
        automatically (searched upward from this pipeline's module file) and
        searched recursively by filename, so subfolders are supported::

            def define_source(self, runtime_params):
                return sql_source(query=self.load_query("events_by_date.sql"))

        Args:
            filename: The query file's name, e.g. ``"events_by_date.sql"``.
                Only the filename is used; the folder is located automatically.

        Raises:
            FileNotFoundError: No file with that name exists under ``queries/``.
            ValueError: The name matches files in more than one subfolder.
        """
        path = self._resolve_query_path(filename)
        text = self._read_query_file(path)
        if path.suffix.lower() == ".json":
            return json.loads(text)
        return text

    def load_query_text(self, filename: str) -> str:
        """Load a query template as raw text, without extension-based parsing.

        Use this when a ``.json`` file should be returned verbatim instead of
        being parsed into a dict. See :meth:`load_query` for lookup semantics.
        """
        return self._read_query_file(self._resolve_query_path(filename))

    def _read_query_file(self, path: Path) -> str:
        """Read a resolved query file, caching text by absolute path."""
        if self._query_text_cache is None:
            self._query_text_cache = {}
        key = str(path)
        if key not in self._query_text_cache:
            self._query_text_cache[key] = path.read_text()
        return self._query_text_cache[key]

    def _resolve_query_path(self, filename: str) -> Path:
        """Find ``filename`` under the project's ``queries/`` folder.

        The folder is located by walking upward from this pipeline's defining
        module file and is searched recursively by filename.
        """
        queries_dir = self._resolve_queries_dir()
        matches = sorted(queries_dir.rglob(filename))
        if not matches:
            raise FileNotFoundError(f"Query '{filename}' not found under {queries_dir}")
        if len(matches) > 1:
            relative = ", ".join(str(m.relative_to(queries_dir)) for m in matches)
            raise ValueError(
                f"Query '{filename}' is ambiguous: found {len(matches)} matches "
                f"under {queries_dir} ({relative}). Make query filenames unique "
                "across subfolders."
            )
        return matches[0]

    def _resolve_queries_dir(self) -> Path:
        """Locate the ``queries/`` directory for this pipeline.

        Walks upward from the pipeline's module file looking for a ``queries/``
        subdirectory (covers both the file-adjacent and project-root layouts).
        Falls back to ``<cwd>/queries`` when the module file is unavailable.
        """
        module = sys.modules.get(type(self).__module__)
        module_file = getattr(module, "__file__", None)
        if module_file:
            for parent in Path(module_file).resolve().parents:
                candidate = parent / "queries"
                if candidate.is_dir():
                    return candidate
        return Path.cwd() / "queries"
