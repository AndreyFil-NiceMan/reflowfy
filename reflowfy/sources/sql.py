"""SQL database source connector."""

from typing import Any, Dict, Iterator, List, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from reflowfy.sources.base import BaseSource, SourceJob, SourceError


class SqlSource(BaseSource):
    """
    SQL database source connector.

    Supports:
    - ID range splitting for parallel processing
    - Time window splitting
    - Custom query with runtime parameters
    - Multiple database backends (Postgres, MySQL, etc.)
    """

    def __init__(
        self,
        connection_url: str,
        query: str,
        id_column: Optional[str] = None,
        time_column: Optional[str] = None,
        batch_size: int = 1000,
        **engine_kwargs,
    ):
        """
        Initialize SQL source.

        Args:
            connection_url: SQLAlchemy connection URL
            query: SQL query (supports Jinja2 templates)
            id_column: Column for ID range splitting
            time_column: Column for time window splitting
            batch_size: Records per job
            **engine_kwargs: Additional SQLAlchemy engine parameters
        """
        config = {
            "connection_url": connection_url,
            "query": query,
            "id_column": id_column,
            "time_column": time_column,
            "batch_size": batch_size,
            **engine_kwargs,
        }
        super().__init__(config)
        self._engine: Optional[Engine] = None

    def _get_engine(self) -> Engine:
        """Get or create SQLAlchemy engine."""
        if self._engine is None:
            self._engine = create_engine(
                self.config["connection_url"],
                pool_pre_ping=True,
            )
        return self._engine

    def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> List[Any]:
        """
        Fetch data from SQL database (local mode).

        Args:
            runtime_params: Runtime parameters for query template
            limit: Optional limit for testing

        Returns:
            List of records as dictionaries
        """
        resolved_config = self.resolve_parameters(runtime_params)
        if resolved_config is None:
            raise SourceError("sql", "No valid configuration resolved", None)
        engine = self._get_engine()

        try:
            query = resolved_config["query"]

            # Add LIMIT if specified
            if limit:
                query = f"{query} LIMIT {limit}"

            with engine.connect() as conn:
                result = conn.execute(text(query))
                return [dict(row._mapping) for row in result]

        except SQLAlchemyError as e:
            raise SourceError("sql", f"Failed to fetch data: {e}", e)

    def split(self, runtime_params: Dict[str, Any]) -> Iterator["SqlSource"]:
        """Plan id-range windows using MIN/MAX only — no row fetch.

        Falls back to a single job (yield self) when no id_column is set,
        since offset windows would require counting/among-pages coordination.
        """
        resolved = self.resolve_parameters(runtime_params) or self.config
        id_column = resolved.get("id_column")
        if not id_column:
            yield self
            return

        base_query = resolved["query"]
        batch_size = resolved.get("batch_size", 1000)
        engine = self._get_engine()
        with engine.connect() as conn:
            row = conn.execute(
                text(f"SELECT MIN({id_column}) AS lo, MAX({id_column}) AS hi "
                     f"FROM ({base_query}) AS sub")
            ).fetchone()
        if not row or row[0] is None:
            return
        lo, hi = int(row[0]), int(row[1])

        cur = lo
        while cur <= hi:
            nxt = cur + batch_size
            windowed = (
                f"SELECT * FROM ({base_query}) AS sub "
                f"WHERE {id_column} >= {cur} AND {id_column} < {nxt}"
            )
            sub = SqlSource(
                connection_url=resolved["connection_url"],
                query=windowed,
                id_column=None,
                time_column=resolved.get("time_column"),
                batch_size=batch_size,
            )
            sub.config["slice"] = {"lo": cur, "hi": nxt}
            yield sub
            cur = nxt

    def split_jobs(
        self, runtime_params: Dict[str, Any], batch_size: int = 1000
    ) -> Iterator[SourceJob]:
        """
        Split SQL data into jobs.

        Strategy depends on configuration:
        - If id_column specified: ID range splitting
        - If time_column specified: Time window splitting
        - Otherwise: Offset-based pagination

        Args:
            runtime_params: Runtime parameters for query template
            batch_size: Records per job

        Yields:
            SourceJob instances
        """
        resolved_config = self.resolve_parameters(runtime_params)
        if resolved_config is None:
            raise SourceError("sql", "No valid configuration resolved", None)
        engine = self._get_engine()
        batch_size = resolved_config.get("batch_size", batch_size)

        id_column = resolved_config.get("id_column")

        try:
            if id_column:
                # ID range splitting
                yield from self._split_by_id_range(engine, resolved_config, batch_size)
            else:
                # Offset-based pagination (fallback)
                yield from self._split_by_offset(engine, resolved_config, batch_size)

        except SQLAlchemyError as e:
            raise SourceError("sql", f"Failed to split jobs: {e}", e)

    def _split_by_id_range(
        self, engine: Engine, config: Dict[str, Any], batch_size: int
    ) -> Iterator[SourceJob]:
        """Split jobs by ID range."""
        id_column = config["id_column"]
        base_query = config["query"]

        with engine.connect() as conn:
            # Get min and max ID
            min_max_query = f"SELECT MIN({id_column}) as min_id, MAX({id_column}) as max_id FROM ({base_query}) as subquery"
            result = conn.execute(text(min_max_query))
            row = result.fetchone()

            if not row or row[0] is None:
                return  # No data

            min_id, max_id = row[0], row[1]
            current_id = min_id
            page_num = 0

            while current_id <= max_id:
                next_id = current_id + batch_size

                # Fetch batch
                range_query = f"""
                    SELECT * FROM ({base_query}) as subquery
                    WHERE {id_column} >= {current_id} AND {id_column} < {next_id}
                """

                result = conn.execute(text(range_query))
                records = [dict(row._mapping) for row in result]

                if records:
                    yield SourceJob(
                        records=records,
                        metadata={
                            "id_range": {"start": current_id, "end": next_id},
                            "page_num": page_num,
                            "count": len(records),
                        },
                    )

                current_id = next_id
                page_num += 1

    def _split_by_offset(
        self, engine: Engine, config: Dict[str, Any], batch_size: int
    ) -> Iterator[SourceJob]:
        """Split jobs using OFFSET/LIMIT pagination."""
        base_query = config["query"]
        offset = 0
        page_num = 0

        with engine.connect() as conn:
            while True:
                paginated_query = f"{base_query} LIMIT {batch_size} OFFSET {offset}"

                result = conn.execute(text(paginated_query))
                records = [dict(row._mapping) for row in result]

                if not records:
                    break

                yield SourceJob(
                    records=records,
                    metadata={
                        "offset": offset,
                        "page_num": page_num,
                        "count": len(records),
                    },
                )

                if len(records) < batch_size:
                    break  # Last page

                offset += batch_size
                page_num += 1

    def health_check(self) -> bool:
        """Check database connectivity."""
        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False


def sql_source(
    connection_url: str,
    query: str,
    id_column: Optional[str] = None,
    time_column: Optional[str] = None,
    batch_size: int = 1000,
    **engine_kwargs,
) -> SqlSource:
    """
    Factory function for SQL source.

    Example:
        >>> source = sql_source(
        ...     connection_url="postgresql://user:pass@localhost/db",
        ...     query="SELECT * FROM events WHERE created_at >= '{{ start_time }}'",
        ...     id_column="id",
        ...     batch_size=1000
        ... )
    """
    return SqlSource(
        connection_url=connection_url,
        query=query,
        id_column=id_column,
        time_column=time_column,
        batch_size=batch_size,
        **engine_kwargs,
    )
