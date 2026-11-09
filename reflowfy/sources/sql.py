"""SQL database source connector.

Values in the query template are **bound, never spliced into the SQL**: each
``{{ expr }}`` renders as a bind placeholder (``:reflowfy_p0``…) and its value travels
beside the query, in ``config["params"]`` of every planned slice, so the worker runs
the same statement. An identifier (table, column, schema) cannot be a bind parameter;
it is spliced in with ``{{ name | ident }}``, which only accepts
``[A-Za-z_][A-Za-z0-9_.]*``.
"""

import inspect
import re
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from jinja2 import BaseLoader, Environment, TemplateSyntaxError, Undefined, UndefinedError
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.elements import BindParameter, TextClause

from reflowfy.core.execution_context import parameter_resolver
from reflowfy.core.types import Records, wrap_as_record
from reflowfy.sources.base import BaseSource, SourceJob, SourceError

#: Prefix of the bind parameters a template's ``{{ }}`` values become. Starts with a
#: letter: Oracle rejects bind names that start with an underscore.
BIND_PREFIX = "reflowfy_"

_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_.]*")
_SENTINEL = re.compile("\x00(\\d+)\x00")
# Keywords after which a bare word is an identifier, not a value.
_IDENTIFIER_KEYWORDS = frozenset({"FROM", "JOIN", "INTO", "UPDATE", "TABLE", "BY", "AS"})
_IDENT_HINT = (
    "A table, column or schema name cannot be a bind parameter: write {{ name | ident }} "
    "(it accepts only [A-Za-z_][A-Za-z0-9_.]*)."
)


class SqlTemplateError(ValueError):
    """A ``sql_source`` query template cannot be turned into bound SQL."""


class _Ident(str):
    """Output of the ``ident`` filter: spliced into the SQL as written."""


class _Missing:
    """A ``{{ name }}`` whose parameter is not set: an error unless it sits in a comment."""

    def __init__(self, name: Optional[str]):
        self.name = name


def _ident_filter(value: Any) -> Any:
    if isinstance(value, Undefined):
        return value  # reported by finalize, unless it is in a comment
    name = str(value)
    if not _IDENT.fullmatch(name):
        raise SqlTemplateError(
            f"{{{{ … | ident }}}} got {name!r}, which is not an identifier "
            "([A-Za-z_][A-Za-z0-9_.]*); a value belongs in a plain {{ … }}, which is bound"
        )
    return _Ident(name)


def validate_identifier(name: Any, what: str) -> str:
    """``name`` if it is a plain (optionally dotted) SQL identifier, else raise."""
    if not isinstance(name, str) or not _IDENT.fullmatch(name):
        raise SqlTemplateError(f"{what} {name!r} is not an identifier ([A-Za-z_][A-Za-z0-9_.]*)")
    return name


def strip_trailing_semicolon(sql: str) -> str:
    """Drop one trailing ``;`` (and surrounding whitespace) so the query can be wrapped."""
    stripped = sql.rstrip()
    if stripped.endswith(";"):
        stripped = stripped[:-1].rstrip()
    return stripped


def _needs_space(sql: str, pos: int) -> bool:
    """A placeholder followed by ``:`` (``::type``) or a word char would not parse."""
    return pos < len(sql) and (sql[pos] == ":" or sql[pos].isalnum() or sql[pos] == "_")


def _word_before(sql: str) -> str:
    i = len(sql.rstrip())
    j = i
    while j > 0 and (sql[j - 1].isalnum() or sql[j - 1] == "_"):
        j -= 1
    return sql[j:i]


def _char_before(sql: str) -> str:
    stripped = sql.rstrip()
    return stripped[-1] if stripped else ""


def _char_after(sql: str, pos: int) -> str:
    rest = sql[pos:].lstrip()
    return rest[0] if rest else ""


def _described(fragment: str) -> str:
    """A fragment of rendered SQL with each value shown as ``{{ … }}``, never the value."""
    return _SENTINEL.sub("{{ … }}", fragment)


class _BindPlacer:
    """Replaces each value sentinel in rendered SQL with a bind placeholder.

    A small scanner tracks whether it is in code, a ``'…'`` literal, a ``"…"``
    identifier or a comment, which decides what a value there means.
    """

    def __init__(self, rendered: str, values: List[Any]):
        self.sql = rendered
        self.values = values
        self.binds: Dict[str, Any] = {}
        self.out: List[str] = []
        self.state = "code"  # code | quote | dquote | line_comment | block_comment
        self.literal_start = 0  # index in `out` of the open '…' literal's quote

    def _placeholder(self, index: int) -> str:
        value = self.values[index]
        if isinstance(value, _Missing):
            raise SqlTemplateError(
                f"the query uses {{{{ {value.name} }}}} but no runtime parameter "
                f"{value.name!r} is set"
            )
        name = f"{BIND_PREFIX}p{index}"
        self.binds[name] = value
        return f":{name}"

    def place(self) -> Tuple[str, Dict[str, Any]]:
        sql, i = self.sql, 0
        handlers: Dict[str, Callable[[int], Tuple[int, str]]] = {
            "code": self._code,
            "quote": self._quote,
            "dquote": self._dquote,
            "line_comment": self._comment,
            "block_comment": self._comment,
        }
        while i < len(sql):
            i, self.state = handlers[self.state](i)
        return "".join(self.out), self.binds

    def _code(self, i: int) -> Tuple[int, str]:
        sql = self.sql
        match = _SENTINEL.match(sql, i)
        if match:
            return self._value_in_code(i, match)
        ch = sql[i]
        self.out.append(ch)
        if ch == "'":
            self.literal_start = len(self.out) - 1
            return i + 1, "quote"
        if ch == '"':
            return i + 1, "dquote"
        if sql.startswith("--", i):
            return i + 1, "line_comment"
        if sql.startswith("/*", i):
            return i + 1, "block_comment"
        return i + 1, "code"

    def _value_in_code(self, i: int, match: "re.Match[str]") -> Tuple[int, str]:
        sql = self.sql
        index, end = int(match.group(1)), match.end()
        if isinstance(self.values[index], _Missing):
            self._placeholder(index)  # raises, naming the missing parameter
        so_far = "".join(self.out)
        if (
            _char_before(so_far) == "."
            or _char_after(sql, end) == "."
            or _word_before(so_far).upper() in _IDENTIFIER_KEYWORDS
        ):
            fragment = _described(sql[max(0, i - 30) : end + 30])
            raise SqlTemplateError(
                f"{{{{ … }}}} in identifier position in {fragment!r}. {_IDENT_HINT}"
            )
        if (
            isinstance(self.values[index], (list, tuple))
            and _char_before(so_far) == "("
            and _char_after(sql, end) == ")"
        ):
            # `IN ({{ ids }})`: SQLAlchemy adds an expanding bind's parentheses itself.
            self.out = [so_far.rstrip()[:-1]]
            end = sql.index(")", end) + 1
        token = self._placeholder(index)
        self.out.append(token + " " if _needs_space(sql, end) else token)
        return end, "code"

    def _quote(self, i: int) -> Tuple[int, str]:
        sql = self.sql
        if sql.startswith("''", i):  # escaped quote
            self.out.append("''")
            return i + 2, "quote"
        ch = sql[i]
        if ch != "'":
            self.out.append(ch)
            return i + 1, "quote"
        literal = "".join(self.out[self.literal_start :]) + "'"
        whole = _SENTINEL.fullmatch(literal[1:-1])
        if whole:
            # '{{ value }}' on its own: bind the value in place of the literal.
            del self.out[self.literal_start :]
            token = self._placeholder(int(whole.group(1)))
            self.out.append(token + " " if _needs_space(sql, i + 1) else token)
            return i + 1, "code"
        if _SENTINEL.search(literal):
            raise SqlTemplateError(
                f"{{{{ … }}}} inside the string literal {_described(literal)}: values are "
                "bound as parameters, so they cannot be part of a literal. Bind the whole "
                "value ('{{ x }}' on its own, or build the string in the parameter) or "
                "concatenate in SQL ('%' || {{ x }} || '%')."
            )
        self.out.append(ch)
        return i + 1, "code"

    def _dquote(self, i: int) -> Tuple[int, str]:
        if _SENTINEL.match(self.sql, i):
            raise SqlTemplateError(f"{{{{ … }}}} inside a quoted identifier. {_IDENT_HINT}")
        ch = self.sql[i]
        self.out.append(ch)
        return i + 1, "code" if ch == '"' else "dquote"

    def _comment(self, i: int) -> Tuple[int, str]:
        sql = self.sql
        match = _SENTINEL.match(sql, i)
        if match:  # a value in a comment is dropped: nothing to bind
            return match.end(), self.state
        if self.state == "block_comment" and sql.startswith("*/", i):
            self.out.append("*/")
            return i + 2, "code"
        ch = sql[i]
        self.out.append(ch)
        if self.state == "line_comment" and ch == "\n":
            return i + 1, "code"
        return i + 1, self.state


def render_sql_template(
    template: str, runtime_params: Dict[str, Any]
) -> Tuple[str, Dict[str, Any]]:
    """Render a query template to ``(sql, binds)``: every ``{{ expr }}`` value is bound.

    A value quoted on its own (``'{{ day }}'``) loses the quotes: it is bound, which is
    what the quotes meant. A list or tuple value is an expanding bind
    (``id IN {{ ids }}`` or ``id IN ({{ ids }})``). ``{{ name | ident }}`` splices a
    validated identifier. A value in identifier position, inside a longer string
    literal or inside ``"…"`` raises :class:`SqlTemplateError`.
    """
    values: List[Any] = []

    def finalize(value: Any) -> str:
        if isinstance(value, _Ident):
            return str(value)
        if isinstance(value, Undefined):
            value = _Missing(getattr(value, "_undefined_name", None))
        values.append(value)
        return f"\x00{len(values) - 1}\x00"

    # Autoescape on: every value is a bind placeholder or a validated identifier, neither
    # of which contains a character HTML-escaping would touch, so it changes nothing here.
    env = Environment(loader=BaseLoader(), finalize=finalize, autoescape=True)
    env.filters["ident"] = _ident_filter
    try:
        rendered = env.from_string(template).render(**runtime_params)
    except TemplateSyntaxError as e:
        raise SqlTemplateError(f"Invalid template syntax in query: {e}") from e
    except UndefinedError as e:
        raise SqlTemplateError(f"Missing required parameter in query: {e}") from e
    return _BindPlacer(rendered, values).place()


def _statement(sql: str, binds: Dict[str, Any]) -> TextClause:
    """``text(sql)`` with every list/tuple value bound as an expanding parameter."""
    clause = text(sql)
    expanding: List[BindParameter[Any]] = [
        bindparam(k, expanding=True) for k, v in binds.items() if isinstance(v, (list, tuple))
    ]
    return clause.bindparams(*expanding) if expanding else clause


def _rows(result: Any) -> Records:
    return [
        wrap_as_record(dict(row._mapping)) for row in result  # pyright: ignore[reportPrivateUsage]
    ]


class SqlSource(BaseSource):
    """
    SQL database source connector.

    Supports:
    - ID range splitting for parallel processing
    - Query templates whose values are bound parameters (``{{ x }}``) and whose
      identifiers are validated (``{{ table | ident }}``)
    - Hand-written ``:name`` binds via ``params=``
    - Multiple database backends (Postgres, MySQL, SQLite, etc.)
    """

    # Only the password inside it: hidden in API responses, warned about if literal.
    secret_fields = ("connection_url",)

    def __init__(
        self,
        connection_url: str,
        query: str,
        id_column: Optional[str] = None,
        time_column: Optional[str] = None,
        batch_size: int = 1000,
        params: Optional[Dict[str, Any]] = None,
        template: bool = True,
        **engine_kwargs: Any,
    ):
        """
        Initialize SQL source.

        Args:
            connection_url: SQLAlchemy connection URL (``secret("ENV")`` keeps the
                password out of job messages)
            query: SQL query. A Jinja2 template whose ``{{ }}`` values are bound;
                identifiers need ``{{ name | ident }}``.
            id_column: Column for ID range splitting
            time_column: Column for time window splitting (unused)
            batch_size: Records per job
            params: Values for hand-written ``:name`` binds in ``query``; a list or
                tuple value is an expanding bind (``id IN :ids``)
            template: Whether ``query`` is a Jinja2 template. Planned slices carry the
                rendered SQL and its binds with ``template=False``.
            **engine_kwargs: Additional SQLAlchemy engine parameters
        """
        config = {
            "connection_url": connection_url,
            "query": query,
            "id_column": id_column,
            "time_column": time_column,
            "batch_size": batch_size,
            "params": dict(params or {}),
            "template": template,
            **engine_kwargs,
        }
        super().__init__(config)
        self._engine: Optional[Engine] = None

    def _engine_kwargs(self) -> Dict[str, Any]:
        """**engine_kwargs: config minus the named parameters and split()'s window."""
        named = set(inspect.signature(SqlSource.__init__).parameters) | {"slice"}
        return {k: v for k, v in self.config.items() if k not in named}

    def _get_engine(self) -> Engine:
        """Get or create SQLAlchemy engine."""
        if self._engine is None:
            # **engine_kwargs live in config beside the named settings (so the source
            # round-trips through SourceFactory); hand only those to SQLAlchemy.
            engine_kwargs = self._engine_kwargs()
            engine_kwargs.setdefault("pool_pre_ping", True)
            self._engine = create_engine(self.config["connection_url"], **engine_kwargs)
        return self._engine

    def resolve_parameters(self, runtime_params: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve the config; the query becomes bound SQL with its values in ``params``.

        Every setting but ``query`` and ``params`` is resolved as any source's is. The
        query is rendered by :func:`render_sql_template` (unless ``template`` is off),
        one trailing ``;`` is dropped, and its values are merged into ``params``.
        """
        rest = {k: v for k, v in self.config.items() if k not in ("query", "params")}
        resolved: Dict[str, Any] = parameter_resolver.resolve(rest, runtime_params)
        binds: Dict[str, Any] = dict(self.config.get("params") or {})
        query: str = self.config["query"]
        if self.config.get("template", True):
            query, rendered_binds = render_sql_template(query, runtime_params)
            clash = sorted(set(binds) & set(rendered_binds))
            if clash:
                raise SqlTemplateError(
                    f"params {clash} collide with the binds the template's values become; "
                    f"do not name a parameter {BIND_PREFIX}p<n>"
                )
            binds.update(rendered_binds)
        resolved["query"] = strip_trailing_semicolon(query)
        resolved["params"] = binds
        if resolved.get("id_column"):
            validate_identifier(resolved["id_column"], "id_column")
        self._resolved_config = resolved
        return resolved

    def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> Records:
        """
        Fetch data from SQL database (local mode).

        Args:
            runtime_params: Runtime parameters for query template
            limit: Optional cap on rows, applied with ``fetchmany`` (the SQL is not
                rewritten, so it works with any query and any database)

        Returns:
            List of records as dictionaries
        """
        resolved = self.resolve_parameters(runtime_params)
        engine = self._get_engine()
        try:
            with engine.connect() as conn:
                result = conn.execute(
                    _statement(resolved["query"], resolved["params"]), resolved["params"]
                )
                rows = result.fetchmany(limit) if limit else result
                return _rows(rows)
        except SQLAlchemyError as e:
            raise SourceError("sql", f"Failed to fetch data: {e}", e)

    def split(self, runtime_params: Dict[str, Any]) -> Iterator["SqlSource"]:
        """Plan id-range windows using MIN/MAX only — no row fetch.

        Falls back to a single job (yield self) when no id_column is set,
        since offset windows would require counting/among-pages coordination.
        That fallback first probes for at least one row, so an empty result
        set does not produce a no-op job.

        Each window is the rendered query wrapped in ``WHERE id >= :lo AND id < :hi``;
        the slice carries that SQL and every bound value, with ``template=False``.
        """
        resolved = self.resolve_parameters(runtime_params)
        id_column = resolved.get("id_column")
        base_query: str = resolved["query"]
        binds: Dict[str, Any] = resolved["params"]
        engine = self._get_engine()
        if not id_column:
            probe = f"SELECT 1 FROM ({base_query}\n) AS sub LIMIT 1"
            with engine.connect() as conn:
                row = conn.execute(_statement(probe, binds), binds).fetchone()
            if row is not None:
                yield self
            return

        batch_size = resolved.get("batch_size", 1000)
        min_max = (
            f"SELECT MIN({id_column}) AS lo, MAX({id_column}) AS hi FROM ({base_query}\n) AS sub"
        )
        with engine.connect() as conn:
            row = conn.execute(_statement(min_max, binds), binds).fetchone()
        if not row or row[0] is None:
            return
        lo, hi = int(row[0]), int(row[1])

        lo_name, hi_name = f"{BIND_PREFIX}lo", f"{BIND_PREFIX}hi"
        windowed = (
            f"SELECT * FROM ({base_query}\n) AS sub "
            f"WHERE {id_column} >= :{lo_name} AND {id_column} < :{hi_name}"
        )
        cur = lo
        while cur <= hi:
            nxt = cur + batch_size
            sub = SqlSource(
                connection_url=resolved["connection_url"],
                query=windowed,
                id_column=None,
                time_column=resolved.get("time_column"),
                batch_size=batch_size,
                params={**binds, lo_name: cur, hi_name: nxt},
                template=False,
                **self._engine_kwargs(),
            )
            sub.config["slice"] = {"lo": cur, "hi": nxt}
            yield sub
            cur = nxt

    def split_jobs(
        self, runtime_params: Dict[str, Any], batch_size: int = 1000
    ) -> Iterator[SourceJob]:
        """
        Split SQL data into jobs (legacy: fetches records while planning).

        Strategy depends on configuration:
        - If id_column specified: ID range splitting
        - Otherwise: Offset-based pagination

        Args:
            runtime_params: Runtime parameters for query template
            batch_size: Records per job

        Yields:
            SourceJob instances
        """
        resolved_config = self.resolve_parameters(runtime_params)
        engine = self._get_engine()
        batch_size = resolved_config.get("batch_size", batch_size)

        try:
            if resolved_config.get("id_column"):
                yield from self._split_by_id_range(engine, resolved_config, batch_size)
            else:
                yield from self._split_by_offset(engine, resolved_config, batch_size)
        except SQLAlchemyError as e:
            raise SourceError("sql", f"Failed to split jobs: {e}", e)

    def _split_by_id_range(
        self, engine: Engine, config: Dict[str, Any], batch_size: int
    ) -> Iterator[SourceJob]:
        """Split jobs by ID range."""
        id_column = config["id_column"]
        base_query = config["query"]
        binds: Dict[str, Any] = config["params"]

        with engine.connect() as conn:
            min_max = (
                f"SELECT MIN({id_column}) as min_id, MAX({id_column}) as max_id "
                f"FROM ({base_query}\n) as subquery"
            )
            row = conn.execute(_statement(min_max, binds), binds).fetchone()
            if not row or row[0] is None:
                return  # No data

            min_id, max_id = row[0], row[1]
            current_id = min_id
            page_num = 0
            range_query = (
                f"SELECT * FROM ({base_query}\n) as subquery "
                f"WHERE {id_column} >= :{BIND_PREFIX}lo AND {id_column} < :{BIND_PREFIX}hi"
            )

            while current_id <= max_id:
                next_id = current_id + batch_size
                window = {**binds, f"{BIND_PREFIX}lo": current_id, f"{BIND_PREFIX}hi": next_id}
                records = _rows(conn.execute(_statement(range_query, window), window))

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
        """Split jobs using LIMIT/OFFSET pagination."""
        base_query = config["query"]
        binds: Dict[str, Any] = config["params"]
        paginated_query = f"{base_query}\nLIMIT :{BIND_PREFIX}limit OFFSET :{BIND_PREFIX}offset"
        offset = 0
        page_num = 0

        with engine.connect() as conn:
            while True:
                page = {**binds, f"{BIND_PREFIX}limit": batch_size, f"{BIND_PREFIX}offset": offset}
                records = _rows(conn.execute(_statement(paginated_query, page), page))

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
    params: Optional[Dict[str, Any]] = None,
    **engine_kwargs: Any,
) -> SqlSource:
    """
    Factory function for SQL source.

    ``{{ expr }}`` values are bound parameters; identifiers need ``| ident``.

    Example:
        >>> source = sql_source(
        ...     connection_url=secret("EVENTS_DB_URL"),
        ...     query="SELECT * FROM {{ table | ident }} WHERE created_at >= {{ start_time }}",
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
        params=params,
        **engine_kwargs,
    )
