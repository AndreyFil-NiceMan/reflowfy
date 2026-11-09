-- =============================================================================
-- Example SQL Query Template
-- =============================================================================
-- Save your reusable query templates in this folder.
-- Use Jinja2-style placeholders ({{ param_name }}) for dynamic values
-- that will be resolved at runtime from pipeline parameters. Each value is
-- sent as a bind parameter, never pasted into the SQL, so it cannot inject.
-- A table or column name cannot be a bind parameter: write
-- {{ table_name | ident }} (letters, digits, _ and . only).
--
-- Usage in a pipeline:
--   @source("my_sql_source")
--   def my_sql_source(**overrides):
--       from reflowfy import sql_source
--       from pathlib import Path
--
--       query = Path("queries/events_by_date.sql").read_text()
--       return sql_source(
--           connection_url=secret("EVENTS_DB_URL"),  # from reflowfy.core.secrets
--           query=query,
--           **overrides,
--       )
-- =============================================================================

SELECT
    id,
    event_type,
    user_id,
    status,
    created_at
FROM events
WHERE created_at >= '{{ start_time }}'::timestamp
  AND created_at <= '{{ end_time }}'::timestamp
ORDER BY created_at DESC;
