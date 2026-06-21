-- =============================================================================
-- Example SQL Query Template
-- =============================================================================
-- Save your reusable query templates in this folder.
-- Use Jinja2-style placeholders ({{ param_name }}) for dynamic values
-- that will be resolved at runtime from pipeline parameters.
--
-- Usage in a pipeline:
--   @source("my_sql_source")
--   def my_sql_source(**overrides):
--       from reflowfy import sql_source
--       from pathlib import Path
--
--       query = Path("queries/events_by_date.sql").read_text()
--       return sql_source(
--           connection_url="postgresql://user:pass@host/db",
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
