SELECT id, event_type, user_id, user_name, status, amount, created_at, metadata
FROM test_events
WHERE created_at >= '{{ start_time }}'::timestamp
  AND created_at <= '{{ end_time }}'::timestamp
ORDER BY id
