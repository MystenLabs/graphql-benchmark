View count of active connections
```sql
SELECT
  COUNT(*) AS current_connections,
  (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') AS max_connections,
  (SELECT setting::int FROM pg_settings WHERE name = 'superuser_reserved_connections') AS reserved_for_superusers,
  (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') - COUNT(*) AS available_connections
FROM
  pg_stat_activity;
```

Estimated row count
```sql
SELECT
  relname AS table_name,
  n_live_tup AS estimated_row_count
FROM pg_stat_user_tables where relname = 'tx_recipients_cp';
```

kill active connections
```sql
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE pid <> pg_backend_pid() -- Exclude your own connection
AND datname = 'defaultdb';
```

check index progress
```sql
SELECT
  pid,
  datname,
  relid::regclass AS table_name,
  phase,
  lockers_total,
  lockers_done,
  current_locker_pid,
  blocks_total,
  blocks_done,
  tuples_total,
  tuples_done,
  partitions_total,
  partitions_done
FROM pg_stat_progress_create_index
WHERE relid = 'tx_senders_cp'::regclass;
```
