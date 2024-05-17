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


fixing invalid index
`SELECT * FROM pg_index WHERE pg_index.indisvalid = false;`

 SELECT
    i.relname AS index_name,
    t.relname AS table_name
FROM pg_class t
JOIN pg_index idx ON idx.indrelid = t.oid
JOIN pg_class i ON i.oid = idx.indexrelid
WHERE i.oid = 601098198;


count partitions
```
SELECT count(*)
FROM pg_inherits
JOIN pg_class parent ON parent.oid = inhparent
JOIN pg_class child ON child.oid = inhrelid
WHERE parent.relname = 'objects_history';
```

grab indexes of a name
```
SELECT indexrelid::regclass AS index_name, indisvalid
FROM pg_index
JOIN pg_class ON pg_class.oid = pg_index.indexrelid
WHERE pg_class.relname LIKE 'objects_history_coin_only_%';
```

verify index attachments
```
SELECT
    n.nspname AS schema_name,
    parent_idx.relname AS parent_index_name,
    idx.relname AS partition_index_name,
    t.relname AS partition_table_name
FROM pg_inherits
JOIN pg_class t ON t.oid = inhrelid  -- Partition table
JOIN pg_namespace n ON n.oid = t.relnamespace
JOIN pg_index pi ON pi.indrelid = t.oid
JOIN pg_class idx ON idx.oid = pi.indexrelid
JOIN pg_class parent ON parent.oid = inhparent  -- Parent table
JOIN pg_index parent_pi ON parent_pi.indrelid = parent.oid
JOIN pg_class parent_idx ON parent_idx.oid = parent_pi.indexrelid
WHERE parent_idx.relname = 'objects_history_coin_only';
```

check for invalid indexes
```
SELECT indexrelid::regclass AS index_name, indisvalid
FROM pg_index
WHERE indisvalid = false AND indexrelid::regclass::text LIKE 'objects_history_coin_only_%';
```


List all partitions and expected indexes
```
WITH partition_data AS (
    SELECT
        t.relname AS partition_table_name,
        format('objects_history_coin_only_%s', substring(t.relname from 'objects_history_partition_(.*)')) AS expected_index_name
    FROM pg_class t
    JOIN pg_inherits ON pg_inherits.inhrelid = t.oid
    JOIN pg_class parent ON parent.oid = pg_inherits.inhparent
    WHERE parent.relname = 'objects_history' -- Adjust if your parent table has a different name
),
actual_indexes AS (
    SELECT
        idx.relname AS index_name
    FROM pg_class idx
    JOIN pg_index ON pg_index.indexrelid = idx.oid
    JOIN pg_class t ON t.oid = pg_index.indrelid
    WHERE idx.relname LIKE 'objects_history_coin_only_%'
)
SELECT
    p.partition_table_name,
    p.expected_index_name,
    a.index_name
FROM partition_data p
LEFT JOIN actual_indexes a ON a.index_name = p.expected_index_name
ORDER BY p.partition_table_name;
```

disable an index within the context of a transaction for testing
```sql
begin;
drop index objects_snapshot_package_module_name_full_type;
--- other queries
rollback;
```

check for indexes that were not created among aprtitions
```sql
SELECT partition_name
FROM (
    SELECT
        partition_name,
        indexname IS NOT NULL AS index_exists
    FROM (
        SELECT
            partition_name,
            (SELECT indexname
             FROM pg_indexes
             WHERE tablename = partition_name
             AND indexname = 'objects_history_coin_owner_object_id_' || partition_number) AS indexname
        FROM (
            SELECT 'objects_history_partition_' || i AS partition_name, i AS partition_number
            FROM generate_series(0, 390) AS i
        ) partitions
    ) indexed_partitions
) check_partitions
WHERE NOT index_exists;
```
