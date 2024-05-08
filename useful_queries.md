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

kill active connections more safely
```sql
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE pid <> pg_backend_pid() and state = 'active' and query like '%tx_recipient%' and datname = 'defaultdb';
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



```
explain select * from transactions where tx_sequence_number = ANY( select tx_sequence_number from tx_digests where tx_digest IN (
    '\xf446765c76ffdcc0180603017b8c44ea8af60d3989c89667a37a55e22e6a0af8',
    '\xaa23ed8f457a6fbf8978d43b7d2ab25e4be38287428b20251770b42169116d79',
    '\xb964535db902741e2750bb3ed24a9ab47036236ee9f7ce2f6c2daedd6411a0e5',
    '\xea391a881175be96e3e120104f76b38d9bb21837a1a72db2f0efc1cf655dda28',
    '\x1eb83230e46b0810e0ab5be6f3144e15bd263e3271c7df77c369406b3108a539',
    '\xb50fe223544ed56f9f11d29003010e488bdeb0f1caeb577affb4029508563407',
    '\x28871545596cf088594fb546adf63a01265afee0237e7331b6e33c23b7ff6f46',
    '\x3d2f58786c8ea57396bcb83f2c77248b92ae496ae1cc47a8fa6030abf99a951b',
    '\x6b8fe59f705955fb821cb5d9745ad502c424877874d3a49838af4834078ba01a',
    '\x0502a3a5140b898f375666f321575b379876b01a36d6a281aea79c66ad560b64',
    '\x32e31faab628f7c2fbd13f5978571e842adb7fd795e3255aa3487966ee1b2097',
    '\xeae13afb67416c22947e2098b0ad40689f56157509f0d0b198618073437f6e45',
    '\x6fdbb17379d868b62de2c85f450fb791500cead385b99c678741dfd0434b1686',
    '\x8666c09c5db5624508c0958cd547745f9ab573743170738a5765c363ca723d72',
    '\x8af2f79234bb10f2513ef803666ddee62e5cb7df9a415328472fdc7906ed4eb9',
    '\xcf0eceaf3bf7b14be69af3cdd1492f69e26cf73ef53e83482e2e08c2cc2cfc26',
    '\x9dcd1521d512119116ab3608a55ecdf960dad5e9f81fdd899dbc1282a8cc3974',
    '\xd19ea1dfc18d922c7f55c7f2c3b7e9c896bc5988de099ba44b12606121904fda',
    '\x0337fb507a0d49afdbe824f730a17906383aaed4d0a2bb02344b33cc7b3184ea',
    '\x9c3adddabb21cfe07e86ebac762df84e91d46398272b69aea83977073294b920',
    '\xb9cf9dd692181d6ac76a6da6ad4288cc56ad91acd1cbbb3f8d088b4bf5328110',
    '\xffb2687b78e06a4934d81aa6f621e971cbc0799aaa4c8775907c35e009a05315',
    '\x02cfc982dacb117b228b64b4bc4b0a167db56ef2717cda081ec2eb2a0aa6a3a5',
    '\xdcaa66cb5f0c930eb662f0dffa197205c990b3d9d76c38ab3af3d8579363d2e2',
    '\xebb576219bbb2e18a9b55899f9ce1b83ffa04229e9c95b4c0eb0e5902f37d83c')
);
    ```
