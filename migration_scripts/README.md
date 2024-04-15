Ad-hoc Python scripts to transform the db to enable further benchmarking.

`set maintenance_work_mem to '1 GB';` to speed up index creation.

# migrating txs

1. Create unpartitioned table of `(tx, cp)`: `python3 just_the_txs.py --setup`
   a. Can resume with `python3 just_the_txs.py --setup --resume --log pathToLogFile`
2. Add `cp` info to `tx_senders` and `tx_recipients`: `python3 just_the_txs.py --add-cp --table tx_senders | tx_recipients`
3. Merge into a single `tx_addresses` table: `python3 just_the_txs.py --merge-addresses`
4. Apply the merged table to other `tx_` lookup tables: `python3 just_the_txs.py --add-addresses --table tx_calls`

trying out on subset: `tx_senders`, `tx_recipients`, `tx_calls`


# objects history
`objects_history.py` leverages the fact that if you want to create an index on a partitioned table, you can "init" the index on the base table, then fire off a bunch of create index concurrently on each table partition https://www.postgresql.org/docs/current/ddl-partitioning.html#DDL-PARTITIONING-DECLARATIVE (see bit starting from CREATE INDEX measurement_usls_idx ON ONLY measurement (unitsales)), finally attaching them back to the base index once completed. The objects_history table has 328 partitions. By doing this, setting max_connections on the connection pool to 400, and max threads to somewhere in the ballpark, you can effectively create the index in one go
