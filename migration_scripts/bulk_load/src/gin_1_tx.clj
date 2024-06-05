(ns gin-1-tx
  (:require [db]
            [logger :refer [->Logger] :as l]
            [next.jdbc :as jdbc]
            [pool-v2 :refer [->Pool worker]]
            [transactions :refer [bounds->batches]]))

;; # 1-GIN Transactions Schema
;;
;; Proposed new schema for the transactions table which eschews most side
;; tables. Instead, the data is stored in a single (partitioned) table, acting
;; as a document store (transactions are stored in a denormalized form).
;;
;; The one exception to this rule is the `tx_digests` table which is still used
;; for point (and multi-point/data-loader) lookups.
;;
;; Like the previously proposed "normalized" schema, partitioning and cursor
;; logic will rely solely on transaction sequence number (not involve checkpoint
;; sequence number).
;;
;; Queries are powered by a single, multi-column GIN (Generalized Inverted
;; iNdex) containing all the queryable fields on the denormalized transaction.
;;
;; All queries will effectively be translated into a bitmap index scan, with the
;; bitmap produced by the GIN. Combination filters are naturally supported in
;; this mode (a multi-column GIN can support arbirary combinations of its
;; columns).
;;
;; The `btree_gin` extension needs to be enabled to support GIN indices
;; with "normal" (scalar) types.
;;
;; This schema should be good at supporting arbitrary combinations of filters,
;; without much storage overhead, but:
;;
;; - Write throughput may suffer as GINs are somewhat slow to update (although
;;   maybe not because the GIN is replacing many indices).
;;
;; - It may naturally time-out on queries that don't bound the number of
;;   partitions it needs to look-up.

;; Table Names ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:private prefix "amnn_0_gin_1_")

(def +transactions+ (str prefix "transactions"))
(def +tx-digests+   (str prefix "tx_digests"))
(def +cp-tx+        (str prefix "cp_tx"))

(defn transactions:partition-name [n]
  (str +transactions+ "_partition_" n))

;; Table: gin_1_transactions ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn transactions:create! [db]
  (jdbc/with-transaction [tx db]
    (jdbc/execute! tx ["CREATE EXTENSION IF NOT EXISTS btree_gin"])
    (db/with-table! tx +transactions+
      "CREATE TABLE %s (
           tx_sequence_number         BIGINT          PRIMARY KEY,
           transaction_digest         BYTEA           NOT NULL,
           raw_transaction            BYTEA           NOT NULL,
           raw_effects                BYTEA           NOT NULL,
           checkpoint_sequence_number BIGINT          NOT NULL,
           timestamp_ms               BIGINT          NOT NULL,
           object_changes             BYTEA[]         NOT NULL,
           balance_changes            BYTEA[]         NOT NULL,
           events                     BYTEA[]         NOT NULL,
           transaction_kind           SMALLINT        NOT NULL,
           success_command_count      SMALLINT        NOT NULL,
           packages                   BYTEA[]         NOT NULL,
           modules                    TEXT[]          NOT NULL,
           functions                  TEXT[]          NOT NULL,
           senders                    BYTEA[]         NOT NULL,
           recipients                 BYTEA[]         NOT NULL,
           inputs                     BYTEA[]         NOT NULL,
           changed                    BYTEA[]         NOT NULL
       ) PARTITION BY RANGE (tx_sequence_number)")
    (db/with-table! tx +transactions+
      "CREATE INDEX %1$s_filter ON %1$s USING gin(
           transaction_digest,
           transaction_kind,
           packages, modules, functions,
           senders, recipients,
           inputs, changed
       )")))

(defn transactions:create-partition! [db n]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx (transactions:partition-name n)
      "CREATE TABLE %s (
           tx_sequence_number         BIGINT,
           transaction_digest         BYTEA,
           raw_transaction            BYTEA,
           raw_effects                BYTEA,
           checkpoint_sequence_number BIGINT,
           timestamp_ms               BIGINT,
           object_changes             BYTEA[],
           balance_changes            BYTEA[],
           events                     BYTEA[],
           transaction_kind           SMALLINT,
           success_command_count      SMALLINT,
           packages                   BYTEA[],
           modules                    TEXT[],
           functions                  TEXT[],
           senders                    BYTEA[],
           recipients                 BYTEA[],
           inputs                     BYTEA[],
           changed                    BYTEA[]
       )")
    (db/disable-autovacuum! tx (transactions:partition-name n))))

(defn transactions:constrain!
  "Add constraints to partition `n` of the `transactions` table.

  `lo` and `hi` are the inclusive and exclusive bounds on transaction
  sequence numbers in the partition."
  [db n lo hi]
  (->> [(format
         "ALTER TABLE %1$s
          ADD PRIMARY KEY (tx_sequence_number),
          ALTER COLUMN transaction_digest         SET NOT NULL,
          ALTER COLUMN raw_transaction            SET NOT NULL,
          ALTER COLUMN raw_effects                SET NOT NULL,
          ALTER COLUMN checkpoint_sequence_number SET NOT NULL,
          ALTER COLUMN timestamp_ms               SET NOT NULL,
          ALTER COLUMN object_changes             SET NOT NULL,
          ALTER COLUMN balance_changes            SET NOT NULL,
          ALTER COLUMN events                     SET NOT NULL,
          ALTER COLUMN transaction_kind           SET NOT NULL,
          ALTER COLUMN success_command_count      SET NOT NULL,
          ALTER COLUMN packages                   SET NOT NULL,
          ALTER COLUMN modules                    SET NOT NULL,
          ALTER COLUMN functions                  SET NOT NULL,
          ALTER COLUMN senders                    SET NOT NULL,
          ALTER COLUMN recipients                 SET NOT NULL,
          ALTER COLUMN inputs                     SET NOT NULL,
          ALTER COLUMN changed                    SET NOT NULL,
          ADD CONSTRAINT %1$s_partition_check CHECK (
              %2$d <= tx_sequence_number
          AND tx_sequence_number < %3$d
          )"
         (transactions:partition-name n) lo hi)]
       (jdbc/execute! db)))

(defn transactions:index-filters! [db n]
  (db/with-table! db (transactions:partition-name n)
    "CREATE INDEX %1$s_filter ON %1$s USING gin(
         transaction_digest,
         transaction_kind,
         packages, modules, functions,
         senders, recipients,
         inputs, changed
     )"))

(defn transactions:attach! [db n lo hi]
  (jdbc/with-transaction [tx db]
    (let [part   (transactions:partition-name n)
          attach (fn [template]
                   (jdbc/execute!
                    tx [(format template +transactions+ part lo hi)]))]
      (attach "ALTER TABLE %1$s
               ATTACH PARTITION %2$s FOR VALUES FROM (%3$d) TO (%4$d)")
      (attach "ALTER INDEX %1$s_filter ATTACH PARTITION %2$s_filter"))))

(defn transactions:drop-range-check! [db n]
  (db/with-table! db (transactions:partition-name n)
    "ALTER TABLE %1$s DROP CONSTRAINT %1$s_partition_check"))

;; Table: gin_1_tx_digests ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tx-digests:create! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! db +tx-digests+
      "CREATE TABLE %s (
            tx_digest                   BYTEA,
            tx_sequence_number          BIGINT
       )")
    (db/disable-autovacuum! tx +tx-digests+)))

(defn tx-digests:constrain! [db]
  (db/with-table! db +tx-digests+
    "ALTER TABLE %1$s
     ADD PRIMARY KEY (tx_digest, tx_sequence_number),
     ADD CONSTRAINT %1$s_unique UNIQUE (tx_digest)"))

;; Table: gin_1_cp_tx ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn cp-tx:create! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! db +cp-tx+
      "CREATE TABLE %s (
            checkpoint_sequence_number  BIGINT,
            min_tx_sequence_number      BIGINT,
            max_tx_sequence_number      BIGINT
       )")
    (db/disable-autovacuum! tx +cp-tx+)))

(defn cp-tx:constrain! [db]
  (db/with-table! db +cp-tx+
    "ALTER TABLE %1$s
     ADD PRIMARY KEY (checkpoint_sequence_number),
     ALTER COLUMN min_tx_sequence_number SET NOT NULL,
     ALTER COLUMN max_tx_sequence_number SET NOT NULL"))

;; Bulk Loading ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn transactions:create-all!
  "Create all relevant transactions tables.

  Creates the main table, partitions between `lo` (inclusive) and
  `hi` (exclusive), and all the indexing tables, on `db`."
  [db lo hi logger & {:keys [retry]}]
  (->Pool :name    "create-tables"
          :logger   logger
          :workers (Math/clamp (- hi lo) 4 20)

          :pending
          (or retry
              (conj (for [n (range lo hi)]
                      {:fn #(transactions:create-partition! % n)
                       :label (str "partition-" n)})
                    {:fn transactions:create! :label "transactions"}
                    {:fn tx-digests:create! :label "tx-digests"}
                    {:fn cp-tx:create! :label "cp-tx"}))

          :impl
          (worker {builder :fn}
            (builder db) nil)))

(defn transactions:drop-all!
  "Drop all relevant transactions tables."
  [db lo hi logger & {:keys [retry]}]
  (->Pool :name    "drop-tables"
          :logger   logger
          :workers (Math/clamp (- hi lo) 4 20)

          :pending
          (or retry
              (conj (for [n (range lo hi)]
                      {:table (transactions:partition-name n)})
                    {:table +transactions+}
                    {:table +tx-digests+}
                    {:table +cp-tx+}))

          :impl
          (worker {:keys [table]}
            (db/with-table! db table "DROP TABLE IF EXISTS %s")
            nil)))

(defn transactions:bulk-load!
  "Bulk load all transactions into the 1-GIN subset tables.

  Transfers data from the partitions of the `transactions` table, as
  well as from the index tables. `bounds` is a sequence containing the
  bounds of partitions to load into, in transaction sequence numbers,
  and `batch` is their batch size."
  [db bounds batch logger & {:keys [retry]}]
  (->Pool :name   "tx:bulk-load"
          :logger  logger
          :workers 100

          :pending
          (or retry
              (for [batch (-> bounds (bounds->batches batch))
                    table [:transactions :tx-digests]]
                (assoc batch :fill table)))

          :impl
          (worker {:keys [lo hi part fill]}
            (->> [(case fill
                    :transactions
                    (format
                     "INSERT INTO %s
                      SELECT
                              t.tx_sequence_number,
                              t.transaction_digest,
                              t.raw_transaction,
                              t.raw_effects,
                              t.checkpoint_sequence_number,
                              t.timestamp_ms,
                              t.object_changes,
                              t.balance_changes,
                              t.events,
                              t.transaction_kind,
                              t.success_command_count,
                              ARRAY(
                                      SELECT DISTINCT
                                              package
                                      FROM
                                              tx_calls i
                                      WHERE
                                              i.tx_sequence_number =
                                              t.tx_sequence_number
                              ) packages,
                              ARRAY(
                                      SELECT DISTINCT
                                              '0x' || ENCODE(package, 'hex') ||
                                              '::' || module
                                      FROM
                                              tx_calls i
                                      WHERE
                                              i.tx_sequence_number =
                                              t.tx_sequence_number
                              ) modules,
                              ARRAY(
                                      SELECT DISTINCT
                                              '0x' || ENCODE(package, 'hex') ||
                                              '::' || module ||
                                              '::' || func
                                      FROM
                                              tx_calls i
                                      WHERE
                                              i.tx_sequence_number =
                                              t.tx_sequence_number
                              ) functions,
                              ARRAY(
                                      SELECT
                                              sender
                                      FROM
                                              tx_senders i
                                      WHERE
                                              i.tx_sequence_number =
                                              t.tx_sequence_number
                              ) senders,
                              ARRAY(
                                      SELECT
                                              recipient
                                      FROM
                                              tx_recipients i
                                      WHERE
                                              i.tx_sequence_number =
                                              t.tx_sequence_number
                              ) recipients,
                              ARRAY(
                                      SELECT
                                              object_id
                                      FROM
                                              tx_input_objects i
                                      WHERE
                                              i.tx_sequence_number =
                                              t.tx_sequence_number
                              ) inputs,
                              ARRAY(
                                      SELECT
                                              object_id
                                      FROM
                                              tx_changed_objects i
                                      WHERE
                                              i.tx_sequence_number =
                                              t.tx_sequence_number
                              ) changed
                      FROM
                              transactions t
                      WHERE
                              tx_sequence_number BETWEEN ? AND ?"
                     (transactions:partition-name part))

                    :tx-digests
                    (format
                     "INSERT INTO %s
                       SELECT
                               transaction_digest AS tx_digest,
                               tx_sequence_number
                       FROM
                               transactions
                       WHERE
                               tx_sequence_number BETWEEN ? AND ?"
                     +tx-digests+)
                    )
                  lo (dec hi)]
                 (jdbc/execute! db)
                 first :next.jdbc/update-count
                 (hash-map :updated)))

          :finalize
          (fn [{:as task :keys [status fill updated]} signals]
            (when (= :success status)
              (swap! signals update fill (fnil + 0) updated)
              nil))))

(defn checkpoints:bulk-load!
  "Load data into `+cp-tx+` corresponding to checkpoints between
  `cp-lo` (inclusive) and `cp-hi` (exclusive). Work is split up into
  batches of at most `batch`."
  [db cp-lo cp-hi batch logger & {:keys [retry]}]
  (->Pool :name   "cp:bulk-load"
          :logger  logger
          :workers 20

          :pending
          (or retry
              (for [lo (range cp-lo cp-hi batch)
                    :let [hi (min (+ lo batch) cp-hi)]]
                {:lo lo :hi hi}))

          :impl
          (worker {:keys [lo hi]}
            (->> [(format
                   "INSERT INTO %s
                    SELECT
                            checkpoint_sequence_number,
                            MIN(tx_sequence_number) AS min_tx_sequence_number,
                            MAX(tx_sequence_number) AS max_tx_sequence_number
                    FROM
                            transactions
                    WHERE
                            checkpoint_sequence_number BETWEEN ? AND ?
                    GROUP BY
                            checkpoint_sequence_number"
                   +cp-tx+)
                  lo (dec hi)]
                 (jdbc/execute! db)
                 first :next.jdbc/update-count
                 (hash-map :updated)))

          :finalize
          (fn [{:as task :keys [status updated]} signals]
            (when (= :success status)
              (swap! signals update :cp-tx (fnil + 0) updated)
              nil))))

(defn transactions:index-and-attach-partitions!
  "Attach `part`ition`s` to the `transactions` table.

  `parts` describes the partitions to attach. It is a map containing
  the `part`ition number, its inclusive transaction lower bound, and
  its exclusive transaction upper bound."
  [db parts logger & {:keys [retry]}]
  (->Pool :name   "index-and-attach"
          :logger  logger
          :workers 50

          :pending
          (or retry
              (for [part parts]
                (assoc part :job :autovacuum)))

          :impl
          (worker {:keys [part lo hi job]}
            (case job
              :autovacuum   (db/reset-autovacuum! db (transactions:partition-name part))
              :analyze      (db/vacuum-and-analyze! db (transactions:partition-name part))
              :constrain    (transactions:constrain! db part lo hi)
              :index-filter (transactions:index-filters! db part)
              :attach       (transactions:attach! db part lo hi)
              :drop-check   (transactions:drop-range-check! db part))
            nil)

          :finalize
          (fn [{:as task :keys [part lo hi status job]} signals]
            (let [phases [:autovacuum :analyze :constrain :index-filter :attach :drop-check]
                  edges  (into {} (map vector phases (rest phases)))]
                (when (= :success status)
                  (swap! signals update job (fnil inc 0))
                  (when-let [next (edges job)]
                    [(assoc task :job next)]))))))

(defn transactions:constrain-tx!
  "Add indices and constraints to side tables."
  [db logger & {:keys [retry]}]
  (->Pool :name   "index-and-constrain"
          :logger  logger
          :workers 10

          :pending
          (or retry
              [{:fn tx-digests:constrain! :label :tx-digests/constrain}
               {:fn cp-tx:constrain!      :label :cp-tx/constrain}])

          :impl
          (worker {work :fn} (work db) nil)

          :finalize
          (fn [{:keys [status label]} signals]
            (when (= :success status)
              (swap! signals update :done
                     (fnil conj []) label)
              nil))))

(defn transactions:vacuum-index!
  "Re-enable auto-vacuum on the index tables, and perform a vacuum/analyze."
  [db logger & {:keys [retry]}]
  (->Pool :name   "vacuum-index"
          :logger  logger
          :workers 10

          :pending
          (or retry
              (map #(hash-map :job :autovacuum :index %)
                   [+tx-digests+ +cp-tx+]))

          :impl
          (worker {:keys [job index]}
            (case job
              :autovacuum (db/reset-autovacuum! db index)
              :analyze    (db/vacuum-and-analyze! db index))
            nil)

          :finalize
          (fn [{:as task :keys [status job]} signals]
            (when (= :success status)
              (swap! signals update job (fnil inc 0))
              (when (= :autovacuum job)
                [(assoc task :job :analyze)])))))
