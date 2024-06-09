(ns hybrid-tx
  (:require [db]
            [logger :refer [->Logger] :as l]
            [next.jdbc :as jdbc]
            [pool :refer [->Pool worker]]
            [transactions :refer [bounds->batches]]))

;; # Hybrid Transactions Schema
;;
;; Proposed new schema for the transactions table merging ideas from the GIN and
;; normalized schemas:
;;
;; - The `transactions` table is partitioned by just `tx_sequence_number`.
;;
;; - There is a side table for every possible "atomic" query, with an optional
;;   additional `sender` column to limit the search to a particular sender.
;;
;; - Each side table gets two indices, one for its main query, and one for the
;;   `sender` plus the main query column. The exceptions are the `tx-digests`
;;   (which should be selective enough on its own) and `tx-kinds` tables (see
;;   below).
;;
;; - We go back to assuming a single `sender` per transaction.
;;
;; - We replace the `tx-system` table with a `tx-kind` table, which lists all
;;   programmable and non-programmable transactions. It does not need a `sender`
;;   column because we know that if a sender is supplied it implies a
;;   transaction kind: (`0x0` implies a system transaction, otherwise
;;   programmable).
;;
;; - All queries are modeled in two steps: The first fetches a list of
;;   transaction sequence numbers (this is what we will model), and the second
;;   fetches those transaction sequence numbers from the `transactions` table.
;;   We split these into two steps to give postgres less of a chance to
;;   incorrectly optimize the join between the two tables.
;;
;; - We also introduce two denormalized tables: `tx-filters` and
;;   `tx-filters-gin` which both contain the combination of all filters into one
;;   denormalized table (but not the transaction contents). These will be used
;;   to test whether it is more efficient to implement compound filters as:
;;
;;   - A sequential scan over a filter table.
;;   - Bitmap index scans over a GIN on the filter table.
;;   - Joins over the atomic filter tables.
;;
;;   In particular, we want to ensure that compound filters work for fairly
;;   large ranges of transaction sequence numbers, so that we can avoid timeouts
;;   by requiring users to set a limit on the number of transaction sequence
;;   numbers that can be scanned in the case of such queries.

;; Table Names ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:private prefix "amnn_0_hybrid_")

(def +transactions+       (str prefix "transactions"))
(def +tx-filters+         (str prefix "tx_filters"))
(def +tx-filters-gin+     (str prefix "tx_filters_gin"))
(def +tx-calls-pkg+       (str prefix "tx_calls_pkg"))
(def +tx-calls-mod+       (str prefix "tx_calls_mod"))
(def +tx-calls-fun+       (str prefix "tx_calls_fun"))
(def +tx-senders+         (str prefix "tx_senders"))
(def +tx-recipients+      (str prefix "tx_recipients"))
(def +tx-input-objects+   (str prefix "tx_input_objects"))
(def +tx-changed-objects+ (str prefix "tx_changed_objects"))
(def +tx-digests+         (str prefix "tx_digests"))
(def +tx-kinds+           (str prefix "tx_kinds"))
(def +cp-tx+              (str prefix "cp_tx"))

(defn transactions:partition-name [n]
  (str +transactions+ "_partition_" n))

(defn tx-filters:partition-name [n]
  (str +tx-filters+ "_partition_" n))

(defn tx-filters-gin:partition-name [n]
  (str +tx-filters-gin+ "_partition_" n))

;; Table: hybrid_transactions ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn transactions:create! [db]
  (db/with-table! db +transactions+
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
           success_command_count      SMALLINT        NOT NULL
       ) PARTITION BY RANGE (tx_sequence_number)"))

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
           success_command_count      SMALLINT
       )")
    (db/disable-autovacuum! tx (transactions:partition-name n))))

(defn transactions:constrain! [db n lo hi]
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
          ADD CONSTRAINT %1$s_partition_check CHECK (
              %2$d <= tx_sequence_number
          AND tx_sequence_number < %3$d
          )"
         (transactions:partition-name n) lo hi)]
       (jdbc/execute! db)))

(defn transactions:attach! [db n lo hi]
  (->> [(format
         "ALTER TABLE %s
          ATTACH PARTITION %s FOR VALUES FROM (%d) to (%d)"
          +transactions+ (transactions:partition-name n) lo hi)]
       (jdbc/execute! db)))

(defn transactions:drop-range-check! [db n]
  (db/with-table! db (transactions:partition-name n)
    "ALTER TABLE %1$s DROP CONSTRAINT %1$s_partition_check"))

;; Table: hybrid_tx_filters ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tx-filters:create! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx +tx-filters+
      "CREATE TABLE %s (
           tx_sequence_number         BIGINT          PRIMARY KEY,
           packages                   BYTEA[]         NOT NULL,
           modules                    TEXT[]          NOT NULL,
           functions                  TEXT[]          NOT NULL,
           sender                     BYTEA           NOT NULL,
           recipients                 BYTEA[]         NOT NULL,
           inputs                     BYTEA[]         NOT NULL,
           changed                    BYTEA[]         NOT NULL
       ) PARTITION BY RANGE (tx_sequence_number)")))

(defn tx-filters:create-partition! [db n]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx (tx-filters:partition-name n)
      "CREATE TABLE %s (
           tx_sequence_number         BIGINT,
           packages                   BYTEA[],
           modules                    TEXT[],
           functions                  TEXT[],
           sender                     BYTEA,
           recipients                 BYTEA[],
           inputs                     BYTEA[],
           changed                    BYTEA[]
       )")
    (db/disable-autovacuum! tx (tx-filters:partition-name n))))

(defn tx-filters:constrain! [db n lo hi]
  (->> [(format
         "ALTER TABLE %1$s
          ADD PRIMARY KEY (tx_sequence_number),
          ALTER COLUMN packages                   SET NOT NULL,
          ALTER COLUMN modules                    SET NOT NULL,
          ALTER COLUMN functions                  SET NOT NULL,
          ALTER COLUMN sender                     SET NOT NULL,
          ALTER COLUMN recipients                 SET NOT NULL,
          ALTER COLUMN inputs                     SET NOT NULL,
          ALTER COLUMN changed                    SET NOT NULL,
          ADD CONSTRAINT %1$s_partition_check CHECK (
              %2$d <= tx_sequence_number
          AND tx_sequence_number < %3$d
          )"
         (tx-filters:partition-name n) lo hi)]
       (jdbc/execute! db)))

(defn tx-filters:attach! [db n lo hi]
  (->> [(format
         "ALTER TABLE %1$s
          ATTACH PARTITION %2$s FOR VALUES FROM (%3$d) TO (%4$d)"
         +tx-filters+ (tx-filters:partition-name n) lo hi)]
       (jdbc/execute! db)))

(defn tx-filters:drop-range-check! [db n]
  (db/with-table! db (tx-filters:partition-name n)
    "ALTER TABLE %1$s DROP CONSTRAINT %1$s_partition_check"))

;; Table: hybrid_tx_filters_gin ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tx-filters-gin:create! [db]
  (jdbc/with-transaction [tx db]
    (jdbc/execute! tx ["CREATE EXTENSION IF NOT EXISTS btree_gin"])
    (db/with-table! tx +tx-filters-gin+
      "CREATE TABLE %s (
           tx_sequence_number         BIGINT          PRIMARY KEY,
           packages                   BYTEA[]         NOT NULL,
           modules                    TEXT[]          NOT NULL,
           functions                  TEXT[]          NOT NULL,
           sender                     BYTEA           NOT NULL,
           recipients                 BYTEA[]         NOT NULL,
           inputs                     BYTEA[]         NOT NULL,
           changed                    BYTEA[]         NOT NULL
       ) PARTITION BY RANGE (tx_sequence_number)")
    (db/with-table! tx +tx-filters-gin+
      "CREATE INDEX %1$s_gin ON %1$s USING gin (
           packages, modules, functions,
           sender, recipients,
           inputs, changed
       )")))

(defn tx-filters-gin:create-partition! [db n]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx (tx-filters-gin:partition-name n)
      "CREATE TABLE %s (
           tx_sequence_number         BIGINT,
           packages                   BYTEA[],
           modules                    TEXT[],
           functions                  TEXT[],
           sender                     BYTEA,
           recipients                 BYTEA[],
           inputs                     BYTEA[],
           changed                    BYTEA[]
       )")
    (db/disable-autovacuum! tx (tx-filters-gin:partition-name n))))

(defn tx-filters-gin:constrain! [db n lo hi]
  (->> [(format
         "ALTER TABLE %1$s
          ADD PRIMARY KEY (tx_sequence_number),
          ALTER COLUMN packages                   SET NOT NULL,
          ALTER COLUMN modules                    SET NOT NULL,
          ALTER COLUMN functions                  SET NOT NULL,
          ALTER COLUMN sender                     SET NOT NULL,
          ALTER COLUMN recipients                 SET NOT NULL,
          ALTER COLUMN inputs                     SET NOT NULL,
          ALTER COLUMN changed                    SET NOT NULL,
          ADD CONSTRAINT %1$s_partition_check CHECK (
              %2$d <= tx_sequence_number
          AND tx_sequence_number < %3$d
          )"
         (tx-filters-gin:partition-name n) lo hi)]
       (jdbc/execute! db)))

(defn tx-filters-gin:index-filters! [db n]
  (db/with-table! db (tx-filters-gin:partition-name n)
    "CREATE INDEX %1$s_gin ON %1$s USING gin (
         packages, modules, functions,
         sender, recipients,
         inputs, changed
     )"))

(defn tx-filters-gin:attach! [db n lo hi]
  (jdbc/with-transaction [tx db]
    (let [part   (tx-filters-gin:partition-name n)
          attach (fn [template]
                   (jdbc/execute!
                    tx [(format template +tx-filters-gin+ part lo hi)]))]
      (attach "ALTER TABLE %1$s
               ATTACH PARTITION %2$s FOR VALUES FROM (%3$d) TO (%4$d)")
      (attach "ALTER INDEX %1$s_gin ATTACH PARTITION %2$s_gin"))))

(defn tx-filters-gin:drop-range-check! [db n]
  (db/with-table! db (tx-filters-gin:partition-name n)
    "ALTER TABLE %1$s DROP CONSTRAINT %1$s_partition_check"))

;; Table: hybrid_tx_calls_pkg ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tx-calls-pkg:create! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx +tx-calls-pkg+
      "CREATE TABLE %s (
            package                     BYTEA,
            tx_sequence_number          BIGINT,
            sender                      BYTEA
       )")
    (db/disable-autovacuum! tx +tx-calls-pkg+)))

(defn tx-calls-pkg:constrain! [db]
  (db/with-table! db +tx-calls-pkg+
    "ALTER TABLE %1$s
     ADD PRIMARY KEY (package, tx_sequence_number),
     ALTER COLUMN sender SET NOT NULL"))

(defn tx-calls-pkg:index! [db]
  (db/with-table! db +tx-calls-pkg+
    "CREATE INDEX %1$s_sender ON %1$s (sender, package, tx_sequence_number)"))

;; Table: hybrid_tx_calls_mod ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tx-calls-mod:create! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx +tx-calls-mod+
      "CREATE TABLE %s (
            package                     BYTEA,
            module                      TEXT,
            tx_sequence_number          BIGINT,
            sender                      BYTEA
       )")
    (db/disable-autovacuum! tx +tx-calls-mod+)))

(defn tx-calls-mod:constrain! [db]
  (db/with-table! db +tx-calls-mod+
    "ALTER TABLE %1$s
     ADD PRIMARY KEY (package, module, tx_sequence_number),
     ALTER COLUMN sender SET NOT NULL"))

(defn tx-calls-mod:index! [db]
  (db/with-table! db +tx-calls-mod+
    "CREATE INDEX %1$s_sender ON %1$s (
         sender, package, module, tx_sequence_number
     )"))

;; Table: hybrid_tx_calls_fun ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tx-calls-fun:create! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx +tx-calls-fun+
      "CREATE TABLE %s (
            package                     BYTEA,
            module                      TEXT,
            func                        TEXT,
            tx_sequence_number          BIGINT,
            sender                      BYTEA
       )")
    (db/disable-autovacuum! tx +tx-calls-fun+)))

(defn tx-calls-fun:constrain! [db]
  (db/with-table! db +tx-calls-fun+
    "ALTER TABLE %1$s
     ADD PRIMARY KEY (package, module, func, tx_sequence_number),
     ALTER COLUMN sender SET NOT NULL"))

(defn tx-calls-fun:index! [db]
  (db/with-table! db +tx-calls-fun+
    "CREATE INDEX %1$s_sender ON %1$s (
         sender, package, module, func, tx_sequence_number
     )"))

;; Table: hybrid_tx_senders ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tx-senders:create! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx +tx-senders+
      "CREATE TABLE %s (
            sender                      BYTEA,
            tx_sequence_number          BIGINT
       )")
    (db/disable-autovacuum! tx +tx-senders+)))

(defn tx-senders:constrain! [db]
  (db/with-table! db +tx-senders+
    "ALTER TABLE %1$s ADD PRIMARY KEY (sender, tx_sequence_number)"))

;; Table: hybrid_tx_recipients ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tx-recipients:create! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx +tx-recipients+
      "CREATE TABLE %s (
            recipient                   BYTEA,
            tx_sequence_number          BIGINT,
            sender                      BYTEA
       )")
    (db/disable-autovacuum! tx +tx-recipients+)))

(defn tx-recipients:constrain! [db]
  (db/with-table! db +tx-recipients+
    "ALTER TABLE %1$s
     ADD PRIMARY KEY (recipient, tx_sequence_number),
     ALTER COLUMN sender SET NOT NULL"))

(defn tx-recipients:index! [db]
  (db/with-table! db +tx-recipients+
    "CREATE INDEX %1$s_sender ON %1$s (sender, recipient, tx_sequence_number)"))

;; Table: hybrid_tx_input_objects ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tx-input-objects:create! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx +tx-input-objects+
      "CREATE TABLE %s (
            object_id                   BYTEA,
            tx_sequence_number          BIGINT,
            sender                      BYTEA
       )")
    (db/disable-autovacuum! tx +tx-input-objects+)))

(defn tx-input-objects:constrain! [db]
  (db/with-table! db +tx-input-objects+
    "ALTER TABLE %1$s
     ADD PRIMARY KEY (object_id, tx_sequence_number),
     ALTER COLUMN sender SET NOT NULL"))

(defn tx-input-objects:index! [db]
  (db/with-table! db +tx-input-objects+
    "CREATE INDEX %1$s_sender ON %1$s (sender, object_id, tx_sequence_number)"))

;; Table: hybrid_tx_changed_objects ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tx-changed-objects:create! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx +tx-changed-objects+
      "CREATE TABLE %s (
            object_id                   BYTEA,
            tx_sequence_number          BIGINT,
            sender                      BYTEA
       )")
    (db/disable-autovacuum! tx +tx-changed-objects+)))

(defn tx-changed-objects:constrain! [db]
  (db/with-table! db +tx-changed-objects+
    "ALTER TABLE %1$s
     ADD PRIMARY KEY (object_id, tx_sequence_number),
     ALTER COLUMN sender SET NOT NULL"))

(defn tx-changed-objects:index! [db]
  (db/with-table! db +tx-changed-objects+
    "CREATE INDEX %1$s_sender ON %1$s (sender, object_id, tx_sequence_number)"))

;; Table: hybrid_tx_digests ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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

;; Table: hybrid_tx_kinds ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tx-kinds:create! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! db +tx-kinds+
      "CREATE TABLE %s (
            tx_kind                     SMALLINT,
            tx_sequence_number          BIGINT
       )")
    (db/disable-autovacuum! tx +tx-kinds+)))

(defn tx-kinds:constrain! [db]
  (db/with-table! db +tx-kinds+
    "ALTER TABLE %1$s ADD PRIMARY KEY (tx_kind, tx_sequence_number)"))

;; Table: hybrid_cp_tx ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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

(defn transactions:create-all! [db lo hi logger & {:keys [retry]}]
  (->Pool :name    "create-tables"
          :logger   logger
          :workers (Math/clamp (- hi lo) 4 20)

          :pending
          (or retry
              (concat
               (for [n (range lo hi)]
                 {:fn #(transactions:create-partition! % n)
                  :label (str "transactions:partition-" n)})
               (for [n (range lo hi)]
                 {:fn #(tx-filters:create-partition! % n)
                  :label (str "tx-filters:partition-" n)})
               (for [n (range lo hi)]
                 {:fn #(tx-filters-gin:create-partition! % n)
                  :label (str "tx-filters-gin:partition-" n)})
               [{:fn transactions:create! :label "transactions"}
                {:fn tx-filters:create! :label "tx-filters"}
                {:fn tx-filters-gin:create! :label "tx-filters-gin"}
                {:fn tx-calls-pkg:create! :label "tx-calls-pkg"}
                {:fn tx-calls-mod:create! :label "tx-calls-mod"}
                {:fn tx-calls-fun:create! :label "tx-calls-fun"}
                {:fn tx-senders:create! :label "tx-senders"}
                {:fn tx-recipients:create! :label "tx-recipients"}
                {:fn tx-input-objects:create! :label "tx-input-objects"}
                {:fn tx-changed-objects:create! :label "tx-changed-objects"}
                {:fn tx-digests:create! :label "tx-digests"}
                {:fn tx-kinds:create! :label "tx-kinds"}
                {:fn cp-tx:create! :label "cp-tx"}]))

          :impl (worker {builder :fn} (builder db) nil)))


(defn transactions:drop-all! [db lo hi logger & {:keys [retry]}]
  (->Pool :name    "drop-tables"
          :logger   logger
          :workers (Math/clamp (- hi lo) 4 20)

          :pending
          (or retry
              (concat
               (for [part (range lo hi)
                     name [transactions:partition-name
                           tx-filters:partition-name
                           tx-filters-gin:partition-name]]
                 {:table (name part)})
               (map #(hash-map :table %)
                    [+transactions+ +tx-filters+ +tx-filters-gin+
                     +tx-calls-pkg+ +tx-calls-mod+ +tx-calls-fun+
                     +tx-senders+ +tx-recipients+
                     +tx-input-objects+ +tx-changed-objects+
                     +tx-digests+ +tx-kinds+ +cp-tx+])))

          :impl
          (worker {:keys [table]}
            (db/with-table! db table "DROP TABLE IF EXISTS %s") nil)))


(defn transactions:bulk-load!
  "`bound` is a sequence containing the bounds of partitions to load
  into, in transaction sequence numbers, and `batch` is the max batch
  size of a single `INSERT` (counted in number of transactions)."
  [db bounds batch logger & {:keys [retry]}]
  (->Pool :name   "tx:bulk-load"
          :logger  logger
          :workers 100

          :pending
          (or retry
              (for [batch (-> bounds (bounds->batches batch))
                    table [:transactions :tx-filters :tx-filters-gin
                           :tx-calls-pkg :tx-calls-mod  :tx-calls-fun
                           :tx-senders :tx-recipients
                           :tx-input-objects :tx-changed-objects
                           :tx-digests]]
                (assoc batch :fill table)))

          :impl
          (worker {:keys [lo hi part fill]}
            (->> [(case fill
                    :transactions
                    (format
                     "INSERT INTO %s
                      SELECT
                              *
                      FROM
                              transactions
                      WHERE
                              tx_sequence_number BETWEEN ? AND ?"
                     (transactions:partition-name part))

                    :tx-filters
                    (format
                     "INSERT INTO %s
                      SELECT
                              tx_sequence_number,
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
                              s.sender AS sender,
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
                      LEFT JOIN
                              tx_senders s
                      USING  (tx_sequence_number)
                      WHERE
                              tx_sequence_number BETWEEN ? AND ?"
                     (tx-filters:partition-name part))

                    :tx-filters-gin
                    (format
                     "INSERT INTO %s
                      SELECT
                              tx_sequence_number,
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
                              s.sender AS sender,
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
                      LEFT JOIN
                              tx_senders s
                      USING  (tx_sequence_number)
                      WHERE
                              tx_sequence_number BETWEEN ? AND ?"
                     (tx-filters-gin:partition-name part))

                    :tx-calls-pkg
                    (format
                     "INSERT INTO %s
                      SELECT DISTINCT
                              package,
                              tx_sequence_number,
                              sender
                      FROM
                              tx_calls
                      LEFT JOIN
                              tx_senders
                      USING  (tx_sequence_number)
                      WHERE
                              tx_sequence_number BETWEEN ? AND ?"
                     +tx-calls-pkg+)

                    :tx-calls-mod
                    (format
                     "INSERT INTO %s
                      SELECT DISTINCT
                              package,
                              module,
                              tx_sequence_number,
                              sender
                      FROM
                              tx_calls
                      LEFT JOIN
                              tx_senders
                      USING  (tx_sequence_number)
                      WHERE
                              tx_sequence_number BETWEEN ? AND ?"
                     +tx-calls-mod+)

                    :tx-calls-fun
                    (format
                     "INSERT INTO %s
                      SELECT DISTINCT
                              package,
                              module,
                              func,
                              tx_sequence_number,
                              sender
                      FROM
                              tx_calls
                      LEFT JOIN
                              tx_senders
                      USING  (tx_sequence_number)
                      WHERE
                              tx_sequence_number BETWEEN ? AND ?"
                     +tx-calls-fun+)

                    :tx-senders
                    (format
                     "INSERT INTO %s
                      SELECT
                              sender,
                              tx_sequence_number
                      FROM
                              tx_senders
                      WHERE
                              tx_sequence_number BETWEEN ? AND ?"
                     +tx-senders+)

                    :tx-recipients
                    (format
                     "INSERT INTO %s
                      SELECT
                              recipient,
                              tx_sequence_number,
                              sender
                      FROM
                              tx_recipients
                      LEFT JOIN
                              tx_senders
                      USING  (tx_sequence_number)
                      WHERE
                              tx_sequence_number BETWEEN ? AND ?"
                     +tx-recipients+)

                    :tx-input-objects
                    (format
                     "INSERT INTO %s
                      SELECT
                              object_id,
                              tx_sequence_number,
                              sender
                      FROM
                              tx_input_objects
                      LEFT JOIN
                              tx_senders
                      USING  (tx_sequence_number)
                      WHERE
                              tx_sequence_number BETWEEN ? AND ?"
                     +tx-input-objects+)

                    :tx-changed-objects
                    (format
                     "INSERT INTO %s
                      SELECT
                              object_id,
                              tx_sequence_number,
                              sender
                      FROM
                              tx_changed_objects
                      LEFT JOIN
                              tx_senders
                      USING  (tx_sequence_number)
                      WHERE
                              tx_sequence_number BETWEEN ? AND ?"
                     +tx-changed-objects+)

                    :tx-digests
                    (format
                     "INSERT INTO %s
                      SELECT
                              tx_digest,
                              tx_sequence_number
                      FROM
                              tx_digests
                      WHERE
                              tx_sequence_number BETWEEN ? AND ?"
                     +tx-digests+)

                    :tx-kinds
                    (format
                     "INSERT INTO %s
                      SELECT
                              transaction_kind,
                              tx_sequence_number
                      FROM
                              transactions
                      WHERE
                              tx_sequence_number BETWEEN ? AND ?"
                     +tx-kinds+)
                    )
                  lo (dec hi)]
                 (jdbc/execute! db)
                 first :next.jdbc/update-count
                 (hash-map :updated)))

          :finalize
          (fn [{:keys [status fill updated]} signals]
            (when (= :success status)
              (swap! signals update fill (fnil + 0) updated) nil))))


(defn checkpoints:bulk-load! [db cp-lo cp-hi batch logger & {:keys [retry]}]
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
          (fn [{:keys [status updated]} signals]
            (when (= :success status)
              (swap! signals update :cp-tx (fnil + 0) updated) nil))))


(defn transactions:index-and-attach-partitions!
  [db parts logger & {:keys [retry]}]
  (->Pool :name   "tx:index-and-attach"
          :logger  logger
          :workers 50

          :pending (or retry (for [part parts] (assoc part :job :autovacuum)))

          :impl
          (worker {:keys [part job lo hi]}
            (case job
              :autovacuum (db/reset-autovacuum! db (transactions:partition-name part))
              :analyze    (db/vacuum-and-analyze! db (transactions:partition-name part))
              :constrain  (transactions:constrain! db part lo hi)
              :attach     (transactions:attach! db part lo hi)
              :drop-check (transactions:drop-range-check! db part))
            nil)

          :finalize
          (fn [{:as task :keys [job status]} signals]
            (let [phases [:autovacuum :analyze :constrain :attach :drop-check]
                  edges  (into {} (map vector phases (rest phases)))]
              (when (= :success status)
                (swap! signals update job (fnil inc 0))
                (when-let [next (edges job)]
                  [(assoc task :job next)]))))))


(defn tx-filters:index-and-attach-partitions!
  [db parts logger & {:keys [retry]}]
  (->Pool :name   "tf:index-and-attach"
          :logger  logger
          :workers 50

          :pending (or retry (for [part parts] (assoc part :job :autovacuum)))

          :impl
          (worker {:keys [part job lo hi]}
            (case job
              :autovacuum (db/reset-autovacuum! db (tx-filters:partition-name part))
              :analyze    (db/vacuum-and-analyze! db (tx-filters:partition-name part))
              :constrain  (tx-filters:constrain! db part lo hi)
              :attach     (tx-filters:attach! db part lo hi)
              :drop-check (tx-filters:drop-range-check! db part))
            nil)

          :finalize
          (fn [{:as task :keys [job status]} signals]
            (let [phases [:autovacuum :analyze :constrain :attach :drop-check]
                  edges  (into {} (map vector phases (rest phases)))]
              (when (= :success status)
                (swap! signals update job (fnil inc 0))
                (when-let [next (edges job)]
                  [(assoc task :job next)]))))))


(defn tx-filters-gin:index-and-attach-partitions!
  [db parts logger & {:keys [retry]}]
  (->Pool :name   "tg:index-and-attach"
          :logger  logger
          :workers 50

          :pending (or retry (for [part parts] (assoc part :job :autovacuum)))

          :impl
          (worker {:keys [part job lo hi]}
            (case job
              :autovacuum   (db/reset-autovacuum! db (tx-filters-gin:partition-name part))
              :analyze      (db/vacuum-and-analyze! db (tx-filters-gin:partition-name part))
              :constrain    (tx-filters-gin:constrain! db part lo hi)
              :index-filter (tx-filters-gin:index-filters! db part)
              :attach       (tx-filters-gin:attach! db part lo hi)
              :drop-check   (tx-filters-gin:drop-range-check! db part))
            nil)

          :finalize
          (fn [{:as task :keys [job status]} signals]
            (let [phases [:autovacuum :analyze :constrain :index-filter :attach :drop-check]
                  edges  (into {} (map vector phases (rest phases)))]
              (when (= :success status)
                (swap! signals update job (fnil inc 0))
                (when-let [next (edges job)]
                  [(assoc task :job next)]))))))


(defn transactions:constrain-and-index-tx! [db logger & {:keys [retry]}]
  (->Pool :name   "index-and-constrain"
          :logger  logger
          :workers 10

          :pending
          (or retry
              [{:fn tx-calls-pkg:constrain!       :label :tx-calls-pkg/constrain}
               {:fn tx-calls-mod:constrain!       :label :tx-calls-mod/constrain}
               {:fn tx-calls-fun:constrain!       :label :tx-calls-fun/constrain}
               {:fn tx-senders:constrain!         :label :tx-senders/constrain}
               {:fn tx-recipients:constrain!      :label :tx-recipients/constrain}
               {:fn tx-input-objects:constrain!   :label :tx-input-objects/constrain}
               {:fn tx-changed-objects:constrain! :label :tx-changed-objects/constrain}
               {:fn tx-digests:constrain!         :label :tx-digests/constrain}
               {:fn tx-kinds:constrain!           :label :tx-kinds/constrain}
               {:fn cp-tx:constrain!              :label :cp-tx/constrain}
               {:fn tx-calls-pkg:index!           :label :tx-calls-pkg/index}
               {:fn tx-calls-mod:index!           :label :tx-calls-mod/index}
               {:fn tx-calls-fun:index!           :label :tx-calls-fun/index}
               {:fn tx-recipients:index!          :label :tx-recipients/index}
               {:fn tx-input-objects:index!       :label :tx-input-objects/index}
               {:fn tx-changed-objects:index!     :label :tx-changed-objects/index}])

          :impl
          (worker {work :fn} (work db) nil)

          :finalize
          (fn [{:as task :keys [status label]} signals]
            (when (= :success status)
              (swap! signals update :done (fnil conj []) label) nil))))


(defn transactions:vacuum-index! [db logger & {:keys [retry]}]
  (->Pool :name   "vacuum-index"
          :logger  logger
          :workers 10

          :pending
          (or retry
              (map #(hash-map :job :autovacuum :index %)
                   [+tx-calls-pkg+ +tx-calls-mod+ +tx-calls-fun+
                    +tx-senders+ +tx-recipients+
                    +tx-input-objects+ +tx-changed-objects+
                    +tx-digests+ +tx-kinds+ +cp-tx+]))

          :impl
          (worker {:keys [job index]}
            (case job
              :autovacuum (db/reset-autovacuum! db   index)
              :analyze    (db/vacuum-and-analyze! db index))
            nil)

          :finalize
          (fn [{:as task :keys [status job]} signals]
            (when (= :success status)
              (swap! signals update job (fnil inc 0))
              (when (= :autovacuum job)
                [(assoc task :job :analyze)])))))
