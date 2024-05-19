(ns norm-tx
  (:require [transactions :refer [bounds->batches]]
            [db]
            [logger :refer [->Logger] :as l]
            [pool :refer [->Pool, signal-swap! signals]]
            [next.jdbc :as jdbc]
            [clojure.string :refer [starts-with?]]))

;; # Normalized Transactions Schema
;;
;; Proposed new schema for the transactions table and associated side
;; tables, designed to take full advantage of streaming merge joins:
;;
;; - The `transactions` table is partitioned by just
;;   `tx_sequence_number` to make it easier for the query planner to
;;   leverage the sequence numbers generaged by filters to limit the
;;   partitions it looks in.
;; - There is a side table for every possible "atomic" query that can
;;   be composed (by intersection or union).
;; - Side tables are indexed on their query parameters and then
;;   `tx_sequence_number`, to ensure that a query on that table will
;;   yield a sorted run of sequence numbers, ideally for merging with
;;   other "atomic" queries.
;; - Extra indices have been largely removed from the main table,
;;   where they are hard to leverage because of the partitioning
;;   scheme -- they get their own side tables now (like digests and system
;;   transactions)

;; Table Names ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:private prefix "amnn_0_norm_")

;; Normalized versions of the `transactions` tables, holding the same data.
(def +transactions+       (str prefix "transactions"))
(def +tx-calls-pkg+       (str prefix "tx_calls_pkg"))
(def +tx-calls-mod+       (str prefix "tx_calls_mod"))
(def +tx-calls-fun+       (str prefix "tx_calls_fun"))
(def +tx-senders+         (str prefix "tx_senders"))
(def +tx-recipients+      (str prefix "tx_recipients"))
(def +tx-input-objects+   (str prefix "tx_input_objects"))
(def +tx-changed-objects+ (str prefix "tx_changed_objects"))
(def +tx-digests+         (str prefix "tx_digests"))
(def +tx-system+          (str prefix "tx_system"))

(defn transactions:partition-name [n]
  (str +transactions+ "_partition_" n))

;; Table: norm_transactions ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn transactions:create! [db _timeout]
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

(defn transactions:create-partition! [db n timeout]
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
    (db/disable-autovacuum! tx (transactions:partition-name n) timeout)))

(defn transactions:constrain!
  "Add constraints to partition `n` of the `transactions` table.

  `lo` and `hi` are the inclusive and exclusive bounds on transasction
  sequence numbers in the partition."
  [db n lo hi timeout]
  (as-> [(format
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
      % (jdbc/execute! db % {:timeout timeout})))

(defn transactions:attach! [db n lo hi timeout]
  (as-> [(format
          "ALTER TABLE %s
           ATTACH PARTITION %s FOR VALUES FROM (%d) to (%d)"
          +transactions+ (transactions:partition-name n) lo hi)]
      % (jdbc/execute! db % {:timeout timeout})))

(defn transactions:drop-range-check! [db n timeout]
  (db/with-table! db (transactions:partition-name n)
    "ALTER TABLE %1$s DROP CONSTRAINT %1$s_partition_check"
    {:timeout timeout}))

;; Table: norm_tx_calls_pkg ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tx-calls-pkg:create! [db timeout]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx +tx-calls-pkg+
      "CREATE TABLE %s (
            package                     BYTEA,
            tx_sequence_number          BIGINT
       )")
    (db/disable-autovacuum! tx +tx-calls-pkg+ timeout)))

(defn tx-calls-pkg:constrain! [db]
  (db/with-table! db +tx-calls-pkg+
    "ALTER TABLE %1$s ADD PRIMARY KEY (package, tx_sequence_number)"))

;; Table: norm_tx_calls_mod ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tx-calls-mod:create! [db timeout]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx +tx-calls-mod+
      "CREATE TABLE %s (
            package                     BYTEA,
            module                      TEXT,
            tx_sequence_number          BIGINT
       )")
    (db/disable-autovacuum! tx +tx-calls-mod+ timeout)))

(defn tx-calls-mod:constrain! [db]
  (db/with-table! db +tx-calls-mod+
    "ALTER TABLE %1$s ADD PRIMARY KEY (package, module, tx_sequence_number)"))

;; Table: norm_tx_calls_fun ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tx-calls-fun:create! [db timeout]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx +tx-calls-fun+
      "CREATE TABLE %s (
            package                     BYTEA,
            module                      TEXT,
            func                        TEXT,
            tx_sequence_number          BIGINT
       )")
    (db/disable-autovacuum! tx +tx-calls-fun+ timeout)))

(defn tx-calls-fun:constrain! [db]
  (db/with-table! db +tx-calls-fun+
    "ALTER TABLE %1$s
     ADD PRIMARY KEY (package, module, func, tx_sequence_number)"))

;; Table: norm_tx_senders ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tx-senders:create! [db timeout]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx +tx-senders+
      "CREATE TABLE %s (
            sender                      BYTEA,
            tx_sequence_number          BIGINT
       )")
    (db/disable-autovacuum! tx +tx-senders+ timeout)))

(defn tx-senders:constrain! [db]
  (db/with-table! db +tx-senders+
    "ALTER TABLE %1$s ADD PRIMARY KEY (sender, tx_sequence_number)"))

;; Table: norm_tx_recipients ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tx-recipients:create! [db timeout]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx +tx-recipients+
      "CREATE TABLE %s (
            recipient                   BYTEA,
            tx_sequence_number          BIGINT
       )")
    (db/disable-autovacuum! tx +tx-recipients+ timeout)))

(defn tx-recipients:constrain! [db]
  (db/with-table! db +tx-recipients+
    "ALTER TABLE %1$s ADD PRIMARY KEY (recipient, tx_sequence_number)"))

;; Table: norm_tx_input_objects ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tx-input-objects:create! [db timeout]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx +tx-input-objects+
      "CREATE TABLE %s (
            object_id                   BYTEA,
            tx_sequence_number          BIGINT
       )")
    (db/disable-autovacuum! tx +tx-input-objects+ timeout)))

(defn tx-input-objects:constrain! [db]
  (db/with-table! db +tx-input-objects+
    "ALTER TABLE %1$s ADD PRIMARY KEY (object_id, tx_sequence_number)"))


;; Table: norm_tx_changes_objects ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tx-changed-objects:create! [db timeout]
  (jdbc/with-transaction [tx db]
    (db/with-table! db +tx-changed-objects+
      "CREATE TABLE %s (
            object_id                   BYTEA,
            tx_sequence_number          BIGINT
       )")
    (db/disable-autovacuum! tx +tx-changed-objects+ timeout)))

(defn tx-changed-objects:constrain! [db]
  (db/with-table! db +tx-changed-objects+
    "ALTER TABLE %1$s ADD PRIMARY KEY (object_id, tx_sequence_number)"))

;; Table: norm_tx_digests ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tx-digests:create! [db timeout]
  (jdbc/with-transaction [tx db]
    (db/with-table! db +tx-digests+
      "CREATE TABLE %s (
            tx_digest                   BYTEA,
            tx_sequence_number          BIGINT
       )")
    (db/disable-autovacuum! tx +tx-digests+ timeout)))

(defn tx-digests:constrain! [db]
  (db/with-table! db +tx-digests+
    "ALTER TABLE %1$s ADD PRIMARY KEY (tx_digest, tx_sequence_number)"))

;; Table: norm_tx_system ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tx-system:create! [db timeout]
  (jdbc/with-transaction [tx db]
    (db/with-table! db +tx-system+
      "CREATE TABLE %s (
            tx_sequence_number          BIGINT
       )")
    (db/disable-autovacuum! tx +tx-system+ timeout)))

(defn tx-system:constrain! [db]
  (db/with-table! db +tx-system+
    "ALTER TABLE %1$s ADD PRIMARY KEY (tx_sequence_number)"))

;; Bulk Loading ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn transactions:create-all!
  "Create all relevant transactions tables.

  Creates the main table, partitions between `lo` (inclusive) and
  `hi` (exclusive), and all the indexing tables, on `db`."
  [db lo hi logger delta-t]
  (->Pool :name    "create-tables"
          :logger   logger
          :workers (Math/clamp (- hi lo) 4 20)

          :pending
          (conj (for [n (range lo hi)]
                  {:fn #(transactions:create-partition! %1 n %2)
                   :label (str "partition-" n)
                   :timeout delta-t
                   :retries 3})
                {:fn transactions:create! :label "transactions"
                 :timeout delta-t :retries 3}
                {:fn tx-calls-pkg:create! :label "tx-calls-pkg"
                 :timeout delta-t :retries 3}
                {:fn tx-calls-mod:create! :label "tx-calls-mod"
                 :timeout delta-t :retries 3}
                {:fn tx-calls-fun:create! :label "tx-calls-fun"
                 :timeout delta-t :retries 3}
                {:fn tx-senders:create! :label "tx-senders"
                 :timeout delta-t :retries 3}
                {:fn tx-recipients:create! :label "tx-recipients"
                 :timeout delta-t :retries 3}
                {:fn tx-input-objects:create! :label "tx-input-objects"
                 :timeout delta-t :retries 3}
                {:fn tx-changed-objects:create! :label "tx-changed-objects"
                 :timeout delta-t :retries 3}
                {:fn tx-digests:create! :label "tx-digests"
                 :timeout delta-t :retries 3}
                {:fn tx-system:create! :label "tx-system"
                 :timeout delta-t :retries 3})

          :impl
          (db/worker {builder :fn timeout :timeout}
            (builder db timeout) nil)

          :finalize
          (fn [{:as task :keys [status timeout retries]}]
            (case status
              :success nil

              :timeout
              [(assoc task :timeout (+ timeout delta-t))]

              :error
              (and (not= 0 retries)
                   [(assoc task :retries (dec retries))])))))

(defn transactions:drop-all!
  "Drop all relevant transactions tables."
  [db lo hi logger delta-t]
  (->Pool :name    "drop-tables"
          :logger   logger
          :workers (Math/clamp (- hi lo) 4 20)

          :pending
          (concat
           (for [n (range lo hi)]
             {:table (transactions:partition-name n)
              :timeout delta-t :retries 3})
           (map (fn [t] {:table t :timeout delta-t :retries 3})
                [+transactions+ +tx-calls-pkg+ +tx-calls-mod+ +tx-calls-fun+
                 +tx-senders+ +tx-recipients+ +tx-input-objects+
                 +tx-changed-objects+ +tx-digests+ +tx-system+]))

          :impl
          (db/worker {:keys [table timeout]}
            (db/with-table! db table
              "DROP TABLE %s" {:timeout timeout})
            nil)

          :finalize
          (fn [{:as task :keys [status timeout retries]}]
            (case status
              :success nil

              :timeout
              [(assoc task :timeout (+ timeout delta-t))]

              :error
              (and (not= 0 retries)
                   [(assoc task :retries (dec retries))])))))

(defn transactions:load-signals []
  (signals :transactions       0
           :tx-calls-pkg       0
           :tx-calls-mod       0
           :tx-calls-fun       0
           :tx-senders         0
           :tx-recipients      0
           :tx-input-objects   0
           :tx-changed-objects 0
           :tx-digests         0
           :tx-system          0
           :failed-jobs        []))

(defn transactions:bulk-load!
  "Bulk load all transactions into the normalized subset tables.

  Transfers data from the partitions of the `transactions` table, as
  well as from the index tables. `bounds` is a sequence containing the
  bounds of partitions to load into, in transaction sequence numbers,
  and `batch` is their batch size."
  [db bounds batch logger signals]
  (->Pool :name   "bulk-load"
          :logger  logger
          :workers 100

          :pending
          (if (some-> signals :failed-jobs deref first)
            (map #(select-keys % [:lo :hi :part :fill])
                 @(:failed-jobs signals))
            (for [batch (-> bounds (bounds->batches batch))
                  table [:transactions
                         :tx-calls-pkg :tx-calls-mod :tx-calls-fun
                         :tx-senders :tx-recipients
                         :tx-input-objects :tx-changed-objects
                         :tx-digests :tx-system]]
              (assoc batch :fill table)))

          :impl
          (db/worker {:keys [lo hi part fill]}
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

                    :tx-calls-pkg
                    (format
                     "INSERT INTO %s
                      SELECT DISTINCT
                              package,
                              tx_sequence_number
                      FROM
                              tx_calls
                      WHERE
                              tx_sequence_number BETWEEN ? AND ?"
                     +tx-calls-pkg+)

                    :tx-calls-mod
                    (format
                     "INSERT INTO %s
                      SELECT DISTINCT
                              package,
                              module,
                              tx_sequence_number
                      FROM
                              tx_calls
                      WHERE
                              tx_sequence_number BETWEEN ? AND ?"
                     +tx-calls-mod+)

                    :tx-calls-fun
                    (format
                     "INSERT INTO %s
                      SELECT
                              package,
                              module,
                              func,
                              tx_sequence_number
                      FROM
                              tx_calls
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
                              tx_sequence_number
                      FROM
                              tx_recipients
                      WHERE
                              tx_sequence_number BETWEEN ? AND ?"
                     +tx-recipients+)

                    :tx-input-objects
                    (format
                     "INSERT INTO %s
                      SELECT
                              object_id,
                              tx_sequence_number
                      FROM
                              tx_input_objects
                      WHERE
                              tx_sequence_number BETWEEN ? AND ?"
                     +tx-input-objects+)

                    :tx-changed-objects
                    (format
                     "INSERT INTO %s
                      SELECT
                              object_id,
                              tx_sequence_number
                      FROM
                              tx_changed_objects
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

                    :tx-system
                    (format
                     "INSERT INTO %s
                      SELECT
                              tx_sequence_number
                      FROM
                              transactions
                      WHERE
                              transaction_kind = 0
                      AND     tx_sequence_number BETWEEN ? AND ?"
                     +tx-system+))
                  lo (dec hi)]
                 (jdbc/execute! db)
                 first :next.jdbc/update-count
                 (hash-map :updated)))

          :finalize
          (fn [{:as task :keys [status fill updated]}]
            (case status
              :success
              (do (signal-swap! signals fill + updated) nil)

              (:timeout :error)
              (do (signal-swap! signals :failed-jobs conj task) nil)))))

(defn transactions:attach-signals []
  (signals :autovacuum       0
           :analyze          0
           :constrain        0
           :attach           0
           :drop-check       0
           :failed-jobs      []))

(defn transactions:index-and-attach-partitions!
  "Attach `part`ition`s` to the `transactions` table.

  `parts` describes the partitions to attach. It is a map containing
  the `part`ition number, its inclusive transaction lower bound, and
  its exclusive transaction upper bound.

  `signals` is updated with the number of partitions that have passed
  through each phase.
  "
  [db parts logger delta-t signals]
  (->Pool :name   "index-and-attach"
          :logger  logger
          :workers 50

          :pending
          (for [part parts]
            (assoc part :job :autovacuum :timeout delta-t))

          :impl
          (db/worker {:keys [part job lo hi timeout]}
            (case job
              :autovacuum (db/reset-autovacuum! db (transactions:partition-name part) timeout)
              :analyze    (db/vacuum-and-analyze! db (transactions:partition-name part) timeout)
              :constrain  (transactions:constrain! db part lo hi timeout)
              :attach     (transactions:attach! db part lo hi timeout)
              :drop-check (transactions:drop-range-check! db part timeout))
            nil)

          :finalize
          (fn [{:as batch :keys [part job status timeout]}]
            (let [phases [:autovacuum :analyze :constrain :attach :drop-check]
                  edges  (into {} (map vector phases (rest phases)))]
              (case status
                :success
                (do (signal-swap! signals job inc)
                    (when-let [next (edges job)]
                      [(assoc batch :job next)]))

                :timeout
                [(assoc batch :timeout (+ timeout delta-t))]

                :error
                (do (signal-swap! signals :failed-jobs conj batch) nil))))))

(defn transactions:constrain-and-index-tx!
  "Add indices and constraints to side tables.

  `signals` is assumed to be an atom containing a vector, that the
  labels for completed jobs are pushed into."
  [db logger signals]
  (->Pool :name    "index-and-constrain"
          :logger  logger
          :workers 10

          :pending
          [
           {:fn tx-calls-pkg:constrain!       :label :tx-calls-pkg/constrain}
           {:fn tx-calls-mod:constrain!       :label :tx-calls-mod/constrain}
           {:fn tx-calls-fun:constrain!       :label :tx-calls-fun/constrain}
           {:fn tx-senders:constrain!         :label :tx-senders/constrain}
           {:fn tx-recipients:constrain!      :label :tx-recipients/constrain}
           {:fn tx-input-objects:constrain!   :label :tx-input-objects/constrain}
           {:fn tx-changed-objects:constrain! :label :tx-changed-objects/constrain}
           {:fn tx-digests:constrain!         :label :tx-digests/constrain}
           {:fn tx-system:constrain!          :label :tx-system/constrain}]

          :impl
          (db/worker {work :fn} (work db) nil)

          :finalize
          (fn [{:as task :keys [status label timeout]}]
            (case status
              :success (do (swap! signals conj label) nil)
              ;; Ignore timeouts and errors. Timeouts can only be
              ;; triggered by another process canceling this pool's
              ;; jobs. Errors will be signaled in the logs and we can
              ;; retry at our leisure.
              :timeout nil :error nil))))
