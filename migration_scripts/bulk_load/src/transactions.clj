(ns transactions
  (:require [db]
            [logger :refer [->Logger] :as l]
            [pool :refer [->Pool worker]]
            [next.jdbc :as jdbc]
            [clojure.string :refer [starts-with?]]))

;; Table Names ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:private prefix "amnn_0_")

;; Replicas of existing tables, to be populated with a subset of the data in
;; main tables, for an apples to apples comparison.
(def +transactions+       (str prefix "transactions"))
(def +tx-calls+           (str prefix "tx_calls"))
(def +tx-senders+         (str prefix "tx_senders"))
(def +tx-recipients+      (str prefix "tx_recipients"))
(def +tx-input-objects+   (str prefix "tx_input_objects"))
(def +tx-changed-objects+ (str prefix "tx_changed_objects"))
(def +tx-digests+         (str prefix "tx_digests"))

(defn transactions:partition-name [n]
  (str +transactions+ "_partition_" n))

;; Table: transactions ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn transactions:create! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx +transactions+
      "CREATE TABLE %s (
           tx_sequence_number         BIGINT          NOT NULL,
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
           PRIMARY KEY (tx_sequence_number, checkpoint_sequence_number)
       ) PARTITION BY RANGE (checkpoint_sequence_number)")
    (db/with-table! tx +transactions+
      "CREATE INDEX %1$s_transaction_digest ON %1$s (transaction_digest)")
    (db/with-table! tx +transactions+
      "CREATE INDEX %1$s_checkpoint_sequence_number ON %1$s (checkpoint_sequence_number)")
    (db/with-table! tx +transactions+
      "CREATE INDEX %1$s_transaction_kind ON %1$s (transaction_kind) WHERE transaction_kind = 0")))

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

(defn transactions:populate!
  "Populate `to` with data from `from`.

  `lo` (inclusive) and `hi` (exclusive) are bounds on the transaction
  sequence numbers from the source data-set."
  [db from to lo hi]
  (->> [(format
         "INSERT INTO %s SELECT * FROM %s
          WHERE
              tx_sequence_number BETWEEN ? AND ?"
         to from)
        lo (dec hi)]
       (jdbc/execute! db)))

(defn transactions:constrain!
  "Add constraints to partition `n` of the `transactions` table.

  `lo` and `hi` are the inclusive and exclusive bounds on checkpoint
  sequence numbers in the partition."
  [db n lo hi]
  (->> [(format
         "ALTER TABLE %1$s
          ADD PRIMARY KEY (tx_sequence_number, checkpoint_sequence_number),
          ALTER COLUMN transaction_digest    SET NOT NULL,
          ALTER COLUMN raw_transaction       SET NOT NULL,
          ALTER COLUMN raw_effects           SET NOT NULL,
          ALTER COLUMN timestamp_ms          SET NOT NULL,
          ALTER COLUMN object_changes        SET NOT NULL,
          ALTER COLUMN balance_changes       SET NOT NULL,
          ALTER COLUMN events                SET NOT NULL,
          ALTER COLUMN transaction_kind      SET NOT NULL,
          ALTER COLUMN success_command_count SET NOT NULL,
          ADD CONSTRAINT %1$s_partition_check CHECK (
              %2$d <= checkpoint_sequence_number
          AND checkpoint_sequence_number < %3$d
          )"
         (transactions:partition-name n) lo hi)]
       (jdbc/execute! db)))

(defn transactions:index-digest! [db n]
  (db/with-table! db (transactions:partition-name n)
    "CREATE INDEX %1$s_transaction_digest ON %1$s (transaction_digest)"))

(defn transactions:index-checkpoint! [db n]
  (db/with-table! db (transactions:partition-name n)
    "CREATE INDEX %1$s_checkpoint_sequence_number ON %1$s (checkpoint_sequence_number)"))

(defn transactions:index-kind! [db n]
  (db/with-table! db (transactions:partition-name n)
    "CREATE INDEX %1$s_transaction_kind ON %1$s (transaction_kind) WHERE transaction_kind = 0"))

(defn transactions:attach! [db n lo hi]
  (jdbc/with-transaction [tx db]
    (let [part   (transactions:partition-name n)
          attach (fn [template]
                   (jdbc/execute!
                    tx [(format template +transactions+ part lo hi)]))]
      (attach "ALTER TABLE %1$s
               ATTACH PARTITION %2$s FOR VALUES FROM (%3$d) TO (%4$d)")
      (attach "ALTER INDEX %1$s_transaction_digest
               ATTACH PARTITION %2$s_transaction_digest")
      (attach "ALTER INDEX %1$s_checkpoint_sequence_number
               ATTACH PARTITION %2$s_checkpoint_sequence_number")
      (attach "ALTER INDEX %1$s_transaction_kind
               ATTACH PARTITION %2$s_transaction_kind"))))

(defn transactions:drop-range-check! [db n]
  (db/with-table! db (transactions:partition-name n)
    "ALTER TABLE %1$s DROP CONSTRAINT %1$s_partition_check"))

;; Table: tx_calls ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tx-calls:create! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx +tx-calls+
      "CREATE TABLE %s (
            cp_sequence_number          BIGINT,
            tx_sequence_number          BIGINT,
            package                     BYTEA,
            module                      TEXT,
            func                        TEXT
       )")
    (db/disable-autovacuum! tx +tx-calls+)))

(defn tx-calls:constrain! [db]
  (db/with-table! db +tx-calls+
    "ALTER TABLE %1$s
     ADD PRIMARY KEY (package, module, func, tx_sequence_number, cp_sequence_number)"))

(defn tx-calls:index-modules! [db]
  (db/with-table! db +tx-calls+
    "CREATE INDEX %1$s_module
     ON %1$s (package, module, tx_sequence_number, cp_sequence_number)"))

(defn tx-calls:index-packages! [db]
  (db/with-table! db +tx-calls+
    "CREATE INDEX %1$s_package
     ON %1$s (package, tx_sequence_number, cp_sequence_number)"))

(defn tx-calls:index-tx! [db]
  (db/with-table! db +tx-calls+
    "CREATE INDEX %1$s_tx_sequence_number
     ON %1$s (tx_sequence_number, cp_sequence_number)"))

;; Table: tx_senders ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tx-senders:create! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx +tx-senders+
      "CREATE TABLE %s (
            cp_sequence_number          BIGINT,
            tx_sequence_number          BIGINT,
            sender                      BYTEA
       )")
    (db/disable-autovacuum! tx +tx-senders+)))

(defn tx-senders:constrain! [db]
  (db/with-table! db +tx-senders+
    "ALTER TABLE %1$s
     ADD PRIMARY KEY (sender, tx_sequence_number, cp_sequence_number)"))

(defn tx-senders:index-tx! [db]
  (db/with-table! db +tx-senders+
    "CREATE INDEX %1$s_tx_sequence_number_index
     ON %1$s (tx_sequence_number, cp_sequence_number)"))

;; Table: tx_recipients ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tx-recipients:create! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx +tx-recipients+
      "CREATE TABLE %s (
            cp_sequence_number          BIGINT,
            tx_sequence_number          BIGINT,
            recipient                   BYTEA
       )")
    (db/disable-autovacuum! tx +tx-recipients+)))

(defn tx-recipients:constrain! [db]
  (db/with-table! db +tx-recipients+
    "ALTER TABLE %1$s
     ADD PRIMARY KEY (recipient, tx_sequence_number, cp_sequence_number)"))

(defn tx-recipients:index-tx! [db]
  (db/with-table! db +tx-recipients+
    "CREATE INDEX %1$s_tx_sequence_number_index
     ON %1$s (tx_sequence_number, cp_sequence_number)"))

;; Table: tx_input_objects ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tx-input-objects:create! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx +tx-input-objects+
      "CREATE TABLE %s (
            cp_sequence_number          BIGINT,
            tx_sequence_number          BIGINT,
            object_id                   BYTEA
       )")
    (db/disable-autovacuum! tx +tx-input-objects+)))

(defn tx-input-objects:constrain! [db]
  (db/with-table! db +tx-input-objects+
    "ALTER TABLE %1$s
     ADD PRIMARY KEY (object_id, tx_sequence_number, cp_sequence_number)"))

;; Table: tx_changed_objects ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tx-changed-objects:create! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! db +tx-changed-objects+
      "CREATE TABLE %s (
            cp_sequence_number          BIGINT,
            tx_sequence_number          BIGINT,
            object_id                   BYTEA
       )")
    (db/disable-autovacuum! tx +tx-changed-objects+)))

(defn tx-changed-objects:constrain! [db]
  (db/with-table! db +tx-changed-objects+
    "ALTER TABLE %1$s
     ADD PRIMARY KEY (object_id, tx_sequence_number, cp_sequence_number)"))

;; Table: tx_digests ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tx-digests:create! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! db +tx-digests+
      "CREATE TABLE %s (
            tx_digest                   BYTEA,
            cp_sequence_number          BIGINT,
            tx_sequence_number          BIGINT
       )")
    (db/disable-autovacuum! tx +tx-digests+)))

(defn tx-digests:constrain! [db]
  (db/with-table! db +tx-digests+
    "ALTER TABLE %1$s
     ADD PRIMARY KEY (tx_digest)"))

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
                      {:fn #(transactions:create-partition! %1 n)
                       :label (str "partition-" n)})
                    {:fn transactions:create! :label "transactions"}
                    {:fn tx-calls:create! :label "tx-calls"}
                    {:fn tx-senders:create! :label "tx-senders"}
                    {:fn tx-recipients:create! :label "tx-recipients"}
                    {:fn tx-input-objects:create! :label "tx-input-objects"}
                    {:fn tx-changed-objects:create! :label "tx-changed-objects"}
                    {:fn tx-digests:create! :label "tx-digests"}))

          :impl (worker {builder :fn} (builder db) nil)))

(defn transactions:drop-all!
  "Drop all relevant transactions tables."
  [db lo hi logger & {:keys [retry]}]
  (->Pool :name    "drop-tables"
          :logger   logger
          :workers (Math/clamp (- hi lo) 4 20)

          :pending
          (or retry
              (concat
               (for [n (range lo hi)]
                 {:table (transactions:partition-name n)})
               (map #(hash-map :table %)
                    [+transactions+ +tx-calls+ +tx-senders+ +tx-recipients+
                     +tx-input-objects+ +tx-changed-objects+ +tx-digests+])))

          :impl
          (worker {:keys [table]}
            (db/with-table! db table "DROP TABLE IF EXISTS %s") nil)))

(defn transactions:transaction-bounds!
  "Transaction sequence number bounds for transaction partitions.

  Each interval has an inclusive `:lo`wer bound and an exclusive upper
  bound -- `:hi` -- (measured in transaction sequence numbers), as
  well as the `part`ition of `transactions` it maps to.

  Bounds are appended to a `:bounds` key on the signal map returned
  from this function."
  [db lo hi logger & {:keys [retry]}]
  (->Pool :name    "fetch-bounds"
          :logger   logger
          :workers (Math/clamp (- hi lo) 4 20)
          :pending (or retry (for [part (range lo hi)] {:part part}))

          :impl
          (worker {:keys [part]}
            (->> "SELECT
                      MIN(tx_sequence_number),
                      MAX(tx_sequence_number)
                  FROM %s"
                 (db/with-table! db (str "transactions_partition_" part))
                 (first)))

          :finalize
          (fn [{:keys [status part min max]} signals]
            (when (= :success status)
              (swap! signals update :bounds (fnil conj [])
                     {:part part :lo min :hi (inc max)})
              nil))))

(defn bounds->batches
  "Convert transaction sequence number bounds to batches of work.

  Takes a sequence of transaction sequence number bounds, mapped to
  the partition they correspond to, and returns a new sequence, of
  units of work, also mapped to partitions.

  Each unit of work has an inclusive `:lo`wer bound and an exclusive
  upper bound -- `:hi` -- (measured in transaction sequence numbers),
  as well as the `part`ition of `transactions` it maps to.

  The intervals combine to cover the same range of partitions as
  `bounds`, they are also non-overlapping, and have a max width of
  `batch`."
  [bounds batch]
  (for [{part-lo :lo part-hi :hi part :part} bounds
        lo (range part-lo part-hi batch)
        :let [hi (min (+ lo batch) part-hi)]]
    {:part part :lo lo :hi hi}))

(defn transactions:bulk-load!
  "Bulk load all transactions into our subset tables.

  Transfers data from the partitions of the `transactions` table, as
  well as from the index tables. `batches` controls the intervals of
  transactions (by sequence number) that are loaded."
  [db bounds batch logger & {:keys [retry]}]
  (->Pool :name   "bulk-load"
          :logger  logger
          :workers 100

          :pending
          (or retry
              (concat
               ;; Main partitions
               (for [{:keys [part lo hi]} (-> bounds (bounds->batches batch))]
                 {:lo lo :hi hi
                  :from (str "transactions_partition_" part)
                  :to   (transactions:partition-name part)})

               ;; Index tables
               (for [tables [{:from "tx_calls"           :to +tx-calls+}
                             {:from "tx_senders"         :to +tx-senders+}
                             {:from "tx_recipients"      :to +tx-recipients+}
                             {:from "tx_input_objects"   :to +tx-input-objects+}
                             {:from "tx_changed_objects" :to +tx-changed-objects+}
                             {:from "tx_digests"         :to +tx-digests+}]
                     {:keys [lo hi]} (-> bounds (bounds->batches batch))]
                 (assoc tables :lo lo :hi hi))))

          :impl
          (worker {:keys [from to lo hi]}
            (->> (transactions:populate! db from to lo hi)
                 first :next.jdbc/update-count
                 (hash-map :updated)))

          :finalize
          (fn [{:keys [status from updated]} signals]
            (when (= :success status)
              (swap! signals update
                     (if (starts-with? from "transactions")
                       :transactions (keyword from))
                     (fnil + 0) updated)
              nil))))

(defn transactions:checkpoint-bounds
  "Checkpoint sequence number bounds for transaction partitions.

  Each interval has an inclusive `:lo`wer bound and an exclusive upper
  bound -- `:hi` -- (measured in checkpoint sequence numbers), as well
  as the `part`ition of `transactions` it maps to.

  Bounds are appended to a `:bounds` key on the signal map returned
  from this function."
  [db lo hi logger & {:keys [retry]}]
  (->Pool :name    "fetch-checkpoints"
          :logger   logger
          :workers (Math/clamp (- hi lo) 4 20)
          :pending (or retry (for [part (range lo hi)] {:part part}))

          :impl
          (worker {:keys [part]}
            (->> "SELECT
                      MIN(checkpoint_sequence_number),
                      MAX(checkpoint_sequence_number)
                  FROM %s"
                 (db/with-table! db (str "transactions_partition_" part))
                 (first)))

          :finalize
          (fn [{:keys [status part min max]} signals]
            (when (= :success status)
              (swap! signals update :bounds (fnil conj [])
                     {:part part :lo min :hi (inc max)})
              nil))))

(defn transactions:index-and-attach-partitions!
  "Attach `part`ition`s` to the `transactions` table.

  `parts` describes the partitions to attach. It is a map containing
  the `part`ition number, its inclusive checkpoint lower bound, and
  its exclusive checkpoint upper bound.

  The returned signals map is updated with the number of partitions
  that have passed through each phase."
  [db parts logger & {:keys [retry]}]
  (->Pool :name   "index-and-attach"
          :logger  logger
          :workers 50
          :pending (or retry (for [part parts] (assoc part :job :autovacuum)))

          :impl
          (worker {:keys [part job lo hi]}
            (case job
              :autovacuum (db/reset-autovacuum! db (transactions:partition-name part))
              :analyze    (db/vacuum-and-analyze! db (transactions:partition-name part))
              :constrain  (transactions:constrain! db part lo hi)

              ;; The index jobs technically don't need to be
              ;; serialized relative to one another, but it's simpler
              ;; to structure it that way.
              :index-digest     (transactions:index-digest! db part)
              :index-checkpoint (transactions:index-checkpoint! db part)
              :index-kind       (transactions:index-kind! db part)

              :attach     (transactions:attach! db part lo hi)
              :drop-check (transactions:drop-range-check! db part))
            nil)

          :finalize
          (fn [{:as task :keys [status job]} signals]
            (let [phases [:autovacuum :analyze :constrain
                          :index-digest :index-checkpoint :index-kind
                          :attach :drop-check]
                  edges  (into {} (map vector phases (rest phases)))]
              (when (= :success status)
                (swap! signals update job (fnil inc 0))
                (when-let [next (edges job)]
                  [(assoc task :job next)]))))))

(defn transactions:constrain-and-index-tx!
  "Add indices and constraints to side tables."
  [db logger & {:keys [retry]}]
  (->Pool :name    "index-and-constrain"
          :logger  logger
          :workers 10

          :pending
          (or retry
              [{:fn tx-calls:constrain!           :label :tx-calls/constrain}
               {:fn tx-senders:constrain!         :label :tx-senders/constrain}
               {:fn tx-recipients:constrain!      :label :tx-recipients/constrain}
               {:fn tx-input-objects:constrain!   :label :tx-input-objects/constrain}
               {:fn tx-changed-objects:constrain! :label :tx-changed-objects/constrain}
               {:fn tx-digests:constrain!         :label :tx-digests/constrain}
               {:fn tx-calls:index-tx!            :label :tx-calls/index-tx}
               {:fn tx-calls:index-modules!       :label :tx-calls/index-modules}
               {:fn tx-calls:index-packages!      :label :tx-calls/index-packages}
               {:fn tx-senders:index-tx!          :label :tx-senders/index-tx}
               {:fn tx-recipients:index-tx!       :label :tx-recipients/index-tx}])

          :impl
          (worker {work :fn} (work db) nil)

          :finalize
          (fn [{:as task :keys [status label]} signals]
            (when (= :success status)
              (swap! signals update :done (fnil conj []) label) nil))))

(defn transactions:vacuum-index!
  "Re-enable auto-vacuum on the index tables, and perform a vacuum/analyze."
  [db logger & {:keys [retry]}]
  (->Pool :name   "vacuum-index"
          :logger  logger
          :workers 10

          :pending
          (or retry
              (map #(hash-map :job :autovacuum :index %)
                   [+tx-calls+ +tx-senders+ +tx-recipients+
                    +tx-input-objects+ +tx-changed-objects+
                    +tx-digests+]))

          :impl
          (worker {:keys [job index]}
            (case job
              :autovacuum (db/reset-autovacuum!   db index)
              :analyze    (db/vacuum-and-analyze! db index))
            nil)

          :finalize
          (fn [{:as task :keys [status job]} signals]
            (when (= :success status)
              (swap! signals update job (fnil inc 0))
              (when (= :autovacuum job)
                [(assoc task :job :analyze)])))))
