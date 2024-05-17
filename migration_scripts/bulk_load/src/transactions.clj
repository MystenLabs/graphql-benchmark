(ns transactions
  (:require [db]
            [logger :refer [->Logger] :as l]
            [pool :refer [->Pool, signal-swap! signals]]
            [next.jdbc :as jdbc]
            [clojure.string :refer [starts-with?]]))

;; Table Names ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:private prefix "amnn_0_")

;; Replicas of existing tables, to be populated with a subset of the data in main tables, for an
;; apples to apples comparison.
(def +transactions+       (str prefix "transactions"))
(def +tx-calls+           (str prefix "tx_calls"))
(def +tx-senders+         (str prefix "tx_senders"))
(def +tx-recipients+      (str prefix "tx_recipients"))
(def +tx-input-objects+   (str prefix "tx_input_objects"))
(def +tx-changed-objects+ (str prefix "tx_changed_objects"))
(def +tx-digests+         (str prefix "tx_digests"))

(defn transactions:partition-name [n]
  (str +transactions+ "_partition_" n))

;; Normalized versions of the above tables, holding the same data.
(def +tx-norm+            (str prefix "norm_transactions"))
(def +tx-norm-calls-pkg+  (str prefix "norm_tx_calls_pkg"))
(def +tx-norm-calls-mod+  (str prefix "norm_tx_calls_mod"))
(def +tx-norm-calls-fun+  (str prefix "norm_tx_calls_fun"))
(def +tx-norm-senders+    (str prefix "norm_tx_senders"))
(def +tx-norm-recipients+ (str prefix "norm_tx_recipients"))
(def +tx-norm-inputs+     (str prefix "norm_tx_inputs"))
(def +tx-norm-changed+    (str prefix "norm_tx_changed"))
(def +tx-norm-digests+    (str prefix "norm_tx_digests"))

;; Table: transactions ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn transactions:create! [db _timeout]
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

(defn transactions:populate!
  "Populate `to` with data from `from`.

  `lo` (inclusive) and `hi` (exclusive) are bounds on the transaction
  sequence numbers from the source data-set."
  [db from to lo hi timeout]
  (as-> [(format
          "INSERT INTO %s SELECT * FROM %s
           WHERE
               tx_sequence_number BETWEEN ? AND ?"
          to from)
         lo (dec hi)]
      % (jdbc/execute! db % {:timeout timeout})))

(defn transactions:constrain!
  "Add constraints to partition `n` of the `transactions` table.

  `lo` and `hi` are the inclusive and exclusive bounds on checkpoint
  sequence numbers in the partition."
  [db n lo hi timeout]
  (as-> [(format
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
      % (jdbc/execute! db % {:timeout timeout})))

(defn transactions:index-digest! [db n timeout]
  (db/with-table! db (transactions:partition-name n)
    "CREATE INDEX %1$s_transaction_digest ON %1$s (transaction_digest)"
    {:timeout timeout}))

(defn transactions:index-checkpoint! [db n timeout]
  (db/with-table! db (transactions:partition-name n)
    "CREATE INDEX %1$s_checkpoint_sequence_number ON %1$s (checkpoint_sequence_number)"
    {:timeout timeout}))

(defn transactions:index-kind! [db n timeout]
  (db/with-table! db (transactions:partition-name n)
    "CREATE INDEX %1$s_transaction_kind ON %1$s (transaction_kind) WHERE transaction_kind = 0"
    {:timeout timeout}))

(defn transactions:attach! [db n lo hi timeout]
  (jdbc/with-transaction [tx db]
    (let [part   (transactions:partition-name n)
          attach (fn [template]
                   (jdbc/execute!
                    tx [(format template +transactions+ part lo hi)]
                    {:timeout timeout}))]
      (attach "ALTER TABLE %1$s
               ATTACH PARTITION %2$s FOR VALUES FROM (%3$d) TO (%4$d)")
      (attach "ALTER INDEX %1$s_transaction_digest
               ATTACH PARTITION %2$s_transaction_digest")
      (attach "ALTER INDEX %1$s_checkpoint_sequence_number
               ATTACH PARTITION %2$s_checkpoint_sequence_number")
      (attach "ALTER INDEX %1$s_transaction_kind
               ATTACH PARTITION %2$s_transaction_kind"))))

(defn transactions:drop-range-check! [db n timeout]
  (db/with-table! db (transactions:partition-name n)
    "ALTER TABLE %1$s DROP CONSTRAINT %1$s_partition_check"
    {:timeout timeout}))

;; Table: tx_calls ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tx-calls:create! [db timeout]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx +tx-calls+
      "CREATE TABLE %s (
            cp_sequence_number          BIGINT,
            tx_sequence_number          BIGINT,
            package                     BYTEA,
            module                      TEXT,
            func                        TEXT
       )")
    (db/disable-autovacuum! tx +tx-calls+ timeout)))

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

(defn tx-senders:create! [db timeout]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx +tx-senders+
      "CREATE TABLE %s (
            cp_sequence_number          BIGINT,
            tx_sequence_number          BIGINT,
            sender                      BYTEA
       )")
    (db/disable-autovacuum! tx +tx-senders+ timeout)))

(defn tx-senders:constrain! [db]
  (db/with-table! db +tx-senders+
    "ALTER TABLE %1$s
     ADD PRIMARY KEY (sender, tx_sequence_number, cp_sequence_number)"))

(defn tx-senders:index-tx! [db]
  (db/with-table! db +tx-senders+
    "CREATE INDEX %1$s_tx_sequence_number_index
     ON %1$s (tx_sequence_number, cp_sequence_number)"))

;; Table: tx_recipients ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tx-recipients:create! [db timeout]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx +tx-recipients+
      "CREATE TABLE %s (
            cp_sequence_number          BIGINT,
            tx_sequence_number          BIGINT,
            recipient                   BYTEA
       )")
    (db/disable-autovacuum! tx +tx-recipients+ timeout)))

(defn tx-recipients:constrain! [db]
  (db/with-table! db +tx-recipients+
    "ALTER TABLE %1$s
     ADD PRIMARY KEY (recipient, tx_sequence_number, cp_sequence_number)"))

(defn tx-recipients:index-tx! [db]
  (db/with-table! db +tx-recipients+
    "CREATE INDEX %1$s_tx_sequence_number_index
     ON %1$s (tx_sequence_number, cp_sequence_number)"))

;; Table: tx_input_objects ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tx-input-objects:create! [db timeout]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx +tx-input-objects+
      "CREATE TABLE %s (
            cp_sequence_number          BIGINT,
            tx_sequence_number          BIGINT,
            object_id                   BYTEA
       )")
    (db/disable-autovacuum! tx +tx-input-objects+ timeout)))

(defn tx-input-objects:constrain! [db]
  (db/with-table! db +tx-input-objects+
    "ALTER TABLE %1$s
     ADD PRIMARY KEY (object_id, tx_sequence_number, cp_sequence_number)"))

;; Table: tx_changed_objects ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tx-changed-objects:create! [db timeout]
  (jdbc/with-transaction [tx db]
    (db/with-table! db +tx-changed-objects+
      "CREATE TABLE %s (
            cp_sequence_number          BIGINT,
            tx_sequence_number          BIGINT,
            object_id                   BYTEA
       )")
    (db/disable-autovacuum! tx +tx-changed-objects+ timeout)))

(defn tx-changed-objects:constrain! [db]
  (db/with-table! db +tx-changed-objects+
    "ALTER TABLE %1$s
     ADD PRIMARY KEY (object_id, tx_sequence_number, cp_sequence_number)"))

;; Table: tx_digests ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tx-digests:create! [db timeout]
  (jdbc/with-transaction [tx db]
    (db/with-table! db +tx-digests+
      "CREATE TABLE %s (
            tx_digest                   BYTEA,
            cp_sequence_number          BIGINT,
            tx_sequence_number          BIGINT
       )")
    (db/disable-autovacuum! tx +tx-digests+ timeout)))

(defn tx-digests:constrain! [db]
  (db/with-table! db +tx-digests+
    "ALTER TABLE %1$s
     ADD PRIMARY KEY (tx_digest)"))

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
                {:fn tx-calls:create! :label "tx-calls"
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
                [+transactions+ +tx-calls+ +tx-senders+ +tx-recipients+
                 +tx-input-objects+ +tx-changed-objects+ +tx-digests+]))

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

(defn transactions:batches!
  "Generates a sequence of intervals over transaction sequence numbers.

  Intervals are pushed into `intervals`, assumed to be an `atom`
  containing a vector.

  Each interval has an inclusive `:lo`wer bound and an exclusive upper
  bound -- `:hi` -- (measured in transaction sequence numbers), as well
  as the `part`ition of `transactions` it maps to.

  The intervals combine to cover the range of partitions starting at
  partition `lo` (inclusive) and ending at partition `hi` (exclusive),
  and each interval will be no bigger than `batch`."
  [db lo hi batch logger delta-t intervals]
  (->Pool :name    "fetch-batches"
          :logger   logger
          :workers (Math/clamp (- hi lo) 4 20)

          :pending
          (for [part (range lo hi)]
            {:part part :timeout delta-t :retries 3})

          :impl
          (db/worker {:keys [part]}
            (->> "SELECT MIN(tx_sequence_number), MAX(tx_sequence_number) FROM %s"
                 (db/with-table! db (str "transactions_partition_" part))
                 (first)))

          :finalize
          (fn [{:as task min-tx :min max-tx :max :keys [part status retries timeout]}]
            (case status
              :success
              (do (swap! intervals
                         #(apply conj %
                                 (for [lo (range min-tx max-tx batch)
                                       :let [hi (min (+ lo batch) (inc max-tx))]]
                                   {:part part :lo lo :hi hi})))
                  nil)

              :timeout
              [(assoc task :timeout (+ timeout delta-t))]

              :error
              (and (not= 0 retries)
                   [(assoc task :retries (dec retries))])))))

(defn transactions:load-signals []
  (signals "transactions" 0
           "tx_calls" 0
           "tx_senders" 0
           "tx_recipients" 0
           "tx_input_objects" 0
           "tx_changed_objects" 0
           "tx_digests" 0))

(defn transactions:bulk-load!
  "Bulk load all transactions into our subset tables.

  Transfers data from the partitions of the `transactions` table, as
  well as from the index tables. `batches` controls the intervals of
  transactions (by sequence number) that are loaded."
  [db batches logger timeout signals]
  (->Pool :name   "bulk-load"
          :logger  logger
          :workers 100

          :pending
          (concat
           ;; Main partitions
           (for [{:keys [part lo hi]} batches]
             {:lo lo :hi hi
              :from (str "transactions_partition_" part)
              :to   (transactions:partition-name part)
              :retries 3})

           ;; Index tables
           (for [tables [{:from "tx_calls"           :to +tx-calls+}
                         {:from "tx_senders"         :to +tx-senders+}
                         {:from "tx_recipients"      :to +tx-recipients+}
                         {:from "tx_input_objects"   :to +tx-input-objects+}
                         {:from "tx_changed_objects" :to +tx-changed-objects+}
                         {:from "tx_digests"         :to +tx-digests+}]
                 {:keys [lo hi]} batches]
             (assoc tables :lo lo :hi hi :retries 3)))

          :impl
          (db/worker {:keys [from to lo hi]}
            (->> (transactions:populate! db from to lo hi timeout)
                 first :next.jdbc/update-count
                 (hash-map :updated)))

          :finalize
          (fn [{:as batch :keys [lo hi retries status updated from to]}]
            (case status
              :success
              (do (if (starts-with? from "transactions")
                    (signal-swap! signals "transactions" + updated)
                    (signal-swap! signals from + updated))
                  nil)

              :timeout
              (let [m (+ lo (quot (- hi lo) 2))]
                (and (not= lo m)
                     [{:lo lo :hi m :from from :to to :retries retries}
                      {:lo m :hi hi :from from :to to :retries retries}]))

              :error
              (and (not= 0 retries)
                   [{:lo lo :hi hi :from from :to to :retries (dec retries)}])))))

(defn transactions:checkpoint-bounds
  "Checkpoint sequence number bounds for transaction partitions.

  Bounds are appended to `bounds`, assumed to be an `atom` containing
  a vector.

  Each interval has an inclusive `:lo`wer bound and an exclusive upper
  bound -- `:hi` -- (measured in checkpoint sequence numbers), as well
  as the `part`ition of `transactions` it maps to."
  [db lo hi logger delta-t bounds]
  (->Pool :name    "fetch-checkpoints"
          :logger   logger
          :workers (Math/clamp (- hi lo) 4 20)

          :pending
          (for [part (range lo hi)]
            {:part part :timeout delta-t :retries 3})

          :impl
          (db/worker {:keys [part]}
            (->> "SELECT
                      MIN(checkpoint_sequence_number),
                      MAX(checkpoint_sequence_number)
                  FROM %s"
                 (db/with-table! db (str "transactions_partition_" part))
                 (first)))

          :finalize
          (fn [{:as task :keys [part status min max retries timeout]}]
            (case status
              :success
              (do (swap! bounds conj {:part part :lo min :hi (inc max)})
                  nil)

              :timeout
              [(assoc task :timeout (+ timeout delta-t))]

              :error
              (and (not= 0 retries)
                   [(assoc task :retries (dec retries))])))))

(defn transactions:attach-signals []
  (signals :autovacuum       0
           :analyze          0
           :constrain        0
           :index-digest     0
           :index-checkpoint 0
           :index-kind       0
           :attach           0
           :drop-check       0
           :failed-jobs      []))

(defn transactions:index-and-attach-partitions!
  "Attach `part`ition`s` to the `transactions` table.

  `parts` describes the partitions to attach. It is a map containing
  the `part`ition number, its inclusive checkpoint lower bound, and
  its exclusive checkpoint upper bound.

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

              ;; The index jobs technically don't need to be
              ;; serialized relative to one another, but it's simpler
              ;; to structure it that way.
              :index-digest     (transactions:index-digest! db part timeout)
              :index-checkpoint (transactions:index-checkpoint! db part timeout)
              :index-kind       (transactions:index-kind! db part timeout)

              :attach     (transactions:attach! db part lo hi timeout)
              :drop-check (transactions:drop-range-check! db part timeout))
            nil)

          :finalize
          (fn [{:as batch :keys [part job status timeout]}]
            (let [phases [:autovacuum :analyze :constrain
                          :index-digest :index-checkpoint :index-kind
                          :attach :drop-check]
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
          [{:fn tx-calls:constrain!           :label :tx-calls/constrain}
           {:fn tx-senders:constrain!         :label :tx-senders/constrain}
           {:fn tx-recipients:constrain!      :label :tx-recipients/constain}
           {:fn tx-input-objects:constrain!   :label :tx-input-objects/constrain}
           {:fn tx-changed-objects:constrain! :label :tx-changed-objects/constrain}
           {:fn tx-digests:constrain!         :label :tx-digests/constrain}
           {:fn tx-calls:index-tx!            :label :tx-calls/index-tx}
           {:fn tx-calls:index-modules!       :label :tx-calls/index-modules}
           {:fn tx-calls:index-packages!      :label :tx-calls/index-packages}
           {:fn tx-senders:index-tx!          :label :tx-senders/index-tx}
           {:fn tx-recipients:index-tx!       :label :tx-recipients/index-tx}]

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
