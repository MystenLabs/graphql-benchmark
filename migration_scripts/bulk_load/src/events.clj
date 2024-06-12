(ns events
  (:require [db]
            [logger :refer [->Logger] :as l]
            [pool :refer [->Pool worker]]
            [next.jdbc :as jdbc]
            [transactions :refer [transactions:populate! bounds->batches]]))

;; Table Names ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:private prefix "amnn_0_orig_")

;; Replicas of existing tables, to be populated with a subset of the data in
;; main tables, for an apples to apples comparison.
(def +events+ (str prefix "events"))

(defn events:partition-name [n]
  (str +events+ "_partition_" n))

;; Table: events ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn events:create! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx +events+
      "CREATE TABLE %s (
           tx_sequence_number          BIGINT       NOT NULL,
           event_sequence_number       BIGINT       NOT NULL,
           transaction_digest          BYTEA        NOT NULL,
           checkpoint_sequence_number  BIGINT       NOT NULL,
           senders                     BYTEA[]      NOT NULL,
           package                     BYTEA        NOT NULL,
           module                      TEXT         NOT NULL,
           event_type                  TEXT         NOT NULL,
           event_type_package          BYTEA        NOT NULL,
           event_type_module           TEXT         NOT NULL,
           event_type_name             TEXT         NOT NULL,
           timestamp_ms                BIGINT       NOT NULL,
           bcs                         BYTEA        NOT NULL,
           PRIMARY KEY(
               tx_sequence_number,
               event_sequence_number,
               checkpoint_sequence_number
           )
       ) PARTITION BY RANGE (checkpoint_sequence_number)")
    (db/with-table! tx +events+
      "CREATE INDEX %1$s_package ON %1$s (
           package,
           tx_sequence_number,
           event_sequence_number
       )")
    (db/with-table! tx +events+
      "CREATE INDEX %1$s_package_module ON %1$s (
           package,
           module,
           tx_sequence_number,
           event_sequence_number
       )")
    (db/with-table! tx +events+
      "CREATE INDEX %1$s_event_type ON %1$s (
           event_type text_pattern_ops,
           tx_sequence_number,
           event_sequence_number
       )")
    (db/with-table! tx +events+
      "CREATE INDEX %1$s_type_package_module_name ON %1$s (
           event_type_package,
           event_type_module,
           event_type_name,
           tx_sequence_number,
           event_sequence_number
       )")
    (db/with-table! tx +events+
      "CREATE INDEX %1$s_checkpoint_sequence_number ON %1$s (
           checkpoint_sequence_number
       )")))

(defn events:create-partition [db n]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx (events:partition-name n)
      "CREATE TABLE %s (
           tx_sequence_number          BIGINT,
           event_sequence_number       BIGINT,
           transaction_digest          BYTEA,
           checkpoint_sequence_number  BIGINT,
           senders                     BYTEA[],
           package                     BYTEA,
           module                      TEXT,
           event_type                  TEXT,
           event_type_package          BYTEA,
           event_type_module           TEXT,
           event_type_name             TEXT,
           timestamp_ms                BIGINT,
           bcs                         BYTEA
       )")
    (db/disable-autovacuum! tx (events:partition-name n))))

(defn events:constrain!
  "Add constraints to partition `n` of the `transactions` table.

  `lo` and `hi` are the inclusive and exclusive bounds on checkpoint
  sequence numbers in the partition."
  [db n lo hi]
  (->> [(format
         "ALTER TABLE %1$s
          ADD PRIMARY KEY (
              tx_sequence_number,
              event_sequence_number,
              checkpoint_sequence_number
          ),
          ALTER COLUMN transaction_digest SET NOT NULL,
          ALTER COLUMN senders            SET NOT NULL,
          ALTER COLUMN package            SET NOT NULL,
          ALTER COLUMN module             SET NOT NULL,
          ALTER COLUMN event_type         SET NOT NULL,
          ALTER COLUMN event_type_package SET NOT NULL,
          ALTER COLUMN event_type_module  SET NOT NULL,
          ALTER COLUMN event_type_name    SET NOT NULL,
          ALTER COLUMN timestamp_ms       SET NOT NULL,
          ALTER COLUMN bcs                SET NOT NULL,
          ADD CONSTRAINT %1s_partition_check CHECK (
              %2$d <= checkpoint_sequence_number
          AND checkpoint_sequence_number < %3$d
          )"
         (events:partition-name n) lo hi)]
       (jdbc/execute! db)))

(defn events:index-package [db n]
  (db/with-table! db (events:partition-name n)
    "CREATE INDEX %1$s_package ON %1$s (
         package,
         tx_sequence_number,
         event_sequence_number
     )"))

(defn events:index-package-module [db n]
  (db/with-table! db (events:partition-name n)
    "CREATE INDEX %1$s_package_module ON %1$s (
         package,
         module,
         tx_sequence_number,
         event_sequence_number
     )"))

(defn events:index-event-type [db n]
  (db/with-table! db (events:partition-name n)
    "CREATE INDEX %1$s_event_type ON %1$s (
         event_type text_pattern_ops,
         tx_sequence_number,
         event_sequence_number
     )"))

(defn events:index-type-package-module-name [db n]
  (db/with-table! db (events:partition-name n)
    "CREATE INDEX %1$s_type_package_module_name ON %1$s (
         event_type_package,
         event_type_module,
         event_type_name,
         tx_sequence_number,
         event_sequence_number
     )"))

(defn events:index-checkpoint-sequence-number [db n]
  (db/with-table! db (events:partition-name n)
    "CREATE INDEX %1$s_checkpoint_sequence_number ON %1$s (
         checkpoint_sequence_number
     )"))

(defn events:attach! [db n lo hi]
  (jdbc/with-transaction [tx db]
    (let [part   (events:partition-name n)
          attach (fn [template]
                   (jdbc/execute! tx [(format template +events+ part lo hi)]))]
      (attach "ALTER TABLE %1$s
               ATTACH PARTITION %2$s FOR VALUES FROM (%3$d) TO (%4$d)")
      (attach "ALTER INDEX %1$s_package
               ATTACH PARTITION %2$s_package")
      (attach "ALTER INDEX %1$s_package_module
               ATTACH PARTITION %2$s_package_module")
      (attach "ALTER INDEX %1$s_event_type
               ATTACH PARTITION %2$s_event_type")
      (attach "ALTER INDEX %1$s_type_package_module_name
               ATTACH PARTITION %2$s_type_package_module_name")
      (attach "ALTER INDEX %1$s_checkpoint_sequence_number
               ATTACH PARTITION %2$s_checkpoint_sequence_number"))))

(defn events:drop-range-check! [db n]
  (db/with-table! db (events:partition-name n)
    "ALTER TABLE %1$s DROP CONSTRAINT %1$s_partition_check"))

;; Bulk Loading ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn events:create-all!
  [db lo hi logger & {:keys [retry]}]
  (->Pool :name    "create-tables"
          :logger   logger
          :workers (Math/clamp (- hi lo) 4 20)

          :pending
          (or retry
              (conj (for [n (range lo hi)]
                      {:fn #(events:create-partition % n)
                       :label (str "partition-" n)})
                    {:fn events:create! :label "events"}))

          :impl (worker {builder :fn} (builder db) nil)))

(defn events:drop-all! [db lo hi logger & {:keys [retry]}]
  (->Pool :name    "drop-tables"
          :logger   logger
          :workers (Math/clamp (- hi lo) 4 20)

          :pending
          (or retry
              (conj (for [n (range lo hi)]
                      {:table (events:partition-name n)})
                    {:table +events+}))

          :impl
          (worker {:keys [table]}
            (db/with-table! db table "DROP TABLE IF EXISTS %s") nil)))

(defn events:bulk-load!
  "Bulk load all events into subset tables.

  Transfers data from partitions of the `events` table. `batches`
  controls the intervals of events (by transaction sequence number)
  that are loaded."
  [db bounds batch logger & {:keys [retry]}]
  (->Pool :name   "bulk-load"
          :logger  logger
          :workers 100

          :pending (or retry (-> bounds (bounds->batches batch)))

          :impl
          (worker {:keys [part lo hi]}
            ;; `transactions:populate!` handles generic updates between tables
            ;; that include tx sequence number among their keys.
            (->> (transactions:populate!
                  db
                  (str "events_partition_" part)
                  (events:partition-name part)
                  lo hi)
                 first :next.jdbc/update-count
                 (hash-map :updated)))

          :finalize
          (fn [{:keys [status updated]} signals]
            (when (= :success status)
              (swap! signals update :updated (fnil + 0) updated)) nil)))

(defn events:index-and-attach-partitions!
  "Attach `part`ition`s` to the `events` table.

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
              :autovacuum (db/reset-autovacuum! db (events:partition-name part))
              :analyze    (db/vacuum-and-analyze! db (events:partition-name part))
              :constrain  (events:constrain! db part lo hi)

              ;; The index jobs technically don't need to be serialized relative
              ;; to one another, but it's simpler to structure it that way.
              :index-package
              (events:index-package db part)

              :index-package-module
              (events:index-package-module db part)

              :index-event-type
              (events:index-event-type db part)

              :index-type-package-module-name
              (events:index-type-package-module-name db part)

              :index-checkpoint-sequence-number
              (events:index-checkpoint-sequence-number db part)

              :attach     (events:attach! db part lo hi)
              :drop-check (events:drop-range-check! db part))
            nil)

          :finalize
          (fn [{:as task :keys [status job]} signals]
            (let [phases [:autovacuum :analyze :constrain
                          :index-package :index-package-module :index-event-type
                          :index-type-package-module-name
                          :index-checkpoint-sequence-number
                          :attach :drop-check]
                  edges  (into {} (map vector phases (rest phases)))]
              (when (= :success status)
                (swap! signals update job (fnil inc 0))
                (when-let [next (edges job)]
                  [(assoc task :job next)]))))))
