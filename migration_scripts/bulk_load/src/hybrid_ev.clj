(ns hybrid-ev
  (:require [db]
            [logger :refer [->Logger] :as l]
            [pool :refer [->Pool worker]]
            [next.jdbc :as jdbc]
            [transactions :refer [bounds->batches]]))

;; Table Names ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:private prefix "amnn_0_hybrid_")

;; Replicas of existing tables, to be populated with a subset of the data in
;; main tables, for an apples to apples comparison.
(def +events+        (str prefix "events"))
(def +ev-emit-pkg+   (str prefix "ev_emit_pkg"))
(def +ev-emit-mod+   (str prefix "ev_emit_mod"))
(def +ev-event-pkg+  (str prefix "ev_event_pkg"))
(def +ev-event-mod+  (str prefix "ev_event_mod"))
(def +ev-event-name+ (str prefix "ev_event_name"))
(def +ev-event-type+ (str prefix "ev_event_type"))
(def +ev-senders+    (str prefix "ev_senders"))

(defn events:partition-name [n]
  (str +events+ "_partition_" n))

;; Table: hybrid_events ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn events:create! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx +events+
      "CREATE TABLE %s (
           tx_sequence_number          BIGINT       NOT NULL,
           event_sequence_number       BIGINT       NOT NULL,
           sender                      BYTEA        NOT NULL,
           module                      TEXT         NOT NULL,
           timestamp_ms                BIGINT       NOT NULL,
           bcs                         BYTEA        NOT NULL,
           PRIMARY KEY(tx_sequence_number, event_sequence_number)
       ) PARTITION BY RANGE (tx_sequence_number)")))

(defn events:create-partition! [db n]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx (events:partition-name n)
      "CREATE TABLE %s (
           tx_sequence_number          BIGINT,
           event_sequence_number       BIGINT,
           sender                      BYTEA,
           module                      TEXT,
           timestamp_ms                BIGINT,
           bcs                         BYTEA
       )")))

(defn events:constrain!
  "Add constraints to partition `n` of the `transactions` table.

  `lo` and `hi` are the inclusive and exclusive bounds on transaction
  sequence numbers in the partition."
  [db n lo hi]
  (->> [(format
         "ALTER TABLE %1$s
          ADD PRIMARY KEY (tx_sequence_number, event_sequence_number),
          ALTER COLUMN sender             SET NOT NULL,
          ALTER COLUMN module             SET NOT NULL,
          ALTER COLUMN timestamp_ms       SET NOT NULL,
          ALTER COLUMN bcs                SET NOT NULL,
          ADD CONSTRAINT %1s_partition_check CHECK (
              %2$d <= tx_sequence_number
          AND tx_sequence_number < %3$d
          )"
         (events:partition-name n) lo hi)]
       (jdbc/execute! db)))

(defn events:attach! [db n lo hi]
  (->> [(format
         "ALTER TABLE %s
          ATTACH PARTITION %s FOR VALUES FROM (%d) TO (%d)"
         +events+ (events:partition-name n) lo hi)]
       (jdbc/execute! db)))

(defn events:drop-range-check! [db n]
  (db/with-table! db (events:partition-name n)
    "ALTER TABLE %1$s DROP CONSTRAINT %1$s_partition_check"))

;; Table: hybrid_ev_emit_pkg ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ev-emit-pkg:create! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! db +ev-emit-pkg+
      "CREATE TABLE %s (
           package                     BYTEA,
           tx_sequence_number          BIGINT,
           event_sequence_number       BIGINT,
           sender                      BYTEA
       )")
    (db/disable-autovacuum! tx +ev-emit-pkg+)))

(defn ev-emit-pkg:constrain! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx +ev-emit-pkg+
      "ALTER TABLE %s
       ADD PRIMARY KEY (package, tx_sequence_number, event_sequence_number),
       ALTER COLUMN sender SET NOT NULL")))

(defn ev-emit-pkg:index! [db]
  (db/with-table! db +ev-emit-pkg+
    "CREATE INDEX %1$s_sender ON %1$s (
         sender,
         package,
         tx_sequence_number,
         event_sequence_number
     )"))

;; Table: hybrid_ev_emit_mod ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ev-emit-mod:create! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! db +ev-emit-mod+
      "CREATE TABLE %s (
           package                     BYTEA,
           module                      TEXT,
           tx_sequence_number          BIGINT,
           event_sequence_number       BIGINT,
           sender                      BYTEA
       )")
    (db/disable-autovacuum! tx +ev-emit-mod+)))

(defn ev-emit-mod:constrain! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx +ev-emit-mod+
      "ALTER TABLE %s
       ADD PRIMARY KEY (
           package,
           module,
           tx_sequence_number,
           event_sequence_number
       ),
       ALTER COLUMN sender SET NOT NULL")))

(defn ev-emit-mod:index! [db]
  (db/with-table! db +ev-emit-mod+
    "CREATE INDEX %1$s_sender ON %s (
         sender,
         package,
         module,
         tx_sequence_number,
         event_sequence_number
     )"))

;; Table: hybrid_ev_event_pkg ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ev-event-pkg:create! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! db +ev-event-pkg+
      "CREATE TABLE %s (
           package                     BYTEA,
           tx_sequence_number          BIGINT,
           event_sequence_number       BIGINT,
           sender                      BYTEA
       )")
    (db/disable-autovacuum! tx +ev-event-pkg+)))

(defn ev-event-pkg:constrain! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx +ev-event-pkg+
      "ALTER TABLE %s
       ADD PRIMARY KEY (
           package,
           tx_sequence_number,
           event_sequence_number
       ),
       ALTER COLUMN sender SET NOT NULL")))

(defn ev-event-pkg:index! [db]
  (db/with-table! db +ev-event-pkg+
    "CREATE INDEX %1$s_sender ON %s (
         sender,
         package,
         tx_sequence_number,
         event_sequence_number
     )"))

;; Table: hybrid_ev_event_mod ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ev-event-mod:create! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! db +ev-event-mod+
      "CREATE TABLE %s (
           package                     BYTEA,
           module                      TEXT,
           tx_sequence_number          BIGINT,
           event_sequence_number       BIGINT,
           sender                      BYTEA
       )")
    (db/disable-autovacuum! tx +ev-event-mod+)))

(defn ev-event-mod:constrain! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx +ev-event-mod+
      "ALTER TABLE %s
       ADD PRIMARY KEY (
           package,
           module,
           tx_sequence_number,
           event_sequence_number
       ),
       ALTER COLUMN sender SET NOT NULL")))

(defn ev-event-mod:index! [db]
  (db/with-table! db +ev-event-mod+
    "CREATE INDEX %1$s_sender ON %s (
         sender,
         package,
         module,
         tx_sequence_number,
         event_sequence_number
     )"))

;; Table: hybrid_ev_event_name ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ev-event-name:create! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! db +ev-event-name+
      "CREATE TABLE %s (
           package                     BYTEA,
           module                      TEXT,
           name                        TEXT,
           tx_sequence_number          BIGINT,
           event_sequence_number       BIGINT,
           sender                      BYTEA
       )")
    (db/disable-autovacuum! tx +ev-event-name+)))

(defn ev-event-name:constrain! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx +ev-event-name+
      "ALTER TABLE %s
       ADD PRIMARY KEY (
           package,
           module,
           name,
           tx_sequence_number,
           event_sequence_number
       ),
       ALTER COLUMN sender SET NOT NULL")))

(defn ev-event-name:index! [db]
  (db/with-table! db +ev-event-name+
    "CREATE INDEX %1$s_sender ON %s (
         sender,
         package,
         module,
         name,
         tx_sequence_number,
         event_sequence_number
     )"))

;; Table: hybrid_ev_event_type ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ev-event-type:create! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! db +ev-event-type+
      "CREATE TABLE %s (
           package                     BYTEA,
           module                      TEXT,
           name                        TEXT,
           tx_sequence_number          BIGINT,
           event_sequence_number       BIGINT,
           sender                      BYTEA
       )")
    (db/disable-autovacuum! tx +ev-event-type+)))

(defn ev-event-type:constrain! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx +ev-event-type+
      "ALTER TABLE %s
       ADD PRIMARY KEY (
           package,
           module,
           name,
           tx_sequence_number,
           event_sequence_number
       ),
       ALTER COLUMN sender SET NOT NULL")))

(defn ev-event-type:index! [db]
  (db/with-table! db +ev-event-type+
    "CREATE INDEX %1$s_sender ON %s (
         sender,
         package,
         module,
         name,
         tx_sequence_number,
         event_sequence_number
     )"))

;; Table: hybrid_ev_senders ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ev-senders:create! [db]
  (jdbc/with-transaction [tx db]
    (db/with-table! tx +ev-senders+
      "CREATE TABLE %s (
           sender                      BYTEA,
           tx_sequence_number          BIGINT,
           event_sequence_number       BIGINT
       )")
    (db/disable-autovacuum! tx +ev-senders+)))

(defn ev-senders:constrain! [db]
  (db/with-table! db +ev-senders+
    "ALTER TABLE %1$s ADD PRIMARY KEY (
         sender,
         tx_sequence_number,
         event_sequence_number
     )"))

;; Bulk Loading ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn events:create-all! [db lo hi logger & {:keys [retry]}]
  (->Pool :name    "create-tables"
          :logger   logger
          :workers (Math/clamp (- hi lo) 4 20)

          :pending
          (or retry
              (conj
               (for [n (range lo hi)]
                 {:fn #(events:create-partition! % n)
                  :label (str "partition-" n)})
               {:fn events:create! :label "events"}
               {:fn ev-emit-pkg:create! :label "ev-emit-pkg"}
               {:fn ev-emit-mod:create! :label "ev-emit-mod"}
               {:fn ev-event-pkg:create! :label "ev-event-pkg"}
               {:fn ev-event-mod:create! :label "ev-event-mod"}
               {:fn ev-event-name:create! :label "ev-event-name"}
               {:fn ev-event-type:create! :label "ev-event-type"}
               {:fn ev-senders:create! :label "ev-senders"}))

          :impl (worker {builder :fn} (builder db) nil)))

(defn events:drop-all! [db lo hi logger & {:keys [retry]}]
  (->Pool :name   "drop-tables"
          :logger  logger
          :workers (Math/clamp (- hi lo) 4 20)

          :pending
          (or retry
              (conj
               (for [n (range lo hi)]
                 {:table (events:partition-name n)})
               {:table +events+}
               {:table +ev-emit-pkg+}
               {:table +ev-emit-mod+}
               {:table +ev-event-pkg+}
               {:table +ev-event-mod+}
               {:table +ev-event-name+}
               {:table +ev-event-type+}
               {:table +ev-senders+}))

          :impl
          (worker {:keys [table]}
                  (db/with-table! db table "DROP TABLE IF EXISTS %s") nil)))

(defn events:bulk-load! [db bounds batch logger & {:keys [retry]}]
  (->Pool :name   "bulk-load"
          :logger  logger
          :workers 50

          :pending (or retry
                       (for [batch (-> bounds (bounds->batches batch))
                             table [:events :ev-emit-pkg :ev-emit-mod
                                    :ev-event-pkg :ev-event-mod :ev-event-name
                                    :ev-event-type :ev-senders]]
                         (assoc batch :fill table)))

          :impl
          (worker {:keys [lo hi part fill]}
            (->> [(case fill
                    :events
                    (format
                     "INSERT INTO %s
                      SELECT
                              tx_sequence_number,
                              event_sequence_number,
                              senders[1],
                              module,
                              timestamp_ms,
                              bcs
                      FROM
                              events
                      WHERE
                              tx_sequence_number BETWEEN ? AND ?"
                     (events:partition-name part))

                    :ev-emit-pkg
                    (format
                     "INSERT INTO %s
                      SELECT
                              package,
                              tx_sequence_number,
                              event_sequence_number,
                              senders[1]
                      FROM
                              events
                      WHERE
                              tx_sequence_number BETWEEN ? AND ?"
                     +ev-emit-pkg+)

                    :ev-emit-mod
                    (format
                     "INSERT INTO %s
                      SELECT
                              package,
                              module,
                              tx_sequence_number,
                              event_sequence_number,
                              senders[1]
                      FROM
                              events
                      WHERE
                              tx_sequence_number BETWEEN ? AND ?"
                     +ev-emit-mod+)

                    :ev-event-pkg
                    (format
                     "INSERT INTO %s
                      SELECT
                              event_type_package,
                              tx_sequence_number,
                              event_sequence_number,
                              senders[1]
                      FROM
                              events
                      WHERE
                              tx_sequence_number BETWEEN ? AND ?"
                     +ev-event-pkg+)

                    :ev-event-mod
                    (format
                     "INSERT INTO %s
                      SELECT
                              event_type_package,
                              event_type_module,
                              tx_sequence_number,
                              event_sequence_number,
                              senders[1]
                      FROM
                              events
                      WHERE
                              tx_sequence_number BETWEEN ? AND ?"
                     +ev-event-mod+)

                    :ev-event-name
                    (format
                     "INSERT INTO %s
                      SELECT
                              event_type_package,
                              event_type_module,
                              event_type_name,
                              tx_sequence_number,
                              event_sequence_number,
                              senders[1]
                      FROM
                              events
                      WHERE
                              tx_sequence_number BETWEEN ? AND ?"
                     +ev-event-name+)

                    :ev-event-type
                    (format
                     "INSERT INTO %s
                      SELECT
                              event_type_package,
                              event_type_module,
                              SUBSTRING(event_type FROM '^[^:]+::[^:]+::(.*)'),
                              tx_sequence_number,
                              event_sequence_number,
                              senders[1]
                      FROM
                              events
                      WHERE
                              tx_sequence_number BETWEEN ? AND ?
                      AND     event_type LIKE '%%<%%'"
                     +ev-event-type+)

                    :ev-senders
                    (format
                     "INSERT INTO %s
                      SELECT
                              senders[1],
                              tx_sequence_number,
                              event_sequence_number
                      FROM
                              events
                      WHERE
                              tx_sequence_number BETWEEN ? AND ?"
                     +ev-senders+))
                  lo (dec hi)]
                 (jdbc/execute! db)
                 first :next.jdbc/update-count
                 (hash-map :updated)))

          :finalize
          (fn [{:keys [status fill updated]} signals]
            (when (= :success status)
              (swap! signals update fill (fnil + 0) updated) nil))))

(defn events:index-and-attach-partitions!
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
              :attach     (events:attach! db part lo hi)
              :drop-check (events:drop-range-check! db part))
            nil)

          :finalize
          (fn [{:as task :keys [job status]} signals]
            (let [phases [:autovacuum :analyze :constrain :attach :drop-check]
                  edges  (into {} (map vector phases (rest phases)))]
              (when (= :success status)
                (swap! signals update job (fnil inc 0))
                (when-let [next (edges job)]
                  [(assoc task :job next)]))))))

(defn events:constrain-and-index-ev! [db logger & {:keys [retry]}]
  (->Pool :name   "index-and-constrain"
          :logger  logger
          :workers 10

          :pending
          (or retry
              [{:fn ev-emit-pkg:constrain! :label "ev-emit-pkg"}
               {:fn ev-emit-mod:constrain! :label "ev-emit-mod"}
               {:fn ev-event-pkg:constrain! :label "ev-event-pkg"}
               {:fn ev-event-mod:constrain! :label "ev-event-mod"}
               {:fn ev-event-name:constrain! :label "ev-event-name"}
               {:fn ev-event-type:constrain! :label "ev-event-type"}
               {:fn ev-senders:constrain! :label "ev-senders"}
               {:fn ev-emit-pkg:index! :label "ev-emit-pkg"}
               {:fn ev-emit-mod:index! :label "ev-emit-mod"}
               {:fn ev-event-pkg:index! :label "ev-event-pkg"}
               {:fn ev-event-mod:index! :label "ev-event-mod"}
               {:fn ev-event-name:index! :label "ev-event-name"}
               {:fn ev-event-type:index! :label "ev-event-type"}])

          :impl (worker {work :fn} (work db) nil)

          :finalize
          (fn [{:as task :keys [status label]} signals]
            (when (= :success status)
              (swap! signals update :done (fnil conj []) label)
              nil))))

(defn events:vacuum-index! [db logger & {:keys [retry]}]
  (->Pool :name   "vacuum-index"
          :logger  logger
          :workers 10

          :pending
          (or retry
              (for [index [+ev-emit-pkg+ +ev-emit-mod+ +ev-event-pkg+
                           +ev-event-mod+ +ev-event-name+ +ev-event-type+
                           +ev-senders+]]
                {:index index :job :autovacuum}))

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
