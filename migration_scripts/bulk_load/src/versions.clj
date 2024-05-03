(ns versions
  (:require [db]
            [logger :refer [->Logger] :as l]
            [pool :refer [->Pool signal-swap!]]
            [next.jdbc :as jdbc]
            [clojure.core :as c]
            [clojure.core.async :as async :refer [go]])
  (:import [org.postgresql.util PSQLException]))

(def +object-versions+ "amnn_2_object_versions")

(defn partition:name [byte]
  (format "%s_%02x" +object-versions+ byte))

(defn partition:lb
  "Inclusive lower bound of the `part`ition, represented as a byte array."
  [part]
  (let [bytes (byte-array 32)]
    (aset-byte bytes 0 (unchecked-byte part))
    bytes))

(defn partition:ub
  "Inclusive upperbound of the `part`ition, represented as a byte array."
  [part]
  (let [bytes (byte-array (replicate 32 0xFF))]
    (aset-byte bytes 0 (unchecked-byte part))
    bytes))

(defn partition-literal
  "Postgres literal representation of the byte-array representing a
  partition boundary"
  [bytes]
  (with-out-str
    (print "E'\\\\x")
    (doseq [b bytes] (printf "%02x" b))
    (print "'")))

(defn object-versions:create!
  "Create the main object versions table.

  We set-up all its indices and constraints up-front because we will
  bulk load into the partitions."
  [db]
  (->> [(format
         "CREATE TABLE IF NOT EXISTS %s (
            object_id          BYTEA,
            object_version     BIGINT,
            cp_sequence_number BIGINT NOT NULL,
            PRIMARY KEY (object_id, object_version)
          )
          PARTITION BY RANGE (object_id)"
         +object-versions+)]
       (jdbc/execute! db)))

(defn object-versions:create-partition!
  "Create a table for a specific partition of object versions.

  New partitions don't get any constraints or indices, to make it
  cheap to load into them."
  [db part]
  (->> [(format
         "CREATE TABLE IF NOT EXISTS %s (
            object_id          BYTEA,
            object_version     BIGINT,
            cp_sequence_number BIGINT
          )"
         (partition:name part))]
       (jdbc/execute! db)))

(defn disable-autovacuum!
  "Disable auto-vacuum for a table."
  [db name timeout]
  (as-> [(format "ALTER TABLE %s SET (autovacuum_enabled = false)"
                 name)]
      % (jdbc/execute! db % {:timeout timeout})))

(defn reset-autovacuum!
  "Reset the decision on whether to auto-vacuum or not to the
  database-wide setting."
  [db name timeout]
  (as-> [(format "ALTER TABLE %s RESET (autovacuum_enabled)"
                 name)]
      % (jdbc/execute! db % {:timeout timeout})))

(defn vacuum-and-analyze!
  "Vacuum and analyze a table."
  [db name timeout]
  (as-> [(format "VACUUM ANALYZE %s" name)]
      % (jdbc/execute! db % {:timeout timeout})))

(defn max-checkpoint
  "Get the maximum checkpoint sequence number."
  [db] (->> ["SELECT MAX(checkpoint_sequence_number) FROM objects_history"]
            (jdbc/execute-one! db)
            (:max)))

(defn object-versions:populate!
  "Populate a specific partition table with object versions from the
  history table between checkpoints `lo` (inclusive) and
  `hi` (exclusive). Applies a timeout, in seconds to the request."
  [db part lo hi timeout]
  (as-> [(format
          "INSERT INTO %s (
               object_id,
               object_version,
               cp_sequence_number
           )
           SELECT
               object_id,
               object_version,
               checkpoint_sequence_number
           FROM
               objects_history
           WHERE
               checkpoint_sequence_number BETWEEN ? AND ?
           AND object_id BETWEEN ? AND ?"
          (partition:name part))
         lo (dec hi)
         (partition:lb part)
         (partition:ub part)]
      % (jdbc/execute! db % {:timeout timeout})))

(defn object-versions:constrain!
  "Add constraints to a given `part`ition.

  Readying it to be added to the main table."
  [db part timeout]
  (let [name (partition:name part)]
    (as-> [(format "ALTER TABLE %s
                    ADD PRIMARY KEY (object_id, object_version),
                    ALTER COLUMN cp_sequence_number SET NOT NULL,
                    ADD CONSTRAINT %s_partition_check CHECK (
                      %s <= object_id %s
                    )"
                   name name
                   (->> part
                        (partition:lb)
                        (partition-literal))
                   (->> (inc part)
                        (partition:lb)
                        (partition-literal)
                        (str "AND object_id < ")
                        (if (= part 255) "")))]
        % (jdbc/execute! db % {:timeout timeout}))))

(defn object-versions:attach!
  "Attach a partition to the main table."
  [db part timeout]
  (as-> [(format "ALTER TABLE %s ATTACH PARTITION %s
                  FOR VALUES FROM (%s) TO (%s)"
                 +object-versions+
                 (partition:name part)
                 (->> part
                      (partition:lb)
                      (partition-literal))
                 (->> (inc part)
                      (partition:lb)
                      (partition-literal)
                      (if (= part 255) "MAXVALUE")))]
      % (jdbc/execute! db % {:timeout timeout})))

(defn object-versions:drop-range-check!
  "Drop the constraint that was added to speed up attaching the partition."
  [db part timeout]
  (let [name (partition:name part)]
    (as-> [(format "ALTER TABLE %s DROP CONSTRAINT %s_partition_check"
                   name name)]
        % (jdbc/execute! db % {:timeout timeout}))))

(defn object-versions:drop-all!
  "Drop the main table and partitions"
  [db logger]
  (->> [(str "DROP TABLE " +object-versions+)]
       (jdbc/execute! db))
  (->Pool :name     "drop-partitions"
          :logger   logger
          :workers  100
          :pending  (range 256)
          :impl     (fn [part reply]
                      (->> [(str "DROP TABLE " (partition:name part))]
                           (jdbc/execute! db))
                      (reply true))
          :finalize (fn [_] nil)))

(defn object-versions:create-all!
  "Create the main table, (with constraints and indices) and all
  partitions (without constraints and indices)."
  [db logger timeout]
  (object-versions:create! db)
  (->Pool :name     "create-partitions"
          :logger   logger
          :workers  100
          :pending  (range 256)
          :impl     (fn [part reply]
                      (object-versions:create-partition! db part)
                      (disable-autovacuum! db (partition:name part) timeout)
                      (reply true))
          :finalize (fn [_] nil)))

(defn object-versions:bulk-load!
  "Bulk load all object versions from `objects_history` to the partition tables.

  If `signals` includes a `:row-count` key, it will be updated with
  the number of rows inserted.

  Returns a `kill` channel and a `join` channel for interacting with
  the job as it is in progress."
  [db logger timeout signals]
  (let [max-cp (inc (max-checkpoint db)) batch  1000000]
    (->Pool :name "bulk-load"
            :logger logger
            :workers 100

            :pending
            (for [lo (range 0 max-cp batch)
                  :let [hi (min (+ lo batch) max-cp)]
                  part (range 256)]
              {:lo lo :hi hi :part part :retries 3})

            :impl
            (fn [{:as batch :keys [lo hi part]} reply]
              (try (->> (object-versions:populate! db part lo hi timeout)
                        first :next.jdbc/update-count
                        (assoc batch
                               :status :success
                               :updated)
                        (reply))
                   (catch PSQLException e
                     (if (= "57014" (.getSQLState e))
                       (reply (assoc batch :status :timeout))
                       (reply (assoc batch :status :error :error e))))
                   (catch Throwable t
                     (reply (assoc batch :status :error :error t)))))

            :finalize
            (fn [{:keys [lo hi part retries status updated]}]
              (case status
                :success
                (do (signal-swap! signals :row-count + updated) nil)

                :timeout
                (let [m (+ lo (quot (- hi lo) 2))]
                  (and (not= lo m)
                       [{:lo lo :hi m :part part :retries retries}
                        {:lo m :hi hi :part part :retries retries}]))

                :error
                (and (not= 0 retries)
                     [{:lo lo :hi hi :part part :retries (dec retries)}]))))))

(defn object-versions:index-and-attach!
  "Add indices and constraints to the partitions and attach them to the
  main table.

  If `signals` includes a `:autovacuum`, `:analyze`, `:constrain`,
  `:attach`, and `:drop-check` key, they will be updated with the
  number of partitions that have reached that phase of the process.

  If `signals` contains a `failed-jobs` key, it will be updated with a
  list of jobs that failed with some error (other than timeouts).

  `delta-t` specifies the increment of time that we use to update
  timeouts with: If a job times out, we re-queue it with a timeout
  that is `delta-t` seconds bigger (`delta-t` is also the initial
  timeout).

  Returns a `kill` channel and a `join` channel for interacting with
  the job as it is in progress."
  [db logger delta-t signals]
  (->Pool :name "attach"
          :logger logger
          :workers 100

          :pending
          (for [part (range 256)]
            {:part part :job :autovacuum :timeout delta-t})

          :impl
          (fn [{:as batch :keys [part job timeout]} reply]
            (try
              (case job
                :autovacuum (reset-autovacuum! db (partition:name part) timeout)
                :analyze    (vacuum-and-analyze! db (partition:name part) timeout)
                :constrain  (object-versions:constrain! db part timeout)
                :attach     (object-versions:attach! db part timeout)
                :drop-check (object-versions:drop-range-check! db part timeout))
              (reply (assoc batch :status :success))
              (catch PSQLException e
                (if (= "57014" (.getSQLState e))
                  (reply (assoc batch :status :timeout))
                  (reply (assoc batch :status :error :error e))))
              (catch Throwable t
                (reply (assoc batch :status :error :error t)))))

          :finalize
          (fn [{:as batch :keys [part job status timeout]}]
            (let [phases [:autovacuum :analyze :constrain :attach :drop-check]
                  edges  (into {} (map vector phases (rest phases)))]
              (case status
                :success
                (do (signal-swap! signals job inc)
                    (when-let [next (edges job)]
                      [{:timeout delta-t :part part :job next}]))

                :timeout
                [{:part part :job job :timeout (+ timeout delta-t)}]

                :error
                (do (signal-swap! signals :failed-jobs conj batch) nil))))))
