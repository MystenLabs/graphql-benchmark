(ns versions
  (:require [db]
            [logger :refer [->Logger] :as l]
            [pool :refer [->Pool worker]]
            [next.jdbc :as jdbc]))

(def +objects-version+ "objects_version")

(defn partition:name [byte]
  (format "%s_%02x" +objects-version+ byte))

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

(defn objects-version:create!
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
         +objects-version+)]
       (jdbc/execute! db)))

(defn objects-version:create-partition!
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

(defn objects-version:populate!
  "Populate a specific partition table with object versions from the
  history table between checkpoints `lo` (inclusive) and
  `hi` (exclusive)."
  [db part lo hi]
  (->> [(format
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
       (jdbc/execute! db)))

(defn objects-version:constrain!
  "Add constraints to a given `part`ition.

  Readying it to be added to the main table."
  [db part]
  (->> [(format
         "ALTER TABLE %1$s
          ADD PRIMARY KEY (object_id, object_version),
          ALTER COLUMN cp_sequence_number SET NOT NULL,
          ADD CONSTRAINT %1$s_partition_check CHECK (
            %2$s <= object_id %3$s
          )"
         (partition:name part)
         (->> part
              (partition:lb)
              (partition-literal))
         (->> (inc part)
              (partition:lb)
              (partition-literal)
              (str "AND object_id < ")
              (if (= part 255) "")))]
       (jdbc/execute! db)))

(defn objects-version:attach!
  "Attach a partition to the main table."
  [db part]
  (->> [(format
          "ALTER TABLE %s ATTACH PARTITION %s
           FOR VALUES FROM (%s) TO (%s)"
          +objects-version+
          (partition:name part)
          (->> part
               (partition:lb)
               (partition-literal))
          (->> (inc part)
               (partition:lb)
               (partition-literal)
               (if (= part 255) "MAXVALUE")))]
      (jdbc/execute! db)))

(defn objects-version:drop-range-check!
  "Drop the constraint that was added to speed up attaching the partition."
  [db part]
  (->> [(format "ALTER TABLE %1$s DROP CONSTRAINT %1$s_partition_check"
                (partition:name part))]
       (jdbc/execute! db)))

(defn objects-version:drop-all!
  "Drop the main table and partitions"
  [db logger & {:keys [retry]}]
  (db/with-table! db +objects-version+
    "DROP TABLE IF EXISTS %s")
  (->Pool :name     "drop-partitions"
          :logger   logger
          :workers  100
          :pending  (or retry (for [part (range 256)] {:part part}))
          :impl     (worker {:keys [part]}
                      (db/with-table! db (partition:name part)
                        "DROP TABLE IF EXISTS %s")
                      nil)))

(defn objects-version:create-all!
  "Create the main table, (with constraints and indices) and all
  partitions (without constraints and indices)."
  [db logger & {:keys [retry]}]
  (objects-version:create! db)
  (->Pool :name     "create-partitions"
          :logger   logger
          :workers  100
          :pending  (or retry (for [part (range 256)] {:part part}))
          :impl     (worker {:keys [part]}
                      (jdbc/with-transaction [tx db]
                        (objects-version:create-partition! tx part)
                        (db/disable-autovacuum! tx (partition:name part))
                        nil))))

(defn objects-version:bulk-load!
  "Bulk load all object versions from `objects_history` to the partition tables.

  Loads object versions created in checkpoints up to
  `max-cp` (exclusive). The returned signals map will contain a
  `:row-count` key that is updated in real time with the number of
  rows inserted."
  [db logger max-cp & {:keys [retry]}]
  (let [batch 100000]
    (->Pool :name "bulk-load"
            :logger logger
            :workers 50

            :pending
            (or retry
                (for [lo (range 0 max-cp batch)
                      :let [hi (min (+ lo batch) max-cp)]
                      part (range 256)]
                  {:lo lo :hi hi :part part}))

            :impl
            (worker {:keys [lo hi part]}
              (->> (objects-version:populate! db part lo hi)
                   first :next.jdbc/update-count
                   (hash-map :updated)))

            :finalize
            (fn [{:keys [status updated]} signals]
              (when (= :success status)
                (swap! signals update :row-count (fnil + 0) updated) nil)))))

(defn objects-version:index-and-attach!
  "Add indices and constraints to the partitions and attach them to the
  main table."
  [db logger & {:keys [retry]}]
  (->Pool :name "attach"
          :logger logger
          :workers 100

          :pending
          (or retry (for [part (range 256)] {:part part :job :autovacuum}))

          :impl
          (worker {:keys [part job]}
            (case job
              :autovacuum (db/reset-autovacuum! db (partition:name part))
              :analyze    (db/vacuum-and-analyze! db (partition:name part))
              :constrain  (objects-version:constrain! db part)
              :attach     (objects-version:attach! db part)
              :drop-check (objects-version:drop-range-check! db part))
            nil)

          :finalize
          (fn [{:as task :keys [job status]} signals]
            (let [phases [:autovacuum :analyze :constrain :attach :drop-check]
                  edges  (into {} (map vector phases (rest phases)))]
              (when (= :success status)
                (swap! signals update job (fnil inc 0))
                (when-let [next (edges job)]
                  [(assoc task :job next)]))))))
