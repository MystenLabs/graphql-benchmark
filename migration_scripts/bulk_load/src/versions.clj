(ns versions
  (:require [logger :refer [->Logger] :as l]
            [pool :refer [->Pool]]
            [next.jdbc :as jdbc]
            [clojure.core :as c]
            [clojure.core.async :as async :refer [go]]
            [clojure.pprint :refer [cl-format]])
  (:import [org.postgresql.util PSQLException]))

(defn- env [var & [default]]
  (or (System/getenv var) default))

(def db-config
  {:dbtype   "postgresql"
   :dbname   (env "DBNAME" "defaultdb")
   :host     (env "DBHOST" "localhost")
   :port     (env "DBPORT" "5432")
   :user     (env "DBUSER" "postgres")
   :password (env "DBPASS" "postgrespw")})

(def db (jdbc/get-datasource db-config))

(def +object-versions+ "amnn_1_object_versions")

(defn partition [byte]
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
         (partition part))]
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
          (partition part))
         lo (dec hi)
         (partition:lb part)
         (partition:ub part)]
      % (jdbc/execute! db % {:timeout timeout})))

(defn object-versions:constrain!
  "Add constraints to a given `part`ition.

  Readying it to be added to the main table."
  [db part timeout]
  (let [name (partition part)]
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
                 (partition part)
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
  (let [name (partition part)]
    (as-> [(format "ALTER TABLE %s DROP CONSTRAINT %s_partition_check"
                   name name)]
        % (jdbc/execute! db % {:timeout timeout}))))

;; (->> [(str "DROP TABLE " +object-versions+)]
;;      (jdbc/execute! db))
;; (dotimes [i 256]
;;   (->> [(str "DROP TABLE " (partition i))]
;;        (jdbc/execute! db)))

;; (object-versions:create! db)
;; (dotimes [i 256]
;;   (go (object-versions:create-partition! db i)
;;       (autovacuum! db (partition i) false)))

;; ;; Observability into thread pool
;; (def logger (->Logger *out*))
;; (def row-count   (atom 0))
;; (def vacuumed    (atom 0))
;; (def analyzed    (atom 0))
;; (def constrained (atom 0))
;; (def attached    (atom 0))
;; (def finished    (atom 0))
;; (def failed-jobs (atom []))

;; ;; Bulk loading thread pool
;; (let [max-cp (inc (max-checkpoint db))
;;       batch   1000000
;;       timeout 600

;;       pending
;;       (for [lo (range 0 max-cp batch)
;;             :let [hi (min (+ lo batch) max-cp)]
;;             part (range 256)]
;;         {:lo lo :hi hi :part part :retries 3})

;;       do-work
;;       (fn [{:as batch :keys [lo hi part]} reply]
;;         (try (->> (object-versions:populate! db part lo hi timeout)
;;                   first :next.jdbc/update-count
;;                   (assoc batch
;;                          :status :success
;;                          :updated)
;;                   (reply))
;;              (catch PSQLException e
;;                (if (= "57014" (.getSQLState e))
;;                  (reply (assoc batch :status :timeout))
;;                  (reply (assoc batch :status :error :error e))))
;;              (catch Throwable t
;;                (reply (assoc batch :status :error :error t)))))

;;       finalize
;;       (fn [{:keys [lo hi part retries status updated]}]
;;         (case status
;;           :success
;;           (do (swap! row-count + updated) nil)

;;           :timeout
;;           (let [m (+ lo (quot (- hi lo) 2))]
;;             (and (not= lo m)
;;                  [{:lo lo :hi m :part part :retries retries}
;;                   {:lo m :hi hi :part part :retries retries}]))

;;           :error
;;           (and (not= 0 retries)
;;                [{:lo lo :hi hi :part part :retries (dec retries)}])))]
;;   (->Pool :name     "bulk-load"
;;           :logger   logger
;;           :pending  pending
;;           :impl     do-work
;;           :finalize finalize
;;           :workers  100))

;; ;; Attachment thread pool
;; (let [delta-t 600

;;       pending
;;       (for [part (range 256)]
;;         {:part part :job :autovacuum :timeout delta-t})

;;       do-work
;;       (fn [{:as batch :keys [part job timeout]} reply]
;;         (try
;;           (case job
;;             :autovacuum (reset-autovacuum! db (partition part) timeout)
;;             :analyze    (vacuum-and-analyze! db (partition part) timeout)
;;             :constrain  (object-versions:constrain! db part timeout)
;;             :attach     (object-versions:attach! db part timeout)
;;             :drop-check (object-versions:drop-range-check! db part timeout))
;;           (reply (assoc batch :status :success))
;;           (catch PSQLException e
;;             (if (= "57014" (.getSQLState e))
;;               (reply (assoc batch :status :timeout))
;;               (reply (assoc batch :status :error :error e))))
;;           (catch Throwable t
;;             (reply (assoc batch :status :error :error t)))))

;;       finalize
;;       (fn [{:as batch :keys [part job status timeout]}]
;;         (case status
;;           :success ;; Queue the next phase for this partition
;;           (case job
;;             :autovacuum
;;             (do (swap! vacuumed inc)
;;                 [{:timeout delta-t :part part :job :analyze}])
;;             :analyze
;;             (do (swap! analyzed inc)
;;                 [{:timeout delta-t :part part :job :constrain}])
;;             :constrain
;;             (do (swap! constrained inc)
;;                 [{:timeout delta-t :part part :job :attach}])
;;             :attach
;;             (do (swap! attached inc)
;;                 [{:timeout delta-t :part part :job :drop-check}])
;;             :drop-check
;;             (do (swap! finished inc) nil))

;;           :timeout ;; Retry a timed out job with an incrementally longer timeout
;;           [{:part part :job job :timeout (+ timeout delta-t)}]

;;           :error   ;; Record failed jobs but keep going
;;           (do (swap! failed-jobs conj batch) nil)))]
;;   (->Pool :name     "attach"
;;           :logger   logger
;;           :pending  pending
;;           :impl     do-work
;;           :finalize finalize
;;           :workers  10))
