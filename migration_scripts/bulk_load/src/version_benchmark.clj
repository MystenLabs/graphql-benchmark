(ns version-benchmark
  (:require [logger :refer [->Logger] :as l]
            [pool :refer [->Pool signals signal-swap!]]
            [versions :as v]
            [next.jdbc :as jdbc]
            [clojure.core :as c]
            [clojure.core.async :as async :refer [go]])
  (:import [org.postgresql.util PSQLException]))

(def +objects-version+ "amnn_1_object_versions")

(defn objects-version:sample-objects
  "Read a sample of objects from the database, across all partitions.

  Fills the `:object-versions` field in the `signals` table as a side-effect."
  [db logger timeout per-partition signals]
  (->Pool :name    "sample-objects"
          :workers 100
          :logger  logger

          :pending (range 256)

          :impl
          (fn [part reply]
            (try
              (as-> ["SELECT DISTINCT ON (object_id)
                              object_id,
                              object_version,
                              cp_sequence_number
                      FROM
                              amnn_1_object_versions
                      WHERE
                              object_id
                      BETWEEN ? AND ?
                      LIMIT
                              ?"
                     (v/partition:lb part)
                     (v/partition:ub part)
                     per-partition]
                  % (jdbc/execute! db % {:timeout timeout})
                  (assoc {:part part} :status :success :results %)
                  (reply %))

              (catch PSQLException e
                (if (= "57014" (.getSQLState e))
                  (reply {:part part :status :timeout})
                  (reply {:part part :status :error :error e})))
              (catch Throwable t
                (reply {:part part :status :error :error t}))))

          :finalize
          (fn [{:keys [part status results]}]
            (when (= :success status)
              (signal-swap! signals :object-versions into results))
            nil)))

(defn- add-object-versions
  "Construct a query from a query string and a list of object versions.

  Adds a `WHERE` clause to the query string with bindings for each of
  the provided object ID and version pairs, and then creates a vector
  containing that query and all the associated binding values."
  [query object-versions]
  (let [conds (repeat (count object-versions)
                      "(v.object_id = ? AND v.object_version = ?)")
        binds (reduce (fn [acc {:amnn_1_object_versions/keys
                                [object_id object_version]}]
                        (conj acc object_id object_version))
                      [] object-versions)]
    (into [(str query (String/join "\nOR      " conds))] binds)))

(defn- extract-query-plan
  [results]
  (->> (map (keyword "QUERY PLAN"))
       (String/join "\n")))

(defn explain-naive-query!
  "A query directly accessing the objects_history table.

  Note that it must bind the table to `v` for the `WHERE` clause
  added by `add-object-versions` to work."
  [db object-versions]
  (->> (add-object-versions
        "EXPLAIN ANALYZE SELECT
                 object_digest
         FROM
                 objects_history v
         WHERE
                 "
        object-versions)
       (jdbc/execute! db)
       (extract-query-plan)
       (println)))

(defn explain-optimized-query!
  "A query that speeds up accessing historical objects using the
  objects_version table."
  [db object-versions]
  (->> (add-object-versions
        (format
         "EXPLAIN ANALYZE SELECT
                  h.object_digest
          FROM
                  objects_history h
          INNER JOIN
                  %s v
          ON (
                  h.object_id = v.object_id
          AND     h.object_version = v.object_version
          AND     h.checkpoint_sequence_number = v.cp_sequence_number
          )
          WHERE
                  "
        +objects-version+)
        object-versions)
       (jdbc/execute! db query)
       (extract-query-plan)
       (println)))
