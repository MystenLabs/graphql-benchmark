(ns version-benchmark
  (:require [pool :refer [->Pool signal-swap!]]
            [versions :as v]
            [next.jdbc :as jdbc]
            [clojure.core :as c]
            [clojure.core.async :as async]
            [clojure.string :as s])
  (:import [org.postgresql.util PSQLException]))

(defn objects-version:sample-objects
  "Read a sample of objects from the database, across all partitions.

  Fills the `:objects-version` field in the `signals` table as a side-effect."
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
                              objects_version
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
              (signal-swap! signals :objects-version into results))
            nil)))

(defn- add-objects-version
  "Construct a query from a query string and a list of object versions.

  Adds a `WHERE` clause to the query string with bindings for each of
  the provided object ID and version pairs, and then creates a vector
  containing that query and all the associated binding values."
  [query objects-version]
  (let [conds (repeat (count objects-version)
                      "(v.object_id = ? AND v.object_version = ?)")
        binds (reduce (fn [acc {:objects_version/keys
                                [object_id object_version]}]
                        (conj acc object_id object_version))
                      [] objects-version)]
    (into [(str query (s/join "\nOR      " conds))] binds)))

(defn- add-objects-version-in
  "Construct a query from a query string and a list of object versions.

  Adds a `WHERE` cluse to the query with bindings for each provided
  object ID, wrapped in an `IN` clause."
  [query objects-version]
  (let [conds (as-> objects-version %
                (count %)
                (repeat % "?")
                (s/join ", " %)
                (str "(" % ")"))
        binds (->> objects-version
                   (map :objects_version/object_id)
                   (into []))]
    (into [(format query conds)] binds)))

(defn- extract-query-plan
  [results]
  (->> results
       (map (keyword "QUERY PLAN"))
       (s/join "\n")))

(defn explain-naive-query!
  "A query directly accessing the objects_history table.

  Note that it must bind the table to `v` for the `WHERE` clause
  added by `add-objects-version` to work."
  [db objects-version]
  (->> (add-objects-version
        "EXPLAIN ANALYZE SELECT
                 object_digest
         FROM
                 objects_history v
         WHERE
                 "
        objects-version)
       (jdbc/execute! db)
       (extract-query-plan)
       (println)))

(defn explain-optimized-query!
  "A query that speeds up accessing historical objects using the
  objects_version table."
  [db objects-version]
  (->> (add-objects-version
        "EXPLAIN ANALYZE SELECT
                 h.object_digest
         FROM
                 objects_history h
         INNER JOIN
                 objects_version v
         ON (
                 h.object_id = v.object_id
         AND     h.object_version = v.object_version
         AND     h.checkpoint_sequence_number = v.cp_sequence_number
         )
         WHERE
                 "
        objects-version)
       (jdbc/execute! db)
       (extract-query-plan)
       (println)))

(defn objects-version:parent-bound-query!
  "Write a query that retrieves objects that are bounded by "
  [db object-versions parent-at-version]
  (as-> (add-objects-version-in
         "EXPLAIN ANALYZE
          SELECT DISTINCT ON (v.object_id)
              h.object_digest
          FROM
              objects_version v
          INNER JOIN
              objects_history h
          ON (
              v.object_id = h.object_id
          AND v.object_version = h.object_version
          AND v.cp_sequence_number = h.checkpoint_sequence_number
          )
          WHERE
              v.object_id IN %s
          AND v.object_version <= ?
          ORDER BY
              v.object_id,
              v.object_version DESC"
         object-versions) %
    (conj % parent-at-version)
    (jdbc/execute! db %)
    (extract-query-plan %)
    (println %)))
