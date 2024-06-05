(ns version-benchmark
  (:require [pool :refer [->Pool worker]]
            [versions :as v]
            [next.jdbc :as jdbc]
            [clojure.core :as c]
            [clojure.core.async :as async]
            [clojure.string :as s])
  (:import [org.postgresql.util PSQLException]))

(def +objects-version+ "objects_version")

(defn objects-version:sample-objects
  "Read a sample of objects from the database, across all partitions.

  Fills the `:objects-version` field in the returned signals map table
  as a side-effect."
  [db logger timeout per-partition]
  (->Pool :name    "sample-objects"
          :workers 100
          :logger  logger

          :pending (for [part (range 256)] {:part part})

          :impl
          (worker {:keys [part]}
            (as-> [(format
                    "SELECT DISTINCT ON (object_id)
                             object_id,
                             object_version,
                             cp_sequence_number
                     FROM
                             %s
                     WHERE
                             object_id
                     BETWEEN ? AND ?
                     LIMIT
                             ?"
                    +objects-version+)
                   (v/partition:lb part)
                   (v/partition:ub part)
                   per-partition]
                  % (jdbc/execute! db % {:timeout timeout})
                  (hash-map :results %)))

          :finalize
          (fn [{:keys [part status results]} signals]
            (when (= :success status)
              (swap! signals update :objects-version
                     (fnil into []) results)
              nil))))

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

(defn explain-naive-query!
  "A query directly accessing the objects_history table.

  Note that it must bind the table to `v` for the `WHERE` clause
  added by `add-object-versions` to work."
  [db timeout object-versions]
  (->> (add-object-versions
        "SELECT
                 object_digest
         FROM
                 objects_history v
         WHERE
                 "
        object-versions)
       (db/explain-analyze-print! db timeout)))

(defn explain-optimized-query!
  "A query that speeds up accessing historical objects using the
  objects_version table."
  [db timeout object-versions]
  (->> (add-object-versions
        (format
         "SELECT
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
       (db/explain-analyze-print! db timeout)))
