(ns db
  (:require [clojure.data.json :as json]
            [clojure.string :as s]
            [honey.sql.protocols :refer [InlineValue]]
            [next.jdbc :as jdbc]
            [pool :refer [->Pool worker]])
  (:import [org.postgresql.util PSQLException]))

(defn- env [var] (System/getenv var))

(defn data
  "Create a DB from the given config."
  [& {:keys [dbname host port user password]}]
  (jdbc/get-datasource
   {:dbtype "postgresql"
    :dbname   (or dbname   (env "DBNAME") "defaultdb")
    :host     (or host     (env "DBHOST") "localhost")
    :port     (or port     (env "DBPORT") "5432")
    :user     (or user     (env "DBUSER") "postgres")
    :password (or password (env "DBPASS") "postgrespw")
    :options  "-c plan_cache_mode=force_custom_plan -c work_mem=64MB"}))

(defn with-table!
  "Run a transaction represented by a format string, expecting a table name.

  `template` is a format string representing the table's schema, that
  should expect a single string parameter, the table's name."
  [db name template]
  (jdbc/execute! db [(format template name)]))

(defn bounds
  "Get lower and upper-bounds of `field` in partitioned `table`.

  `parts` is a sequence of partition identifiers. `table` is a
  function that accepts a partition identifier and returns the table
  name for that partition, and `field` is the field whose bounds are
  being sought in each partition.

  Bounds are appended to a `:bounds` key on the signal map returned
  from this function."
  [db parts logger table field]
  (->Pool :name    "fetch-bounds"
          :logger   logger
          :workers (Math/clamp (long (count parts)) 4 20)
          :pending (for [part parts] {:part part})

          :impl
          (worker {:keys [part]}
                  (->> [(format "SELECT MIN(%1$s), MAX(%1$s) FROM %2$s"
                                field (table part))]
                       (jdbc/execute-one! db)))

          :finalize
          (fn [{:keys [status part min max]} signals]
            (when (= :success status)
              (swap! signals update :bounds (fnil conj [])
                     {:part part :lo min :hi (inc max)})
              nil))))

(defn max-checkpoint-in
  "Get the maxmium checkpoint sequence number in the given table."
  [db table]
  (first (with-table! db table
           "SELECT MAX(checkpoint_sequence_number) FROM %s")))

(defn min-checkpoint-in
  "Get the maxmium checkpoint sequence number in the given table."
  [db table]
  (first (with-table! db table
           "SELECT MIN(checkpoint_sequence_number) FROM %s")))

(defn max-checkpoint
  "Get the maximum checkpoint sequence number."
  [db] (max-checkpoint-in db "objects_history"))

(defn disable-autovacuum!
  "Disable auto-vacuum for a table."
  [db name]
  (with-table! db name
    "ALTER TABLE %s SET (autovacuum_enabled = false)"))

(defn reset-autovacuum!
  "Reset the decision on whether to auto-vacuum or not to the
  database-wide setting."
  [db name]
  (with-table! db name
    "ALTER TABLE %s RESET (autovacuum_enabled)"))

(defn vacuum-and-analyze!
  "Vacuum and analyze a table."
  [db name]
  (with-table! db name "VACUUM ANALYZE %s"))

(defn cancel!
  "Cancel query matching `filter`.

  This should be used carefully, as it can affect other people's jobs
  running on the same DB."
  [db filter]
  (->> ["SELECT
             pg_cancel_backend(pid)
         FROM
             pg_stat_activity
         WHERE
             query LIKE ?
         AND state = 'active'
         AND pid <> pg_backend_pid()"
        filter]
       (jdbc/execute! db)))

(defn explain-analyze-print!
  "EXPLAIN ANALYZE a query.

  Prints the plain text output, or throws an error if the query fails
  for some reason (including timeout)."
  [db timeout [query & binds]]
  (let [QUERY-PLAN (keyword "QUERY PLAN")]
    (as-> query %
      (format "EXPLAIN (ANALYZE, BUFFERS) %s" %)
      (into [%] binds)
      (jdbc/execute! db % {:timeout timeout})
      (doseq [line %]
        (println (QUERY-PLAN line))))))

(defn explain-analyze-json!
  "EXPLAIN ANALYZE a query.

  Returns a map containing the following fields:

  - `::exec` - total execution time (ms)
  - `::plan` - total planning time (ms)
  - `::read` - total I/O read time (ms)
  - `::rows` - number of rows
  - `::miss` - number of block cache misses
  - `::hits` - number of block cache hits"
  [db timeout [query & binds]]
  (let [QUERY-PLAN (keyword "QUERY PLAN")
        key (fn [key] (-> key s/lower-case (s/replace #"\s+" "-") keyword))
        extract
        (fn [{e :execution-time
              p :planning-time
              {r :actual-rows
               m :shared-read-blocks
               h :shared-hit-blocks
               i :i/o-read-time
               } :plan}]
          {::exec e ::plan p ::read i
           ::rows r ::miss m ::hits h})]
    (try
      (as-> query %
        (format "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) %s" %)
        (into [%] binds)
        (jdbc/execute-one! db % {:timeout timeout})
        (.getValue (QUERY-PLAN %))
        (json/read-str % :key-fn key)
        (extract (first %)))
      (catch PSQLException e
        (if (= "57014" (.getSQLState e))
          {:status :timeout}
          (throw e))))))

(defmacro temporarily
  "Perform an operation within a transaction that gets rolled back.

  Evaluate `body` within a DB transaction `tx`, created on `db`, that
  is guaranteed to be rolled back (not committed)."
  [[tx db] & body]
  `(jdbc/with-transaction [~tx ~db]
     (try (do ~@body)
          (finally (jdbc/execute! ~tx ["ROLLBACK"])))))

(defn sqlize-bytea [bs]
  (as-> bs %
    (map #(format "%02x" %) %)
    (s/join %)
    (str "E'\\\\x" % "'::bytea")))

(extend-protocol InlineValue
  (Class/forName "[B")
  (sqlize [this]
    (sqlize-bytea this)))

(extend-protocol InlineValue
  (Class/forName "[[B")
  (sqlize [this]
    (as-> this %
      (map sqlize-bytea %)
      (s/join ", " %)
      (str "(" % ")"))))
