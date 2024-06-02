(ns db
  (:require [clojure.data.json :as json]
            [clojure.string :as s]
            [honey.sql.protocols :refer [InlineValue]]
            [next.jdbc :as jdbc])
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
    :password (or password (env "DBPASS") "postgrespw")}))

(defn with-table!
  "Run a transaction represented by a format string, expecting a table name.

  `template` is a format string representing the table's schema, that
  should expect a single string parameter, the table's name."
  ([db name template]
   (jdbc/execute! db [(format template name)]))
  ([db name template opts]
   (jdbc/execute! db [(format template name)] opts)))

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
  [db] (max-checkpoint-in "objects_history"))

(defn disable-autovacuum!
  "Disable auto-vacuum for a table."
  ([db name]
   (with-table! db name
     "ALTER TABLE %s SET (autovacuum_enabled = false)"))
  ([db name timeout]
   (with-table! db name
     "ALTER TABLE %s SET (autovacuum_enabled = false)"
     {:timeout timeout})))

(defn reset-autovacuum!
  "Reset the decision on whether to auto-vacuum or not to the
  database-wide setting."
  ([db name]
   (with-table! db name
     "ALTER TABLE %s RESET (autovacuum_enabled)"))
  ([db name timeout]
   (with-table! db name
     "ALTER TABLE %s RESET (autovacuum_enabled)"
     {:timeout timeout})))

(defn vacuum-and-analyze!
  "Vacuum and analyze a table."
  ([db name]
   (with-table! db name "VACUUM ANALYZE %s"))
  ([db name timeout]
   (with-table! db name "VACUUM ANALYZE %s" {:timeout timeout})))

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
  (as-> query %
    (format "EXPLAIN (ANALYZE, BUFFERS) %s" %)
    (into [%] binds)
    (jdbc/execute! db % {:timeout timeout})
    (doseq [line %]
      (println line))))

(defn explain-analyze-json!
  "EXPLAIN ANALYZE a query.

  Returns the execution and planning time of the query on success, or
  the keyword :timeout if it was not possible to finish the operation
  within the alotted time."
  [db timeout [query & binds]]
  (let [QUERY-PLAN (keyword "QUERY PLAN")
        key (fn [key] (-> key s/lower-case (s/replace #"\s+" "-") keyword))
        extract
        (fn [{e :execution-time p :planning-time {r :actual-rows} :plan}]
          {:execution-time e :planning-time p :actual-rows r})]
    (try
      (as-> query %
        (format "EXPLAIN (ANALYZE, FORMAT JSON) %s" %)
        (into [%] binds)
        (jdbc/execute-one! db % {:timeout timeout})
        (.getValue (QUERY-PLAN %))
        (json/read-str % :key-fn key)
        (extract (first %)))
      (catch PSQLException e
        (if (= "57014" (.getSQLState e))
          {:status :timeout}
          (throw e))))))

(defmacro worker
  "Create a worker function for a pool.

  The worker is passed a description of the unit of work (expected to
  be a map) which is bound to `param`, and then its `body` is
  evaluated. The return value of `body` is merged with the description
  of the work and sent back to the supervisor, along with a `:status`
  of `:success`.

  The returned worker detects timeouts and errors, returning the
  description of work with a `:status` of `:timeout` or `:error`
  respectively. Errors are additionally annotated with the `:error`
  itself."
  [param & body]
  `(fn [param# reply#]
     (try (->> (let [~param param#] ~@body)
               (merge param# {:status :success})
               (reply#))
          (catch PSQLException e#
            (if (= "57014" (.getSQLState e#))
              (reply# (assoc param# :status :timeout))
              (reply# (assoc param# :status :error :error e#))))
          (catch Throwable t#
            (reply# (assoc param# :status :error :error t#))))))

(defmacro worker-v2
  "Create a worker function for a pool.

  The worker is passed a description of the unit of work (expected to
  be a map) which is bound to `param`, and then its `body` is
  evaluated. The return value of `body` is merged with the description
  of the work and sent back to the supervisor, along with a `:status`
  of `:success`.

  The returned worker detects errors (including timeouts), returning
  the description of work with a `:status` of `:error`. Errors are
  additionally annotated with the `:error` itself."
  [param & body]
  `(fn [param# reply#]
     (try (->> (let [~param param#] ~@body)
               (merge param# {:status :success})
               (reply#))
          (catch Throwable t#
            (reply# (assoc param# :status :error :error t#))))))

(defn- sqlize-bytea [bs]
  (as-> bs %
    (map #(format "%02x" %) %)
    (s/join %)
    (str "E'\\\\x" % "'")))

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
