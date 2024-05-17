(ns db
  (:require [next.jdbc :as jdbc])
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

(defn max-checkpoint
  "Get the maximum checkpoint sequence number."
  [db] (->> ["SELECT MAX(checkpoint_sequence_number) FROM objects_history"]
            (jdbc/execute-one! db)
            (:max)))

(defn disable-autovacuum!
  "Disable auto-vacuum for a table."
  [db name timeout]
  (with-table! db name
    "ALTER TABLE %s SET (autovacuum_enabled = false)"
    {:timeout timeout}))

(defn reset-autovacuum!
  "Reset the decision on whether to auto-vacuum or not to the
  database-wide setting."
  [db name timeout]
  (with-table! db name
    "ALTER TABLE %s RESET (autovacuum_enabled)"
    {:timeout timeout}))

(defn vacuum-and-analyze!
  "Vacuum and analyze a table."
  [db name timeout]
  (with-table! db name "VACUUM ANALYZE %s" {:timeout timeout}))

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
