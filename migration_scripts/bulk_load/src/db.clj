(ns db
  (:require [next.jdbc :as jdbc]))

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
