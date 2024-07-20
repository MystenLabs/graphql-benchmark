(ns footprint
  (:require [clojure.edn :as edn]
            [clojure.set :as set]
            [clojure.string :as s]
            [db]
            [next.jdbc :as jdbc]))

;; Columns ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defn keyword->sql [ident]
  (s/replace (name ident) \- \_))

(defn sql->keyword [ident]
  (keyword (s/replace ident \_ \-)))

;; Table sizes ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn table-stats
  "Query table and index sizes from `db`.

  Returns a map from table names to a sequence of stats maps. If the
  table is unpartitioned, the sequence will contain a single map,
  otherwise it contains an entry per partition, in order.

  Each stats map contains the following keys:

   - `:self`  - the size of the table's own row data (its heap).
   - `:pkey`  - the size of the table's primary key index.
   - `:toast` - the size of the table's large object store.
   - `:idx`   - the size of all other indices."
  [db]
  (let [partitioned
        #(fn [[_ part]] [% (edn/read-string part)])

        table->entity
        (fn [table]
          (condp re-find table
            #"^events_partition_(\d+)" :>>
            (partitioned :events)

            #"^objects_history_partition_(\d+)" :>>
            (partitioned :objects-history)

            #"^objects_version_([0-9a-f]{2})" :>>
            (fn [[_ part]] [:objects-version (Integer/parseInt part 16)])

            #"^transactions_partition_(\d+)" :>>
            (partitioned :transactions)

            #"^chain_identifier" [:chain-identifier]

            #"^checkpoints" [:checkpoints]
            #"^display"     [:display]
            #"^epochs"      [:epochs]

            #"^event_emit_package"         [:event-emit-package]
            #"^event_emit_module"          [:event-emit-module]
            #"^event_senders"              [:event-senders]
            #"^event_struct_package"       [:event-struct-package]
            #"^event_struct_module"        [:event-struct-module]
            #"^event_struct_name"          [:event-struct-name]
            #"^event_struct_instantiation" [:event-struct-instantiation]

            #"^objects_snapshot"           [:objects-snapshot]
            #"^objects"                    [:objects]

            #"^packages"                   [:packages]

            #"^tx_calls_pkg"               [:tx-calls-pkg]
            #"^tx_calls_mod"               [:tx-calls-mod]
            #"^tx_calls_fun"               [:tx-calls-fun]
            #"^tx_changed_objects"         [:tx-changed-objects]
            #"^tx_digests"                 [:tx-digests]
            #"^tx_input_objects"           [:tx-input-objects]
            #"^tx_kinds"                   [:tx-kinds]
            #"^tx_recipients"              [:tx-recipients]
            #"^tx_senders"                 [:tx-senders]))]
    (->> ["SELECT
               relname,
               relkind,
               reltuples,
               pg_table_size(CAST(relname AS VARCHAR)) tab_size,
               pg_relation_size(CAST(relname AS VARCHAR)) rel_size
           FROM
               pg_class
           WHERE
               relkind IN ('i', 'r')
           AND relname NOT LIKE 'pg_%'
           AND relname NOT LIKE 'sql_%'
           AND relname NOT LIKE '__diesel_%'"]
         (jdbc/execute! db)

         ;; Each row represents a table or an index. Figure out
         ;; which (potentially partitioned) table it belongs to.
         (map (fn [{:pg_class/keys [relname relkind reltuples]
                    :keys [tab_size rel_size]}]
                [(table->entity relname)
                 (cond
                   (= relkind "r")
                   {:self rel_size
                    :toast (- tab_size rel_size)
                    :tuples reltuples}
                   (s/ends-with? relname "_pkey")
                   {:pkey rel_size}
                   :else
                   {:idx rel_size})]))

         ;; Group by the table name, and merge entries together, so that for
         ;; each table we get a single stats map.
         (group-by first)
         (map (fn [[key stats]]
                [key (apply merge-with +
                            (map second stats))]))

         ;; Sort results so that partitions end up in order, and group again,
         ;; this time by table names ignoring partitions.
         (sort-by first)
         (group-by (comp first first))
         (map (fn [[key stats]]
                [key (->> stats (map second) (into []))]))

         ;; Gather results into a dictionary keyed by table names, ignoring
         ;; partitions.
         (into {}))))

(defn pretty-size
  "Pretty print byte sizes."
  [sz]
  (loop [sz sz units '("B" "KiB" "MiB" "GiB" "TiB")]
    (if (or (< sz 1024) (not (rest units)))
      (format "%.2f %s" (double sz) (first units))
      (recur (/ sz 1024) (rest units)))))

(defn footprint
  "Calculate the total footprint given the per-table breakdown."
  [tables]
  (->> tables
     (mapcat (fn [[_ stats]] stats))
     (map (fn [{:keys [pkey self idx toast]}]
            (+ self (or pkey 0) (or idx 0) (or toast 0))))
     (reduce +)))

(defn stat-prorate
  "Scale all numeric fields of `stat` by `ratio`."
  [{:keys [self pkey idx toast tuples] :as stat} ratio]
  (cond-> stat
    self   (assoc :self   (long (* self   ratio)))
    pkey   (assoc :pkey   (long (* pkey   ratio)))
    idx    (assoc :idx    (long (* idx    ratio)))
    toast  (assoc :toast  (long (* toast  ratio)))
    tuples (assoc :tuples (long (* tuples ratio)))))

(defn partitions
  "The number of daily partitions in the table stats.

  Fails if different partitioned tables disagree on how many
  partitions there should be."
  [tables]
  (let [objects-history (-> tables :objects-history count)
        transactions    (-> tables :transactions    count)
        events          (-> tables :events          count)]
    (assert (= objects-history transactions events))
    objects-history))

;; Clustered Tables ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn clustered
  "Simulate storing the data in an RDBMS that supports clustered tables.

  In that case, we will use no extra storage for primary keys"
  [tables]
  (let [no-pkey #(dissoc % :pkey)]
    (->> tables
         (map (fn [[table stats]]
                [table (->> stats (map no-pkey) (into []))]))
         (into {}))))

;; Pruning ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn pruned
  "Simulate pruning to only contains the most recent `days` worth of data.

  Pruning for epoch partitioned tables is simulated by removing those
  tables. Pruning for other tables that get new data every day is
  simulated by removing a proportional number of elements."
  [days tables]
  (let [parts (partitions tables)
        ratio (/ days parts)

        ;; Simulate pruning by dropping all but the last `days` partitions.
        drop-parts #(->> % reverse (take days) reverse (into []))

        ;; Simulate pruning by reducing the footprint of all the parts of this
        ;; table uniformly by the ratio between the number of days to prune and
        ;; the number of partitions in the table.
        prorate (partial mapv #(stat-prorate % ratio))]
    (as-> tables $
      ;; Simulate pruning in these tables by dropping partitions
      (reduce #(update %1 %2 drop-parts) $
              [:objects-history
               :transactions
               :events])

      ;; Simulate pruning in these tables by removing a proportional number of
      ;; rows.
      (reduce #(update %1 %2 prorate) $
              [:checkpoints
               :epochs

               :event-emit-module
               :event-emit-package
               :event-senders
               :event-struct-instantiation
               :event-struct-module
               :event-struct-name
               :event-struct-package

               :objects-version

               :tx-calls-fun
               :tx-calls-mod
               :tx-calls-pkg
               :tx-changed-objects
               :tx-digests
               :tx-input-objects
               :tx-kinds
               :tx-recipients
               :tx-senders]))))

(defn efficient-pruning-scan
  "Some tables require an extra index to be added to support efficiently
  selecting the values that should be pruned. Without it, pruning must
  scan over ranges manually.

  This modifier simulates adding the extra index -- it budgets one
  long word for the sequence number (the index key) and one long word
  for the pointer into the heap."
  [tables]
  (let [idx-overhead
        #(->> %
              (map (fn [{:keys [tuples] :as stats}]
                     (update stats :idx (fnil + 0 0)
                             (and tuples (* 16 tuples)))))
              (into []))]
    (reduce #(update %1 %2 idx-overhead) tables
            [:event-emit-module
             :event-emit-package
             :event-senders
             :event-struct-instantiation
             :event-struct-module
             :event-struct-name
             :event-struct-package

             :objects-version

             :tx-calls-fun
             :tx-calls-mod
             :tx-calls-pkg
             :tx-changed-objects
             :tx-digests
             :tx-input-objects
             :tx-kinds
             :tx-recipients
             :tx-senders])))
