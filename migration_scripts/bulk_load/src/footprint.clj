(ns footprint
  (:require [clojure.edn :as edn]
            [clojure.set :as set]
            [clojure.string :as s]
            [db]
            [next.jdbc :as jdbc]))

;; Columns ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def all-columns
  "These are all the columns in the current schema."
  #{:chain-identifier/checkpoint-digest

    :checkpoints/sequence-number
    :checkpoints/checkpoint-digest
    :checkpoints/epoch
    :checkpoints/network-total-transactions
    :checkpoints/previous-checkpoint-digest
    :checkpoints/end-of-epoch
    :checkpoints/tx-digests
    :checkpoints/timestamp-ms
    :checkpoints/total-gas-cost
    :checkpoints/computation-cost
    :checkpoints/storage-cost
    :checkpoints/storage-rebate
    :checkpoints/non-refundable-storage-fee
    :checkpoints/checkpoint-commitments
    :checkpoints/validator-signature
    :checkpoints/end-of-epoch-data
    :checkpoints/min-tx-sequence-number
    :checkpoints/max-tx-sequence-number

    :display/object-type
    :display/id
    :display/version
    :display/bcs

    :epochs/epoch
    :epochs/first-checkpoint-id
    :epochs/epoch-start-timestamp
    :epochs/reference-gas-price
    :epochs/protocol-version
    :epochs/total-stake
    :epochs/storage-fund-balance
    :epochs/system-state
    :epochs/epoch-total-transactions
    :epochs/last-checkpoint-id
    :epochs/epoch-end-timestamp
    :epochs/storage-fund-reinvestment
    :epochs/storage-charge
    :epochs/storage-rebate
    :epochs/stake-subsidy-amount
    :epochs/total-gas-fees
    :epochs/total-stake-rewards-distributed
    :epochs/leftover-storage-fund-inflow
    :epochs/epoch-commitments

    :events/tx-sequence-number
    :events/event-sequence-number
    :events/transaction-digest
    :events/senders
    :events/package
    :events/module
    :events/event-type
    :events/timestamp-ms
    :events/bcs

    :event-emit-package/package
    :event-emit-package/tx-sequence-number
    :event-emit-package/event-sequence-number
    :event-emit-package/sender

    :event-emit-module/package
    :event-emit-module/module
    :event-emit-module/tx-sequence-number
    :event-emit-module/event-sequence-number
    :event-emit-module/sender

    :event-senders/sender
    :event-senders/tx-sequence-number
    :event-senders/event-sequence-number

    :event-struct-package/package
    :event-struct-package/tx-sequence-number
    :event-struct-package/event-sequence-number
    :event-struct-package/sender

    :event-struct-module/package
    :event-struct-module/module
    :event-struct-module/tx-sequence-number
    :event-struct-module/event-sequence-number
    :event-struct-module/sender

    :event-struct-name/package
    :event-struct-name/module
    :event-struct-name/type-name
    :event-struct-name/tx-sequence-number
    :event-struct-name/event-sequence-number
    :event-struct-name/sender

    :event-struct-instantiation/package
    :event-struct-instantiation/module
    :event-struct-instantiation/type-instantiation
    :event-struct-instantiation/tx-sequence-number
    :event-struct-instantiation/event-sequence-number
    :event-struct-instantiation/sender

    :objects/object-id
    :objects/object-version
    :objects/object-digest
    :objects/checkpoint-sequence-number
    :objects/owner-type
    :objects/owner-id
    :objects/object-type
    :objects/object-type-package
    :objects/object-type-module
    :objects/object-type-name
    :objects/serialized-object
    :objects/coin-type
    :objects/coin-balance
    :objects/df-kind
    :objects/df-name
    :objects/df-object-type
    :objects/df-object-id

    :objects-history/object-id
    :objects-history/object-version
    :objects-history/object-status
    :objects-history/object-digest
    :objects-history/checkpoint-sequence-number
    :objects-history/owner-type
    :objects-history/owner-id
    :objects-history/object-type
    :objects-history/object-type-package
    :objects-history/object-type-module
    :objects-history/object-type-name
    :objects-history/serialized-object
    :objects-history/coin-type
    :objects-history/coin-balance
    :objects-history/df-kind
    :objects-history/df-name
    :objects-history/df-object-type
    :objects-history/df-object-id

    :objects-snapshot/object-id
    :objects-snapshot/object-version
    :objects-snapshot/object-status
    :objects-snapshot/object-digest
    :objects-snapshot/checkpoint-sequence-number
    :objects-snapshot/owner-type
    :objects-snapshot/owner-id
    :objects-snapshot/object-type
    :objects-snapshot/object-type-package
    :objects-snapshot/object-type-module
    :objects-snapshot/object-type-name
    :objects-snapshot/serialized-object
    :objects-snapshot/coin-type
    :objects-snapshot/coin-balance
    :objects-snapshot/df-kind
    :objects-snapshot/df-name
    :objects-snapshot/df-object-type
    :objects-snapshot/df-object-id

    :objects-version/object-id
    :objects-version/object-version
    :objects-version/cp-sequence-number

    :packages/package-id
    :packages/original-id
    :packages/package-version
    :packages/move-package
    :packages/checkpoint-sequence-number

    :transactions/tx-sequence-number
    :transactions/transaction-digest
    :transactions/raw-transaction
    :transactions/raw-effects
    :transactions/checkpoint-sequence-number
    :transactions/timestamp-ms
    :transactions/object-changes
    :transactions/balance-changes
    :transactions/events
    :transactions/transaction-kind
    :transactions/success-command-count

    :tx-calls-pkg/tx-sequence-number
    :tx-calls-pkg/package
    :tx-calls-pkg/sender

    :tx-calls-mod/tx-sequence-number
    :tx-calls-mod/package
    :tx-calls-mod/module
    :tx-calls-mod/sender

    :tx-calls-fun/tx-sequence-number
    :tx-calls-fun/package
    :tx-calls-fun/module
    :tx-calls-fun/func
    :tx-calls-fun/sender

    :tx-changed-objects/tx-sequence-number
    :tx-changed-objects/object-id
    :tx-changed-objects/sender

    :tx-digests/tx-digest
    :tx-digests/tx-sequence-number

    :tx-input-objects/tx-sequence-number
    :tx-input-objects/object-id
    :tx-input-objects/sender

    :tx-kinds/tx-sequence-number
    :tx-kinds/tx-kind

    :tx-recipients/tx-sequence-number
    :tx-recipients/recipient
    :tx-recipients/sender

    :tx-senders/tx-sequence-number
    :tx-senders/sender})


(def kv-columns
  "Columns that could be moved to a blob store.

  Columns whose names end with a `*` can be omitted with the
  introduction of a blob store. Those that end in a `+` must be
  duplicated, and plain columns are moved."
  #{:checkpoints/sequence-number+
    :checkpoints/epoch
    :checkpoints/previous-checkpoint-digest
    :checkpoints/end-of-epoch
    :checkpoints/timestamp-ms
    :checkpoints/total-gas-cost
    :checkpoints/computation-cost
    :checkpoints/storage-cost
    :checkpoints/storage-rebate
    :checkpoints/non-refundable-storage-fee
    :checkpoints/checkpoint-commitments
    :checkpoints/validator-signature
    :checkpoints/end-of-epoch-data

    :display/bcs*

    :epochs/epoch
    :epochs/first-checkpoint-id
    :epochs/epoch-start-timestamp
    :epochs/reference-gas-price
    :epochs/protocol-version
    :epochs/total-stake
    :epochs/storage-fund-balance
    :epochs/system-state
    :epochs/epoch-total-transactions
    :epochs/last-checkpoint-id
    :epochs/epoch-end-timestamp
    :epochs/storage-fund-reinvestment
    :epochs/storage-charge
    :epochs/storage-rebate
    :epochs/stake-subsidy-amount
    :epochs/total-gas-fees
    :epochs/total-stake-rewards-distributed
    :epochs/leftover-storage-fund-inflow
    :epochs/epoch-commitments

    :events/tx-sequence-number
    :events/event-sequence-number
    :events/timestamp-ms
    :events/bcs

    :objects/object-id+
    :objects/object-version+
    :objects/serialized-object*

    :objects-history/object-id+
    :objects-history/object-version+
    :objects-history/serialized-object

    :objects-snapshot/object-id+
    :objects-snapshot/object-version+
    :objects-snapshot/serialized-object*

    :objects-version/object-id*
    :objects-version/object-version*
    :objects-version/cp-sequence-number*

    :packages/move-package*

    :transactions/tx-sequence-number
    :transactions/raw-transaction
    :transactions/raw-effects
    :transactions/timestamp-ms})

(defn undecorate
  "Remove any markings suffixing the column name"
  [column]
  (let [tab (namespace column)
        col (name column)]
    (keyword
     tab
     (if (or (s/ends-with? col "*")
             (s/ends-with? col "+"))
       (subs col 0 (dec (count col)))
       col))))

(defn keyword->sql [ident]
  (s/replace (name ident) \- \_))

(defn sql->keyword [ident]
  (keyword (s/replace ident \_ \-)))

(defn tables
  "Get the table names from a sequence of columns.

  Expects columns to be given as namespace-qualified clojure keywords."
  [columns]
  (->> columns (map namespace) (map keyword->sql) (into #{})))

;; Validation ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn validate-columns
  "Test that all the columns are from the `all-columns` set."
  [columns]
  (let [unknown (set/difference columns all-columns)]
    (when (seq unknown)
      (throw (ex-info (str "Unknown columns")
                      {:columns unknown})))))

(->> kv-columns (map undecorate) (into #{}) (validate-columns))

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
            #"^tx_senders"                 [:tx-senders]))

        per-table-size-stats
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
                                (map second stats))])))

        ;; Note that column stats may be missing if the relevant
        ;; tables haven't been analysed.
        per-table-column-stats
        (->> ["SELECT
                   tablename,
                   attname,
                   avg_width
               FROM
                   pg_stats
               WHERE
                   tablename NOT LIKE 'pg_%'
               AND tablename NOT LIKE 'sql_%'
               AND tablename NOT LIKE '__diesel_%'
               AND tablename NOT IN (
                   'events',
                   'transactions',
                   'objects_history'
               )"]
             (jdbc/execute! db)

             ;; Each row represents a column in a table. Re-interpret
             ;; the table name into an "entity" name which will be
             ;; used to group together partitions of a table
             ;; together (later).
             (map (fn [{:pg_stats/keys [tablename attname avg_width]}]
                    [(table->entity tablename)
                     (sql->keyword attname)
                     avg_width]))

             ;; Group all the columns for an entity together, and then
             ;; create a column width dictionary.
             (group-by first)
             (map (fn [[key cols]]
                    [key {:cols (->> cols
                                     (map (fn [[_ col width]] [col width]))
                                     (into {}))}])))]
    (->> (concat per-table-size-stats
                 per-table-column-stats)

         ;; We now have a map of size stats and a map of column stats
         ;; per entity, do another group and merge to bring them
         ;; together under one key.
         (group-by first)
         (map (fn [[key stats]]
                [key (->> stats (map second) (apply merge))]))

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
  "Calculate the total footprint in the DB given the per-table breakdown.

  Ignores size attributed to the KV store, if there is one."
  [tables]
  (->> tables
     (mapcat (fn [[table stats]] (when (not= :kv-store table) stats)))
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

;; KV Store ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn offloaded-to-kv-store
  "Simulate offloading blob data to the key-value store.

  - Columns that have been marked to be moved into a key-value
    store (according to `kv-columns`) have their sizes removed from
    their corresponding table and gathered into a special `:kv-store`
    entry in the sizes mapping.

  - It's assumed that if a table has columns to be moved to a KV
    store, all its TOAST-ed values belong to those columns, and will
    have their size re-attributed to the `:kv-store`.

  - It's also assumed that these columns will not impact indices and
    therefore won't change index or primary key size, unless the entire
    table can be moved to key-value store."
  [tables]
  (let [kv-size (atom 0)

        ;; Indicate whether the column should be moved to the KV
        ;; store (return `:mv`), removed from RDBMS (`:rm`),
        ;; copied (`:cp`), or is not affected by offloading (`nil`).
        kv-column
        (fn [table col]
          (let [fq #(keyword (name table) (str (name col) %))]
            (cond
              (kv-columns (fq ""))  :mv
              (kv-columns (fq "*")) :rm
              (kv-columns (fq "+")) :cp)))

        ;; Accepts a per-table statistic, and tries to extract
        ;; key-value data from it. Returns the updated statistic, and
        ;; updates `kv-size` with the size that should be attributed
        ;; to the key-value store.
        extract-kv
        (fn [table {:keys [cols tuples toast self] :as stat}]
          (cond
            ;; If we don't have column or tuple information or it
            ;; indicates that there are no key-value columns in this
            ;; table, skip it.
            (or (not cols) (not tuples)) stat
            (not-any? #(kv-column table %) (keys cols)) stat

            :else
            ;; Assume the "TOAST" table is offloaded, and then loop
            ;; over the remaining columns.
            (do (swap! kv-size + (or toast 0))
                (loop [cols (seq cols) stat (dissoc stat :toast)]
                  (if-let [[col width] (first cols)]
                    (let [weight (* tuples width)

                          credit-kv!
                          #(swap! kv-size + weight)

                          debit-col
                          #(-> stat
                               (update :self - weight)
                               (update :cols dissoc col))]
                      (case (kv-column table col)
                        :mv (do (credit-kv!) (recur (rest cols) (debit-col)))
                        :rm (recur (rest cols) (debit-col))
                        :cp (do (credit-kv!) (recur (rest cols) stat))
                        nil (recur (rest cols) stat)))

                    ;; If no columns remain, assume the entire table has been
                    ;; offloaded, and return `nil`.
                    (when (seq (:cols stat)) stat))))))

        extract-kvs
        (fn [table stats]
          (->> stats
               (map #(extract-kv table %))
               (filter some?)
               (into [])))

        extracted
        (into {} (for [[table stats] tables
                       :let [extract (extract-kvs table stats)]
                       :when (seq extract)]
                   [table extract]))]

    ;; Materialize `extracted` into a dictionary first, so `kv-size`
    ;; holds the estimated key-value store size (`for` produces a lazy
    ;; sequence).
    (assoc extracted :kv-store [{:self @kv-size}])))
