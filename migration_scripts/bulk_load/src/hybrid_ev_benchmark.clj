(ns hybrid-ev-benchmark
  (:require [alphabase.base58 :as b58]
            [clojure.string :as s]
            [db]
            [hex :refer [hex->bytes]]
            [honey.sql :as sql]
            [hybrid-ev :refer [+events+
                               +ev-emit-pkg+ +ev-emit-mod+
                               +ev-event-pkg+ +ev-event-mod+
                               +ev-event-name+ +ev-event-type+
                               +ev-senders+]]
            [hybrid-tx :refer [+tx-digests+]]))

(defn- consistent?
  "A filter is inconsistent if and only if it is guaranteed to return no
  results."
  [{:keys [tx] [tlo elo] :after [thi ehi] :before}]
  (cond-> true
    (and tlo thi) (and (<= tlo thi))
    (and elo ehi (= tlo thi))
    (and (< elo ehi))))

(defn- bounds->query
  "Create a query calculating a bound.

  `col` is the name of the column containing the aggregated result,
  `agg` is the aggregation function to apply to the bounds, and
  `bounds` are the queries calculating the individual components."
  [col agg bounds]
  (cond
    (= 1 (count bounds))
    {:select [[(first bounds) col]]}

    (not (empty? bounds))
    {:select [[(into [agg] bounds) col]]}))

(defn- tx:equal [{:keys [tx]}]
  (when tx
    {:select [[:tx-sequence-number :eq]]
     :from [(keyword +tx-digests+)]
     :where [:= :tx-digest (b58/decode tx)]}))

(defn- tx:lower-bound [{[tlo _] :after} tx-eq]
  (bounds->query
   :lo :greatest
   (cond-> []
     tlo   (conj tlo)
     tx-eq (conj tx-eq))))

(defn- tx:upper-bound [{[thi _] :before :keys [scan-limit]} tx-lo tx-eq]
  (bounds->query
   :hi :least
   (cond-> []
     thi        (conj thi)
     tx-eq      (conj tx-eq)
     scan-limit (conj (if tx-lo [:+ tx-lo scan-limit] scan-limit)))))

(defn- ev:lower-bound [{[tlo elo] :after} tx-lo]
  (cond
    (not elo) nil
    tx-lo {:select [[[:case [:> tx-lo tlo] 0 :else elo] :lo]]}
    :else {:select [[elo :lo]]}))

(defn- ev:upper-bound [{[thi ehi] :before} tx-hi]
  (cond
    (not ehi) nil
    tx-hi (let [u64-max (+ 1 (* 2 (bigint Long/MAX_VALUE)))]
            {:select [[[:case [:< tx-hi thi] u64-max :else ehi] :hi]]})
    :else {:select [[ehi :hi]]}))

(defn- select-ev
  "Select events by sequence number from table `from`.

  Filtered by condition `where`, optionally applying a sender filter
  from the filter params (first parameter), or bounds (`[tlo elo]`,
  `[thi ehi]`) to sequence numbers."
  [{:keys [sign]} [tlo elo] [thi ehi] from where]
  {:select [:tx-sequence-number
            :event-sequence-number]
   :from (keyword from)
   :where
   (cond-> [:and where]
     sign
     (conj [:= :sender (hex->bytes sign)])

     ;; We redundantly apply the transaction bounds even if we're applying them
     ;; in combination with the event sequence number bounds to give the query
     ;; planner a better chance of figuring out there's an upperbound for the
     ;; index scan.
     tlo (conj [:>= :tx-sequence-number tlo])
     thi (conj [:<= :tx-sequence-number thi])

     (and tlo elo)
     (conj [:>= [:row :tx-sequence-number :event-sequence-number] [tlo elo]])

     (and thi ehi)
     (conj [:<= [:row :tx-sequence-number :event-sequence-number] [thi ehi]]))})

(defn- select-senders [{:as params :keys [sign]} lo hi]
  (select-ev params lo hi +ev-senders+ true))

(defn- select-em-pkg [{:as params :keys [em-pkg]} lo hi]
  (select-ev params lo hi +ev-emit-pkg+
             [:= :package (hex->bytes em-pkg)]))

(defn- select-em-mod [{:as params :keys [em-pkg em-mod]} lo hi]
  (select-ev params lo hi +ev-emit-mod+
             [:and
              [:= :package (hex->bytes em-pkg)]
              [:= :module em-mod]]))

(defn- select-ev-pkg [{:as params :keys [ev-pkg ev-mod]} lo hi]
  (select-ev params lo hi +ev-event-pkg+
             [:= :package (hex->bytes ev-pkg)]))

(defn- select-ev-mod [{:as params :keys [ev-pkg ev-mod]} lo hi]
  (select-ev params lo hi +ev-event-mod+
             [:and
              [:= :package (hex->bytes ev-pkg)]
              [:= :module ev-mod]]))

(defn- select-ev-name [{:as params :keys [ev-pkg ev-mod ev-typ]} lo hi]
  (assert (not (s/includes? ev-typ "<")))
  (select-ev params lo hi +ev-event-name+
             [:and
              [:= :package (hex->bytes ev-pkg)]
              [:= :module ev-mod]
              [:= :name ev-typ]]))

(defn- select-ev-type [{:as params :keys [ev-pkg ev-mod ev-typ]} lo hi]
  (assert (s/includes? ev-typ "<"))
  (select-ev params lo hi +ev-event-type+
             [:and
              [:= :package (hex->bytes ev-pkg)]
              [:= :module ev-mod]
              [:= :name ev-typ]]))

(defn- select-events [params lo hi]
  (select-ev params lo hi +events+ true))

(defn compound-norm
  "Compound filters implemented using joins over normalized tables."
  [{:as params
    :keys [em-pkg em-mod
           ev-pkg ev-mod ev-typ]}
   lo hi]
  (let [sub-queries
        (cond-> []
          (and em-pkg em-mod)
          (conj (select-em-mod params lo hi))

          (and em-pkg (not em-mod))
          (conj (select-em-pkg params lo hi))

          (and ev-pkg ev-mod ev-typ
               (s/includes? ev-typ "<"))
          (conj (select-ev-type params lo hi))

          (and ev-pkg ev-mod ev-typ
               (not (s/includes? ev-typ "<")))
          (conj (select-ev-name params lo hi))

          (and ev-pkg ev-mod (not ev-typ))
          (conj (select-ev-mod params lo hi))

          (and ev-pkg (not ev-mod) (not ev-typ))
          (conj (select-ev-pkg params lo hi)))

        aliased (fn [q] [q (:from q)])]
    (assert (consistent? params))
    (assert (< 1 (count sub-queries)))
    {:select [:tx-sequence-number :event-sequence-number]
     :from [(aliased (first sub-queries))]
     :join (->> (rest sub-queries)
                (mapcat (fn [q] [(aliased q)
                                 [:using :tx-sequence-number
                                  :event-sequence-number]]))
                (into []))}))

(defn ev-filter
  [& {:as params :keys [ev-typ inline pretty]}]
  (let [additional #{:sign :tx :after :before :scan-limit :inline :pretty}

        just
        (fn [& ks] (and (every? (partial contains? params) ks)
                        (every? (partial contains? additional)
                                (keys (apply dissoc params ks)))))

        tx-eq-cte (some->> (tx:equal params) (vector :tx-eq))
        tx-eq     (some->> tx-eq-cte first (hash-map :select :eq :from))
        tx-lo-cte (some->> (tx:lower-bound params tx-eq) (vector :tx-lo))
        tx-lo     (some->> tx-lo-cte first (hash-map :select :lo :from))
        tx-hi-cte (some->> (tx:upper-bound params tx-lo tx-eq) (vector :tx-hi))
        tx-hi     (some->> tx-hi-cte first (hash-map :select :hi :from))
        ev-lo-cte (some->> (ev:lower-bound params tx-lo) (vector :ev-lo))
        ev-lo     (some->> ev-lo-cte first (hash-map :select :lo :from))
        ev-hi-cte (some->> (ev:upper-bound params tx-hi) (vector :ev-hi))
        ev-hi     (some->> ev-hi-cte first (hash-map :select :hi :from))

        lo [tx-lo ev-lo]
        hi [tx-hi ev-hi]

        ctes
        (cond-> []
          tx-eq-cte (conj tx-eq-cte)
          tx-lo-cte (conj tx-lo-cte)
          tx-hi-cte (conj tx-hi-cte)
          ev-lo-cte (conj ev-lo-cte)
          ev-hi-cte (conj ev-hi-cte))

        format
        #(sql/format % :inline inline :pretty pretty)

        bounded
        #(cond-> %
           (not (empty? ctes))
           (assoc :with ctes)

           :always (assoc :order-by [[:tx-sequence-number :asc]
                                     [:event-sequence-number :asc]])
           :always (assoc :limit 52)
           :always (format))]
    (cond (not (consistent? params))
          ;; A query that will always be empty
          (format {:select [:tx-sequence-number :event-sequence-number]
                   :from   [(keyword +events+)]
                   :where  false})

          (just :em-pkg)
          (bounded (select-em-pkg params lo hi))

          (just :em-pkg :em-mod)
          (bounded (select-em-mod params lo hi))

          (just :ev-pkg)
          (bounded (select-ev-pkg params lo hi))

          (just :ev-pkg :ev-mod)
          (bounded (select-ev-mod params lo hi))

          (and (just :ev-pkg :ev-mod :ev-typ)
               (s/includes? ev-typ "<"))
          (bounded (select-ev-type params lo hi))

          (just :ev-pkg :ev-mod :ev-typ)
          (bounded (select-ev-name params lo hi))

          (just :sign)
          (bounded (select-senders params lo hi))

          (just)
          (bounded (select-events params lo hi))

          :else
          (bounded (compound-norm params lo hi)))))
