(ns hybrid-tx-benchmark
  (:require [alphabase.base58 :as b58]
            [db]
            [hex :refer [hex->bytes]]
            [honey.sql :as sql]
            [honey.sql.pg-ops :refer [<at]]
            [hybrid-tx
             :refer [+transactions+ +tx-filters+ +tx-filters-gin+
                     +tx-calls-pkg+ +tx-calls-mod+ +tx-calls-fun+
                     +tx-senders+ +tx-recipients+
                     +tx-input-objects+ +tx-changed-objects+
                     +tx-digests+ +tx-kinds+ +cp-tx+]]))

;; # Hybrid Transactions Benchmark
;;
;; Building transaction block filters for the hybrid schema. Main differences:
;;
;; - The filter query is just responsible for returning some transaction
;;   sequence numbers, and we will issue a separate query to fetch transaction
;;   data.
;;
;; - We detect queries that can be served by just one table, and if we find
;;   them, we avoid joins. These queries are not limited to strict "atomic"
;;   filters, but also optionally include:
;;
;;   - Bounds on the checkpoint the data is from (or a pagination cursor).
;;   - Limiting to transactions from a set of digests.
;;   - Bounds on the sender.
;;
;; - When we cannot implement a query as a `SELECT` on a single table, there are
;;   three competing strategies for how to implement it:
;;
;;   - by joining the individual queries we were using before,
;;   - by doing a sequential scan on a denormalized table, for transactions
;;     within a range,
;;   - by using a GIN on a denormalized table, for transactions within a range.
;;
;;   Which one is best is measured by how many queries of this kind succeed,
;;   meaning it doesn't time out, and how responsive the query is to setting a
;;   smaller range of transactions to scan: An approach that is not responsive
;;   in this way is less ideal, because it won't allow users to tune their
;;   queries to handle the scale of their underlying data-set.
;;
;; - There is an additional optimisation in the compound query case, where we
;;   use the normalized tables to recover some bound on the range of
;;   transactions to scan: The lowerbound is the max-min sequence number of
;;   each individual filter, and the upperbound is the min-max.

(def ^:private sys-addr:norm (hex/normalize "0x0"))
(def ^:private sys-addr:bytes (hex->bytes "0x0"))

(defn- from-cp-tx
  "Select `fields` from `cp-tx` for checkpoint sequence number `cp`."
  [cp & fields]
  {:select fields
   :from [(keyword +cp-tx+)]
   :where [:= :checkpoint-sequence-number cp]})

(defn- consistent?
  "A filter is inconsistent if and only if it is guaranteed to return no
  results."
  [{:keys [cp-< cp-= cp-> kind sign]}]
  (cond-> true
    cp-<            (and (pos? cp-<))
    (and cp-> cp-<) (and (< (inc cp->) cp-<))
    (and cp-> cp-=) (and (< cp-> cp-=))
    (and cp-= cp-<) (and (< cp-= cp-<))
    (and kind sign) (and (= (= :system kind)
                            (= sys-addr:norm
                               (hex/normalize sign))))))

(defn- bounds->query
  "Create a query calculating a bound.

  `agg` is the aggregation function to apply to the bounds, and
  `bounds` are the queries calculating the individual components."
  [col agg bounds]
  (cond
    (= 1 (count bounds))
    {:select [[(first bounds) col]]}

    (not (empty? bounds))
    {:select [[(into [agg] bounds) col]]}))

(defn- lower-bound
  "Return a query computing the lowest transaction sequence number to scan."
  [{:keys [cp-> cp-= after]}]
  (bounds->query
   :lo :greatest
   (cond-> []
     after (conj after)
     cp-=  (conj (from-cp-tx cp-= :min-tx-sequence-number))

     (and (not cp-=) cp->)
     (conj (from-cp-tx cp-> [[:+ 1 :max-tx-sequence-number]])))))

(defn- upper-bound
  "Return a query computing the highest transaction sequence number to scan.

  `lo` is a SQL expression that evaluates the lower bound, if there is one."
  [lo {:keys [cp-< cp-= before scan-limit]}]
  (bounds->query
   :hi :least
   (cond-> []
     before (conj before)
     cp-=  (conj (from-cp-tx cp-= :max-tx-sequence-number))

     ;; Note that we only support forward scanning in the benchmark, but the
     ;; production implementation needs to deal with backward scanning as
     ;; well (if we are querying for the `last` transactions instead of the
     ;; `first`).
     scan-limit
     (conj (if lo [:+ lo scan-limit] scan-limit))

     (and (not cp-=) cp-< (pos? cp-<))
     (conj (from-cp-tx cp-< [[:- 1 :min-tx-sequence-number]])))))

(defn- select-tx
  "Select `tx-sequence-number`s from table `from`.

  Filtered by condition `where`, optionally applying a sender filter
  from the filter params (first parameter), or transaction sequence
  number `lo`wer or upperbounds (`hi`)."
  [{:keys [sign]} lo hi from where]
  {:select [:tx-sequence-number]
   :from (keyword from)
   :where
   (cond-> [:and where]
     sign
     (conj [:= :sender (hex->bytes sign)])

     lo (conj [:>= :tx-sequence-number lo])
     hi (conj [:<= :tx-sequence-number hi]))})

(defn- select-pkg [{:as params :keys [pkg]} lo hi]
  (select-tx params lo hi +tx-calls-pkg+
             [:= :package (hex->bytes pkg)]))

(defn- select-mod [{:as params :keys [pkg mod]} lo hi]
  (select-tx params lo hi +tx-calls-mod+
             [:and
              [:= :package (hex->bytes pkg)]
              [:= :module mod]]))

(defn- select-fun [{:as params :keys [pkg mod fun]} lo hi]
  (select-tx params lo hi +tx-calls-fun+
             [:and
              [:= :package (hex->bytes pkg)]
              [:= :module mod]
              [:= :func fun]]))

(defn- select-kind [{:as params :keys [kind]} lo hi]
  (select-tx params lo hi +tx-kinds+
             [:= :tx-kind ({:system 0 :programmable 1} kind)]))

(defn- select-sender [params lo hi]
  ;; The filter on `sender` is added by `select-tx` already.
  (select-tx params lo hi +tx-senders+ true))

(defn- select-recipient [{:as params :keys [recv]} lo hi]
  (select-tx params lo hi +tx-recipients+
             [:= :recipient (hex->bytes recv)]))

(defn- select-input [{:as params :keys [input]} lo hi]
  (select-tx params lo hi +tx-input-objects+
             [:= :object-id (hex->bytes input)]))

(defn- select-changed [{:as params :keys [changed]} lo hi]
  (select-tx params lo hi +tx-changed-objects+
             [:= :object-id (hex->bytes changed)]))

(defn- select-ids [{:as params :keys [ids]} lo hi]
  (select-tx params lo hi +tx-digests+
             [:in :tx-digest (map b58/decode ids)]))

(defn hybrid-tx-filter
  [compound-filter
   & {:as params :keys [cp-< cp-= cp-> kind sign inline pretty]}]
  (let [additional
        #{:sign :kind
          :cp-< :cp-= :cp->
          :after :before :scan-limit
          :inline :pretty}

        just
        (fn [& ks] (and (every? (partial contains? params) ks)
                        (every? (partial contains? (conj additional :kind))
                                (keys (apply dissoc params ks)))
                        (or (not kind) sign (= '(:kind) ks))))

        lo-cte (some->> (lower-bound params) (vector :tx-lo))
        lo (some->> lo-cte first (hash-map :select :lo :from))

        hi-cte (some->> (upper-bound lo params) (vector :tx-hi))
        hi (some->> hi-cte first (hash-map :select :hi :from))

        ctes (cond-> [] lo-cte (conj lo-cte) hi-cte (conj hi-cte))

        format
        #(sql/format % :inline inline :pretty pretty)

        bounded
        #(cond-> %
           (not (empty? ctes))
           (assoc :with ctes)

           :always (assoc :order-by [[:tx-sequence-number :asc]])
           :always (assoc :limit 52)
           :always (format))]

    (cond (not (consistent? params))
          ;; A query that will always be empty
          (format {:select [:tx-sequence-number]
                   :from (keyword +transactions+)
                   :where false})

          (just :pkg)
          (bounded (select-pkg params lo hi))

          (just :pkg :mod)
          (bounded (select-mod params lo hi))

          (just :pkg :mod :fun)
          (bounded (select-fun params lo hi))

          ;; TODO: We can save a little space by only tracking the programmable
          ;; transactions, and using a tx-senders query with sender set to `0x0`
          ;; to detect system transactions.
          (and (just :kind) (not sign))
          (bounded (select-kind params lo hi))

          ;; Failing the previous condition implies that if we are
          ;; a `(just :kind)` query, `sign` is set. And falling through the
          ;; initial inconsistency test implies that signer is consistent with
          ;; kind. In this case, the `kind` filter is subsumed by the `sign`
          ;; filter.
          (or (just :kind) (just :sign))
          (bounded (select-sender params lo hi))

          (just :recv)
          (bounded (select-recipient params lo hi))

          (just :input)
          (bounded (select-input params lo hi))

          (just :changed)
          (bounded (select-changed params lo hi))

          (just :ids)
          (bounded (select-ids params lo hi))

          ;; At this point, if the filter isn't compound, we know that it only
          ;; imposes bounds on the transaction sequence numbers to be fetched.
          ;; This means we can avoid querying the DB for the exact set of
          ;; sequence numbers to fetch, but we may still need to issue queries
          ;; to translate checkpoint bounds into transaction bounds.

          ;; The first check in this `cond` tested for consistency between
          ;; checkpoint bounds. This means that if there is a `cp-=` bound, we
          ;; can ignore other bounds.
          (just :cp-=)
          (format (from-cp-tx cp-=
                              :min-tx-sequence-number
                              :max-tx-sequence-number))

          (just)
          (format {:select
                   (cond-> []
                     cp-< (conj (from-cp-tx cp-< :min-tx-sequence-number))
                     cp-> (conj (from-cp-tx cp-> :max-tx-sequence-number)))})

          :else
          (bounded (compound-filter params lo hi)))))

(defn compound-norm
  "Compound filters implemented using joins over normalized tables.

  Intended for use as the first parameter to `hybrid-tx-filter`."
  [{:as params
    :keys [pkg mod fun    ;; function
           kind           ;; transaction kind
           sign recv      ;; addresses
           input changed  ;; objects
           ids            ;; transaction ids
           ]}
   lo hi]
  (let [sub-queries
        (cond-> []
          (and pkg mod fun)
          (conj (select-fun params lo hi))

          (and pkg mod (not fun))
          (conj (select-mod params lo hi))

          (and pkg (not mod) (not fun))
          (conj (select-pkg params lo hi))

          (and kind (not sign))
          (conj (select-kind params lo hi))

          ;; No explicit case for `sign` because sender filters are incorporated
          ;; into each select. Assertion below guarantees at least one select
          ;; used.

          ids     (conj (select-ids params lo hi))
          recv    (conj (select-recipient params lo hi))
          input   (conj (select-input params lo hi))
          changed (conj (select-changed params lo hi)))

        aliased (fn [q] [q (:from q)])]
    (assert (consistent? params))
    (assert (< 1 (count sub-queries)))
    {:select [:tx-sequence-number]
     :from [(aliased (first sub-queries))]
     :join (->> (rest sub-queries)
                (mapcat (fn [q] [(aliased q) [:using :tx-sequence-number]]))
                (into []))}))

(defn compound-denorm
  "Compound filters implemented by scanning over a denormalized table.

  `denorm` is the name of the table to be scanned, and `bounded?`
  controls whether we try bound the scan using min/max transactions
  from normalized tables.

  Intended for use as the first parameter to `hybrid-tx-filter` (when
  partially applied to its first two arguments)"
  [denorm bounded?
   {:as params
    :keys [pkg mod fun    ;; function
           kind           ;; transaction kind
           cp-< cp-= cp-> ;; checkpoint
           sign recv      ;; addresses
           input changed  ;; objects
           ids            ;; transaction ids
           after before   ;; pagination
           ]}
   lo hi]
  (let [<text (fn [val col] [<at [:array [[:cast val :text]]] col])
        <byte (fn [val col] [<at [:array [(hex->bytes val)]] col])

        aggregated
        (fn [agg query]
          (assoc query :select [[[agg :tx-sequence-number]]]))

        bounds
        (fn [cmp agg cursor]
          (cond-> [cmp]
            cursor (conj cursor)

            (and bounded? pkg mod fun)
            (conj (aggregated agg (select-fun params lo hi)))

            (and bounded? pkg mod (not fun))
            (conj (aggregated agg (select-mod params lo hi)))

            (and bounded? pkg (not mod) (not fun))
            (conj (aggregated agg (select-pkg params lo hi)))

            (and bounded? kind (not sign))
            (conj (aggregated agg (select-kind params lo hi)))

            (and bounded? sign)
            (conj (aggregated agg (select-sender params lo hi)))

            (and bounded? recv)
            (conj (aggregated agg (select-recipient params lo hi)))

            (and bounded? input)
            (conj (aggregated agg (select-input params lo hi)))

            (and bounded? changed)
            (conj (aggregated agg (select-changed params lo hi)))))

        tx-lo (bounds :greatest :min after)
        tx-hi (bounds :least    :max before)]
    (assert (consistent? params))
    (cond->
        {:select [:tx-sequence-number]
         :from [(keyword denorm)]

         :where
         (cond-> [:and]
           (and pkg mod fun)
           (conj (<text (str (hex/normalize pkg) "::" mod "::" fun) :functions))

           (and pkg mod (not fun))
           (conj (<text (str (hex/normalize pkg) "::" mod) :modules))

           (and pkg (not mod) (not fun))
           (conj (<byte pkg :packages))

           (and (not sign) (= kind :system))
           (conj [:= :sender sys-addr:bytes])

           (and (not sign) (= kind :programmable))
           (conj [:not= :sender sys-addr:bytes])

           sign (conj [:= :sender (hex->bytes sign)])
           recv (conj (<byte recv :recipients))

           input   (conj (<byte input :inputs))
           changed (conj (<byte changed :changed))

           cp-=
           (conj [:between :tx-sequence-number
                  (from-cp-tx cp-= :min-tx-sequence-number)
                  (from-cp-tx cp-= :max-tx-sequence-number)])

           (and (not cp-=) cp-<)
           (conj [:< :tx-sequence-number
                  (from-cp-tx cp-< :min-tx-sequence-number)])

           (and (not cp-=) cp->)
           (conj [:> :tx-sequence-number
                  (from-cp-tx cp-> :max-tx-sequence-number)])

           (< 1 (count tx-lo)) (conj [:>= :tx-sequence-number tx-lo])
           (< 1 (count tx-hi)) (conj [:<= :tx-sequence-number tx-hi]))}

      ids
      (assoc :join [[{:select [:tx-sequence-number]
                      :from [(keyword +tx-digests+)]
                      :where [:in :tx-digest (map b58/decode ids)]}
                     (keyword +tx-digests+)]
                    [:using :tx-sequence-number]]))))

;; Entrypoints ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def norm-filter
  (partial hybrid-tx-filter compound-norm))

(def scan-filter
  (partial hybrid-tx-filter (partial compound-denorm +tx-filters+ false)))

(def gin-filter
  (partial hybrid-tx-filter (partial compound-denorm +tx-filters-gin+ false)))

(def bound-scan-filter
  (partial hybrid-tx-filter (partial compound-denorm +tx-filters+ true)))

(def bound-gin-filter
  (partial hybrid-tx-filter (partial compound-denorm +tx-filters-gin+ true)))
