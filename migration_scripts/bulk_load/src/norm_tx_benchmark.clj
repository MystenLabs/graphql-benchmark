(ns norm-tx-benchmark
  (:require [db]
            [hex :refer [hex->bytes]]
            [norm-tx
             :refer [+transactions+
                     +tx-calls-pkg+ +tx-calls-mod+ +tx-calls-fun+
                     +tx-senders+ +tx-recipients+
                     +tx-input-objects+ +tx-changed-objects+
                     +tx-digests+ +cp-tx+
                     +tx-system+]]
            [alphabase.base58 :as b58]
            [honey.sql :as sql]))

;; # Normalized Transactions Benchmark
;;
;; Testing out similar queries as in `tx-benchmark`, but on the
;; normalized table.
;;
;; The main differences are:
;;
;; - Cursors refer to just the transaction sequence number, and not
;;   also the checkpoint sequence number.
;;
;; - Each filter applies to a separate table, the results are joined
;;   together, and finally the resulting ordered set of transaction
;;   sequence numbers is used to probe into the `transactions` table.
;;
;; - The final join (against `transactions`) is a `LEFT JOIN` to force
;;   all previous inner joins to be done first. This is to take
;;   advantage of the fact that the previous joins should limit the
;;   final result.
;;
;; This approach *did not* work as hoped -- in general it yielded
;; worse performance than the existing schema (see below):
;;
;;     [#{:pkg} 0.18803418803418803]
;;     [#{:mod} 0.1891891891891892]
;;     [#{:fun} 0.21739130434782608]
;;     [#{:kind} 0.29770992366412213]
;;     [#{:cp-<} 0.37988826815642457]
;;     [#{:changed} 0.38596491228070173]
;;     [#{:input} 0.42857142857142855]
;;     [#{:before} 0.45320197044334976]
;;     [#{:cp->} 0.46875]
;;     [#{:after} 0.5526315789473685]
;;     [#{:ids} 0.7857142857142857]
;;     [#{:cp-=} 1.0]
;;
;;       Fig 1. Success rates for queries that include at least one
;;       filter key.
;;
;;
;;     [#{:changed :pkg} 0.0]
;;     [#{:mod :changed} 0.0]
;;     [#{:fun :changed} 0.0]
;;     [#{:kind :pkg} 0.04878048780487805]
;;     [#{:mod :kind} 0.05555555555555555]
;;     [#{:fun :kind} 0.05714285714285714]
;;     [#{:fun :input} 0.07692307692307693]
;;     [#{:mod :input} 0.09523809523809523]
;;
;;       Fig 2. Worst success rates for queries with combinations of
;;       at least two filter keys.
;;
;; The issue seems to be in Postgres' implementation of Merge Join,
;; which does not have a way to skip over pages of one relation based
;; on the next row to merge in another relation (like leapfrog trie
;; join): Sets of queries with low intersectionality, will result
;; in (effectively) whole table scans.

(defn norm-tx-filter
  [& {:keys [pkg mod fun    ;; function
             kind           ;; transaction kind
             cp-< cp-= cp-> ;; checkpoint
             sign recv      ;; addresses
             input changed  ;; objects
             ids            ;; transaction ids
             after before   ;; pagination

             ;; Configuration
             inline]}]
  (let [f-> #(keyword (str %1 "." (name %2)))

        tables
        (cond-> []
          (or pkg mod fun)
          (conj (cond
                  (and pkg mod fun) +tx-calls-fun+
                  (and pkg mod)     +tx-calls-mod+
                  :else             +tx-calls-pkg+))

          sign (conj +tx-senders+)
          recv (conj +tx-recipients+)

          input   (conj +tx-input-objects+)
          changed (conj +tx-changed-objects+)

          ids   (conj +tx-digests+)
          :true (conj +transactions+))

        sources
        {:from    (keyword (first tables))
         :join-by (reduce (fn [joins table]
                            (conj joins
                                  (if (= +transactions+ table)
                                    :left-join :join)
                                  [(keyword table)
                                   [:using :tx-sequence-number]]))
                          [] (rest tables))}

        filters
        (cond-> [:and]
          (and pkg mod fun)
          (conj [:= (f-> +tx-calls-fun+ :package) (hex->bytes pkg)]
                [:= (f-> +tx-calls-fun+ :module) mod]
                [:= (f-> +tx-calls-fun+ :func) fun])

          (and pkg mod (not fun))
          (conj [:= (f-> +tx-calls-mod+ :package) (hex->bytes pkg)]
                [:= (f-> +tx-calls-mod+ :module) mod])

          (and pkg (not mod) (not fun))
          (conj [:= (f-> +tx-calls-pkg+ :package) (hex->bytes pkg)])

          sign (conj [:= (f-> +tx-senders+ :sender) (hex->bytes sign)])
          recv (conj [:= (f-> +tx-recipients+ :recipient) (hex->bytes recv)])

          input
          (conj [:= (f-> +tx-input-objects+ :object-id) (hex->bytes input)])

          changed
          (conj [:= (f-> +tx-changed-objects+ :object-id) (hex->bytes changed)])

          ids
          (conj [:in (f-> +tx-digests+ :tx-digest) (map b58/decode ids)])

          kind
          (conj [({:system :in :programmable :not-in} kind)
                 :tx-sequence-number
                 {:select [:tx-sequence-number]
                  :from   [(keyword +tx-system+)]}])

          cp-<
          (conj [:< :tx-sequence-number
                 {:select [:min-tx-sequence-number]
                  :from   [(keyword +cp-tx+)]
                  :where  [:= :checkpoint-sequence-number cp-<]}])

          cp-=
          (conj [:between :tx-sequence-number
                 {:select [:min-tx-sequence-number]
                  :from   [(keyword +cp-tx+)]
                  :where  [:= :checkpoint-sequence-number cp-=]}
                 {:select [:max-tx-sequence-number]
                  :from   [(keyword +cp-tx+)]
                  :where  [:= :checkpoint-sequence-number cp-=]}])

          cp->
          (conj [:> :tx-sequence-number
                 {:select [:max-tx-sequence-number]
                  :from   [(keyword +cp-tx+)]
                  :where  [:= :checkpoint-sequence-number cp->]}])

          after  (conj [:>= :tx-sequence-number after])
          before (conj [:<= :tx-sequence-number before]))]
    (-> {:select [(f-> +transactions+ :*)]
         :where filters
         :limit  52
         :order-by [[:tx-sequence-number :asc]]}
        (merge sources)
        (sql/format :inline inline))))
