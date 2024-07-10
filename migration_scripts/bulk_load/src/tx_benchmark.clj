(ns tx-benchmark
  (:require [db]
            [hex :refer [hex->bytes]]
            [transactions
             :refer [+transactions+
                     +tx-calls+
                     +tx-senders+ +tx-recipients+
                     +tx-input-objects+ +tx-changed-objects+]]
            [alphabase.base58 :as b58]
            [honey.sql :as sql]))

(defn tx-filter
  [& {:keys [pkg mod fun    ;; function
             kind           ;; transaction kind
             cp-< cp-= cp-> ;; checkpoint
             sign recv      ;; addresses
             input changed  ;; objects
             ids            ;; transaction ids
             after before   ;; pagination
             ]}]
  (let [tx-in
        (fn [side-table where]
          [:in :tx-sequence-number
           {:select [:tx-sequence-number]
            :from [(keyword side-table)]
            :where where}])]
    (-> {:select [:*]
         :from   [(keyword +transactions+)]
         :where
         (cond-> [:and]
           (or pkg mod fun)
           (conj (tx-in +tx-calls+
                        (cond-> [:and]
                          pkg (conj [:= :package (hex->bytes pkg)])
                          mod (conj [:= :module mod])
                          fun (conj [:= :func fun]))))

           kind (conj [:= :transaction-kind ({:system 0 :programmable 1} kind)])
           cp-< (conj [:> cp-< :checkpoint-sequence-number])
           cp-= (conj [:= cp-= :checkpoint-sequence-number])
           cp-> (conj [:< cp-> :checkpoint-sequence-number])

           sign (conj (tx-in +tx-senders+    [:= :sender    (hex->bytes sign)]))
           recv (conj (tx-in +tx-recipients+ [:= :recipient (hex->bytes recv)]))

           input
           (conj (tx-in +tx-input-objects+   [:= :object-id (hex->bytes input)]))
           changed
           (conj (tx-in +tx-changed-objects+ [:= :object-id (hex->bytes changed)]))

           ids
           (conj [:in :transaction-digest (map b58/decode ids)])

           after  (conj [:>= :tx-sequence-number         (:tx after)]
                        [:>= :checkpoint-sequence-number (:cp after)])
           before (conj [:<= :tx-sequence-number         (:tx before)]
                        [:<= :checkpoint-sequence-number (:cp before)]))
         :limit  52
         :order-by [[:tx-sequence-number :asc]]}
        (sql/format))))
