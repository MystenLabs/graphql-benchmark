(ns ev-benchmark
  (:require [alphabase.base58 :as b58]
            [clojure.string :as s]
            [db]
            [hex :refer [hex->bytes]]
            [honey.sql :as sql]
            [events :refer [+events+]]
            [transactions :refer [+transactions+ +tx-senders+]]))

(defn ev-filter
  [& {:keys [sign tx              ; transaction
             em-pkg em-mod        ; emitting module
             ev-pkg ev-mod ev-typ ; event type

             ;; Configuration
             inline pretty]
      [atx aev] :after            ; pagination
      [btx bev] :before}]         ; pagination
  (-> {:select [:*]
       :from   [(keyword +events+)]
       :where
       (cond-> [:and]
         sign
         (conj [:in :tx-sequence-number
                {:select [:tx-sequence-number]
                 :from   [(keyword +tx-senders+)]
                 :where  [:= :sender (hex->bytes sign)]}])

         tx
         (conj [:= :tx-sequence-number
                {:select [:tx-sequence-number]
                 :from   [(keyword +transactions+)]
                 :where  [:= :transaction-digest (b58/decode tx)]}])

         em-pkg (conj [:= :package (hex->bytes em-pkg)])
         em-mod (conj [:= :module  em-mod])

         ev-pkg (conj [:= :event-type-package (hex->bytes ev-pkg)])
         ev-mod (conj [:= :event-type-module  ev-mod])
         ev-typ (conj [:= :event-type-name (first (s/split ev-typ #"<" 2))])

         (and ev-typ (s/includes? ev-typ "<"))
         (conj [:= :event-type (str (hex/normalize ev-pkg)
                                    "::" ev-mod "::" ev-typ)])

         (and atx aev)
         (conj [:or
                [:> :tx-sequence-number atx]
                [:and
                 [:= :tx-sequence-number atx]
                 [:>= :event-sequence-number aev]]])

         (and btx bev)
         (conj [:or
                [:< :tx-sequence-number btx]
                [:and
                 [:= :tx-sequence-number btx]
                 [:<= :event-sequence-number bev]]]))
       :limit 52
       :order-by [[:tx-sequence-number :asc]
                  [:event-sequence-number :asc]]}
      (sql/format :inline inline :pretty pretty)))
