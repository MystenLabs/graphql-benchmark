(ns gin-1-tx-benchmark
  (:require [combinations :refer [&& || |?]]
            [db]
            [hex :refer [hex->bytes]]
            [gin-1-tx :refer [+transactions+ +tx-digests+ +cp-tx+]]
            [alphabase.base58 :as b58]
            [honey.sql :as sql]
            [honey.sql.pg-ops :refer [<at]]))

;; # 1-GIN Transactions Benchmark
;;
;; Testing out similar queries as in `tx-benchmark` and `norm-tx-benchmark` but
;; on the 1-GIN schema.
;;
;; The main differences are:
;;
;; - Cursors refer to just the transaction sequence number, as in `norm-tx`.
;;
;; - All filters are applied to the `transactions` table.

(defn gin-1-tx-filter
  [& {:keys [pkg mod fun    ;; function
             kind           ;; transaction kind
             cp-< cp-= cp-> ;; checkpoint
             sign recv addr ;; addresses
             input changed  ;; objects
             ids            ;; transaction ids
             after before   ;; pagination

             ;; Configuration
             inline]}]
  (let [f->  #(keyword (str %1 "." (name %2)))
        <text (fn [val col] [<at [:array [[:cast val :text]]] col])
        <byte (fn [val col] [<at [:array [(hex->bytes val)]] col])

        joins
        (when ids {:join [(keyword +tx-digests+) [:using :tx-sequence-number]]})

        filters
        (cond-> [:and]
          (and pkg mod fun)
          (conj (<text (str (hex/normalize pkg) "::" mod "::" fun) :functions))

          (and pkg mod (not fun))
          (conj (<text (str (hex/normalize pkg) "::" mod)) :modules)

          (and pkg (not mod) (not fun))
          (conj [<at [:array [(hex->bytes pkg)]] :packages])

          sign (conj (<byte sign :senders))
          recv (conj (<byte recv :recipients))
          addr (conj [:or (<byte addr :senders) (<byte addr :recipients)])

          input   (conj (<byte input :inputs))
          changed (conj (<byte changed :changed))

          ids
          (conj [:in (f-> +tx-digests+ :tx-digest) (map b58/decode ids)])

          kind (conj [:= :transaction-kind ({:programmable 0 :system 1} kind)])

          cp-< (conj [:< :tx-sequence-number
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
    (-> {:select [:*]
         :from (keyword +transactions+)
         :where filters
         :limit 52
         :order-by [[:tx-sequence-number :asc]]}
        (merge joins)
        (sql/format :inline inline))))

(def inputs
  "Lazy sequence containing various inputs to `norm-tx-filter`."
  (&& {:kind (|? :system :programmable)}

      (|? (&& {:pkg "0x2"} (|? (&& {:mod "transfer"}
                                   {:fun (|? "public_transfer"
                                             "public_share_object")})
                               {:mod "package"
                                :fun "make_immutable"}))
          (&& {:pkg "0x225a5eb5c580cb6b6c44ffd60c4d79021e79c5a6cea7eb3e60962ee5f9bc6cb2"}
              (|? {:mod "game_8192"
                   :fun (|? "make_move") })))

      (|| {:cp-< 10428013}
          {:cp-= 10428013}
          {:cp-> 10427513
           :cp-< 10428013})

      (|? {:input   "0x6"}
          {:changed "0x6"})

      (|?
       ;; Arbitrary transactions at a variety of checkpoints
       {:ids ["B5FEom9XbGShf9LkqgC7UzhpEvVFVk6AakhXaFyfRWRf"
              "8GcwVK8cNqyM4CeY77Au2UfTsM6fZftSGjmfNxM8AN9"]}

       ;; Transactions that modify the clock
       {:ids ["FLqdHsKounJHXGsoT983JoZ4fuTF2UvSrVJJLDXRHqGe"
              "85uiG9US4T4ARCiWbFSeGCawryKejZ328rxwZfysutBk"]})

      (|? {:after  746619070})
      (|? {:before 746625335})))
