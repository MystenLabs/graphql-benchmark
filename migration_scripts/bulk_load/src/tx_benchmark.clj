(ns tx-benchmark
  (:require [combinations :refer [&& || |? <- *?]]
            [db]
            [pool :refer [->Pool signals signal-swap!]]
            [transactions :as tx
             :refer [+transactions+ +tx-calls+ +tx-senders+ +tx-recipients+
                     +tx-input-objects+ +tx-changed-objects+]]
            [alphabase.base58 :as b58]
            [honey.sql :as sql]
            [next.jdbc :as jdbc]
            [clojure.string :as s]))

(defn hex->bytes
  "Convert a hex literal string into a length 32 byte array.

  The hex literal may have trailing zeroes removed, and can be in a
  normal literal form (starting with 0x) or in a postgres form
  starting with \\x."
  [hex]
  (assert (or (s/starts-with? hex "0x")
              (s/starts-with? hex "\\x"))
          "Hex literals must start with '0x'")
  (as-> hex %
      (subs % 2)
      (format "%64s" %)
      (s/replace % \space \0)
      (partition 2 %)
      (map #(-> % s/join (Integer/parseInt 16)) %)
      (byte-array %)))

(defn tx-filter
  [& {:keys [pkg mod fun    ;; function
             kind           ;; transaction kind
             cp-< cp-= cp-> ;; checkpoint
             sign recv addr ;; addresses
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

           kind (conj [:= :transaction-kind ({:programmable 0 :system 1} kind)])
           cp-< (conj [:> cp-< :checkpoint-sequence-number])
           cp-= (conj [:= cp-= :checkpoint-sequence-number])
           cp-> (conj [:< cp-> :checkpoint-sequence-number])

           sign (conj (tx-in +tx-senders+    [:= :sender    (hex->bytes sign)]))
           recv (conj (tx-in +tx-recipients+ [:= :recipient (hex->bytes recv)]))
           addr
           (conj [:or (tx-in +tx-senders+    [:= :sender    (hex->bytes addr)])
                      (tx-in +tx-recipients+ [:= :recipient (hex->bytes addr)])])

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

(def inputs
  "Lazy sequence containing various inputs to `tx-filter`."
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

      (|? {:after  {:tx 746619070 :cp 10427600}})
      (|? {:before {:tx 746625335 :cp 10428013}})))

(defn benchmark-signals []
  (signals :success [] :failure []))

(defn run-benchmark!
  "Gather plan times and execution times for various `inputs` to
  `transactions-filter`.

  `signals` is expected to be an `atom` containing a map with keys
  `:success` and `:failure`."
  [db inputs logger timeout signals]
  (->Pool :name   "run-benchmark"
          :logger  logger
          :workers 50

          :pending inputs

          :impl
          (db/worker input
            (->> (apply concat input)
                 (apply transactions-filter)
                 (db/explain-analyze! db timeout)))

          :finalize
          (fn [{:as task :keys [status]}]
            (case status
              (:success :timeout)
              (do (signal-swap! signals :success conj task) nil)
              :error
              (do (signal-swap! signals :failure conj task) nil)))))

(defn success-rates
  "Given a sequence of benchmark results, returns the rate at which
  queries timeout.

  Returns a map from sets of keys in benchmark results to their
  success rate (proportion of queries including these keys that did
  not timeout)."
  [results]
  (let [success (atom {})
        timeout (atom {})
        or-zero  #(fn [x] (% (or x 0)))
        success! #(swap! success update % (or-zero inc))
        timeout! #(swap! timeout update % (or-zero dec))]
    (doseq [result results
            :when (or (= :timeout (:status result))
                      (pos? (:actual-rows result)))
            keys (as-> result % (keys %) (into #{} %)
                   (disj % :status :planning-time :execution-time :actual-rows)
                   (*? (lazy-seq %)))]
      (case (:status result)
        :success (success! keys)
        :timeout (timeout! keys)))
    (-> (merge-with
          (fn [s t] (/ (double s) (- s t)))
          @success @timeout)
        (update-vals
         (fn [rate]
           (cond
             (double? rate) rate
             (neg? rate) 0.0
             (pos? rate) 1.0))))))
