(ns bench
  (:require [combinations :refer [*?]]
            [db]
            [pool :refer [->Pool signal-swap!]]
            [next.jdbc :as jdbc])
  (:refer-clojure :exclude [run!]))

(defn signals
  "Signals produced by running benchmarks.

  Benchmarks split inputs into two categories: success and failure.
  Failed benchmarks are ones that produced some kind of unexpected
  error. All other benchmarks are considered successful, including
  benchmarks that timed out."
  [] (pool/signals :success [] :failure []))

(defn run!
  "Gather plan times and execution times for various `inputs` to
  `benchmark`.

  `signals` is expected to be an `atom` containing a map with keys
  `:success` and `:failure`."
  [benchmark db inputs logger timeout signals]
  (->Pool :name   "run-benchmark"
          :logger  logger
          :workers 50

          :pending inputs

          :impl
          (db/worker input
            (->> (apply concat input)
                 (apply benchmark)
                 (db/explain-analyze-json! db timeout)))

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
