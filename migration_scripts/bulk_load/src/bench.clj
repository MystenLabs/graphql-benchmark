(ns bench
  (:require [combinations :refer [*?]]
            [db]
            [pool :refer [->Pool worker]]
            [next.jdbc :as jdbc])
  (:refer-clojure :exclude [run!]))

(defn run!
  "Gather plan times and execution times for various `inputs` to
  `benchmark`.

  The results are gathered in the signals map returned by this
  function, under the `:results`."
  [benchmark db inputs logger timeout]
  (->Pool :name   "run-benchmark"
          :logger  logger
          :workers 50

          :pending inputs

          :impl
          (worker input
            (->> (apply concat input)
                 (apply benchmark)
                 (db/explain-analyze-json! db timeout)))

          :finalize
          (fn [{:as task :keys [status]} signals]
            (when (or (= :success status) (= :timeout status))
              (swap! signals update :results conj task) nil))))

(defn success-rates
  "Given a sequence of benchmark results, returns the rate at which
  queries timeout.

  Returns a map from sets of keys in benchmark results to their
  success rate (proportion of queries including these keys that did
  not timeout)."
  [results & {:keys [include-empty?]}]
  (let [success (atom {})
        timeout (atom {})
        or-zero  #(fn [x] (% (or x 0)))
        success! #(swap! success update % (or-zero inc))
        timeout! #(swap! timeout update % (or-zero dec))]
    (doseq [result results
            :when (or include-empty?
                      (= :timeout (:status result))
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
