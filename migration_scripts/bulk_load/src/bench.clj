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

(defn simulate
  "Reinterpret `results` simulating various properties.

  - Setting `timeouts` simulates a lower timeout than the benchmark
    was actually run on (in seconds).

  - Setting `no-cache?` simulates all block fetches as cache misses,
    and updates execution time accordingly.

  The result is another stream of results with its `:status` and
  `:db/exec` fields updated according to the simulation."
  [results & {:keys [no-cache? timeout]}]
  (let [io-time (->> results (map :db/read) (reduce #(+ %1 (or %2 0)) 0))
        misses  (->> results (map :db/miss) (reduce #(+ %1 (or %2 0)) 0))
        avg-io  (/ (double io-time) misses)]
    (cond->> results
      no-cache?
      (map (fn [{:as r :db/keys [exec read miss hits]}]
             (cond-> r
               (= :success (:status r))
               (assoc :db/exec (-> (+ miss hits) (* avg-io)
                                   (+ exec) (- read))))))
      timeout
      (map (fn [{:as r :db/keys [exec plan]}]
             (cond-> r
               (or (= :timeout (:status r))
                   (> (+ exec plan) (* timeout 1000)))
               (assoc :status :timeout)))))))

(defn success-rates
  "Given a sequence of benchmark results, returns the rate at which
  queries timeout.

  Returns a map from sets of keys in benchmark results to their
  success rate (proportion of queries including these keys that did
  not timeout).

  `include-empty?` decides whether to include queries that succeeded
  but returned no results.

  `no-cache?` decides how the time for a benchmark run is calculated.
  If set to `true`, time is estimated based on the number of blocks
  read (from cache or otherwise) times the average IO time per block
  -- as if all the blocks were read from disk, and not cache.

  `timeout` is the time to consider as a timeout, in seconds. If it is
  omitted, only queries that actually timed out when run are
  considered timed out."
  [results & {:keys [include-empty? no-cache? timeout]}]
  (let [results (simulate results :no-cache? no-cache? :timeout timeout)

        success (atom {})
        timeout (atom {})
        success! #(swap! success update % (fnil inc 0))
        timeout! #(swap! timeout update % (fnil dec 0))]
    (doseq [result results
            :when (or include-empty?
                      (= :timeout (:status result))
                      (pos? (:db/rows result)))
            keys (as-> result % (keys %) (into #{} %)
                       (disj % :status
                             :db/exec :db/plan :db/read
                             :db/rows :db/miss :db/hits)
                   (*? (lazy-seq %)))]
      (case (:status result)
        :timeout (timeout! keys)
        :success (success! keys)))
    (-> (merge-with
          (fn [s t] (/ (double s) (- s t)))
          @success @timeout)
        (update-vals
         (fn [rate]
           (cond
             (double? rate) rate
             (neg? rate) 0.0
             (pos? rate) 1.0))))))
