(ns pool
  (:require [logger :refer [->Logger] :as l]
            [clojure.core.async :as async
             :refer [<!! >!! alt!! alts!! chan mult tap thread]]))

(defn- queue
  ([] clojure.lang.PersistentQueue/EMPTY)
  ([coll] (reduce conj clojure.lang.PersistentQueue/EMPTY coll)))

(defn- tapped [in]
  (let [out (chan)]
    (tap in out) out))

(defn- ->Worker
  "Create a new worker thread.

  Workers processes batches of work sent on the `work` channel. Work
  is processed by passing it to the `impl` function along with a
  callback that abstracts over responding to the supervisor along with
  `reply` channel.

  The worker will stop when it receives a message on the `kill`
  multi-channel or that channel closes."
  [name logger impl work kill reply]
  (let [kill (tapped kill)]
    (thread
      (l/log! logger "[Worker %s] Starting..." name)
      (loop []
        (alt!!
          kill (l/log! logger "[Worker %s] ...shutting down." name)
          work ([batch]
                (try (impl batch #(>!! reply %))
                     (catch Throwable t
                       (l/log! logger
                               "[Worker %s] Uncaught error on %s: %s"
                               name batch t)))
                (recur))))
      ::worker-done)))

(defn- ->Supervisor
  "Create a new supervisor thread.

  The supervisor is responsible for managing a queue of work that it
  makes available over the `work` channel. It expects workers to reply
  back on th `reply` channel when they have finished processing a
  batch.

  The supervisor's work-queue starts off populated by `pending`. Once
  it receives a `reply` it calls `finalize` on it to potentially
  derive more work for the queue from the results in the `reply`.
  `finalize` can return a (possibly empty) sequence to append to the
  queue, or `false` to indicate an unrecoverable issue (implying the
  whole pool should wind down).

  The supervisor will stop when it receives a message on the `kill*`
  multi-channel or that channel closes. It will also close the
  `-kill` (input) channel when it wants to stop the rest of the pool.

  `signals` is an atom containing a dictionary of signals for the pool
  to communicate with the outside world. The Supervisor itself will
  track the number of in-flight requests and any failed jobs (replies
  from workers with a `:status` of `:error`), and passes the signal
  map to the `finalize` callback (as the second parameter)."
  [name logger finalize work -kill kill* reply signals]
  (let [kill* (tapped kill*)
        log!(fn [f & args]
              (apply l/log! logger (str "[Supervisor %s] " f) name args))
        finalize (or finalize (fn [_ _] nil))]
    (thread
      (try
        (log! "Starting...")
        (loop [shutting-down false]
          (when (and (-> @signals ::pending empty?)
                     (-> @signals ::in-flight zero?))
            (log! "No more work.")
            (async/close! -kill))

          (let [next (-> @signals ::pending peek)
                [v port] (alts!! (cond-> [reply]
                                   (not shutting-down) (conj kill*)
                                   next (conj [work next])))]
            (cond
              (= port kill*)
              (do (log! "...shutting down.")
                  (swap! signals
                         #(-> % (dissoc ::pending)
                              (assoc ::cancelled (::pending %))))
                  (recur true))

              (= port work)
              (do (log! "-> %s" next)
                  (swap! signals
                         #(-> % (update ::in-flight inc)
                              (update ::pending pop)))
                  (recur shutting-down))

              (= port reply)
              (do (log! "<- %s" v)
                  (swap! signals
                         #(cond-> %
                            :always (update ::in-flight dec)
                            :always (update ::landed inc)

                            (= (:status v) :error)
                            (update ::failed conj v)))
                  (let [add (finalize v signals)]
                    (if (false? add)
                      (do (log! "Unrecoverable error!")
                          (async/close! -kill))
                      (do (when (not (empty? add)) (log! "++ %s" add))
                          (swap! signals update ::pending into add)
                          (recur shutting-down))))))))
        ::supervisor-done
        (catch Throwable t
          (log! "Uncaught error: %s" t))))))

(defn ->Pool
  "Create a new pool of `workers` many workers and a supervisor to manage them.

  `pending` is a collection of work items that the supervisor will
  distribute among workers, `impl` is a function for accepting a batch
  and completing the work, and `finalize` takes the response from
  `impl` and updating the state of the queue with it.

  `impl` accepts two parameters, the description of the work, and a
  callback to reply on.

  `finalize` accepts one parameter, the response from `impl`, and
  returns either a collection of follow-up work, or `false` if there
  was an unrecoverable error.

  Returns an atom containing a dictionary of signals that the pool
  uses to communicate with the outside world. The signals indicate the
  number of in flight tasks, any failed tasks, and any other signals
  that the `finalize` callback might add."
  [& {:keys [name logger pending impl finalize workers]}]
  (let* [-kill (chan) kill* (mult -kill)
         work  (chan) reply (chan)

         signals
         (atom {::in-flight 0
                ::landed 0
                ::pending (queue pending)
                ::failed []
                ::kill -kill})

         supervisor
         (->Supervisor name logger finalize
                       work -kill kill* reply
                       signals)

         threads
         (->> (range workers)
              (map #(->Worker % logger impl work kill* reply))
              (into [supervisor]))]
    (swap! signals assoc ::join (async/merge threads))
    signals))

(defmacro worker
  "Create a worker function for a pool.

  The worker is passed a description of the unit of work (expected to
  be a map) which is bound to `param`, and then its `body` is
  evaluated. The return value of `body` is merged with the description
  of the work and sent back to the supervisor, along with a `:status`
  of `:success`.

  The returned worker detects errors (including timeouts), returning
  the description of work with a `:status` of `:error`. Errors are
  additionally annotated with the `:error` itself."
  [param & body]
  `(fn [param# reply#]
     (try (->> (let [~param param#] ~@body)
               (merge param# {:status :success})
               (reply#))
          (catch Throwable t#
            (reply# (assoc param# :status :error :error t#))))))

(defn kill!
  "Kill a worker pool, using the `::kill` channel in its signal map."
  [{::keys [kill]}] (async/close! kill))

(defn fail-count
  "Count the number of failed tasks in a pool's signal map."
  [{::keys [failed]}] (count failed))

(defn progress
  "Estimate the progress of a pool based on the stats in its signal map.

  Note that this is a heuristic and is only accurate if one unit of
  work does not spawn further units of work (which may in general be
  the case)."
  [{::keys [pending in-flight landed]}]
  (let [pending (count pending)
        total (+ pending in-flight landed)]
    {:pending pending :in-flight in-flight :landed landed :total total
     :percent  (/ (double landed) total 1/100)}))

(defn retry
  "Gather tasks that should be retried from a signal map."
  [{::keys [failed cancelled]}]
  (concat failed cancelled))
