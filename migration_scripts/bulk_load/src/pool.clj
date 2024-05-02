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
  back on th `reply` channel when they have finished processing a batch.

  The supervisor's work-queue starts off populated by `pending`. Once
  it receives a `reply` it calls `finalize` on it to potentially derive
  more work for the queue from the results in the `reply`. `finalize`
  can return a (possibly empty) sequence to append to the queue, or
  `false` to indicate an unrecoverable issue (implying the whole pool
  should wind down).

  The supervisor will stop when it receives a message on the `kill*`
  multi-channel or that channel closes. It will also close the
  `-kill` (input) channel when it wants to stop the rest of the pool."
  [name logger pending finalize work -kill kill* reply]
  (let [kill* (tapped kill*)]
    (thread
      (l/log! logger "[Supervisor %s] Starting..." name)
      (loop [q (queue pending) in-flight 0]
        (when (and (empty? q) (zero? in-flight))
          (l/log! logger "[Supervisor %s] No more work." name)
          (async/close! -kill))

        (let [next (peek q)
              [v port] (alts!! (cond-> [kill* reply]
                                 next (conj [work next])))]
          (cond
            (= port kill*)
            (l/log! logger "[Supervisor %s] ...shutting down." name)

            (= port work)
            (let [batch (peek q)]
              (l/log! logger "[Supervisor %s] -> %s" name batch)
              (recur (pop q) (inc in-flight)))

            (= port reply)
            (let [add (finalize v)]
              (l/log! logger "[Supervisor %s] <- %s" name v)
              (cond
                (false? add)
                (do (l/log! logger "[Supervisor %s] Unrecoverable error!" name)
                    (async/close! -kill))

                (empty? add)
                (recur q (dec in-flight))

                :else
                (do (l/log! logger "[Supervisor %s] ++ %s" name add)
                    (recur (into q add) (dec in-flight))))))))
      ::supervisor-done)))

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

  Returns a `kill` channel that can be written to or closed to shut
  down the whole pool, and a `join` channel that can be waited on
  until closure to know that the pool has closed."
  [& {:keys [name logger pending impl finalize workers]}]
  (let* [-kill (chan) kill* (mult -kill)
         work  (chan) reply (chan)

         supervisor
         (->Supervisor name logger pending finalize work -kill kill* reply)

         threads
         (->> (range workers)
              (map #(->Worker % logger impl work kill* reply))
              (into [supervisor]))]
    [-kill (async/merge threads)]))
