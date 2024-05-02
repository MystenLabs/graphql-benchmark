(ns logger
  (:require [clojure.core.async :as async
             :refer [go thread >! >!! <! <!! chan]])
  (:import [java.util Date]))

(deftype Logger [input close])

(defn close!!
  "Close the logger and block this thread until the logger confirms it
  has closed."
  [logger]
  (async/close! (.input logger))
  (<!! (.close logger)))

(defn log!
  "Send a message to the logger in a new go block."
  [logger & msg]
  (go (>! (.input logger)
          (apply format msg))))

(defn ->Logger
  "Create a new logging thread that write logs to `writer` (defaults to
  `stderr`).

  The logger starts a new thread that sequentialises log messages to
  the writer. The logger can be closed by calling `close!` or
  `close!!` and messages are sent using `log!` or `log!!`. Single and
  double bang variants have the same meaning as other `core.async`
  functions (parking vs blocking)."

  ([] (->Logger *err*))

  ([writer]
   (let [ch (chan)
         th (thread
              (binding [*out* writer]
                (loop []
                  (when-let [msg (<!! ch)]
                    (println (Date.) msg)
                    (recur)))))]

     (Logger. ch th))))
