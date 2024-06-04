(ns combinations)

;; Helper functions to generate and combine lazy sequences of values.

(defn- lazy? [x] (instance? clojure.lang.LazySeq x))

(defn <-
  "Takes a value, `val`, containing lazy sequences and returns a lazy
  sequence of values, of the same shape as `val`.

  Looks recursively inside maps, vectors and lists for lazy sequences."
  [val]
  (letfn [(into-stream [val]
            (cond
              (map? val)
              (loop [entries (for [[k v] val] [k (into-stream v)])
                     output  (lazy-seq '({}))]
                (if-let [[k vs] (first entries)]
                  (recur (rest entries)
                         (for [o output v vs]
                           (if (= ::skip v) o (assoc o k v))))
                  output))

              (vector? val)
              (loop [entries (for [v val] (into-stream v))
                     output  (lazy-seq '([]))]
                (if-let [vs (first entries)]
                  (recur (rest entries)
                         (for [o output v vs]
                           (if (= ::skip v) o (conj o v))))
                  output))

              (list? val)
              (loop [entries (for [v (reverse val)] (into-stream v))
                     output  (lazy-seq '(()))]
                (if-let [vs (first entries)]
                  (recur (rest entries)
                         (for [o output v vs]
                           (if (= ::skip v) v (cons v o))))
                  output))

              (lazy? val)
              val

              :else
              (lazy-seq (list val))))]
    (into-stream val)))

(defn &&
  "Cartesian product over lazy sequences of maps.

  Each value in the resulting stream comes from `merge`-ing a map from
  each input stream."
  [& streams]
  (loop [parts  (map <- streams)
         output (lazy-seq '({}))]
    (if-let [part (first parts)]
      (recur (rest parts)
             (for [o output p part]
               (if (= ::skip p) o (merge o p))))
      output)))

(defn ||
  "Choice operator over lazy sequences."
  [& streams]
  (mapcat <- streams))

(defn |?
  "Like `||`, but inserting a sentinel value to represent skipping the
  introduction of a value from the choices, from the perspective of
  other combinators."
  [& streams]
  (->> (mapcat <- streams)
       (filter #(and (not= ::skip %)
                     (not (and (seq? %)
                               (empty? %)))))
       (cons ::skip)
       (lazy-seq)))

(defn *?
  "Kleene star operator for lazy sequences.

  Accepts the description of a lazy sequence -- `stream` -- and
  returns a new lazy sequence, of all subsets of choices from
  `stream` (a lazy sequence of sets), including the empty set."
  [stream]
  (loop [opts   (<- stream)
         output (lazy-seq '(#{}))]
    (if-let [opt (first opts)]
      (recur (rest opts)
             (if (= ::skip opt)
               output
               (concat output (map #(conj % opt) output))))
      output)))
