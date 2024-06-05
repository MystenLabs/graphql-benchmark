(ns hex
  (:require [clojure.string :as s]))

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

(defn bytes->hex
  "Convert a byte array into a hex string."
  [hex]
  (assert (= 32 (count hex))
          "Byte arrays must be 32 bytes long")
  (->> (map #(format "%02x" %) hex)
       (s/join "")
       (str "0x")))

(defn normalize
  "Normalize an address as a hex string into a standard form.

  Starting with 0x, and padded out to all trailing zeroes."
  [hex] (-> hex hex->bytes bytes->hex))
