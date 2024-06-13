(ns tx-filters
  (:require [combinations :refer [&& || |?]]))

;; Transaction Filters ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Inputs to transaction filters for transactions between partitions 75 and 125
;; inclusive, which translates to:
;;
;;     TX 14,877,223 to 748,348,393 = 733,471,170
;;     CP  6,131,063 to  10,513,353 =   4,382,290 = ~50 days

;; Transaction Kind ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Of these transactions, 4,382,342 where system transactions (one per
;; checkpoint, plus one per epoch), and the remainder were programmable.

(def tx-kinds
  (|? :system :programmable))

;; Calls ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def tx-calls                                                      ;      # Txns
  (|? (&& {:pkg "0x2"}                                             ;   2,532,524
          (|? {:mod "coin"                                         ;   2,157,158
               :fun (|? "value"                                    ;     609,930
                        "zero")}                                   ;   1,435,831
              {:mod "package"                                      ;         336
               :fun (|? "make_immutable")}                         ;           1
              {:mod "transfer"                                     ;      15,151
               :fun (|? "public_transfer"                          ;         784
                        "public_share_object")}))                  ;      14,367
      (&& {:pkg "0x225a5eb5c580cb6b6c44ffd60c4d79021e79c5a6cea7eb3e60962ee5f9bc6cb2"}
                                                                   ; 663,509,462
          (|? {:mod "game_8192"                                    ; 663,376,010
               :fun (|? "make_move")}))))                          ; 661,606,447

(def tx-calls:digests
  [["BZTcGkucQgPyq1AH7QUyNesbo5tTgKUboZHpCzU1N3ma"
    "6ru1q1UebaWfCaHZBSpB6dtFYaRJtUaPRMyTdhJXxMtw"
    "FRpLM9EAPMbiceX7cJoBUEVn3sH765q673onWKQECdqP"]

   ["6HhJMU1UwNoAyLrzWwZUeb7LnJJGzHekLZz5oMuamnwa"
    "HbeiUUMnqJiG58jbRiQEPshcJuH9UXAKygPABY2ZviGX"
    "D9ZJ3pooLb6yBtYvSqzn3YinByYmNUBt4eozGUEAUA9Z"]

   ["6YUKYok7RVJuhfLpDZuW662rKsjM26F5x2Mnqxc47J9X"]

   ["6LyckdsQFW3vKuzakuekCnxbbHmHvjy2WXDvn8PhRC3U"
    "HH72Wz8uJzUSfHNgpUzHKYKipKXPFdHgVeCFaX2xwWMq"
    "3HXQsMBJReFFifRGFYVgQwXBWCT54Sv6mpvdviAunYHE"]

   ["DVTmnzJ7mfe9tzVMdfsbaKZPLqiSfTzixgmEQ44R8wmc"
    "4gUKrXZ662hzn8eH3wMz13KiNBDWv1pVM5yCNqegDkBR"
    "Aubn7tVmE4jmHtAZUs9gT9KRgvq1LVySA9nPJL1GFe9u"]

   ["Dfkw87d1wiWWTRXg477uYqWMmD9SjFVUVw48PaTdR3A7"
    "B2M6AQWkwWj4JZVbhk4ski3F8U66H5E5BwJKzuLDpzWH"
    "8rzHoKGQLbFqvrvLTyXNPW5DFpNXBNNV2f3gVwrSdc5Y"]])

;; Checkpoint and Cursor Bounds ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Test the effect of applying bounds on:
;;
;; - Individual checkpoints (sample of 10, evenly spaced)
;; - Daily checkpoints
;; - Weekly checkpoints
;; - Monthly checkpoints, sliding weekly window
;;
;; Also test the effects of applying the bounds to the query using checkpoints,
;; or using a cursor (which applies a direct transaction sequence number bound).

(def checkpoints:arbitrary
  (|? 6131063 6569292 7007521 7445750 7883979
      8322208 8760437 9198666 9636895 10075124))

(defn- checkpoint:range
  ([width]
   (checkpoint:range width width))
  ([width stride]
   (for [start (range 6131063 10513353 stride)
         :let [end (min 10513353 (+ start width))]]
     {:cp-> start :cp-< end})))

(def checkpoints:daily
  (checkpoint:range (* 60 60 24)))

(def checkpoints:weekly
  (checkpoint:range (* 60 60 24 7)))

(def checkpoints:monthly
  (checkpoint:range (* 60 60 24 30)
                    (* 60 60 24 7)))

(def checkpoints
  (|? {:cp-= checkpoints:arbitrary}
      checkpoints:daily
      checkpoints:weekly
      checkpoints:monthly))

;; Input Objects ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def tx-input-objects                                              ;      # Txns
  (|? "0x5"                                                        ;      24,751
      "0x6"                                                        ;  28,250,760
      "0x225a5eb5c580cb6b6c44ffd60c4d79021e79c5a6cea7eb3e60962ee5f9bc6cb2"))
                                                                   ; 663,509,463

(def tx-input-objects:digests
  [["aViJwMMGUeXyc4VhfRDScmKDR2HeqinYsSnTo9iWHcd"
    "pU3HyoXkCt3Ld2WACXXyi1SBsEGUxyJm2RMveMqM2Jy"
    "jevg8XXSSppgLJjPK4Dyog5a7joXqWRpD6oGN18XXYy"]

   ["F4AnHKUjw2zd11mWb461zZFA87QzDG4McmTbZQiomzTp"
    "BnsEVeZqaqXdRNqebtok4ktC4ZNtTVajPXtj8FNJKdrb"
    "4mXzNDWzmgw9jz9BpUnNhXQprHSu4ha6iqxNTTHgfPFb"]

   ["AAMshKyJQzx9QEsbVYt5TMtxWwKrk3DnGSVwFVACeNV"
    "CAxwahzfbKgGhUw2eGMZVNNS7Gtr7j6are8gMztJ5ef"
    "2BGfTSQVXDpA44qNguHHJDFqsb4Xyq6Q8mWBSb5Mo5ne"]])

;; Changed Objects ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def tx-changed-objects                                              ;    # Txns
  (|? "0x5"                                                          ;    24,249
      "0x6"))                                                        ; 4,382,291

(def tx-changed-objects:digests
  [["AEvPwzcbGdMYcfxvDbgS4sE2RdMbSSXZnv7YPjQT5apz"
    "E6TdgfqUvH5eQwR3YBRYeK7wzrfGiSXu9PimoJkLXMxb"
    "CmToo5RQ28C9rKth9ytafsnG8roqWP7r2LbmEo35RRpg"]

   ["CHLnmi2GFEHb5eVwzGehf2ga3SuPR8983BMxXc821Xq"
    "EzKDHAnArH9utB2P4tom3fFaDAMkdsLipyFW8asyw4Me"
    "FBLiYApLjKWxuit3XLe13fyBWysj9Cin7H2TSokzEwRf"]])

;; Senders ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def tx-senders                                                    ;      # Txns
  (|| "0xa697aec8505d62dd3b368cb50bfb668f148458cf2ff00e68a892189b982"
                                                                   ;           9
      "0x76fe7abaadcfe835f73bca4b581217e7c23baffb651b23fd987b07c3f6"
                                                                   ;          34
      "0xec98f469d70a8cfc4e80a286096158bf44274c8884c4945b13a43e2d" ;         433
      "0x640b779b330a44fb0047b2a3f462de579285b1846143141619d6c3fa3f414"
                                                                   ;       1,568
      "0x1034c8cef9b673767644328e7c1df02f6095861868850edc40fdef6379b8d"
                                                                   ;      29,481
      "0x6ac862e9f1336a09cd15d439d7f535f784b2e54d99826b14bb17aad235f64"
                                                                   ;     121,350
      "0x0"                                                        ;   4,382,342
      "0x2a212de6a9dfa3a69e22387acfbafbb1a9e591bd9d636e7895dcfc8de05f331"))
                                                                   ;  10,101,139

(def tx-senders:digests
  [["8KcosyLFuZW4F3Q6FQ4NnR619Zp2R9ZXpkxTr1poeghX"
    "CGNhGQaE9eLxa5pkdvcVFVZ5TQoSpandaQe8wM7YorFS"
    "FhGJpoRmzsTY5khzA6gTtpD2qWXpMWYDs9aSMnWYgkxN"]

   ["4KtCNbdZmt66AguTkk5gJfwZNjMnMq541Kq2YN2FkUSd"
    "88sXHLspwWDsz9GpSjEipvFNzQnUcGi18HXtgYNpunkb"
    "4RrgstZskT5UddfYNYwijEdSX48j2iCX7dhsfHLWeguz"]

   ["5M1qC7vWCysfMDJLwFdwGwjmJeVwgYmYXQM8BaiQRU8H"
    "BQycQS72X6JTWtRcCaAAVKfQ7mxTRPz9i9SLq1SyewNb"
    "4Pxm3mJFHa61KDuyUvTA2uXGygMKrxdZKaE1hmuk5wBB"]

   ["CgYJbvCVzuG9icetPftowmtvJKkE7zwYekW6jovdeoPk"
    "5RcwCn71JMCiXPbGaVcCJCtHTJzHc8BK9C6cB72Vmw4C"
    "6YGLZza6W253sPstwmmKcnFoUtqc79trWEZbf9df5baU"]

   ["7qWMZZoEaC4UaKVGsyZbPZzNmeHzv7PrE9PL8XULQVjX"
    "6aaSt3zjAAajd2F1R7a6tt3sgswvNn2ocRj8pvdxs3oV"
    "Be2ewFCPjriGvD7Qa5mcvD3YpaEKdu51hMdrU7e3D7C6"]

   ["57PyYd1Cyb9nN8AvCuoAVAXoLDbugrS64SFzYEJpxe8n"
    "6vooSoi3XtD1TorgXBUbe1PKDZk47bkvpMHLbmfFvS3s"
    "EfPBiM431ErPZWGWCFSc8KP6vLerBCfyj9KSSKeHgaeh"]

   ["EzKDHAnArH9utB2P4tom3fFaDAMkdsLipyFW8asyw4Me"
    "HFvW1JrJx4yDStUM6tSgeA96vP4BRpLV1R1CYCAhYTEQ"
    "34QHe2wHsnXpus2CkVqCAa1HDdD4BF78VKguh6L3Q84Z"]

   ["DvjAbXgbQcA7BFbK4fUWwMfgEAQGDKVkRyzxyNYnZczU"
    "9aXGGoemVcSivTNgstmvSjTefYMzBBKDAP2THGj2drca"
    "DawyanzJExRmkXdohvdPH51gGmW9m9GzBWmcKPTxQ3Ck"]])

;; Recipients ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def tx-recipients                                                 ;      # Txns
  (|| "0x2"                                                        ;           4
      "0xdead"                                                     ;          75
      "0x0"                                                        ;         141
      "0xa3eaef99f21c9f41c5b7c43f189a251177f9dc49b001d365225df67b11a12"
                                                                   ;       1,949
      "0x3452da4b9f4bfe13f217ef8b7f948e691e670494084f34b3da9bb7fbc3c8d"
                                                                   ;      20,754
      "0x1a71af664cca2456c6692061b8fb517effeb5ccd6953cedc03b89b4ec2a799"
                                                                   ;     208,102
      "0x1d632d46ff70491033fefc4e6398dceaa4943dcf62512b4d57378b5ab703bc5e"
                                                                   ;   1,674,620
      "0x8a0fac6e8b1ddbec8b61f2a55a1025c94a60f5dadda8e0990eed4029f52bea39"))
                                                                   ;  10,228,031

(def tx-recipients:digests
  [["5gdgzhKB5mAjbkqtPPMTrTADTtTZySunPPdsGbiCqTNn"
    "Fj1T1HnYmcvhdvnWPDoFPeVZkbUreKW8qvPo9Yhx27W7"
    "C1tJykvMGRs4mbG4CG58tfrCTYqswbYtDPsfvw4kd1zi"]

   ["3gYqXC4ASQjujz8Td2QFC5FPLqfjiM6oR5rdxAapobTo"
    "5KWUmsY5oFqZeQSoEzasb9VJQ65CT3ou6J7wuBUSH2Yv"
    "5cuxs3akzojmia8FPNummg2rfL5dsFPUFqd3MddqaguB"]

   ["9TGCAZsXWME7akNxtHswza3KwVZ9ApFWdmatztVokVj1"
    "7UkLEdZEJzHSBd6AbazZgC5oqtM8No3BDVuZZxQzwYcs"
    "8Wp5pE1EMxr1TTNBEjx4MQ8vjUGddFycw7HwDSfno3sx"]

   ["GUARZr8UkjEbxoQjoigTi8C6gyLxLdEBxSBW4ncGHK7h"
    "Atasgj1FcVzcKohMjqtP2gFtH3Q4hBLXnxPHACUDc66K"
    "CadEP9VtFmeE7nVemu6EeZ3LpQdjpCuVYXEyxCqYwVgB"]

   ["8gHd3hHQ1KGwpANgjqiogtFsRfrwFRbJqSi2GdPJ6FAa"
    "8g7hUGRa3YnKLnVTDqMrJABnNzahM8CN8ttFJmFyH3Mm"
    "8jQYycdiAqKVypE1fw9ZFvnoFGV3BTA7QCYwzRWFn6GN"]

   ["G9AeHEv8hGnDeSzHbNpSFttSDbTdU9ZVjWZPow3xmriL"
    "EpDc4eifq6PEYkRxQpfH3AeSbtxAEYt13Uk6wc5WqWdk"
    "FKRgnyq6RWrFRyznPYuWCC7oMAuA1gq3bi4WYwHCLDrP"]

   ["6FMt7uDmj5i8zAzEJWo1DxJAr2FVPJrw5yZXrnbc3bR9"
    "BsKAJ7KNCeUWP6jCYCbjRivDBZ1ygZPaE8mFbza7zW2D"
    "3uUbaNCT7dhooY3zGisaCtMrNoH7xNHzG5QQsnqXRkYd"]

   ["DAYjAwXNeuDrX6tZZHMwAcR34zT5bo1mJuEpqYULPqA4"
    "DvjAbXgbQcA7BFbK4fUWwMfgEAQGDKVkRyzxyNYnZczU"
    "6pTT4D6dpLpDp1AqSgqE3J6qAdaMBTmUVLktUHuYdWaA"]])


;; IDs ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Each section above includes a sample of transaction digests for transactions
;; that fit the criteria in that section. Below are some arbitrary IDs (not in
;; any of the previous sets)
;;
;; We can run tests with no constraint on IDs, a tiny constraint (one ID from
;; every section above), a small constraint (one ID from every group in every
;; section above), and a big constraint (all IDs).

(def tx-arbitrary:digests
  [["CHLnmi2GFEHb5eVwzGehf2ga3SuPR8983BMxXc821Xq"
    "4UAGZS7NEfjVPNS2REXDAXoJwHwEwm5kyRKKma6ukakQ"
    "2qGsyVXBZqL9Yk43rTbNbg3qjtD5TXvEnAwyEJYJqyad"]])

(def nested-tx-ids
  [tx-calls:digests
   tx-input-objects:digests tx-changed-objects:digests
   tx-senders:digests tx-recipients:digests
   tx-arbitrary:digests])

(def tiny-tx-ids
  (->> nested-tx-ids (map #(-> % first first)) (into [])))

(def small-tx-ids
  (->> nested-tx-ids (mapcat #(map first %)) (into [])))

(def big-tx-ids
  (->> nested-tx-ids flatten (into [])))

(def tx-filters
  (&& {:kind tx-kinds}
      tx-calls
      {:input tx-input-objects}
      {:changed tx-changed-objects}
      (|? {:sign tx-senders}
          {:recv tx-recipients}
          {:ids small-tx-ids}
          checkpoints)))
