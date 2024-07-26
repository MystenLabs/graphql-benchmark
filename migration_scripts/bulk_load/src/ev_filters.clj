(ns ev-filters
  (:require [combinations :refer [<- && || |?]]))

;; Events Filters ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Inputs to event filters for events between partitions 75 and 125
;; inclusive, which translates to:
;;
;;     EV                           = 770,175,162
;;     TX 14,877,223 to 748,348,393 = 733,471,170
;;     CP  6,131,063 to  10,513,353 =   4,382,290 = ~50 days

;; Senders ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ev-senders                                                    ;    # Events
  (|| "0xa697aec8505d62dd3b368cb50bfb668f148458cf2ff00e68a892189b982"
                                                                   ;           0
      "0x76fe7abaadcfe835f73bca4b581217e7c23baffb651b23fd987b07c3f6"
                                                                   ;          39
      "0xec98f469d70a8cfc4e80a286096158bf44274c8884c4945b13a43e2d" ;         436
      "0x640b779b330a44fb0047b2a3f462de579285b1846143141619d6c3fa3f414"
                                                                   ;       1,567
      "0x0"                                                        ;      10,744
      "0x1034c8cef9b673767644328e7c1df02f6095861868850edc40fdef6379b8d"
                                                                   ;      29,524
      "0x6ac862e9f1336a09cd15d439d7f535f784b2e54d99826b14bb17aad235f64"
                                                                   ;     121,485
      "0x2a212de6a9dfa3a69e22387acfbafbb1a9e591bd9d636e7895dcfc8de05f331"))
                                                                   ;  31,746,752

;; Transaction Digests ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ev-digests                                                    ;    # Events
  (|| "7sWqPHdqzJBh4wi1Sjc3L4L9MVa1DTRras1RSHzbXVk"                ;           1
      "7DJLXY34Z8ApidXE6NuFtQuEYc2NQSh3sW9EYE8vjyhb"               ;          50
      "7a1t2jHigtZszAotCxJtSH3ntKuJkEw2GbsEtobEDQpF"))             ;         200

;; Emitting Modules ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ev-modules                                                    ; #    Events
  (|| {:em-pkg "0x2"                                               ;         778
       :em-mod (|? "display"                                       ;          99
                   "kiosk"                                         ;         670
                   "transfer_policy")}                             ;           9
      {:em-pkg "0xcbe984fe2b4f01742bf89af7e03de499bd81780fe74e94b5ca9d3a5d16308df2"
       :em-mod (|? "launchpad_module")}                            ;         200
      {:em-pkg "0x14d61521e24eac5ada7ccfc6d07814fb9477b4c5ffe65b7950765caf5fd82d93"
       :em-mod (|? "liquidate")}                                   ;       1,516
      {:em-pkg "0x3" :em-mod (|? "sui_system")}                    ;      33,976
      {:em-pkg "0xdee9" :em-mod (|? "clob_v2")}                    ;   2,846,291
      {:em-pkg "0x72f9c76421170b5a797432ba9e1b3b2e2b7cf6faa26eb955396c773af2479e1e"
       :em-mod (|? "clob_v2")}))                                   ; 684,903,122

;; Event Types ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ev-event-types                                                ;    # Events
  (|| (&& {:ev-pkg "0x2"}                                          ;      61,631
          (|? {:ev-mod "display"                                   ;         739
               :ev-typ (|? "DisplayCreated"                        ;         317
                           "DisplayCreated<0x007cca142ad3e51f4079b5d7f674a99376e7805c54193dd149802f16e2268d74::mockghost::MockGhost>"
                                                                   ;           1
                           "VersionUpdated"                        ;         422
                           "VersionUpdated<0x58156e414780a5a237db71afb0d852674eff8cd98f9572104cb79afeb4ad1e9d::suinet::SUITOMAINNET>")}
                                                                   ;          22
              {:ev-mod "kiosk"                                     ;      60,698
               :ev-typ (|? "ItemDelisted"                          ;       1,846
                           "ItemDelisted<0xee496a0cc04d06a345982ba6697c90c619020de9e274408c7819f787ff66e1a1::suifrens::SuiFren<0x8894fa02fc6f36cbc485ae9145d05f247a78e220814fb8419ab261bd81f08f32::bullshark::Bullshark>>"
                                                                   ;       1,836
                           "ItemPurchased"                         ;      55,701
                           "ItemPurchased<0xbf1431324a4a6eadd70e0ac6c5a16f36492f255ed4d011978b2cf34ad738efe6::day_one::DayOne>"
                                                                   ;         287
                           "ItemPurchased<0xee496a0cc04d06a345982ba6697c90c619020de9e274408c7819f787ff66e1a1::suifrens::SuiFren<0x8894fa02fc6f36cbc485ae9145d05f247a78e220814fb8419ab261bd81f08f32::bullshark::Bullshark>>")}))
                                                                   ;      54,847
      (&& {:ev-pkg "0xdee9"}                                       ;   2,968,185
          (|? {:ev-mod "clob_v2"                                   ;   2,968,185
               :ev-typ (|? "OrderCanceled"                         ;     954,890
                           "OrderCanceled<0x0000000000000000000000000000000000000000000000000000000000000002::sui::SUI,0x5d4b302506645c37ff133b98c4b50a5ae14841659738d6d733d59d0d217a93bf::coin::COIN>")}))
                                                                   ;     262,421
      (&& {:ev-pkg "0xa0eba10b173538c8fecca1dff298e488402cc9ff374f8a12ca7758eebe830b66"}
                                                                   ;   6,135,856
          (|? {:ev-mod "spot_dex"                                  ;   6,135,856
               :ev-typ (|? "SwapEvent"                             ;   5,401,281
                           "SwapEvent<0x0000000000000000000000000000000000000000000000000000000000000002::sui::SUI>")}))))
                                                                   ;   2,137,499

;; Cursor Bounds ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:private tx:arbitrary
  (range 14877224 748348393 100000000))

(def cursors:arbitrary
  (map (fn [tx ev] [tx ev]) tx:arbitrary (range)))

(def cursors:lower
  (<- {:after (take 3 cursors:arbitrary)}))

(def cursors:upper
  (<- {:before (take 3 (drop 3 cursors:arbitrary))}))

(def cursors:weekly
  (map (fn [a b] {:after a :before b})
       cursors:arbitrary (rest cursors:arbitrary)))

(def cursors
  (|| cursors:lower
      cursors:upper
      cursors:weekly))

;; Entry Point ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ev-filters
  (&& (|? {:sign ev-senders})
      (|? {:tx ev-digests})
      (|? ev-modules)
      (|? ev-event-types)
      (|? cursors)))
