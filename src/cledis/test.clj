(ns cledis.test
  (:require [cledis.client :as redis]))

(defn -main
  "Just for testing"
  [& args]
  (let [conn (redis/connect)]
    (prn (redis/flushdb conn))              ; OK
    (prn (redis/ping conn))                 ; PONG
    (prn (redis/set conn "test" "hello"))   ; OK
    (prn (redis/get conn "test"))           ; "hello"
    (prn (redis/strlen conn "test"))        ; 5
    (prn (redis/getbit conn "bittest" 2))   ; 0
    (prn (redis/setbit conn "bittest" 2 1)) ; 0
    (prn (redis/getbit conn "bittest" 2))   ; 1
    ))
