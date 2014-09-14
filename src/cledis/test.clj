(ns cledis.test
  (:require [cledis.client :as redis]))

(defn -main
  "Just for testing"
  [& args]
  (let [conn (redis/connect)]
    (prn (redis/ping conn))
    (prn (redis/get conn "test"))))
