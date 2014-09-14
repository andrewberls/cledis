## cledis

An in-progress Clojure client for Redis, written as a learning exercise. If you're looking for a production-grade
Redis client, consider [Carmine](https://github.com/ptaoussanis/carmine).

### Usage

```clojure
(require '[cledis.client :as redis])

(def conn (redis/connect))
(prn (redis/ping conn))        ; "PONG"
(prn (redis/get conn "hello")) ; "world"
```
