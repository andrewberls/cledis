(ns cledis.client
  (:require [cledis.protocol :as protocol])
  (:import [java.io Reader BufferedReader InputStreamReader StringReader]
           [java.net Socket]))


(def default-host "127.0.0.1")
(def default-port 6379)
(def default-url (str "redis://" default-host ":" default-port))


(defn- send-command
  "Send a formatted command through a socket and read the reply"
  [^Socket socket, ^String command]
  (let [out (.getOutputStream socket)
        bytes (.getBytes command)]
   (.write out bytes))
   (protocol/read-reply socket))


(defn- send-inline-command
  [socket command & args]
  (send-command socket
                (protocol/inline-command command args)))


(defn connect
  "Establish a connection to a server and return a Socket handle
   to the connection"
  (^Socket []
   (connect { :host default-host, :port default-port}))

   (^Socket [{host :host, port :port, :as options}]
    (let [socket (Socket. #^String host, #^Integer port)]
      (doto socket
        (.setTcpNoDelay true)
        (.setKeepAlive true)))))


; Connection Commands

(defn ping [conn] (send-inline-command conn "PING"))


; Server Commands

(defn flushdb [conn] (send-inline-command conn "FLUSHDB"))


; String commands

; append key val
; bitcount key start end [start end ...]
; bitop operation destkey key [key...]
; bitpos key bit [start] [end]
; decr key
; decrby key decrement
(defn get [conn key] (send-inline-command conn "GET" key))

(defn getbit [conn key offset] (send-inline-command conn "GETBIT" key offset))

; getrange key start end
; getset key value
; incr key
; incrby key increment
; incrbyfloat key increment
; mget key [key ...]
; mset key value [key value ...]
; msetnx key value [key value ...]
; psetex key milliseconds value

; TODO
; [EX seconds] [PX milliseconds] [NX|XX]
(defn set [conn key value] (send-inline-command conn "SET" key value))

(defn setbit [conn key offset value]
  (send-inline-command conn "SETBIT" key offset value))

; setex key seconds value
; setnx key value
; setrange key offset value
(defn strlen [conn key] (send-inline-command conn "STRLEN" key))


; Hash commands

; hdel key field [field ...]
; hexists key field
; hget key field
; hgetall key
; hincrby key field increment
; hincrbyfloat key field increment
; hkeys key
; hlen key
; hmget key field [field ...]
; hmset key field value [field value ...]
; hset key field value
; hsetnx key field value
; hvals key
; hscan key cursor [MATCH pattern] [COUNT count]
