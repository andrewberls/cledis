(ns cledis.client
  (:require [cledis.protocol :as protocol]
            [clojure.string :as string])
  (:import [java.io Reader BufferedReader InputStreamReader StringReader]
           [java.net Socket]))


(def default-host "127.0.0.1")
(def default-port 6379)
(def default-url (str "redis://" default-host ":" default-port))


; Command generation / sending

(defn- send-command
  "Send a formatted command through a socket and read the reply"
  [^Socket socket, ^String command]
  (let [out (.getOutputStream socket)
        bytes (.getBytes command)]
   (.write out bytes))
   (protocol/read-reply socket))


(defn inline-command
  "Generate an inline command string"
  [^String command, ^clojure.lang.PersistentVector args]
  (let [cmd (string/join " " (cons command args))]
    (str cmd "\r\n")))


(defn- send-inline-command
  "Send an inline command over a socket"
  [socket command & args]
    (send-command socket (inline-command command args)))


(defn- bulk-element
  "Format a single bulk string element given either a string or a number"
  [arg]
  (let [bytecount (count (str arg))]
    (str "$" bytecount "\r\n" arg "\r\n")))


(defn- bulk-command
  "Format a RESP array command"
  [^String command, ^clojure.lang.PersistentVector args]
  (let [prefix   (str "*" (inc (count args)) "\r\n")
       elements (map bulk-element args)]
   (str prefix (bulk-element command) (string/join "" elements))))


(defn- send-bulk-command
  "Send a bulk sstring command over a socket"
  [socket command & more]
  (let [args (remove nil? (flatten more))]
    (send-command socket (bulk-command command args))))


(defn connect
  "Establish a connection to a server and return a Socket handle
   to the connection"
  (^Socket []
    (connect { :host default-host, :port default-port }))

  (^Socket [{host :host, port :port, :as options}]
    (let [socket (Socket. ^String host, ^Integer port)]
      (doto socket
        (.setTcpNoDelay true)
        (.setKeepAlive true)))))


; Connection commands

(defn ping [conn]
  (send-inline-command conn "PING"))


; Server Commands

(defn flushdb [conn]
  (send-inline-command conn "FLUSHDB"))


; String commands

; append key val
; bitcount key start end [start end ...]
; bitop operation destkey key [key...]
; bitpos key bit [start] [end]
; decr key
; decrby key decrement
(defn get [conn key]
  (send-inline-command conn "GET" key))

(defn getbit [conn key offset]
  (send-inline-command conn "GETBIT" key offset))

; getrange key start end
; getset key value
; incr key
; incrby key increment
; incrbyfloat key increment
(defn mget [conn key & more]
  (send-bulk-command conn "MGET" key more))

(defn mset [conn key value & more]
  (send-bulk-command conn "MSET" key value more))
; msetnx key value [key value ...]
; psetex key milliseconds value

; TODO
; [EX seconds] [PX milliseconds] [NX|XX]
(defn set [conn key value]
  (send-inline-command conn "SET" key value))

(defn setbit [conn key offset value]
  (send-inline-command conn "SETBIT" key offset value))

; setex key seconds value
; setnx key value
; setrange key offset value
(defn strlen [conn key]
  (send-inline-command conn "STRLEN" key))


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

(defn hmset [conn key field value & more]
  (send-bulk-command conn "HMSET" key field value more))

(defn hmget [conn key field & more]
  (send-bulk-command conn "HMGET" key field more))

; hset key field value
; hsetnx key field value
; hvals key
; hscan key cursor [MATCH pattern] [COUNT count]
