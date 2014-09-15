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
  [command args]
  (let [cmd (string/join " " (cons command args))]
    (str cmd "\r\n")))


(defn- send-inline-command
  "Send an inline command over a socket"
  [socket command & args]
    (send-command socket (inline-command command args)))


; TODO: bulk generation needs refactoring

(defn- bulk-element
  "Format a single bulk string element"
  [arg]
  (let [bytecount (count (str arg))]
    (str "$" bytecount "\r\n" arg "\r\n")))


(defn- bulk-elements
  "Format a vector of bulk string elements"
  [^clojure.lang.PersistentVector args]
  (string/join "" (map bulk-element args)))


(defn- bulk-prefix
  "Format the length prefix for a vector of elements"
  [^clojure.lang.PersistentVector args]
  (let [elem-count (+ (count args) 1)]
    (str "*" elem-count "\r\n")))


(defn- bulk-command
  "Generate a bulk string command"
  [^String command, ^clojure.lang.PersistentVector args]
  (str (bulk-prefix args)
       (bulk-element command)
       (bulk-elements args)))


(defn- send-bulk-command
  "Send a bulk sstring command over a socket"
  [socket command & more]
  (let [args (flatten more)]
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
(defn set [conn key value]
  (send-inline-command conn "SET" key value))

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

(defn hmset [conn key field value & more]
  (send-bulk-command conn "HMSET" key field value more))

(defn hmget [conn key field & more]
  (send-bulk-command conn "HMGET" key field more))

; hset key field value
; hsetnx key field value
; hvals key
; hscan key cursor [MATCH pattern] [COUNT count]
