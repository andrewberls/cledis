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


; Commands

(defn ping
  [conn]
  (send-inline-command conn "PING"))

(defn get
  [conn key]
  (send-inline-command conn "GET" key))
