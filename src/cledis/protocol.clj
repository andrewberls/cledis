(ns cledis.protocol
  (:import [java.io Reader BufferedReader InputStreamReader StringReader]
           [java.net Socket]))


; Request construction

(defn inline-command
  ^String [^String command, args]
  (let [cmd (clojure.string/join " " (cons command args))]
    (str cmd "\r\n")))


; Reply parsing

(defn- cr? [c] (= c 0x0d))
(defn- lf? [c] (= c 0x0a))


(defn- read-crlf
  "Read a CRLF from a Reader, or throw if not found"
  [^BufferedReader reader]
  (let [c (.read reader)]
    (if (cr? c)
      (if-not (lf? (.read reader))
        (throw (Exception. "Error: Missing LF"))))))


(defn- read-line-crlf
  "Read a line of text terminated by CRLF from a Reader
   Returns the line excluding CRLF"
  [^BufferedReader reader]
  (loop [char-buf [], c (.read reader)]
    (when (< c 0)
      (throw (Exception. "Error: Reached EOF while parsing reply")))
    (if (cr? c)
      (let [next (.read reader)]
        (if (lf? next)
          (clojure.string/join "" char-buf)
          (throw (Exception. "Error reading line: Missing LF"))))
      (recur (conj char-buf (char c)) (.read reader)))))


(defn reply-type
  "Dispatching function for reply parsing"
  [^BufferedReader reader]
  (char (.read reader)))


(defmulti parse-reply reply-type :default :unknown)

(defmethod parse-reply :unknown
  [^BufferedReader reader]
  (throw (Exception. (str "Unknown reply type"))))

; Simple string
; Ex: "+PONG\r\n"
(defmethod parse-reply \+
  [^BufferedReader reader]
  (read-line-crlf reader))

; Simple integer
; Ex: ":1000\r\n"
(defmethod parse-reply \:
  [^BufferedReader reader]
  (Integer/parseInt (read-line-crlf reader)))

; Bulk string
; Ex: "$6\r\nfoobar\r\n"
(defmethod parse-reply \$
  [^BufferedReader reader]
  (let [bytecount (Integer/parseInt (read-line-crlf reader))]
    (if (< bytecount 0)
      nil ; Null bulk string
      (let [^chars cbuf (char-array bytecount)]
        (do
          (.read reader cbuf 0 bytecount)
          (read-crlf reader)
          (String. cbuf))))))

; Error
(defmethod parse-reply \-
  [^BufferedReader reader]
  (read-line-crlf reader))


(defn read-reply
  "Read and parse a response directly from a Socket"
  [^Socket socket]
  (let [input-stream (.getInputStream socket)
        reader (BufferedReader. (InputStreamReader. input-stream))]
    (parse-reply reader)))
