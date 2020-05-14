(ns com.nevaris.onsite.api-log-parser
  (:require [clojure.java.io :as io]
            [clojure.string :as string])
  (:import java.time.LocalDateTime
           java.time.format.DateTimeFormatter
           java.util.zip.GZIPOutputStream))

(comment
  (set! *warn-on-reflection* true)
  (set! *unchecked-math* :warn-on-boxed))

(defn log-reader
  [file]
  (reify clojure.lang.IReduceInit
    (reduce [this f start]
      (with-open [rdr (io/reader file :encoding "ISO-8859-1")]
        (loop [[^String line & lines] (line-seq rdr)
               sb (StringBuilder.)
               acc start]
          (if line
            (if (and (< 36 (.length line))
                     (= \- (.charAt line 8))
                     (= \; (.charAt line 36)))
              (if (zero? (.length sb))
                (recur lines (.append sb line) acc)
                (let [new-acc (f acc (str sb))]
                  (.setLength sb 0)
                  (if (reduced? new-acc)
                    @new-acc
                    (recur lines (.append sb line) new-acc))))
              (recur lines (-> sb (.append "\n") (.append line)) acc))
            (f acc (str sb))))))))

(defn parse-log-entry
  [^String entry]
  (let [a (.indexOf entry ";")
        b (.indexOf entry ";" (inc a))
        c (.indexOf entry ";" (inc b))
        d (.indexOf entry ";" (inc c))
        e (.indexOf entry ";" (inc d))
        f (.indexOf entry ";" (inc e))
        g (.indexOf entry ";" (inc f))
        z (.lastIndexOf entry ";'")
        y (.lastIndexOf entry "';" z)
        ^int
        x (loop [i (.lastIndexOf entry ";'" (dec y))]
            (if (re-find #"\d+(\.\d+){3}$" (subs entry (- i 15) i))
              i
              (recur (.lastIndexOf entry ";'" (dec i)))))
        w (.lastIndexOf entry ";" (dec x))
        v (.lastIndexOf entry ";" (dec w))
        ^int
        u (loop [i (.lastIndexOf entry ";" (dec v))]
            (if (= \space (.charAt entry (inc i)))
              (recur (.lastIndexOf entry ";" (dec i)))
              i))]
    [(subs entry 0 a)       ;; ident
     (subs entry (inc a) b) ;; customer_id
     (subs entry (inc b) c) ;; http_status
     (subs entry (inc c) d) ;; timestamp
     (subs entry (inc d) e) ;; process_duration
     (subs entry (inc e) f) ;; login
     (subs entry (inc f) g) ;; path
     (subs entry (inc g) u) ;; url
     (subs entry (inc u) v) ;; user_agent
     (subs entry (inc v) w) ;; http_verb
     (subs entry (inc w) x) ;; ip
     (subs entry (+ 2 x) y) ;; payload
     (subs entry (+ 2 y) z) ;; payload_size
     ;; exception
     (subs entry (+ 2 z) (dec (.length entry)))]))

(def DATETIME_PATTERN (DateTimeFormatter/ofPattern "dd.MM.yyyy HH:mm:ss"))

(defn parse-timestamp
  [string]
  (LocalDateTime/parse string DATETIME_PATTERN))

(defn escape-string
  [string]
  (-> string
      (string/replace "\\" "\\\\")
      (string/replace "\b" "\\b")
      (string/replace "\f" "\\f")
      (string/replace "\n" "\\n")
      (string/replace "\r" "\\r")
      (string/replace "\t" "\\t")
      (string/replace "\"" "\\\"")))

(defn format-as-json
  [entry]
  (let [it (.iterator ^Iterable entry)
        sb (StringBuilder.)]
    (.append sb "{\"ident\":\"")
    (.append sb (.next it))
    (.append sb "\",\"customer_id\":\"")
    (.append sb (.next it))
    (.append sb "\",\"http_status\":\"")
    (.append sb (.next it))
    (.append sb "\",\"timestamp\":\"")
    (.append sb (str (parse-timestamp (.next it))))
    (let [proc-time (.next it)]
      (.append sb "\",\"process_duration\":")
      (.append sb (if (= "" proc-time) "0" proc-time)))
    (.append sb ",\"login\":\"")
    (.append sb (.next it))
    (.append sb "\",\"path\":\"")
    (.append sb (.next it))
    (.append sb "\",\"url\":\"")
    (.append sb (.next it))
    (.append sb "\",\"user_agent\":\"")
    (.append sb (.next it))
    (.append sb "\",\"http_verb\":\"")
    (.append sb (.next it))
    (.append sb "\",\"ip\":\"")
    (.append sb (.next it))
    (.append sb "\",\"payload\":\"")
    (.append sb (escape-string (.next it)))
    (.append sb "\",\"payload_size\":")
    (.append sb (.next it))
    (.append sb ",\"exception\":\"")
    (.append sb (escape-string (.next it)))
    (.append sb "\"}\n")
    (str sb)))

(defn read-logs
  [file]
  (let [ks [:ident
            :customer-id
            :http-status
            :timestamp
            :process-duration
            :login
            :path
            :url
            :user-agent
            :http-verb
            :ip
            :payload
            :payload-size
            :exception]]
    (into [] (comp (map parse-log-entry)
                   (map #(zipmap ks %))
                   (map (fn [data]
                          (-> data
                              (update :timestamp (comp str parse-timestamp))
                              (update :payload-size #(Long/parseLong %))
                              (update :process-duration #(if (= "" %) 0 (Double/parseDouble %)))))))
          (log-reader file))))

(defn -main
  [& args]
  (let [[in out gzip?] args]
    (with-open [os (cond-> (io/output-stream out)
                     gzip? (GZIPOutputStream.))
                writer (io/writer os)]
      (->> (log-reader in)
           (eduction
            (map parse-log-entry)
            (map format-as-json))
           (run! #(.write writer (str %)))))))
