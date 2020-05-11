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
              (recur lines (.append sb line) acc))
            (f acc (str sb))))))))

(defn parse-log-entry
  [^String entry]
  (let [;; ident
        a (.indexOf entry ";")
        ;; customer_id
        b (.indexOf entry ";" (inc a))
        ;; timestamp
        c (.indexOf entry ";" (inc b))
        ;; process_duration
        d (.indexOf entry ";" (inc c))
        ;; login
        e (.indexOf entry ";" (inc d))
        ;; path
        f (.indexOf entry ";" (inc e))
        ;; url
        g (.indexOf entry ";" (inc f))
        ;; exception
        z (.lastIndexOf entry ";'")
        ;; payload_size
        y (.lastIndexOf entry "';" z)
        ;; payload
        ^int
        x (loop [i (.lastIndexOf entry ";'" (dec y))]
            (if (re-find #"\d+(\.\d+){3}$" (subs entry (- i 15) i))
              i
              (recur (.lastIndexOf entry ";'" (dec i)))))
        ;; ip
        w (.lastIndexOf entry ";" (dec x))
        ;; http_verb
        v (.lastIndexOf entry ";" (dec w))
        ;; user_agent
        ^int
        u (loop [i (.lastIndexOf entry ";" (dec v))]
            (if (= \space (.charAt entry (inc i)))
              (recur (.lastIndexOf entry ";" (dec i)))
              i))]
    [(subs entry 0 a)
     (subs entry (inc a) b)
     (subs entry (inc b) c)
     (subs entry (inc c) d)
     (subs entry (inc d) e)
     (subs entry (inc e) f)
     (subs entry (inc f) g)
     (subs entry (inc g) u)
     (subs entry (inc u) v)
     (subs entry (inc v) w)
     (subs entry (inc w) x)
     (subs entry (+ 2 x) y)
     (subs entry (+ 2 y) z)
     (subs entry (+ 2 z) (dec (.length entry)))]))

(def DATETIME_PATTERN (DateTimeFormatter/ofPattern "dd.MM.yyyy HH:mm:ss"))

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
    (.append sb (LocalDateTime/parse (.next it) DATETIME_PATTERN))
    (let [proc-time (.next it)]
      (if (not= "" proc-time)
        (do (.append sb "\",\"process_duration\":")
            (.append sb proc-time))
        (do (.append sb "\""))))
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
    (.append sb (-> (.next it)
                    (string/replace "\\" "\\\\")
                    (string/replace "\"" "\\\"")))
    (.append sb "\",\"payload_size\":")
    (.append sb (.next it))
    (.append sb ",\"exception\":\"")
    (.append sb (-> (.next it)
                    (string/replace "\\" "\\\\")
                    (string/replace "\"" "\\\"")))
    (.append sb "\"}\n")
    (str sb)))

(defn -main
  [& args]
  (let [[in out] args]
    (with-open [os (io/output-stream out)
                gzos (GZIPOutputStream. os)
                writer (io/writer gzos)]
      (->> (log-reader in)
           (eduction
            (map parse-log-entry)
            (map format-as-json))
           (run! #(.write writer (str %)))))))
