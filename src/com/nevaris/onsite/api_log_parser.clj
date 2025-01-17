(ns com.nevaris.onsite.api-log-parser
  (:require [clojure.java.io :as io]
            [clojure.string :as string])
  (:import java.time.LocalDateTime
           java.time.format.DateTimeFormatter
           java.util.regex.Matcher))

(comment
  (set! *warn-on-reflection* true)
  (set! *unchecked-math* :warn-on-boxed))

(defn log-reader
  [file]
  (reify clojure.lang.IReduceInit
    (reduce [this f start]
      (with-open [^java.io.BufferedReader
                  rdr (io/reader file :encoding "ISO-8859-1")]
        (loop [sb (StringBuilder.)
               acc start]
          (if-some [line (.readLine rdr)]
            (if (and (< 36 (.length line))
                     (= \- (.charAt line 8))
                     (= \- (.charAt line 13))
                     (= \- (.charAt line 18))
                     (= \- (.charAt line 23))
                     (= \; (.charAt line 36)))
              (if (zero? (.length sb))
                (recur (.append sb line) acc)
                (let [new-acc (f acc (str sb))]
                  (.setLength sb 0)
                  (if (reduced? new-acc)
                    @new-acc
                    (recur (.append sb line) new-acc))))
              (recur (-> sb (.append "\n") (.append line)) acc))
            (f acc (str sb))))))))

(defrecord LogEntry [ident customer-id http-status timestamp
                     process-duration login path url user-agent
                     http-verb ip payload payload-size exception])

(def DATETIME_PATTERN (DateTimeFormatter/ofPattern "dd.MM.yyyy HH:mm:ss"))

(defn parse-timestamp
  [string]
  (LocalDateTime/parse string DATETIME_PATTERN))

(def IP_PATTERN #"^;\d+(\.\d+){3}$")

(defn parse-log-entry
  ([entry] (parse-log-entry entry (re-matcher IP_PATTERN "")))
  ([^String entry ^Matcher matcher]
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
             (let [j (.lastIndexOf entry ";" (dec i))]
               (if (.matches (.reset matcher (subs entry j i)))
                 i
                 (recur (.lastIndexOf entry ";'" (dec i))))))
         w (.lastIndexOf entry ";" (dec x))
         v (.lastIndexOf entry ";" (dec w))
         ^int
         u (loop [i (.lastIndexOf entry ";" (dec v))]
             (if (= \space (.charAt entry (inc i)))
               (recur (.lastIndexOf entry ";" (dec i)))
               i))]
     (LogEntry.
      (subs entry 0 a)           ;; ident
      (subs entry (inc a) b)     ;; customer_id
      (subs entry (inc b) c)     ;; http_status
      (-> (subs entry (inc c) d) ;; timestamp
          (parse-timestamp)
          (str))
      (if (== (inc d) e)
        0.0
        (-> (subs entry (inc d) e) ;; process_duration
            (Double/parseDouble)))
      (subs entry (inc e) f)     ;; login
      (subs entry (inc f) g)     ;; path
      (subs entry (inc g) u)     ;; url
      (subs entry (inc u) v)     ;; user_agent
      (subs entry (inc v) w)     ;; http_verb
      (subs entry (inc w) x)     ;; ip
      (subs entry (+ 2 x) y)     ;; payload
      (-> (subs entry (+ 2 y) z) ;; payload_size
          (Long/parseLong))
      ;; exception
      (subs entry (+ 2 z) (dec (.length entry)))))))

(defn log-parser
  [file]
  (let [matcher (re-matcher IP_PATTERN "")]
    (eduction (map #(parse-log-entry % matcher)) (log-reader file))))

(defn eval-logs
  [xf reduce-fn combine-fn files]
  (->> files
       (pmap #(transduce
               xf
               (completing reduce-fn)
               (reduce-fn)
               (log-parser %)))
       (reduce combine-fn (combine-fn))))

(comment

  ;; EXAMPLES

  ;; Requests per Customer
  (eval-logs
   identity
   (fn
     ([] {})
     ([result log]
      (update result (:customer-id log) (fnil inc 0))))
   (fn
     ([] {})
     ([result batch]
      (merge-with + result batch)))
   files)

  ;; Processing Time per API Path
  (eval-logs
   identity
   (fn
     ([] {})
     ([result log]
      (update result (:path log) (fnil + 0.0) (:process-duration log))))
   (fn
     ([] {})
     ([result batch]
      (merge-with + result batch)))
   files)

  ;; Processing Time for specific Customer
  (eval-logs
   (filter (comp #{"20001"} :customer-id))
   (fn
     ([] {})
     ([result log]
      (update result (:path log) (fnil + 0.0) (:process-duration log))))
   (fn
     ([] {})
     ([result batch]
      (merge-with + result batch)))
   files)

  )
