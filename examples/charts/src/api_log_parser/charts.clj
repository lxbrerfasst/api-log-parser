(ns api-log-parser.charts
  (:require [com.nevaris.onsite.api-log-parser :as parser]
            [com.hypirion.clj-xchart :as xchart]))

(defn make-log-entry
  []
  (parser/map->LogEntry
   {:customer-id (rand-nth ["20001" "20012" "30154" "30700"])
    :process-duration (rand 1e3)
    :timestamp (-> (java.time.LocalDateTime/now)
                   (.withNano 0)
                   (.withSecond (rand-int 60))
                   (.withMinute (rand-int 60))
                   (.withHour (rand-int 24)))}))

(comment
  
  (let [groups (->> (repeatedly 1e4 make-log-entry)
                    (group-by (fn [log]
                                (-> (:timestamp log)
                                    (.withSecond 0))))
                    (into (sorted-map)))]
    (xchart/view
     (xchart/xy-chart
      {"Processing Time per Minute"
       {:x (->> (keys groups)
                (map #(java.util.Date/from (.toInstant (.atZone % (java.time.ZoneId/systemDefault))))))
        :y (->> (vals groups)
                (map #(transduce (map :process-duration) + 0.0 %)))}}
      {:render-style :scatter})))

  )

(comment
  
  (let [groups (->> (repeatedly 1e5 make-log-entry)
                    (group-by (fn [log]
                                (-> (:timestamp log)
                                    (.withSecond 0))))
                    (into (sorted-map) (filter (comp pos? count val))))]
    (xchart/view
     (xchart/xy-chart
      {"Requests per Minute"
       {:x (->> (keys groups)
                (map #(java.util.Date/from (.toInstant (.atZone % (java.time.ZoneId/systemDefault))))))
        :y (->> (vals groups)
                (map count))}}
      {:render-style :scatter})))

  )

(comment
  
  (let [groups (->> (repeatedly 1e4 make-log-entry)
                    (group-by :customer-id)
                    (into {} (map (fn [[id data]]
                                    [id (->> data
                                             (group-by (fn [log]
                                                         (-> (:timestamp log)
                                                             (.withSecond 0))))
                                             (into (sorted-map)))]))))]
    (xchart/view
     (xchart/xy-chart
      (into {} (map (fn [[id groups]]
                      [(str "Customer " id)
                       {:x (->> (keys groups)
                                (map #(java.util.Date/from (.toInstant (.atZone % (java.time.ZoneId/systemDefault))))))
                        :y (->> (vals groups)
                                (map #(transduce (map :process-duration) + 0.0 %)))}]))
            groups)
      {:title "Processing Time per Customer per Minute"
       :render-style :scatter})))

  )

(comment
  
  (let [groups (->> (repeatedly 1e3 make-log-entry)
                    (group-by :customer-id))]
    (xchart/view
     (xchart/pie-chart
      (map (fn [[id group]] [id (transduce (map :process-duration) + 0.0 group)]) groups)
      {:title "Processing Time per Customer"
       :donut-thickness 0.33
       :render-style :donut
       :annotation-distance 0.82})))

  )

(comment
  
  (let [groups (->> (repeatedly 1e2 make-log-entry)
                    (group-by :customer-id)
                    (into {} (map (fn [[id group]]
                                    [id {"Time" (transduce (map :process-duration) + 0.0 group)}]))))]
    (xchart/view
     (xchart/category-chart
      groups
      {:title "Processing Time per Customer"
       :series-order (->> groups
                          (sort-by #(get (val %) "Time"))
                          (map key)
                          (reverse))})))

  )
