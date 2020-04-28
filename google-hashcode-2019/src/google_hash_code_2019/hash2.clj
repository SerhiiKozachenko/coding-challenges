(ns google-hash-code-2019.hash2
  (:require [clojure.string :as str]
            [clojure.set :as set]
            [clojure.core.reducers :as r]
            [google-hash-code-2019.helpers :refer :all]))


(defn -photo-lines->items-hash-1
  "Approach 1 - 4 times iteration through data set - O(4)
  Elapsed time: 10.192185 msecs
  Elapsed time: 7.218196 msecs
  Elapsed time: 6.908627 msecs"
  [photo-lines]
  (let [items-vec        (->> photo-lines
                              (map-indexed (fn [idx l]
                                             (let [p    (str/split l #" ")
                                                   type (get p 0)
                                                   ;tags-count (get p 1)
                                                   tags (-> p
                                                            (subvec 2)
                                                            (set))]
                                               {:id   idx
                                                :type type
                                                :tags tags})))
                              (into []))


        h-items-vec      (->> items-vec (filterv #(-> % :type (= "H"))))
        v-items-vec      (->> items-vec (filterv #(-> % :type (= "V"))))

        h-items-hash-map (->> h-items-vec
                              (reduce (fn [res v] (assoc res (-> v :id (str)) (:tags v))) {}))

        v-items-hash-map (->> v-items-vec
                              (partition 2)
                              (reduce (fn [res v] (assoc res
                                                    (->> v
                                                         (map :id)
                                                         (str/join " "))
                                                    (->> v
                                                         (map :tags)
                                                         (apply set/union)))) {}))
        items-hash       (set/union h-items-hash-map v-items-hash-map)]
    items-hash))


(defn -photo-lines->items-hash-2
  "Approach 2 - 1 times iteration through data set - O(1)
  Elapsed time: 8.168035 msecs
  Elapsed time: 6.579161 msecs
  Elapsed time: 6.854513 msecs"
  [photo-lines]
  (let [items-hash (->> photo-lines
                        (into [])
                        (reduce-kv (fn [res idx l]
                                     (let [p    (str/split l #" ")
                                           type (get p 0)
                                           tags (-> p
                                                    (subvec 2)
                                                    (set))]
                                       (if (= type "H")
                                         (assoc-in res [:h (str idx)] tags)
                                         (do
                                           ;; for vertical check if there is already one exists
                                           (let [v (:v res)]
                                             (if (empty? v)
                                               (assoc-in res [:v (str idx)] tags)
                                               (let [v-id          (->> v
                                                                        (keys)
                                                                        (first))
                                                     v-tags        (->> v
                                                                        (vals)
                                                                        (first))
                                                     combined-id   (str idx " " v-id)
                                                     combined-tags (set/union tags v-tags)]
                                                 (-> res
                                                     (assoc-in [:h combined-id] combined-tags)
                                                     (assoc :v {})))))))))
                                   {:h {} :v {}})
                        :h)]
    items-hash))


(defn -fold-using-map [partition-size items-vec]
  (->> items-vec
       (r/fold
         partition-size
         ;; Fixed by merging deeply
         (r/monoid (fn [a b] {:h (merge (:h a) (:h b))
                              :v (merge (:v a) (:v b))})
                   (fn [] {:h {} :v {}}))
         (fn [res item]
           (let [idx  (:id item)
                 type (:type item)
                 tags (:tags item)]
             (if (= type "H")
               (assoc-in res [:h (str idx)] tags)
               (let [v (:v res)]
                 (if (empty? v)
                   (assoc-in res [:v (str idx)] tags)
                   (let [v-id          (->> v
                                            (keys)
                                            (first))
                         v-tags        (->> v
                                            (vals)
                                            (first))
                         combined-id   (str idx " " v-id)
                         combined-tags (set/union tags v-tags)]
                     (-> res
                         (assoc-in [:h combined-id] combined-tags)
                         (assoc :v {})))))))))
       :h))


(defn -fold-using-vec [partition-size items-vec]
  (->> items-vec
       (r/fold
         partition-size
         ;; Need to fix concat
         (r/monoid r/cat (fn [] [[] []]))
         (fn [[h-vec v-vec] item]
           (let [idx  (:id item)
                 type (:type item)
                 tags (:tags item)]
             (if (= type "H")
               [(conj h-vec {:id (str idx) :tags tags}) v-vec]
               (if (empty? v-vec)
                 [h-vec (conj v-vec {:id (str idx) :tags tags})]
                 (let [v             (first v-vec)
                       v-id          (:id v)
                       v-tags        (:tags v)
                       combined-id   (str idx " " v-id)
                       combined-tags (set/union tags v-tags)]
                   [(conj h-vec {:id combined-id :tags combined-tags}) []]))))))
       (first)
       (reduce (fn [res v] (assoc res (:id v) (:tags v))) {})))

;; Wrong - since parallel makes it to skip some vertical items which can be merged
;; Need to investigate more - the issue in combinef fn - sinc if pass partition size bigger than vec size
;; it will use usual reduce and count is correct!!!
(defn -photo-lines->items-hash-3
  "Approach 3 - Parallel"
  [photo-lines]
  (let [items-vec (->> photo-lines
                       (map-indexed (fn [idx l]
                                      (let [p    (str/split l #" ")
                                            type (get p 0)
                                            ;tags-count (get p 1)
                                            tags (-> p
                                                     (subvec 2)
                                                     (set))]
                                        {:id   idx
                                         :type type
                                         :tags tags})))
                       (into []))]
    ;; Some items still missed depends on partition-size
    ;; caused by orphan vertical items - left in each partition
    (-fold-using-map 10000 items-vec)
    ;(-fold-using-vec 999 items-vec)
    ))


(defn -parsed-lines->items-tags-map-4
  "Approach 4 - Mixed parallel.
  Map horizontal and vertical items in parallel and convert them to maps sequentially.
  Using transient data structures."
  [parsed-lines limit]
  (let [h-and-v     (->> parsed-lines
                         ;; reduce-kv to map line indexes
                         (reduce-kv
                           (fn [res idx l]
                             (conj! res {:id   idx
                                         :line l}))
                           (transient []))
                         (persistent!)

                         ;; custom fold
                         ;(fold
                         ;  100
                         ;  ;; partition-fn
                         ;  (r/monoid
                         ;    (fn [res i]
                         ;      (if (-> i :id (= 0))
                         ;        ;; skip photos count line
                         ;        res
                         ;        ;; process next lines, dec index to compensate skipped line
                         ;        (let [h-tvec (:horizontal res)
                         ;              v-tvec (:vertical res)
                         ;              idx    (-> i :id (dec))
                         ;              l      (:line i)
                         ;              p      (str/split l #" ")
                         ;              type   (get p 0)
                         ;              tags   (-> p
                         ;                         ;; skip type and tags count
                         ;                         (subvec 2)
                         ;                         ;; convert to hash-set
                         ;                         (set))
                         ;              item   {:id   (str idx)
                         ;                      :type type
                         ;                      :tags tags}]
                         ;          (if (= type "H")
                         ;            (assoc! res :horizontal (conj! h-tvec item))
                         ;            (assoc! res :vertical (conj! v-tvec item))))))
                         ;    (fn [] (transient {:horizontal (transient [])
                         ;                       :vertical   (transient [])})))
                         ;  ;; combine-fn
                         ;  (r/monoid
                         ;    ;; Don't use destructuring to speed up
                         ;    (fn [a b]
                         ;      (let [ah (:horizontal a)
                         ;            av (:vertical a)
                         ;            bh (:horizontal b)
                         ;            bv (:vertical b)]
                         ;        (-> a
                         ;            (assoc! :horizontal (reduce conj! ah (persistent! bh)))
                         ;            (assoc! :vertical (reduce conj! av (persistent! bv))))))
                         ;    (fn []
                         ;      ;; Use transients data structures to speed up
                         ;      (transient {:horizontal (transient [])
                         ;                  :vertical   (transient [])}))))

                         ;; parse in parallel
                         (r/fold
                           (r/monoid
                             ;; Don't use destructuring to speed up
                             (fn [a b]
                               (let [ah (:horizontal a)
                                     av (:vertical a)
                                     bh (:horizontal b)
                                     bv (:vertical b)]
                                 (-> a
                                     (assoc! :horizontal (reduce conj! ah (persistent! bh)))
                                     (assoc! :vertical (reduce conj! av (persistent! bv))))))
                             (fn []
                               ;; Use transients data structures to speed up
                               (transient {:horizontal (transient [])
                                           :vertical   (transient [])})))
                           (fn [res i]
                             (if (or (-> i :id (= 0))
                                     (-> i :id (>= limit)))
                               ;; skip photos count line
                               res
                               ;; process next lines, dec index to compensate skipped line
                               (let [h-tvec (:horizontal res)
                                     v-tvec (:vertical res)
                                     idx    (-> i :id (dec))
                                     l      (:line i)
                                     p      (str/split l #" ")
                                     type   (get p 0)
                                     tags   (-> p
                                                ;; skip type and tags count
                                                (subvec 2)
                                                ;; convert to hash-set
                                                (set))
                                     item   {:id   (str idx)
                                             :type type
                                             :tags tags}]
                                 (if (= type "H")
                                   (assoc! res :horizontal (conj! h-tvec item))
                                   (assoc! res :vertical (conj! v-tvec item)))))))
                         (persistent!))
        h-items-vec (-> h-and-v :horizontal (persistent!))
        v-items-vec (-> h-and-v :vertical (persistent!))
        ;; convert vec -> to map, sequentially since building maps in parallel is expensive
        h-items-map (->> h-items-vec
                         (reduce
                           (fn [res v] (assoc! res (:id v) (:tags v)))
                           (transient {}))
                         (persistent!))
        v-items-map (->> v-items-vec
                         (partition-all 2)
                         (reduce
                           (fn [res v]
                             (assoc! res
                                     (->> v
                                          (map :id)
                                          (str/join " "))
                                     (->> v
                                          (map :tags)
                                          (apply set/union))))
                           (transient {}))
                         (persistent!))]
    ;(prn (type h-items-map))
    ;(prn (type v-items-map))
    ;; set/union to return PersistentHashMap - foldable!
    ;; Clojure decides whether it would be PerssistenArrayMap or PerssistentHashMap depending on size
    (merge h-items-map v-items-map)
    ;h-items-map
    ;nil
    ))


(defn parse [file limit]
  (let [parsed-lines (parse-file file)]

    (prn "total lines" (count parsed-lines))
    ;(-photo-lines->items-hash-2 photo-lines)
    ;(-photo-lines->items-hash-3 photo-lines)
    ;(count (-photo-lines->items-hash-1 photo-lines))
    ;(count (-photo-lines->items-hash-2 photo-lines))
    ;(count (-photo-lines->items-hash-3 photo-lines))
    ;(count (-photo-lines->items-hash-4 photo-lines))
    ;nil

    ;; Mixed algorithm looks the best so far
    (-parsed-lines->items-tags-map-4 parsed-lines limit)
    ;nil
    ))


;(time (parse "c_memorable_moments.txt"))
;(time (parse "b_lovely_landscapes.txt"))
;(time (parse "d_pet_pictures.txt"))


(defn score
  "Calculate a score value between 2 slides.
  Using laziness and 0 check to optimize."
  [a-tags-set b-tags-set]
  (let [ic   (count (set/intersection a-tags-set b-tags-set))
        ac   (count a-tags-set)
        bc   (count b-tags-set)
        acic (delay (- ac ic))
        bcic (delay (- bc ic))]
    (cond (= 0 ic) 0
          (= 0 @acic) 0
          (= 0 @bcic) 0
          :else (min ic @acic @bcic))))


(defn find-best-connect
  "Go through all connections and find first connection with max connections."
  [connections-vec items-connections-map except-ids-set]
  (->> connections-vec
       (partition-all 512)
       (map
         (fn [connections-group]
           (->> connections-group
                (reduce
                  (fn [best-candidate id-score-vec]
                    (let [id (first id-score-vec)]
                      (if (contains? except-ids-set id)
                        ;; skip connection which already added to result
                        best-candidate
                        ;; process other connections
                        (let [curr-candidate-connections      (get items-connections-map id)
                              curr-candidate-connection-count (count curr-candidate-connections)]
                          ;; compare best-candidate and current-candidate connections count
                          (if (-> best-candidate
                                  :connected-count
                                  (or 0)
                                  (< curr-candidate-connection-count))
                            ;; return new best-candidate
                            {:id              id
                             :connections-vec curr-candidate-connections
                             :connected-count curr-candidate-connection-count
                             :score           (second id-score-vec)}
                            ;; keep best-candidate
                            best-candidate)))))
                  ;; initial val - best-candidate - nil
                  nil))))
       ;; join partition results - best candidates among partitions
       (reduce
         (fn [best-candidate curr-candidate]
           (if (-> best-candidate
                   :connected-count
                   (or 0)
                   (< (-> curr-candidate
                          :connected-count
                          (or 0))))
             ;; return new best-candidate
             curr-candidate
             ;; keep best-candidate
             best-candidate))
         ;; initial val - best-candidate - nil
         nil)))


(defn estimate
  "Estimate each slide transition rates and sum total rate."
  [items-tags-map result-vec]
  (loop [items       result-vec
         total-score 0]
    (let [a (first items)]
      (if-some [b (second items)]
        (let [a-tags (get items-tags-map a)
              b-tags (get items-tags-map b)
              s      (score a-tags b-tags)]
          (recur
            ;; shift by 1
            (subvec items 1)
            ;; add score
            (+ total-score s)))
        ;; when second slide is nil - end is reached
        total-score))))


;; Todo: optimization
;; if a connected with b -> b connected with a
;; collect reversed connections and use them to reduce search collections


;; Make score - negative and find shortest-path

;; Use uber/graph and alg/shortest-path
;; https://github.com/Engelberg/ubergraph

;(->> {:a "dd"
;       :b "as"
;      :d "as"
;      :e "as"
;      :f "as"
;      :g "as"}
;     (partition-all 2)
;     (mapv (fn [a] (prn "ab" a))))
;"ab" ([:a "dd"] [:b "as"])
;"ab" ([:d "as"] [:e "as"])
;"ab" ([:f "as"] [:g "as"])

;{
; "123" #{"tag1" "tag2" "tag3"}
; "123" #{"tag1" "tag2" "tag3"}
; "123" #{"tag1" "tag2" "tag3"}
; "123" #{"tag1" "tag2" "tag3"}
; }

;; Next I find connections for each item
;; and take only max score connections
;; Next step would be to filter max score connections to find connection with max connections count
;; But we can't b/c not all connections are found yet.
;; Approach 1:
;; Combine building graph and search graph in 1 step

;; Go through each item-tags-map
;; Find max score connections
;; Go through each found max score connections
;; Find their max score connections
;; Recur until no connections found
;; Record result candidate and total-score

;; Drawbacks:
;; - Possible repeat finding connections
;; - In order to avoid repeat - need to store found connections result globally
;;   which makes impossible parallel processing.
;; Workaround is to store found connections map in global atom - shared resource among parallel threads.
;; Benefits:
;; - Traverse in parallel once item-tags-map instead of 2 times in parallel.
;; - Using shared atom with found-connections-map to optimize

;; Take 1 item-tags -> found connections
;; -> max -> each max - get connections count
;; -> using shared found-connections-map or iterating all except parent ->
;; take 1 max connection with max connections-count ->
;; go through it connections -> recur until no connections found -> store result-vec total score

;{
; "123" {
;        "456" 2
;        "432" 2
;        "456" 2
;        }
; }

;; contains set if ids which are not connected - 0 score
; found-unconnected-items-map -> { "123" #{"456 345 321} }
;

(defn items-tags-map->items-connections-map
  "Go through each item-tags and convert it to item-connections,
  where connections is a map of id->score.
  Filter only max score connections."
  [items-tags-map]
  (prn "step 1 generating items-connections-map")
  (let [found-unconnected-items-map (volatile! {})
        ;; Using atom with transients and parallel threads - inconsistently throws exceptions
        ;; But it works fast!
        ;; volatile! same as atom, but works faster
        found-item-connections-map  (volatile! {})
        items-connections-map       (->> items-tags-map
                                         (r/fold
                                           ;50000
                                           (r/monoid
                                             (fn [a b]
                                               (reduce conj! a (persistent! b)))
                                             (fn [] (transient [])))
                                           (fn [tres idx tags]
                                             ;(prn "step 1 processing idx" idx)
                                             ;; find connections for each item
                                             (let [found-connections     (get @found-item-connections-map idx)
                                                   found-connections-ids (keys found-connections)
                                                   connected             (->> idx
                                                                              (conj (get @found-unconnected-items-map idx))
                                                                              (concat found-connections-ids)
                                                                              ;; skip current item and found unconnected items
                                                                              (apply dissoc items-tags-map)

                                                                              ;; can't do nested r/fold on map throws error
                                                                              ;; https://dev.clojure.org/jira/browse/CLJ-1662?page=com.atlassian.jira.plugin.system.issuetabpanels:all-tabpanel

                                                                              ;; partition all by n chunks
                                                                              (partition-all 512)

                                                                              ;; Todo: is it ok to do pmap inside fold, too parallel?
                                                                              ;; pmap for parallel processing of partitions
                                                                              (map
                                                                                (fn [items-group]
                                                                                  (->> items-group
                                                                                       (reduce
                                                                                         (fn [r itm]
                                                                                           (let [best-score             (:score r)
                                                                                                 best-score-connections (:connections r)
                                                                                                 i                      (first itm)
                                                                                                 t                      (second itm)
                                                                                                 s                      (score tags t)]

                                                                                             ;; optimization
                                                                                             (if (<= s 0)
                                                                                               ;; store unconnected ids in shared map for future use
                                                                                               (vreset! found-unconnected-items-map
                                                                                                        (assoc
                                                                                                          @found-unconnected-items-map
                                                                                                          i
                                                                                                          (conj
                                                                                                            (get @found-unconnected-items-map i)
                                                                                                            idx)))

                                                                                               ;; store connected items in shared map for future use
                                                                                               ;; This makes results inconsistent b/c of multithreading
                                                                                               ;; Time increases but total-score is bigger ~ 450
                                                                                               (vreset! found-item-connections-map
                                                                                                        (assoc
                                                                                                          @found-item-connections-map
                                                                                                          i
                                                                                                          (assoc
                                                                                                            (get @found-item-connections-map i)
                                                                                                            idx
                                                                                                            s)))
                                                                                               )


                                                                                             ;; compare score
                                                                                             ;; and collect only connections with best score
                                                                                             (cond
                                                                                               ;; 0 score - not connected
                                                                                               (= 0 s) r

                                                                                               ;; less than best - skip
                                                                                               (< s best-score) r

                                                                                               ;; equal to best-score - append
                                                                                               (= s best-score)
                                                                                               (assoc! r :connections
                                                                                                       (conj! best-score-connections [i s]))

                                                                                               ;; new best-score - replace connections
                                                                                               (> s best-score)
                                                                                               (-> r
                                                                                                   (assoc! :score s)
                                                                                                   (assoc! :connections (transient [[i s]])))
                                                                                               )))
                                                                                         (transient
                                                                                           {:score       0
                                                                                            :connections (transient [])})))))
                                                                              ;; join partitions - reduce and combine each partition result
                                                                              ;; into 1 transient vector
                                                                              ;; keep in vector since no need in map
                                                                              (reduce
                                                                                (fn [res partition-res]
                                                                                  (->> partition-res
                                                                                       :connections
                                                                                       (persistent!)
                                                                                       (reduce
                                                                                         (fn [r i]
                                                                                           (conj! r [(first i) (second i)]))
                                                                                         res)))
                                                                                (transient []))
                                                                              (persistent!))
                                                   merged-connected      (->> found-connections
                                                                              (mapv (fn [[id score]] [id score]))
                                                                              (concat connected)
                                                                              (into []))]
                                               (conj! tres [idx merged-connected])))
                                           )
                                         (persistent!)

                                         ;; convert to map -> { id -> connections }
                                         ;; to optimize access by index
                                         (reduce
                                           (fn [r i] (assoc! r (first i) (second i)))
                                           (transient {}))
                                         (persistent!))]

    ;(prn (persistent! @found-unconnected-items-map))
    items-connections-map))


(defn find-best-result
  "Given items-connections map, calculate all results and return best by total-score."
  [items-connections-map]
  (prn "find best result...")
  (->> items-connections-map

       ;; custom fold
       #_(fold
           100
           (r/monoid
             (fn [best-res idx-connections]
               (let [idx             (first idx-connections)
                     connections-map (second idx-connections)
                     candidate       (loop [curr-con   connections-map
                                            ;; cur-res must be vector to persist order
                                            curr-res   [idx]
                                            ;; current score
                                            curr-score 0]
                                       (if (-> curr-con (count) (> 0))
                                         (if-some [best-connect
                                                   (find-best-connect curr-con
                                                                      items-connections-map
                                                                      (set curr-res))]
                                           (recur
                                             (:connections-map best-connect)
                                             (conj curr-res (:id best-connect))
                                             (+ curr-score (:score best-connect)))
                                           {:res         curr-res
                                            :total-score curr-score})
                                         {:res         curr-res
                                          :total-score curr-score}))]
                 (if (-> best-res
                         :total-score
                         (or 0)
                         (< (-> candidate
                                :total-score)))
                   ;; return new best-res
                   candidate
                   ;; keep best-res
                   best-res)))
             (fn [] nil))
           (r/monoid
             ;; compare total-scores and return best on combine
             (fn [a b]
               (if (-> a
                       :total-score
                       (or 0)
                       (> (-> b
                              :total-score
                              (or 0))))
                 a
                 b))
             ;; default best result is nil
             (fn [] nil)))

       ;; find best result by total-score
       (r/fold
         (r/monoid
           ;; compare total-scores and return best on combine
           (fn [a b]
             (if (-> a
                     :total-score
                     (or 0)
                     (> (-> b
                            :total-score
                            (or 0))))
               a
               b))
           ;; default best result is nil
           (fn [] nil))
         (fn [best-res idx connections-vec]
           ;(prn "step 2 processing idx" idx)
           (let [candidate (loop [curr-con   connections-vec
                                  ;; cur-res must be vector to persist order
                                  curr-res   [idx]
                                  ;; current score
                                  curr-score 0]
                             (if (-> curr-con (count) (> 0))
                               (if-some [best-connect
                                         (find-best-connect curr-con
                                                            items-connections-map
                                                            (set curr-res))]
                                 (recur
                                   (:connections-vec best-connect)
                                   (conj curr-res (:id best-connect))
                                   (+ curr-score (:score best-connect)))
                                 {:res         curr-res
                                  :total-score curr-score})
                               {:res         curr-res
                                :total-score curr-score}))]
             (if (-> best-res
                     :total-score
                     (or 0)
                     (< (-> candidate
                            :total-score)))
               ;; return new best-res
               candidate
               ;; keep best-res
               best-res))))))


(defn run2 [file-name limit]
  (let [items-tags-map        (parse file-name limit)
        items-connections-map (items-tags-map->items-connections-map items-tags-map)
        res                   (find-best-result items-connections-map)
        ]
    ;items-connections-map
    ;res
    (prn "res" (:total-score res) (:res res))
    (print-file file-name (:total-score res) (:res res))
    ))

;; Summary:
;; 1. Further optimization on combining steps of building and searching graph doesn't make sense - bad for parallel processing.
;; 2. Adding optimizations in build graph step
;;    - store and exclude reversed unconnected items - slightly speed optimization on small data set 1000, need to try on large.
;;    - store and exclude reversed connected items with merging them to other found connections
;;      made large improve in result total-score (x4 with verticals on small data set)
;;      result inconsistent but still always x4 higher than without it
;;      inconsistency b/c of multithreaded processing.
;; 3. With increased total score - processing time also increased from 300ms -> 2300ms on small data set with verticals.
;; 4. Current bottleneck is last step of recursive path search.
;;
;; Theory:
;; Current implementation is a Greedy algorithm which chooses locally best option, potentially given up the best solution,
;; https://en.wikipedia.org/wiki/Greedy_algorithm


;(time (run2 "a_example.txt" 1000))
;(time (run2 "b_lovely_landscapes.txt" 10000))
;(time (run2 "c_memorable_moments.txt" 2000))
;(time (run2 "d_pet_pictures.txt" 3000))
;(time (run2 "e_shiny_selfies.txt" 5000))




(def -res1 [
            "121"
            "201"
            "577 578"
            "604"
            "663"
            "251"
            "761"
            "985 988"
            "802"
            "589 590"
            "821"
            "891 892"
            "521 522"
            "924"
            "118 119"
            "793 794"
            "942"
            "728"
            "651 653"
            "927 928"
            "620 622"
            "759 760"
            "543 544"
            "795 796"
            "474 475"
            "114 115"
            "840 841"
            ])


(def -res2 [
            "42"
            "804"
            "299"
            "792"
            "425"
            "986"
            "396"
            "680"
            "29"
            "610"
            "311"
            "349"
            "786"
            "672"
            "658"
            "667"
            "787"
            "944"
            "45"
            "597"
            "975"
            "369"
            "554"
            "785"
            "43"
            "136"
            "315"
            "212"
            "851"
            "687"
            "791"
            "284"
            "342"
            "656"
            "77"
            "305"
            "400"
            "530"
            "621"
            "693"
            "863"
            "232"
            "649"
            "39"
            "282"
            "303"
            "593"
            "110"
            "489"
            "293"
            "875"
            "240"
            "389"
            "691"
            "177"
            "112"
            "14"
            "790"
            "646"
            "205"
            "374"
            "377"
            "954"
            "346"
            "842"
            "682"
            "914"
            "448"
            "228"
            "942"
            "728"
            "277"
            "254"
            "181"
            "546"
            "203"
            "38"
            "736"
            "889"
            "924"
            "89"
            "300"
            "393"
            "962"
            "802"
            "677"
            "268"
            "516"
            "4"
            "823"
            "460"
            "654"
            "275"
            "899"
            "533"
            "28"
            "604"
            "480"
            "663"
            "251"
            "761"
            "479"
            "559"
            "302"
            "472"
            "725"
            "907"
            "688"
            "48"
            "779"
            "963"
            "7"
            "614"
            "176"

            ])

;(estimate -res2)


;; Version 3 - Result slideshow may have unconnected slides, it's not a graph path problem since path may interrupt.
;; Todo: Try sort by tags count descending..
;; Shuffle
;; Parallel


(defn parsed-lines->item-tags-coll
  "Approach 4 - Mixed parallel.
  Reduce in parallel(fold) input lines into horizontal and vertical items.
  Using transient data structures.
  Output lazy list."
  [parsed-lines limit sort-key sort-order]
  (let [h-and-v         (->> parsed-lines
                             ;; reduce-kv to map line indexes
                             (reduce-kv
                               (fn [res idx l]
                                 (cond
                                   ;; skip total count line
                                   (= 0 idx) res

                                   ;; terminate reduce if limit reached
                                   (>= idx limit) (reduced
                                                    (conj!
                                                      res
                                                      {:id   idx
                                                       :line l}))
                                   ;; append line
                                   :else (conj!
                                           res
                                           {:id   idx
                                            :line l})))
                               (transient []))
                             (persistent!)
                             ;; parse in parallel
                             (r/fold
                               (r/monoid
                                 ;; Don't use destructuring to speed up
                                 (fn [a b]
                                   (let [ah (:horizontal a)
                                         av (:vertical a)
                                         bh (:horizontal b)
                                         bv (:vertical b)]
                                     (-> a
                                         (assoc! :horizontal (reduce conj! ah (persistent! bh)))
                                         (assoc! :vertical (reduce conj! av (persistent! bv))))))
                                 (fn []
                                   ;; Use transients data structures to speed up
                                   (transient {:horizontal (transient [])
                                               :vertical   (transient [])})))
                               (fn [res i]
                                 (let [h-tvec (:horizontal res)
                                       v-tvec (:vertical res)
                                       idx    (:id i)
                                       l      (:line i)
                                       p      (str/split l #" ")
                                       type   (get p 0)
                                       tags   (-> p
                                                  ;; skip type and tags count
                                                  (subvec 2)
                                                  ;; convert to hash-set
                                                  (set))
                                       item   {:id   (str idx)
                                               :type type
                                               :tags tags}]
                                   (if (= type "H")
                                     (assoc! res :horizontal (conj! h-tvec item))
                                     (assoc! res :vertical (conj! v-tvec item))))
                                 ;(if (or (-> i :id (= 0))
                                 ;        (-> i :id (>= limit)))
                                 ;  ;; skip photos count line
                                 ;  res
                                 ;  ;; process next lines, dec index to compensate skipped line
                                 ;  (let [h-tvec (:horizontal res)
                                 ;        v-tvec (:vertical res)
                                 ;        idx    (-> i :id (dec))
                                 ;        l      (:line i)
                                 ;        p      (str/split l #" ")
                                 ;        type   (get p 0)
                                 ;        tags   (-> p
                                 ;                   ;; skip type and tags count
                                 ;                   (subvec 2)
                                 ;                   ;; convert to hash-set
                                 ;                   (set))
                                 ;        item   {:id   (str idx)
                                 ;                :type type
                                 ;                :tags tags}]
                                 ;    (if (= type "H")
                                 ;      (assoc! res :horizontal (conj! h-tvec item))
                                 ;      (assoc! res :vertical (conj! v-tvec item)))))
                                 ))
                             (persistent!))
        h-items-vec     (-> h-and-v :horizontal (persistent!))
        v-items-vec     (-> h-and-v :vertical (persistent!))
        items-tags-coll (->> v-items-vec
                             (partition-all 2)
                             (reduce
                               (fn [res v]
                                 (if (-> v (count) (= 1))
                                   ;; skip unpaired vertical photo
                                   res
                                   (conj!
                                     res
                                     {:id   (->> v
                                                 (map :id)
                                                 (str/join " "))
                                      :type "V"
                                      :tags (->> v
                                                 (map :tags)
                                                 (apply set/union))})))
                               (transient []))
                             (persistent!)
                             ;; order in which concat and sort-by affects result - b/c it shuffle
                             (into h-items-vec)
                             ;(sort-by #(-> % :tags (count)) <)
                             ;(concat h-items-vec)
                             )]
    (cond->> items-tags-coll
             (some? sort-key) (sort-by sort-key (if (= :desc sort-order)
                                                  >
                                                  <)))))

;; Using vectors
(defn run3 [file-name config]
  (let [input-lines-limit      (:limit config)
        input-lines-sort-key   (:sort-key config)
        input-lines-sort-order (:sort-order config)
        max-score-limit        (:max-score-limit config)
        max-score-source-limit (or (:max-score-source-limit config)
                                   1000000)
        max-score-source-randomize? (:max-score-source-randomize? config)
        write-to-file?         (:write-to-file? config)
        parsed-lines           (parse-file file-name)
        items-tags-vector      (parsed-lines->item-tags-coll
                                 parsed-lines
                                 input-lines-limit
                                 input-lines-sort-key
                                 input-lines-sort-order)
        result                 (loop [last-added-item (first items-tags-vector)
                                      total-score     0
                                      res             (transient [(:id last-added-item)])
                                      not-added-items (rest items-tags-vector)]
                                 ;(prn "ttt1")
                                 (let [tags            (:tags last-added-item)
                                       best-connection (->> not-added-items
                                                            (if max-score-source-randomize?
                                                              (shuffle not-added-items))
                                                            (take max-score-source-limit)
                                                            (reduce
                                                              (fn [r itm]
                                                                (let [t     (:tags itm)
                                                                      score (score tags t)]
                                                                  (cond
                                                                    ;; not connected - continue
                                                                    (= 0 score) r

                                                                    ;; lower score - continue
                                                                    (-> r :score (>= score)) r

                                                                    ;; max score found - stop!
                                                                    (= max-score-limit score)
                                                                    (reduced
                                                                      (assoc!
                                                                        r
                                                                        :score score
                                                                        :tags t
                                                                        :id (:id itm)))

                                                                    ;; new max score found - save
                                                                    (-> r :score (< score))
                                                                    (assoc!
                                                                      r
                                                                      :score score
                                                                      :tags t
                                                                      :id (:id itm)))))
                                                              (transient
                                                                {:score 0
                                                                 :id    nil})))]
                                   (if-some [best-connection-id (:id best-connection)]
                                     (let [updated-total-score     (+ total-score (:score best-connection))
                                           updated-res             (conj! res best-connection-id)
                                           updated-not-added-items (->> not-added-items
                                                                        (filterv #(-> % :id (not= best-connection-id))))]
                                       ;(prn "found best connection with score" (:score best-connection))
                                       (if (empty? updated-not-added-items)
                                         {:total-score updated-total-score
                                          :result      updated-res}
                                         (recur
                                           best-connection
                                           updated-total-score
                                           updated-res
                                           updated-not-added-items)))
                                     (let [best-connection         (first not-added-items)
                                           best-connection-id      (:id best-connection)
                                           updated-res             (conj! res best-connection-id)
                                           updated-not-added-items (->> not-added-items
                                                                        (filterv #(-> % :id (not= best-connection-id))))]
                                       ;(prn "best connection not found, added random item")
                                       (if (empty? updated-not-added-items)
                                         {:total-score total-score
                                          :result      updated-res}
                                         (recur
                                           best-connection
                                           total-score
                                           updated-res
                                           updated-not-added-items))))))]
    (prn "res total-score" (:total-score result))
    (when write-to-file?
      (print-file file-name (:total-score result) (-> result :result (persistent!))))))


;(time (run3 "b_lovely_landscapes.txt" 2000))

;; Best score config for c_memorable_moments.txt
;"res total-score" 1549
;"Elapsed time: 709.282814 msecs"
;(time (run3 "c_memorable_moments.txt"
;            {:limit           2000
;             :sort-key        #(-> % :tags (count))
;             :sort-order      :asc
;             :max-score-limit 4
;             ;:max-score-source-limit 200
;             :max-score-source-randomize? false
;             :write-to-file?  false}))

;(time (run3 "d_pet_pictures.txt"
;            {:limit           10000
;             :sort-key        #(-> % :tags (count))
;             :sort-order      :asc
;             :max-score-limit 2
;             :max-score-source-limit 100
;             ;:max-score-source-randomize? true
;             :write-to-file?  false}))

;"res total-score" 12936
;"Elapsed time: 5968.601493 msecs"
;(time (run3 "d_pet_pictures.txt" 300000))

;"res total-score" 398029
;"Elapsed time: 6835157.683448 msecs"
;(time (run3 "e_shiny_selfies.txt" 5000000))

;"res total-score" 202569
;"Elapsed time: 1.3666506850731E7 msecs"
;(time (run3 "b_lovely_landscapes.txt" 500000))


;; Memoization doesn't help - just making it slower b/c of memory usage.
;(def score-memo (memoize score))

;; Using hash-maps - best result on small data set - 1462 out of all 750 items in 700ms!!
;; But it's slow on large data sets - when loop through hash-map - throws an error of OutOfMemory GC Overhead
;; It's better to use lazy collection here!!! - run3
(defn run4 [file-name limit]
  (let [items-tags-map              (parse file-name limit)
        first-item-tags-vec         (first items-tags-map)
        first-id                    (first first-item-tags-vec)
        first-tags                  (second first-item-tags-vec)
        found-unconnected-items-map (volatile! {})
        result                      (loop [last-added-item-id   first-id
                                           last-added-item-tags first-tags
                                           res                  (transient {:total-score 0
                                                                            :result      (transient [first-id])})
                                           not-added-items      (dissoc items-tags-map first-id)]
                                      ;(prn "left" (count not-added-items))
                                      (let [best-connection (->> last-added-item-id
                                                                 (get @found-unconnected-items-map)
                                                                 (apply dissoc not-added-items)
                                                                 (reduce-kv
                                                                   (fn [r i t]
                                                                     (let [score (score last-added-item-tags t)]
                                                                       (when (= score 0)
                                                                         (vreset!
                                                                           found-unconnected-items-map
                                                                           (assoc
                                                                             @found-unconnected-items-map
                                                                             i
                                                                             (conj
                                                                               (get @found-unconnected-items-map i)
                                                                               last-added-item-id))))
                                                                       (if (-> r :score (< score))
                                                                         (-> r
                                                                             (assoc! :score score)
                                                                             (assoc! :tags t)
                                                                             (assoc! :id i))
                                                                         r)))
                                                                   (transient {:score 0
                                                                               :id    nil})))]
                                        (if-some [best-connection-id (:id best-connection)]
                                          (let [updated-res             (-> res
                                                                            (assoc!
                                                                              :total-score
                                                                              (-> res
                                                                                  :total-score
                                                                                  (+ (:score best-connection))))
                                                                            (assoc!
                                                                              :result
                                                                              (-> res
                                                                                  :result
                                                                                  (conj! best-connection-id))))
                                                updated-not-added-items (dissoc not-added-items best-connection-id)]
                                            ;(prn "found best connection with score" (:score best-connection))
                                            (if (empty? updated-not-added-items)
                                              updated-res
                                              (recur
                                                best-connection-id
                                                (:tags best-connection)
                                                updated-res
                                                updated-not-added-items)))
                                          (let [first-item-tags-vec     (first not-added-items)
                                                best-connection-id      (first first-item-tags-vec)
                                                best-connection-tags    (second first-item-tags-vec)
                                                updated-res             (-> res
                                                                            (assoc!
                                                                              :result
                                                                              (-> res
                                                                                  :result
                                                                                  (conj! best-connection-id))))
                                                updated-not-added-items (dissoc not-added-items best-connection-id)]
                                            ;(prn "best connection not found, added random item")
                                            (if (empty? updated-not-added-items)
                                              updated-res
                                              (recur
                                                best-connection-id
                                                best-connection-tags
                                                updated-res
                                                updated-not-added-items))))))]
    (prn "res total-score" (:total-score result))
    (print-file file-name (:total-score result) (-> result :result (persistent!)))))


;; 1462 - 700ms
;(time (run4 "c_memorable_moments.txt" 2000))

;(time (run4 "d_pet_pictures.txt" 1000000))



;(let [items-tags-map (parse "c_memorable_moments.txt" 2000)]
;  (estimate items-tags-map [
;                            "339 341"
;                            "474 475"
;                            "861 862"
;                            "543 544"
;                            "581 582"
;                            "947 949"
;                            "259 262"
;                            "407 408"
;                            "993 995"
;                            "144 146"
;                            "740 744"
;                            "816 817"
;                            ])
;  )