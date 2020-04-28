(ns google-hash-code-2019.helpers
  (:require [clojure.string :as str]
            [clojure.set :as set]
            [clojure.core.reducers :as r])
  (:import (java.io File)
           (java.util HashSet)
           (clojure.core.reducers Cat)))


(def pwd (.getAbsolutePath (File. "")))


(defn parse-file [file-name]
  (->> file-name
       (str pwd "/resources/input/")
       (slurp)
       (str/split-lines)))

;(parse-file "a_example.txt")


(defn print-file [file-name result-score result-vec]
  (spit (str pwd "/resources/output/" result-score "_" file-name)
        (str (count result-vec)
             "\n"
             (str/join "\n" result-vec))))


;;
;; Parallel processing helpers
;;
;; Things to remember:
;; clojure.core.reducers/fold - is for parallel reduce - using fork-join with task stealing strategy
;; it accepts n - partition size default 512
;; it will split input data (by half) into chunks recursively, for parallel processing, until chunk size is > n
;; then combine each chunk result using combinef - it should have 2 arities:
;; 1. 0 params - to create initial val
;; 2. 2 params - to combine 2 chunks results
;; If no combinef - then reducef should have 2 arity, where 1 with 0 params
;;
;; Not all collections are foldable, only Vectors, Maps and clojure.core.reducers.Cat - catenation - which is just Tuple.
;; Result of r/fold can be anything what combinef - generate.
;;
;; **Merging big maps is expensive - so it's recommended to use Vectors or ArrayList with clojure.core.reducers.Cat**
;; **Also immutable collections are memory expensive - sometimes for performance transient (mutable) version are used.**
;;
;; **Building a map is not easily parallelizable,
;; most of the work is in managing the keys in the hashmap
;; which really has to be single-threaded,
;; since combination step takes time proportional to the size of the chunk
;; rather than constant time, you gain little by doing the chunks on a separate thread anyway**
;;
;; **One pitfall of clojure.core.reducers/fold is that the reducing function must
;; take either three or two arguments according to whether the foldable
;; collection is a map or not!**
;;
;; Use r/monoid to build combinef fn with 2 arities.
;; (r/monoid into vector) - vector is fn without params to generate initial value []
;; into - is fn with 2 params to merge 2 vectors


;; The example showcase use of r/cat to build HashSets (instead
;; of the default ArrayList) of distinct words in parallel
;; and then merge all together walking the binary tree produced by r/fold.
;(def book
;  (-> "http://www.gutenberg.org/files/2600/2600-0.txt"
;      slurp
;      str/split-lines))

;; Note: Transducers can be created by providing only transform/predicate fn without coll.
;; comp - is composing multiple transducers into 1 complex - fusion fn.
;(def r-word (comp
;              (r/map str/lower-case)
;              (r/remove str/blank?)
;              (r/map #(re-find #"\w+" %))
;              (r/mapcat #(str/split % #"\s+"))))
;
;(def btree
;  (r/fold
;    (r/cat #(HashSet.))
;    r/append!
;    (r-word book)))
;
;(defn merge-tree [root res]
;  (cond
;    (instance? Cat root)
;    (do (merge-tree (.left root) res) (merge-tree (.right root) res))
;    (instance? HashSet root)
;    (doto res (.addAll root))
;    :else res))
;
;(def distinct-words (merge-tree btree (HashSet.)))
;(count distinct-words)

;; Summary:
;; 1. Vectors, Maps, Car are foldable - available for parallel processing.
;; 2. Don't build maps in parallel - rather build vectors or lists,
;;    where append step taking constant time and doesn't depend on current result size.
;; 3. Consider when to apply parallel processing - b/c of fork-join overhead.
;; 4. Use vectors if order matter.
;; 5. Use into to merge 2 vectors.
;; 6. Use transient collections to gain performance.
;;    Use r/cat with default ArrayList, and r/append! for transient ArrayList append.


;(defn p2frequencies [coll]
;  (apply merge-with + (pmap clojure.core/frequencies (partition-all 512 coll))))

(defn tiled-pmap
  "Tiled pmap, for grouping map ops together to make parallel overhead worthwhile"
  [grain-size fun coll]
  (->> coll
       (partition-all grain-size)
       (pmap (fn [pgroup] (doall (map fun pgroup))))
       (apply concat)))

;(persistent! (reduce
;  (fn [res i]
;    (reduce conj! res (persistent! i))
;    )
;  (transient [])
;  [(transient [1 2 3])
;   (transient [4 5 6])
;   (transient [7 8 9])]))


;; What is faster r/fold or custom-fold with partition-all and pmap ???
;; fold using fork-join and pmap - just parallel threads with custom single threaded combine

;; partition all by n chunks
;(partition-all 512)
;; pmap for parallel processing of partitions
;(pmap
;  (fn [items-group]
;    (->> items-group
;         (reduce
;           (fn [r itm]
;             (let [i (first itm)
;                   t (second itm)
;                   s (score tags t)]
;               (if (> s 0)
;                 (conj! r [i s])
;                 r)))
;           (transient [])))))
;;; join partitions - reduce each partition vec into 1 res vec
;;; like flatten but for transient vectors
;(reduce
;  (fn [res i]
;    (reduce conj! res (persistent! i)))
;  (transient []))
;(persistent!)


;(.next (.iterator (java.util.ArrayList. [1 2 3 4])))

;(:test (transient {:test "123"}))
;(get (transient [1 2 3]) 1)


;; fix sort
;(def -test1 [{:obj {:id 6}}
;             {:obj {:id 1}}
;             {:obj {:id 4}}
;             {:obj {:id 0}}
;             {:obj {:id 5}}
;             {:obj {:id 5}}])
;
;(->> -test1
;     (sort (fn [a b] (-> a :obj :id (< (-> b :obj :id))))))



(defn fold
  "Custom fold based on pmap.
      partition-fn - monoid - 0 param - return partition initial val.
      combine-fn - monoid - 0 param - return result initial val"
  [partition-size partition-fn combine-fn coll]
  (->> coll
       (partition-all partition-size)
       (pmap
         (fn [items-group]
           (->> items-group
                (reduce partition-fn (partition-fn)))))
       (reduce combine-fn (combine-fn))))


(defn fold-kv
  "Custom fold based on pmap.
      partition-fn - monoid - 0 param - return partition initial val.
      combine-fn - monoid - 0 param - return result initial val"
  [partition-size partition-fn combine-fn coll]
  (->> coll
       (partition-all partition-size)
       (pmap
         (fn [items-group]
           (->> items-group
                (reduce-kv partition-fn (partition-fn)))))
       (reduce combine-fn (combine-fn))))