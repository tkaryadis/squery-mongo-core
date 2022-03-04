(ns cmql-core.operators.uoperators
  (:refer-clojure :exclude [+ * min max set conj pop sort])
  (:require [ cmql-core.internal.convert.stages :refer [cmql-vector->cmql-map]]
            [cmql-core.internal.convert.qoperators :refer [remove-q-combine-fields]]
            [clojure.core :as c]))

;;------------------------------tools------------------------------------------------------

(defn get-function-map [keyword-f args]
  (let [args (map #(if (keyword? %) (name %) %) args)
        args-map (into {} (map vec (partition 2 args)))
        ;- (prn {(str "$" (name keyword-f)) args-map})
        ]
    {"$__u__" {(str "$" (name keyword-f)) args-map}}))

(defn apply-to-odd [f coll]
  (map-indexed (fn [i a]
                 (if (even? i)
                   a
                   (f a)))
               coll))

#_(defn remove-key-from-maps [k maps f-maps]
    (let [values (apply merge (filter #(some? %) (map #(get % k) maps)))
          maps (filter #(not (empty? %)) (map #(dissoc % k) maps))]
      [maps (assoc f-maps k values)]))

#_(defn do_ [maps]
    (loop [maps maps
           f-maps {}]
      (if (empty? maps)
        (do                                                   ;(prn f-maps)
          f-maps)
        (let [cur-map (first maps)
              cur-key (first (keys cur-map))
              [maps f-maps] (remove-key-from-maps cur-key maps f-maps)]
          (recur maps f-maps)))))

;;---------------------------------operators----------------------------------------------------------------------------
;; instead of maps, i give arguments one after another like  field1 value1 field2 value2 etc

;;-----------------------------field update operators-------------------------------------------------------------------

(defn date-now
  "Call example
   All 3 first means date {$type 'date'}
   (now-date :field1 'date' :field1 true :field1 '' :field1 'timestamp')"
  [& args]
  (let [args (apply-to-odd (fn [a]
                             (if (or (= a true) (= a "") (= a "date"))
                               {"$type" "date"}
                               {"$type" "timestamp"}))
                           args)]
    (get-function-map :currentDate args)))


(defn u+ [& args] (get-function-map :inc args))
(defn u* [& args] (get-function-map :mul args))
(defn umin [& args] (get-function-map :min args))
(defn umax [& args] (get-function-map :max args))
(defn urename [& args] (get-function-map :rename (map name args)))
(defn uset [& args]  (get-function-map :set args))
(defn uset-on-insert [& args]  (get-function-map :setOnInsert args))
(defn uunset
  "(unset 'field1' 'field2' ...)"
  [& args]
  (get-function-map :unset (interleave args (take (count args) (repeat "")))))


;;-------------------------------------------arrays---------------------------------------------------------------------

(defn uconj-distinct [& args]  (get-function-map :addToSet args))
(defn uconj [& args]  (get-function-map :push args))
(defn upop [& args] (get-function-map :pop (flatten (into [] (cmql-vector->cmql-map args -1)))))
(defn pull [& args] {"$pull" (apply merge (remove-q-combine-fields args))})
(defn pull-all [& args]  (get-function-map :pullAll args))
(defn each
  "Used with $addToSet operator and the $push operator, to add each element
   Options can be 3 '(position n)', '(slice n)' , '(sort :field1 :!field2)'"
  ([ar & options]
   (apply (partial merge {"$each" ar}) options)))

(defn position [index] {"$position" index})
(defn slice [sliceIndex] {"$slice" sliceIndex})
(defn usort [& args] {"$sort" (cmql-vector->cmql-map args -1)})
