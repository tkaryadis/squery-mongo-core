(ns cmql-core.operators.uoperators)

;;most work like that,meaning with 1 function i update many fields
;;(update-function :field value
;;                 :field1 value1
;;                 ....)

;;with 1 update i cant update something 2 times (update conflict)
;;but i can have nested operations like  sort/slice etc on arrays for example

;;--------------------------tools----------------------------------------------------
(defn check-even [args]
  (if-not (even? (count args))
    (throw (Exception. "update  must contain an even number of arguments"))))

(defn get-function-map [keyword-f args]
  (let [- (check-even args)
        args-map (into {} (map vec (partition 2 args)))
        ;- (prn {(str "$" (name keyword-f)) args-map})
        ]
    {(str "$" (name keyword-f)) args-map}))

(defn remove-key-from-maps [k maps f-maps]
  (let [values (apply merge (filter #(some? %) (map #(get % k) maps)))
        maps (filter #(not (empty? %)) (map #(dissoc % k) maps))]
    [maps (assoc f-maps k values)]))

(defn do_ [maps]
  (loop [maps maps
         f-maps {}]
    (if (empty? maps)
      (do                                                   ;(prn f-maps)
        f-maps)
      (let [cur-map (first maps)
            cur-key (first (keys cur-map))
            [maps f-maps] (remove-key-from-maps cur-key maps f-maps)]
        (recur maps f-maps)))))

(defn update_
  ([[db coll] match-map update-map]
   (mc/update db (name coll) match-map (do_ update-map)))
  ([[db coll] match-map update-map options-map]
   (mc/update db (name coll) match-map (do_ update-map) options-map)))

;;------------------------field updates--------------------------------------------

(defn +_ [& args] (get-function-map :inc args))
(defn *_ [& args] (get-function-map :mul args))
(defn min_ [& args] (get-function-map :min args))
(defn max_ [& args] (get-function-map :max args))
;;doc is {myfield1 :true/(type "date")/(type "timestamp")}   true means (type "date")
(defn current-date [& args]  (get-function-map :currentDate args))
(defn rename [& args] (get-function-map :rename (map str args)))
(defn set_ [& args]  (get-function-map :set args))
;;only with { upsert: true },sets only if insert , not on update (update=>does nothing)
(defn set-insert [& args]  (get-function-map :setOnInsert args))
;;only the fields as arguments
(defn unset [& args] (get-function-map :unset (interleave args (take (count args) (repeat "")))))

;;-----------------------arrays---------------------------------------

;;variables
;;Array updates
;(Help operators
; $each => used in push/addToSet to add many elements
; $sort/$position used only in $push,to sort and to set the position where to add)

;;----------------------add to end , 1 or many-------------------------------------
;;add to end
(defn conj_ [& args]  (get-function-map :push args))
;;add to the end if element dont exist
(defn conjd [& args]  (get-function-map :addToSet args))
;;add array to the end
(defn concat_ [& args]
  (let [args (first (reduce (fn [[args index] v]
                              (if (even? index)
                                [(conj args v) (inc index)]
                                [(conj args {'$each v}) (inc index)]))
                            [[] 0]
                            args))]
    (get-function-map :push args)))
;;add array to the end (only the elements that not already exists)
(defn concatd [& args]
  (let [args (first (reduce (fn [[args index] v]
                              (if (even? index)
                                [(conj args v) (inc index)]
                                [(conj args {'$each v}) (inc index)]))
                            [[] 0]
                            args))]
    (get-function-map :addToSet args)))

;;--------------------push 1 or many at arbitary positions+process after(sort/slice)-----------------------------
;;Inside the push i add those options
;;  $each	 => add elements (useful if not end,else concat), TO USE ALL 3 OPTIONS BELOW I NEED EACH EVEN IF []
;;  $slice => take some of them , 0 => [] , 4 the first 4 , -4 the last 4
;;  $sort	 => 1/-1 if members not documents, {:fieldOfDocument 1/-1} if members are documents    ($each required,also)
;;  $position  => where to insert the elements(positive or negative(count from end)),by default i add to the end

;;i have to do that for all keys
(defn sort_ [m sort-option]
  (let [field-names (keys (get m "$push"))
        m (reduce (fn [m field-name]
                    (update-in m
                               ["$push" field-name]
                               (fn [each-map k v]
                                 (assoc each-map k v))
                               "$sort"
                               sort-option))
                  m
                  field-names)]
    m))

;;can also take negative argument
(defn take_ [m n]
  (let [field-names (keys (get m "$push"))
        m (reduce (fn [m field-name]
                    (update-in m
                               ["$push" field-name]
                               (fn [each-map k v]
                                 (assoc each-map k v))
                               "$slice"
                               n))
                  m
                  field-names)]
    m))

(defn position [m n]
  (let [field-names (keys (get m "$push"))
        m (reduce (fn [m field-name]
                    (update-in m
                               ["$push" field-name]
                               (fn [each-map k v]
                                 (assoc each-map k v))
                               "$position"
                               n))
                  m
                  field-names)]
    m))

;;remove first 1, remove last -1
(defn pop_ [& args] (get-function-map :pop (add-constant-values args -1)))
;;remove members that =value of satisfy the condition (condition uses query operators)
;; { $pull: { fruits: { $in: [ "apples", "oranges" ] }, vegetables: "carrots" } }
(defn pull [& args]  (get-function-map :pull args))
;;remove members that =with any value in the array,same as pull { $in: [ 0 , 5 ] }
;; { $pullAll: { scores: [ 0, 5 ] } }
(defn pull-all [& args]  (get-function-map :pullAll args))


;;------------------------update 1 member --------------------------------------------
;;1)$ => update only 1 member of the array
;;2)which element will be is determened on the match query (not index based) (1st argument)
;;                          (= or $elemMatch)
;;  { _id: 1, grades: 80 }   ;;here the match is the member of array grades with value 80
;;  (grades here = [70 80 100 ...])
;;  { _id: 1, grades: { $elemMatch: { grade: { $lte: 90 }, mean: { $gt: 80 } } }}
;;  (grades here = [{ grade: 80, mean: 75, std: 8 } ......])
;;3)when i have this element as $   (2nd argument)
;;  i do { $set: { "grades.$" : 82 } }
;;  or   { $set : { "grades.$.grade" : 82 }}    ;;if member is document i can use .


;;-----------------------update all members----------------------------------------------
;;$[] do something to all the elements of the array,like map
;; { $inc: { "grades.$[]": 10 } }   ;;all increase 10

;;(update_ grades
;         {}
;         (set_ "grades.$[element]" 100)
;         [{ :element (>=_ 100) }]
;         {:multi true
;          :array-filters [ { :element (>=_ 100) } ]})



