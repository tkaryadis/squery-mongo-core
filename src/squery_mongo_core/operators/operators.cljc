(ns squery-mongo-core.operators.operators
  (:refer-clojure :exclude [abs + inc - dec * mod
                            = not= > >= < <=
                            and or not
                            bit-or bit-and bit-xor bit-not
                            if-not cond
                            into type boolean double int long   nil? some? true? false?
                            string? int? decimal? double? boolean? number? rand 
                            let get get-in assoc assoc-in dissoc
                            concat conj contains? range reverse count take take-last subvec empty?
                            fn map filter reduce
                            first last merge max min
                            str subs re-find re-matcher re-seq replace identity ])
  (:require [clojure.core :as c]
            [squery-mongo-core.internal.convert.common :refer [args->nested-2args squery-var-ref->mql-var-ref]]
            [squery-mongo-core.internal.convert.operators :refer [squery-var-name get-nested-lets get-lets]]
            [squery-mongo-core.internal.convert.js-functions :refer [compile-library js-args-body js-info]]
            [squery-mongo-core.internal.convert.stages :refer [squery-vector->squery-map]]
            [squery-mongo-core.utils :refer [ordered-map]]))


;;---------------------------Arithmetic-------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

(defn abs
  "$abs"
  [e-n]
  {"$abs" e-n})

(defn +
  "$add
  Add numbers or numbers(milliseconds) to dates
  Call
  (+ 1 2)
  (+ :adate (* 5 60 1000))"
  [& es]
  {"$add" (vec es)})

(defn inc [e]
  {"$add" [e 1]})

(defn -
  "$substract
  Substract numbers or numbers(milliseconds) to dates
  Call
  (- 2 1)
  (- :adate (* 5 60 1000))"
  ([e-n]
   {"$substract" [0 e-n]})
  ([e-n & es]
   (args->nested-2args "$subtract" (apply (partial c/conj [e-n]) es))))

(defn dec [e]
  {"$subtract" [e 1]})

(defn *
  "$multiply
  Multiply numbers
  Call
  (* 1 2.3 5)
  "
  [& exprs]
  {"$multiply" (vec exprs)})

(defn pow
  "$pow"
  [e1-n e2-n]
  {"$pow" [ e1-n e2-n ] })

(defn exp
  "$exp
  e(Euler’s number) pow e-n"
  [e-n]
  {"$exp" e-n})

(defn ln
  "$ln"
  [e-n]
  {"$ln" e-n})

(defn log
  "$log
  log10 if no base"
  ([e-n]
   {"$log" e-n})
  ([e-n base]
   {"$log" [ e-n base]}))

(defn ceil
  "$ceil"
  [e-n]
  {"$ceil" e-n})

(defn floor
  "$floor"
  [e-n]
  {"$floor" e-n})

(defn round
  "$round
  e-n  integer, double, decimal, or long
  place-e-n = -20 < e-n < 100  default=0 if missing"
  ([e-n]
   { "$round"  [ e-n]})
  ([e-n place-e-n]
   { "$round"  [ e-n place-e-n ] }))

(defn trunc
  "$trunc
  e-n integer, double, decimal, or long
  place-e-n = -20 < e-n < 100  default=0 if missing
  like round,but just removes,not round"
  ([e-n]
   { "$trunc"  [ e-n]})
  ([e-n place-e-n]
   { "$trunc"  [ e-n place-e-n ] }))

(defn sqrt
  "$sqrt"
  [e-n]
  {"$sqrt" e-n})

(defn mod
  "$mod"
  [e1-n e2-n]
  {"$mod" [ e1-n e2-n ]})

(defn div
  "$divide"
  ([e-n]
   {"$divide" [1 e-n]})
  ([e-n & es]
   (args->nested-2args "$divide" (apply (partial c/conj [e-n]) es))))

;;--------------------------Trigonometry-----------------------------------

(defn acos [e]
  {"$acos" e})

(defn acosh [e]
  {"$acosh" e})

(defn asin [e]
  {"$asin" e})

(defn asinh [e]
  {"$asinh" e})

(defn atan [e]
  {"$atan" e})

(defn atan2 [e]
  {"$atan2" e})

(defn atanh [e]
  {"$atanh" e})

(defn cos [e]
  {"$cos" e})

(defn cosh [e]
  {"$cosh" e})

(defn sin [e]
  {"$sin" e})

(defn sinh [e]
  {"$sinh" e})

(defn degrees-to-radians [d-e]
  {"$degreesToRadians" d-e })

(defn radians-to-degrees [r-e]
  {"$radiansToDegrees" r-e})

;;---------------------------Comparison-------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

(defn cmp
  "$cmp
  Compares value, works on all types
  Returns 1 if e1>e2 , -1 if e1<e2 , 0 if e1=e2"
  [e1 e2]
  {"$cmp" [e1 e2]})

(declare and not)

(defn =
  "$eq
  Check for equality, works on all types"
  [e1 e2 & es]
  (args->nested-2args "$eq" (apply (partial c/conj [e1 e2]) es)))


(defn not=
  "$ne"
  ([e1 e2]
   {"$ne" [e1 e2]})
  ([e1 e2 & es]
   (not (apply (partial = e1 e2) es))))


(defn >
  "$gt"
  [e1 e2]
  {"$gt" [e1 e2]})

(defn >=
  "$gte"
  [e1 e2]
  {"$gte" [e1 e2]})

(defn <
  "$lt"
  [e1 e2]
  {"$lt"  [e1 e2]})

(defn <=
  "$lte"
  [e1 e2]
  {"$lte"  [e1 e2]})



;;---------------------------Boolean----------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

(defn and
  "$and
  Logical and,short circuit,false if one false/0/null/undefined"
  [& es]
  {"$and" (vec es)})

(defn or
  "$or
  Logical or,short circuit,false all false/0/null/undefined"
  [& es]
  {"$or" (vec es)})

(defn not
  "$not
  Logical not,true if not (false/0/null/undefined)"
  [e]
  {"$not" [e]})

(defn nor
  "$not $or
  (not- (or- ...))"
  [& es]
  (not (apply or es)))

;;---------------------------BitOperators-----------------------------------

(defn bit-and [& es]
  {"$bitAnd" (c/into [] es)})

(defn bit-or [& es]
  {"$bitOr" (c/into [] es)})

(defn bit-xor [& es]
  {"$bitXor" (c/into [] es)})

(defn bit-not [e]
  {"bitNot" e})


;;---------------------------Conditional------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

(defn if-
  "$cond
  Call
  (if- e-b e1 e2)"
  [e-b e1 e2]
  {"$cond" [e-b e1 e2]})

(defn if-not
  "$cond
  e1 when e-b false
  Call
  (if-not e-b e1 e2)"
  [e-b e1 e2]
  {"$cond" [(not e-b) e1 e2]})

(defn if-not-value
  "$ifNull
   returns the first not nil arg"
  [& args]
  {"$ifNull" (c/into [] args)})

(defn cond
  "$switch
  Call
  (cond- cond1 e1   ; cond1=boolean-e
         cond2 e2
         ...
         :else en)"
  [& args]
  (c/if-not (c/even? (c/count args))
    #?(:clj (throw (Exception. "cond- must contain an even number of forms"))
       :cljs nil)                                           ;; TODO
    (c/let [else? (c/= (c/second (c/reverse args)) :else)
          default-branch (if else?
                           (c/last args)
                           nil)
          branches (if else?
                     (c/drop-last (c/drop-last args))
                     args)
          branches (partition 2 branches)
          branches (vec (c/map (c/fn [[case then]]
                               {:case case :then then})
                             branches))]
      (if else?
        {"$switch" {:branches branches :default default-branch}}
        {"$switch" {:branches branches}}))))



;;---------------------------Literal----------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

(defn literal
  "$literal
  Used to show that it represents it self,without special MQL meaning.
  Call
  (literal- :a) => will mean a string '$a' not a field reference
  {$project {:myfield {$literal 1}}} => adds :myfield with value 1"
  [e]
  {"$literal" e})

;;---------------------------Types and Convert------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------


(defn expr
  "$expr
  Converts an aggregate expression to a query expression
  For example i can use aggregate operators in a match stage
  Allows us to use aggregate operators instead of query operators"
  [aggregation-e]
  {"$expr" aggregation-e})

(declare map fn)

(defn into [into-type array-or-doc-e]
  "$arrayToObject  (into {} array-or-doc-e)
   $objectToArray  (into [] array-or-doc-e)
  Convert object to array or array to object
  Call
  into {} => Array to document
             Array can be in
               [[k1 v1] [k2 v2] ...]
               [{:k k1 :v v1} {:k k2 :v v2} ...]
    If key same name takes the value of the last(like MQL)
  into [{}] => Object to array with object members
               [{:k k1 :v v1} {:k k2 :v v2} ...]
  into []   => Object to array with array members
               [[k1 v1] [k2 v2] ...]  (clojure-like)"
  (c/cond

    (c/= into-type {})
    {"$arrayToObject" [array-or-doc-e]}

    (c/= into-type [{}])
    {"$objectToArray" array-or-doc-e}

    :else
    (map
      (fn
        [:this.] [:this.k. :this.v.])
      {"$objectToArray" array-or-doc-e})))

(defn type
  "$type
  Returns a string representation of the argument type"
  [e]
  {"$type" e })

(defn convert
  "$convert
  Converts an expression to the type i specify using the type
  etype = double/string/objectId/bool/timestamp/date/int/long/decimal
  I can use the string representation of type or the number
  Call
  (convert- 1.99999 'bool')
  (convert- any-e 'bool' {:onError e1} {:onNull e2})  ;; optional"
  [input-e e-type & args]
  (c/merge (ordered-map "$convert" {:input input-e :to e-type}) (apply (partial c/merge {}) args)))

(defn boolean
  "$toBool"
  [e]
  {"$toBool" e})

;;$toDate is implemented above on types

(defn decimal
  "$toDecimal"
  [e]
  {"$toDecimal" e})

(defn double
  "$toDouble"
  [e]
  {"$toDouble" e})

(defn int
  "$toInt"
  [e]
  {"$toInt" e})

(defn long
  "$toLong"
  [e]
  {"$toLong" e})

(defn object-id
  "$toObjectId"
  [e]
  {"$toObjectId" e})

(defn string
  "$toString"
  [e]
  {"$toString" e})

(defn exists?
  "$type
  True if field exists (not=_ (type- e) 'missing')"
  [e]
  (not= (type e) "missing"))

(defn not-exists?
  "True if field missing (=_ (type- e) 'missing')"
  [e]
  (= (type e) "missing"))

(defn nil? [e]
  (= e nil))

;;TODO replace not-value with shorter way 

#_(defn if-nil
  "$ifNull
  Equivalent with (e nil or e not exist)
  (if- (not-value?- e) nill-e e)"
  [e nill-e]
  {"$ifNull" [e nill-e]})

#_(defn not-value [e]
  {"$ifNull" [e true]})
    
(defn not-value? [e]
  (or (= e nil)
      (= (type e) "missing")))
      


(defn some?
  "True if not nil"
  [e]
  (not= e nil))

(defn value? [e]
  (and (not= e nil)
       (not= (type e) "missing")))

(defn true? [e]
  (= e true))

(defn false? [e]
  (= e false))

(defn array?
  "$isArray"
  [e-array]
  {"$isArray"  [e-array]})

(defn object? [e]
  (= (type e) "object"))

(defn object-id? [e]
  (= (type e) "objectId"))

(defn regex? [e]
  (= (type e) "regex"))

(defn string? [e]
  (= (type e) "string"))

(defn int? [e]
  (= (type e) "int"))

(defn long? [e]
  (= (type e) "long"))

(defn decimal? [e]
  (= (type e) "decimal"))

(defn double? [e]
  (= (type e) "double"))

(defn boolean? [e]
  (= (type e) "bool"))

(defn date? [e]
  (= (type e) "date"))

(defn number? [e]
  (or
       (long? e)
       (int? e)
       (double? e)
       (decimal? e)))

(defn rand []
  {"$rand" {}})

;;---------------------------User Variables---------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

(defn leti
  "$let
  Not usefull,like let- but not allows dependent variables
  Call
  (leti- [:v1- e1 :v2- e2] e)"
  [vars body]
  (squery-mongo-core.internal.convert.operators/leti vars body))

(defn let
  "$let
  Allows depended variables => generate minimun nested mongo $let
  Call
  (let- [:x- 2
         :y- 1
         :z- (+_ :y- :x-)]
    :z-)"
  [vars body]
  (c/let [lets (vec (c/reverse (get-lets vars)))
        nested-lets (get-nested-lets lets body)]
    nested-lets))

(defn identity
  "returns its argument, useful when we want to use an array as argument"
  [e]
  {"$let" { "vars" {}, "in" e}})

;;---------------------------Arrays-----------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------


(declare concat subvec merge count conj reduce)

;;---------------------------------Get/SET(arays/object and nested)-----------------------------------------------------

;;WHEN I HAVE KEYS I CAN DO THE GET FAST var1=get 0 var2=var1.aggr

(declare get)

(defn- aget- [ar index]
  (c/cond

    (c/and (c/contains? index :icond) (c/contains? index :cond))
    (c/let [icond (c/get index :icond)
            gcond (c/get index :cond true)]
      (let [:a. ar]
           (if- gcond
             (let [:foundIndex. (get (reduce
                                       (fn [:d. :v.]
                                           (let [:fi. (get :d. 0)
                                                 :i. (get :d. 1)]
                                                (if- icond
                                                  [:i. (+ :i. 1)]
                                                  [:fi. (+ :i. 1)])))
                                       [nil 0]
                                       :a.)
                                     0)]
                  {"$arrayElemAt" [:a. :foundIndex.]})
             nil)))

    (c/contains? index :icond)
    (c/let [icond (c/get index :icond)]
      (let [:a. ar]
           (let [:foundIndex. (get (reduce
                                     (fn [:d. :v.]
                                         (let [:fi. (get :d. 0)
                                               :i. (get :d. 1)]
                                              (if- icond
                                                [:i. (+ :i. 1)]
                                                [:fi. (+ :i. 1)])))
                                     [nil 0]
                                     :a.)
                                   0)]
                {"$arrayElemAt" [:a. :foundIndex.]})))

    (c/and (c/contains? index :index) (c/contains? index :cond))
    (c/let [gcond (c/get index :cond true)]
      (let [:a. ar]
           (if- gcond
             {"$arrayElemAt" [:a. (c/get index :index)]}
             nil)))

    :else
    {"$arrayElemAt" [ar (c/get index :index)]}))


(defn- oget- [doc k]
  (c/cond

    (c/and (c/contains? k :kcond) (c/contains? k :cond))
    (c/let [kcond (c/get k :kcond)
            gcond (c/get k :cond)]
      (let [:o. doc]
           (if- gcond
             (reduce (fn [:p. :l.]
                         (let [:k. (get :l. 0)
                               :v. (get :l. 1)]
                              (if- kcond
                                (get :l. 1)
                                :p.)))
                     nil
                     (into [] :o.))
             nil)))

    (c/contains? k :kcond)
    (c/let [kcond (c/get k :kcond)]
      (let [:o. doc]
           (reduce (fn [:p. :l.]
                       (let [:k. (get :l. 0)
                             :v. (get :l. 1)]
                            (if- kcond
                              (get :l. 1)
                              :p.)))
                   nil
                   (into [] :o.))))


    (c/and (c/contains? k :key) (c/contains? k :cond))
    (c/let [k-str-var (c/get k :key)
            gcond (c/get k :cond)]
      (let [:o. doc]
            (if- gcond
              (if (c/and (c/string? k-str-var) (c/not (clojure.string/starts-with? k-str-var "$")))
                (keyword (c/str "o" "." k-str-var "."))                      ;; key is string constant(fast)
                (reduce (fn [:v. :m.] (if- (= (get :m. 0) k-str-var) (get :m. 1) :v.)) ;; key is variable(slow)
                        nil
                        (into [] :o.)))
              nil)))

    :else                                                   ;;key is constant or variable no cond
    (c/let [k-str-var (c/get k :key)]
      (if (c/and (c/string? k-str-var) (c/not (clojure.string/starts-with? k-str-var "$")))
        (let [:v0. doc] (keyword (c/str "v0" "." k-str-var ".")))                       ;; key is string constant(fast)
        (reduce (fn [:v. :m.] (if- (= (get :m. 0) k-str-var) (get :m. 1) :v.))          ;; key is variable(slow)
                nil
                (into [] doc))))))

(defn- index-to-map [index]
  (c/cond
    (c/map? index)
    index

    (c/number? index)
    {:index index}

    (c/and (c/string? index) (c/not (clojure.string/starts-with? index "$$")))
    {:key index}))

(defn get [array-or-doc index]
  (c/let [index (index-to-map index)]
    (if (c/or (c/contains? index :index) (c/contains? index :icond))
      (aget- array-or-doc index)
      (oget- array-or-doc index))))

(defn get-in
  [array-or-doc indexes]
  (c/reduce (c/fn [get-expr index]
              (get get-expr index))
            (get array-or-doc (c/get indexes 0))
            (c/rest indexes)))



;;-------------------------------------------SET (arrays/objects and nested)--------------------------------------------

(defn unset-field [doc field-not-variable]
  {"$unsetField" {:field (name field-not-variable)
                  :input doc}})
  
(defn set-field [doc field-not-variable value]
  {"$setField" {:field (name field-not-variable)
                :input doc
                :value value}})
                
(defn get-field [doc field-not-variable]
  {"$getField" {"field" field-not-variable, "input" doc}})

(declare merge filter take)

;;i can add it as option on the assoc-in , to do conj-at
(defn conj-at [ar index value]
  (let [:a. ar
         :i. index
         :asize. (count ar)]
        (if- (= :asize. :i.)
             (conj :a. value)
             (c/let [before-with-i (if- (> :i. 0)
                                      (take 0 :i. :a.)
                                      [])
                   after (if- (< :i. :asize.)
                              (take :i. :asize. :a.)
                              [])]
                  (concat before-with-i [value] after)))))

(defn- assoc-array- [ar index value]
  (let [:a. ar
         :i. index
         :asize. (count :a.)]
        (cond

          (> :i. :asize.)
          nil

          (= :i. :asize.)
          (conj :a. value)

          :else
          (c/let [before (if- (> :i. 0)
                            (take 0 :i. :a.)
                            [])
                after (if- (< :i. (- :asize. 1))
                           (take (+ :i. 1) :asize. :a.)
                           [])]
               (concat before [value] after)))))

(defn- dissoc-array- [ar index]
  (let [:a. ar
         :i. index
         :asize. (count :a.)]
        (cond

          (>= :i. :asize.)
          nil

          :else
          (c/let [before (if- (> :i. 0)
                           (take 0 :i. :a.)
                           [])
                after (if- (< :i. (- :asize. 1))
                        (take (+ :i. 1) :asize. :a.)
                        [])]
               (concat before after)))))

(defn- aset- [ar index value]
  (c/cond

    (c/and (c/contains? index :icond) (c/contains? index :cond))
    (c/let [icond (c/get index :icond)
          gcond (c/get index :cond)]
         (let [:a. ar
               :asize. (count :a.)]
               (if- gcond
                    ;;TODO test with reduce and conj,all with 1 parse(bad)
                    ;;TODO map and keep for each element if it was updated(better than below?)
                    ;;reduce to true/false find if update happened
                    ;;re-map to remove the extra true/false (3 parses of array)
                    (let [:foundIndex. (get (reduce
                                                (fn [:d. :v.]
                                                     (let [:fi. (get :d. 0)
                                                            :i. (get :d. 1)]
                                                           (if- icond
                                                                [:i. (+ :i. 1)]
                                                                [:fi. (+ :i. 1)])))
                                                [:asize. 0]
                                                :a.)
                                              0)]
                          (if (c/= value :REMOVE.)
                               (dissoc-array- :a. :foundIndex.)
                               (assoc-array- :a. :foundIndex. value)))
                    nil)))

    (c/contains? index :icond)
    (c/let [icond (c/get index :icond)]
         (let [:a. ar
               :asize. (count :a.)]
               (let [:foundIndex. (get (reduce
                                           (fn [:d. :v.]
                                                (let [:fi. (get :d. 0)
                                                      :i. (get :d. 1)]
                                                      (if- icond
                                                           [:i. (+ :i. 1)]
                                                           [:fi. (+ :i. 1)])))
                                           [:asize. 0]
                                           :a.)
                                         0)]
                     (if (c/= value :REMOVE.)
                         (dissoc-array- :a. :foundIndex.)
                         (assoc-array- :a. :foundIndex. value)))))

    (c/and (c/contains? index :index) (c/contains? index :cond))
    (c/let [gcond (c/get index :cond)
            index (c/get index :index)]
         (let [:a. ar]
               (if- gcond
                    (if (c/= value :REMOVE.)
                         (dissoc-array- :a. index)
                         (assoc-array- :a. index value))
                    nil)))

    :else      ;;index no global cond
    (c/let [index (c/get index :index)]
         (if (c/= value :REMOVE.)
           (dissoc-array- ar index)
           (assoc-array- ar index value)))))


(defn assoc-object- [doc key-e-string value-e]
  (let [:pairArray. [[key-e-string value-e]]]
        (merge doc (into {} :pairArray.))))

(defn- dissoc-object- [doc key-e-string]
  (if (c/and (c/string? key-e-string) (c/not (clojure.string/starts-with? key-e-string "$")))
    (let [:o. doc]
          (let [:existingKey. (exists? (keyword (c/str "o" "." key-e-string ".")))]
                (if- :existingKey.
                     (into {} (filter (fn [:m.]
                                             (not= (get :m. 0) key-e-string))
                                        (into [] :o.)))
                     :o.)))
    (into {} (filter (fn [:m.]
                            (not= (get :m. 0) key-e-string))
                       (into [] doc)))))

(defn- oset- [doc k v]
  (c/cond

    (c/and (c/contains? k :kcond) (c/contains? k :cond))
    (c/let [kcond (c/get k :kcond)
          gcond (c/get k :cond)]
         (let [:o. doc]
               (if- gcond
                    ;;TODO save into [] doc,concat the new pair and into {} (faster?)
                    (let [:foundK. (reduce (fn [:p. :l.]
                                                  (let [:k. (get :l. 0)
                                                        :v. (get :l. 1)]
                                                        (if- kcond
                                                             (get :l. 0)
                                                             :p.)))
                                             nil
                                             (into [] :o.))]
                          (if- (nil? :foundK.)
                               nil
                               (if (c/= v :REMOVE.)
                                 (dissoc-object- :o. :foundK.)
                                 (assoc-object- :o. :foundK. v))))
                    nil)))

    (c/contains? k :kcond)
    (c/let [kcond (c/get k :kcond)]
         (let [:o. doc]
               (let [:foundK. (reduce (fn [:p. :l.]
                                             (let [:k. (get :l. 0)
                                                   :v. (get :l. 1)]
                                                   (if- kcond
                                                        (get :l. 0)
                                                        :p.)))
                                        nil
                                        (into [] :o.))]
                     (if- (nil? :foundK.)
                          nil
                          (if (c/= v :REMOVE.)
                            (dissoc-object- :o. :foundK.)
                            (assoc-object- :o. :foundK. v))))))

    (c/and (c/contains? k :key) (c/contains? k :cond))
    (c/let [gcond (c/get k :cond)]
         (let [:o. doc]
               (if- gcond
                    (if (c/= v :REMOVE.)
                      (dissoc-object- :o. k)
                      (assoc-object- :o. k v))
                    nil)))

    ;;key(string or expression) and no conditions
    :else
    (c/let [k (c/get k :key)]
         (if (c/= v :REMOVE.)
           (dissoc-object- doc k)
           (let [:o. doc]
                 (assoc-object- :o. k v))))))

(defn assoc
  "if the index/key is variable,i give it as {:key variable-e} or {:index variable-e}"
  [array-or-doc index value]
  (c/let [index (index-to-map index)]
       (if (c/or (c/contains? index :index) (c/contains? index :icond))
         (aset- array-or-doc index value)
         (oset- array-or-doc index value))))

;;TODO expand the value with function
;;if icond/kcond if not found nil,else its assoc/dissoc
(defn assoc-in [array-or-doc indexes value]
  (if (c/= (c/count indexes) 1)
    (assoc array-or-doc (c/get indexes 0) value)
    (let [:arrayOrDoc. array-or-doc]
          (let [:nested. (assoc-in (get :arrayOrDoc. (c/get indexes 0)) (c/subvec indexes 1) value)]
                (if- (some? :nested.)
                     (assoc :arrayOrDoc. (c/get indexes 0) :nested.)
                     nil)))))

(defn dissoc-in [array-or-doc indexes]
  (assoc-in array-or-doc indexes :REMOVE.))

(defn dissoc [array-or-doc index]
  (dissoc-in array-or-doc [index]))

(defn concat
  "$concatArrays
  Concatenates arrays to return the concatenated array.
  If any of the argument is null returns null"
  [& e-arrays]
  {"$concatArrays" (vec e-arrays)})


(defn contains?
  "$in
  True is e is member(not index) of e-array"
  [e-array e]
  {"$in" [e e-array]})

(defn conj
  "with 2 args for arrays like $push but slow with concat not O(n)
   with 1 args its $push for groups and its fast"
  ([e-array e]
   (concat e-array [e]))
  ([e]
   {"$push" e}))

(defn conj-distinct
  "with 2 args for arrays, uses contains? is not O(1)
   with 1 arg is for groups, its $addToSet"
  ([e-array e]
   (if- (contains? e-array e)
        e-array
        (conj e-array e)))
  ([e]
   {"$addToSet" e}))


(defn index-of
  "$indexOfArray
  Returns the index of the matching member,or -1.
  I can search in a range only,of the array, for example from index=5 to index=10"
  ([e-array e start end]
   {"$indexOfArray" [ e-array e start end]})
  ([e-array e]
   {"$indexOfArray" [ e-array e]})
  ([e-array e start]
   {"$indexOfArray" [ e-array e start]}))

(defn range
  "$range
  Produces array of integers range(start end step)
  step defaults to 1
  start defaults to 0"
  ([end-e-integer]
   { "$range" [ 0 end-e-integer] })
  ([start-e-integer end-e-integer ]
   { "$range" [ start-e-integer end-e-integer] })
  ([start-e-integer end-e-integer step-e-integer]
   { "$range" [ start-e-integer end-e-integer step-e-integer ] }))

(defn reverse
  "$reverseArray
  reverses an array,if null => null"
  [e-array]
  {"$reverseArray" e-array})

(defn count
  "$size,$count"
  ([e-array]
   {"$size" e-array})
  ([]
   {"$count" {}}))

(declare min)

(defn subvec
  "uses $slic but works like clojure subvec
  Call
  (subvec [1 2 3 4 5 6 7] 2 4)   = 3,4
  Limit cases
   start>count => empty array (slice do that alone)
  Doesn't do circles if index>count,just takes till the end
  squery's take works like slice,see take"
  ([e-array start-e-n end-e-n]
   (if- (= start-e-n end-e-n)
        []
        { "$slice" [e-array start-e-n (if- (<= end-e-n (count e-array))
                                        (- end-e-n start-e-n)
                                        (if- (>= start-e-n (count e-array))
                                          1                                     ;;doesnt matter [] will return
                                          (- (count e-array) start-e-n)))]}))
  ([e-array start-e-n]
   { "$slice" [e-array start-e-n (if- (>= start-e-n (count e-array))
                                      1                                               ;;doesnt matter [] will return
                                      (- (count e-array) start-e-n))]}))

(defn ziparray
  "$zip
  Arguments are arrays.
  [[firsts of all arrays] [seconds of all arays] ...]
  if array not same sizes,the nth values dont added to result at all
  if useLongestLength (default is false)
    results = largest array size
  else
    results = smaller array size(if i dont have from not added in result)
  If not useLongestLength i always have elements to add from all arrays.
  If useLongestLength i give defaults,else defaults = [nil ...]
  defaults = defaults must have the same size as the number of arrays
  array that i take elements in order,to add to smaller arrays
  Call
  (ziparray- [a1 a2 a3] [b1 b2 b3] [c1 c2 c3])
  =>
  [[a1 b1 c1] [a2 b2 c2] [a3 b3 c3]
  "
  [& args]
  (c/let [options (c/last args)
        default? (c/and (c/map? options) (c/contains? options :defaults))
        add-nil? (c/and default? (c/nil? (c/get options :defaults)))
        add-defaults? (c/and default? (c/some? (c/get options :defaults)))
        arrays (vec (if default? (c/drop-last args) args))]
       (c/cond
         add-nil?
         {"$zip"  {:inputs arrays :useLongestLength true}}

         add-defaults?
         {
          "$zip" {
                  :inputs arrays
                  :useLongestLength true
                  :defaults (c/get options :defaults)
                  }
          }

         :else
         {"$zip" {:inputs arrays}})))

(defn empty? [e-array]
  (= e-array []))

(defn not-empty? [e-array]
  (not= e-array []))



(defn sort-array
  ([e-array sort-order]
   (if (c/number? sort-order)
     {"$sortArray" {"input" e-array "sortBy" sort-order}}
     {"$sortArray" {"input" e-array "sortBy" (squery-vector->squery-map sort-order -1)}}))
  ([e-array]
   (c/let [[a o] (if (clojure.string/starts-with? (name e-array) "!")
                 [(keyword (c/subs (name e-array) 1)) -1]
                 [e-array 1])]
     (sort-array a o))))

;;---------------------------Arrays(set operations)-------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

;; Those are set operations => duplicates are lost,and order is lost

(defn all-true?
  "$allElementsTrue"
  [e-array]
  {"$allElementsTrue" e-array})

(defn any-true?
  "$anyElementTrue"
  [e-array]
  {"$anyElementTrue"  e-array})

(defn subset?
  "$setIsSubset"
  [subset-e-array1 e-array2]
  {"$setIsSubset" [subset-e-array1 e-array2]})

(defn =sets
  "$setEquals"
  [& args]
  { "$setEquals"  (vec args)})

(defn union
  "$setUnion"
  [& args]
  { "$setUnion"  (vec args)})

(defn intersection
  "$setIntersection"
  [& args]
  { "$setIntersection"  (vec args)})

(defn difference
  "$setDifference"
  [e-array1 e-array2]
  {"$setDifference" [e-array1 e-array2]})

(defn disjoint?
  "No common"
  [e-array1 e-array2]
  (empty? (intersection e-array1 e-array2)))

(defn not-disjoint?
  "At least 1 common"
  [e-array1 e-array2]
  (not-empty? (intersection e-array1 e-array2)))

;;---------------------------Arrays(map/filter/reduce)----------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

(defn fn
  "fn- like Clojure fn
  Used only in map/filter/reduce , only as argument
  Call
  (fn- [:avar.] ...)         ; map/filter 1 arg
  (fn- [:avar. :bvar.] ...)  ; reduce 2 args
  *if argument is :this. or :value. no let(reduce)
   and not :as(filter/map) , is used.

  Reduce example
  (reduce (fn [:sum. :n.] (+ :sum. :n.)) [] :myarray)"
  [args body]
  (if (c/= (c/count args) 1)  ;;map or filter
    ;; i wrap them in a special map to know its a fn- type map
    {:mongo-fn [(vec (c/map squery-var-name args)) (squery-var-ref->mql-var-ref body)]}
    ;;reduce
    {:mongo-reduce-fn
     (c/let [arguments []
             arguments (if (c/not= (c/first args) :value.)
                         (c/conj arguments (c/first args)  "$$value")
                         arguments)
             arguments (if (c/not= (c/second args) :this.)
                         (c/conj arguments (c/second args)  "$$this")
                         arguments)]
       (if (c/empty? arguments)
         body
         (leti arguments body)))
     }))

(defn reduce
  "$reduce
  Call
  (reduce- (fn [:sum. :n.] (+ :sum. :n.)) [] :myarray)"
  [in-rfn init-e input-e-array]
  {"$reduce" {
              :input input-e-array
              :initialValue init-e
              :in (c/get in-rfn :mongo-reduce-fn)
              }})

(defn filter
  "$filter
  Call
  (filter  (fn [:n.] (> :n. 0)) :myarray)"
  [cond-fn e-array]
  (c/let [argument (c/first (c/first (c/get cond-fn :mongo-fn)))
          cond-value (c/second (c/get cond-fn :mongo-fn))]
    (if (c/= argument "this")
      {"$filter" {:input e-array :cond  cond-value}}
      {"$filter" {:input e-array :as argument :cond  cond-value}})))

(defn map
  "$map
  Call
  (map  (fn [:n.] (+ :n. 1)) :myarray)"
  [map-fn e-array]
  (c/let [argument (c/first (c/first (c/get map-fn :mongo-fn)))
          in-value (c/second (c/get map-fn :mongo-fn))]
    (if (c/= argument "this")
      {"$map" {:input e-array :in  in-value}}
      {"$map" {:input e-array :as argument :in  in-value}})))


;;-------------------------------bin-data------------------------------------

(defn bin-size [e]
  {"$binarySize" e})

;;---------------------------Documents(some are mixed with arrays aboves)----------------------

(defn bson-size [e-doc]
  { "$bsonSize" e-doc })

;;-------------------------Accumulators(not arrays)------------------------

(defn sort-first [sort-vec result-e]
  {"$top" {"sortBy" (squery-vector->squery-map sort-vec -1)
           "output" result-e}})

(defn sort-take [n sort-vec result-e]
  {"$topN" {"sortBy" (squery-vector->squery-map sort-vec -1)
            "output" result-e
            "n" n}})

(defn sort-last [sort-vec result-e]
  {"$bottom" {"sortBy" (squery-vector->squery-map sort-vec -1)
              "output" result-e}})

(defn sort-take-last [n sort-vec result-e]
  {"$bottomN" {"sortBy" (squery-vector->squery-map sort-vec -1)
               "output" result-e
               "n" n}})

;;count/conj/conj-distinct used with 1 arg, with the 2 arg array version

;;--------------------------Accumulators+arrays-----------------------------
;;arrays + bucket/bucketAuto/group/windowFields

(defn first
  "$first"
  [e-group-e-array]
  {"$first" e-group-e-array})

(defn take
  "$firstN
   $slice if start-index sand its only for arrays"
  ([e-n e-group-e-array]
   {"$firstN" {"input" e-group-e-array "n" e-n}})
  ([e-n start-index e-array]
   { "$slice" [e-array start-index e-n]}))

(defn last
  "$last
   sort before else natural order"
  [e-group-e-array]
  {"$last" e-group-e-array})

(defn take-last
  "$lastN
   used in both groups and arrays"
  [e-n e-group]
  {"$lastN" {"input" e-group "n" e-n}})

(defn avg
  "$avg
  Can be used in
  $group
  $match if inside {$expr ...}
  $addFields=$set
  $project=$unset
  $replaceRoot=$replaceWith
  Ignores non numeric(doesnt count as members)
  if all non-numeric returns null
  Call
  (avg- e) ; the array is made from group 1e/member
  (avg- array)
  (avg- number1 number2 ...)"
  ([group-or-array-e]
   {"$avg" group-or-array-e})
  ([e1 e2 & es]
   {"$avg" (apply (partial c/conj [e1 e2]) es)}))

(defn sum
  "$sum
  Can be used in
  $group
  $match if inside {$expr ...}
  $addFields=$set
  $project=$unset
  $replaceRoot=$replaceWith
  Ignores non numeric,if all non-numeric returns 0
  Call
  (sum- e)   ; the array is made from group 1e/member
  (sum- array)
  (sum- number1 number2 ...)"
  ([group-or-array-e]
   {"$sum"  group-or-array-e})
  ([e1 e2 & es]
   {"$sum" (apply (partial c/conj [e1 e2]) es)}))

(defn max
  "$max
  Can be used in
  addFields/bucket/$bucketAuto/group/match/project/replaceRoot/window
  With 1 argument its always group or array, else >1 argument
  Call
  (max e)  ; the array is made from group 1e/member
  (max array)
  (max number1 number2 ...)"
  ([group-or-array-e]
   {"$max" group-or-array-e})
  ([e1 e2 & es]
   {"$max" (c/apply (c/partial c/conj [e1 e2]) es)}))

(defn take-max
  "$$maxN"
  [e-n e-group-e-array]
  {"$maxN" {"input" e-group-e-array "n" e-n}})

(defn min
  "$min
  Can be used in
  $group
  $match if inside {$expr ...}
  $addFields=$set
  $project=$unset
  $replaceRoot=$replaceWith
  Compares(using $cmp) and returns the min
  Call
  (min- e)   ; the array is made from group 1e/member
  (min- array)
  (min- number1 number2 ...)"
  ([group-or-array-e]
   {"$min" group-or-array-e})
  ([e1 e2 & es]
   {"$min" (apply (partial c/conj [e1 e2]) es)}))

(defn take-min
  "$minN"
  [e-n e-group-e-array]
  {"$minN" {"input" e-group-e-array "n" e-n}})

(defn merge
  "$mergeObjects"
  ([group-or-array-e]
   {"$mergeObjects" group-or-array-e})
  ([e1 e2 & es]
   {"$mergeObjects" (apply (partial c/conj [e1 e2]) es)}))

(defn percentile [e p-vec method-str]
  {"$percentile" {"input" e "p"  p-vec "method" method-str}})

;;-------------------------------------statistical---------------------------------------------------------------

(defn median [e-n method-str]
  {"$median"  {"input" e-n, "method" method-str}})

;;TODO (statistical)
(defn stdDevPop
  "$stdDevPop"
  ([group-or-array-e]
   {"$stdDevPop" group-or-array-e})
  ([e1 e2 & es]
   {"$stdDevPop" (apply (partial c/conj [e1 e2]) es)}))

;;TODO (statistical)
(defn stdDevSamp
  "$stdDevSamp"
  ([group-or-array-e]
   {"$stdDevSamp" group-or-array-e})
  ([e1 e2 & es]
   {"$stdDevSamp" (apply (partial c/conj [e1 e2]) es)}))



;;---------------------------window(ONLY)-------------------------

(defn rank []
  { "$rank" {}})

(defn dense-rank []
  { "$denseRank" {}})

(defn covariance-pop [number-e1 number-e2]
  {"$covariancePop" [number-e1 number-e2]})

(defn covariance-samp [number-e1 number-e2]
  {"$covarianceSamp" [number-e1 number-e2]})

(defn derivative
  "$derivative
  for averages inside the window
   unit=week/day/hour/minute/second/millisecond"
  [numeric-e unit-str]
  {"$derivative" {"input" numeric-e "unit" unit-str}})

(defn position
  "$documentNumber
   gives a number to each doc, based on the position
   it has after sort in the window"
  []
  { "$documentNumber" {}})

(defn exp-moving-avg [input-e n-or-alpha]
  (c/let [m {"input" input-e}
          m (if (c/double? n-or-alpha)
              (assoc m :alpha n-or-alpha)
              (assoc m :N n-or-alpha))]
    {"$expMovingAvg" m}))

(defn integral [input-e unit-e]
  {"$integral" {"input" input-e, "unit" unit-e}})

(defn linear-fill
  "$linearFill"
  [e]
  {"$linearFill" e})

(defn locf [e]
  {"$locf" e})

(defn shift [output-e by-int default-e]
  {
       "$shift" {
                "output" output-e
                "by" by-int
                "default" default-e
                }
       })

;;---------------------------Strings----------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

(defn str
  "$concat
  Concatenates any number of strings,if 1 or more null => null"
  [& es-s]
  {"$concat" (vec es-s)})

(defn lower-case
  "$toLower"
  [e-s]
  {"$toLower" e-s})

(defn upper-case
  "$toUpper"
  [e-s]
  {"$toUpper" e-s})

(defn index-of-in-bytes
  "$indexOfBytes
  UTF-8 byte index of the first occurrence or -1"
  ([e-s sub-e-s start end]
   {"$indexOfBytes" [ e-s sub-e-s start end]})
  ([e-s sub-e-s]
   {"$indexOfBytes" [ e-s sub-e-s]})
  ([e-s sub-e-s start]
   {"$indexOfBytes" [ e-s sub-e-s start]}))

(defn index-of-str
  "$indexOfCP
  UTF-8 code point index of the first occurrence or -1"
  ([e-s sub-e-s start end]
   {"$indexOfCP" [ e-s sub-e-s start end]})
  ([e-s sub-e-s]
   {"$indexOfCP" [ e-s sub-e-s]})
  ([e-s sub-e-s start]
   {"$indexOfCP" [ e-s sub-e-s start]}))

(defn count-str
  "$strLenCP"
  [e-s]
  {"$strLenCP" e-s })

(defn count-str-bytes [e-s]
  {"$strLenBytes" e-s })

(defn subs
  "$substrCP
  Take the substring,starting from start to end UTF8 indexes
  Counts in UTF-8 code points,negatives indexes are not allowed
  Call like Clojure's subs
  (subs s start)            ;; end=string size
  (subs s start end)"
  ([e-s start-e-n end-e-n]
   (if (c/and (c/number? start-e-n) (c/number? end-e-n))
     {"$substrCP" [ e-s  start-e-n (c/- end-e-n start-e-n)]}
     ;;else do the - on server
     {"$substrCP" [ e-s  start-e-n (- end-e-n start-e-n)]}))
  ([e-s start-e-n]
   (subs e-s start-e-n (count-str e-s))))
   
(defn take-str
  "$substrCP
  works like MQL substrCP, but different argument order"
  ([start-index n e-str]
   { "$substrCP" [e-str start-index n]})
  ([n e-str ]
   { "$substrCP" [e-str n]}))
   


(defn subs-bytes
  "$substrBytes
  Take the substring,starting from start to end BYTES indexes
  Counts in BYTES,negatives indexes are not allowed
  Call like Clojure's subs
  (subs s start)     ;; end=string size
  (subs s start end)"
  [e-s start-e-n end-e-n]
  (if (c/and (c/number? start-e-n)
           (c/number? end-e-n))
    (c/let [n (- end-e-n start-e-n)]
         {"$substrBytes" [ e-s  start-e-n n]}
         {"$substrBytes" [ e-s  start-e-n (- end-e-n start-e-n)]})))

(defn split
  "$split
  splits on e2-string(no regex) returns array"
  [e1-string e2-string]
  {"$split" [ e1-string e2-string ]})


(defn cmp-str-icase
  "$strcasecmp
  string equality ignore case"
  [e1-string e2-string]
  {"$strcasecmp" [e1-string e2-string]})

(defn triml
  "$ltrim
  Deletes the characters ofr trim-e-string from the start
  Character order in trim-e-string is ignored,its char1 or char2 ...
  and i can have multiple matches.Stop deleting when no match.
  for example  gggddeee ged  will reutrn '' delete all chars"
  ([e-string]
   {"$ltrim" { :input e-string}})
  ([e-string trim-e-string]
   {"$ltrim" { :input e-string :chars trim-e-string }}))

(defn trimr
  "$rtrim
  Deletes the characters ofr trim-e-string from the end
  Character order in trim-e-string is ignored,its char1 or char2 ...
  and i can have multiple matches.Stop deleting when no match.
  for example  gggddeee ged  will reutrn '' delete all chars"
  ([e-string]
   {"$rtrim" { :input e-string}})
  ([e-string trim-e-string]
   {"$rtrim" { :input e-string :chars trim-e-string }}))

(defn trim
  "$trim
  Deletes the characters ofr trim-e-string from the end and from start
  Character order in trim-e-string is ignored,its char1 or char2 ...
  and i can have multiple matches.Stop deleting when no match.
  for example  gggddeee ged  will reutrn '' delete all chars"
  ([e-string]
   {"$trim" { :input e-string}})
  ([e-string trim-e-string]
   {"$trim" { :input e-string :chars trim-e-string }}))

;;---------------------------Strings(regex)---------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

;;TODO  check again monogo+clojure and mix it better

;;MongoDB uses Perl compatible regular expressions (i.e. “PCRE” ) version 8.41 with UTF-8 support
;; https://www.pcre.org
;; http://www.rexegg.com/pcre-doc/08.40/pcresyntax.html
;; https://www.debuggex.com/cheatsheet/regex/pcre
;; https://mariadb.com/kb/en/pcre/

(defn re-find?
  "$regexMatch"
  ([pattern-expression string-s-expression options]
   {"$regexMatch" {
                   "input"   string-s-expression
                   "regex"   pattern-expression
                   "options" options
                   }})
  ([pattern-expression string-s-expression]
   { "$regexMatch" {
                    "input"   string-s-expression
                    "regex"   pattern-expression
                    }}))

(defn re-find
  "$regexFind"
  ([pattern-expression string-s-expression options]
   { "$regexFind" {
                   "input"   string-s-expression
                   "regex"   pattern-expression
                   "options" options
                   }})
  ([pattern-expression string-s-expression]
   { "$regexFind" {
                   "input"   string-s-expression
                   "regex"   pattern-expression
                   }}))

(defn re-seq
  "$regexFindAll"
  ([pattern-expression string-s-expression options]
   { "$regexFindAll" {
                      "input"   string-s-expression
                      "regex"   pattern-expression
                      "options" options
                      }})
  ([pattern-expression string-s-expression]
   { "$regexFindAll" {
                      "input"   string-s-expression
                      "regex"   pattern-expression
                      }}))

(defn replace
  "$replaceOne
   match is string not pattern"
  [s str-e replacement]
  {"$replaceOne" { "input" s  "find" str-e "replacement" replacement } })

(defn replace-all
  "$replaceAll
   match is string not pattern"
  [s str-e replacement]
  {"$replaceAll" { "input" s  "find" str-e "replacement" replacement } })

;;---------------------------Dates------------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

(defn inc-date
  "$dateAdd
   unit=year/quarter/week/month/day/hour/minute/second/millisecond"
  [date-e amount-e unit-e & timezone]
  (c/let [m {"$dateAdd" {"startDate" date-e
                         "unit" unit-e
                         "amount" amount-e}}
          m (if (c/not (c/empty? timezone))
              (c/assoc m "timezone" (c/first timezone))
              m)]
    m))

(defn dec-date
  "$dateSubtract"
  [date-e amount-e unit-e & timezone]
  (c/let [m {"$dateSubtract" {"startDate" date-e
                              "unit" unit-e
                              "amount" amount-e}}
          m (if (c/not (c/empty? timezone))
              (c/assoc m "timezone" (c/first timezone))
              m)]
    m))

(defn date-diff
  "$dateDiff"
  [date1-e date2-e unit-e & optionals-map]
  (c/let [m {"$dateDiff" {"startDate" date1-e
                          "endDate" date2-e
                          "unit" unit-e}}
          m (if (c/get optionals-map :timezone)
              (c/assoc m "timezone" (c/get optionals-map :timezone))
              m)
          m (if (c/get optionals-map :startOfWeek)
              (c/assoc m "startOfWeek" (c/get optionals-map :startOfWeek))
              m)]
    m))

(defn date-from-parts
  "$dateFromParts"
  [op-map]
  {"$dateFromParts" op-map})

(defn date-from-string
  "$dateFromString"
  [date-e-str & options]
  (c/let [options (c/map (c/fn [o] (if (c/string? o)
                                     {:format o}
                                     o))
                         options)
          m {:dateString date-e-str}]
    {"$dateFromString" (c/merge m (c/apply (c/partial c/merge {}) options))}))

(defn date-to-parts
  "$dateToParts"
  [date-e & optionals-map]
  (c/let [m {:date date-e}
          m (if (c/not (c/empty? optionals-map))
              (c/merge m (c/first optionals-map))
              m)]
    {"$dateToParts" m}))

(defn date-to-string
  "$dateToString"
  [date-e optionals-map]
  (c/let [m {:date date-e}
          m (if (c/not (c/empty? optionals-map))
              (c/merge m (c/first optionals-map))
              m)]
    {"$dateToString" m}))


(defn date-trunc
  "$dateTrunc"
  [date-e unit-e optionals-map]
  (c/let [m {:date date-e
             :unit unit-e}
          m (if (c/not (c/empty? optionals-map))
              (c/merge m (c/first optionals-map))
              m)]
    {"$dateTrunc" m}))

(defn day-of-month [e-date]
  {"$dayOfMonth" e-date })

(defn day-of-week [e-date]
  {"$dayOfWeek" e-date })

(defn day-of-year [e-date]
  {"$dayOfYear" e-date })

(defn date-hour [e-date]
  {"$hour" e-date })

(defn iso-day-of-week [e-date]
  {"isoDayOfWeek" e-date})

(defn iso-week [e-date]
  {"isoWeek" e-date})

(defn iso-week-year [e-date]
  {"isoWeekYear" e-date})

(defn date-millisecond [e-date]
  {"$millisecond" e-date })

(defn date-minute [e-date]
  {"$minute" e-date })

(defn date-month [e-date]
  {"$month" e-date })

(defn date-second [e-date]
  {"$second" e-date })

(defn date [e]
  {"$toDate" e})

(defn date-week [e-date]
  {"$week" e-date })

(defn date-year [e-date]
  {"$year" e-date })

(defn ts-increment [e]
  { "$tsIncrement" e })

(defn $ts-second [e]
  { "$tsSecond" e})

;;-------------------------------------various-------------------------------------------------------------------------

(defn sampleRate [e-double]
  {"$sampleRate" e-double})

;;-------------------------------------Cljs-compile---------------------------------------------------------------------

(defn compile-functions
  "Compiles one cljs to one javascript function with the same name.
  The javascript function code,is saved in   js/f-name.js
  The default location to look for js code"
  [f-name & f-names]
  (dorun (c/map (c/fn [f-name]
                  (compile-library f-name))
              (c/cons f-name f-names))))

;;-------------------------------------js-operators---------------------------------------------------------------------

#_(defn js [args & body-str-or-keyword]
    (let [body (if (keyword? (first body-str-or-keyword))
                 (get @js-fn-map (first body-str-or-keyword))
                 (apply str (interpose " " body-str-or-keyword)))]
         {
          "$function" {
                       :args args
                       :lang "js"
                       :body body
                       }
          }))



(defn njs
  "Makes many nested javascirpt calls,1 javascript function => 1 $function call
  Its for optimization,and much faster.
  In MongoDB n nested calls => n $function calls
  Using njs it can be 1 $function call

  Allows wrapping of js code,to make it look like MongoQL/Clojure code.
  The 3 nested calls will be 1 $function call,and looks like normal aggregation operators.
  (add 2 key-value pairs to a doc,and remove one)

   (ejs (dissoc-j (assoc-j (assoc-j :mydoc 'e' 5) 'f' 6) 'f'))

  After finishing with the nesting use ejs to execute the function.
  Call
    No wrapper,call it from query
    (ejs (njs f-name f-args f-code))

    Wrapper call it on wrapper defn (here on njs no code is given assumes js/assoc_j.js exists)
     (defn assoc-j [doc k v]
      (njs :assoc_j [doc k v]))

    Use the wrapper in the query
    (ejs (assoc-j :mydoc 'akey' 'avalue')
  "
  [f-name f-args & f-code]
  (c/let [[args body] (js-args-body f-name f-args f-code)]
       (js-info args body)))

(defn ejs
  "Execute the javascript code,calls $function operator
  Used in 2 ways
  execute nested js => 1 argument
  execute code from file or argument => 2 or 3+ arguments

  Call 1 argument

  No wrappers 1 nested call (no need for njs use ejs with 3+ arguments directly)
   (ejs (njs f-name f-args f-code))

  No wrappers 2 nested call (hard to read,use wrappers instead)
    (ejs :assoc_j
         [(njs :assoc_j [:myobject 'akey' 'avalue']) ;; doc after first call (nested)
         'akey1'
         'avalue1'])

  Wrappers (here all functions use njs on their body)(usefull for nested calls)
  The prefered way,makes it look like regular MongoQL operator

    (ejs (dissoc-j (assoc-j (assoc-j :mydoc 'e' 5) 'f' 6) 'f'))

    assoc-j or dissoc-j wrapper uses njs
    (defn assoc-cj [doc k v]
      (njs :assoc_cj [doc k v]))

  Call 2 arguments

  Assumes that the js/fname.js already exists so no need for code
  (ejs :assoc_j [:myobject 'akey' 'avalue']

  Call 3+ arguments

  If javascript saves the code in   js/fname.js
  If Clojurescript compiles and saves the code in  js/fname.js
  Then execute the code,with $function
  (if code is given old-code is replaced if js/fname.js already existed)

  fname(string/keyword),f-args vector,function-boy(each line can be 1 arg)

  Javascript

  (ejs :assoc_j
       [:myobject 'akey' 'avalue']
       'function assoc_j (a,k,v) {'
       ' a[k]=v; return a;}')

  ClojureScript

  (ejs :assoc_cj
       [:myobject 'akey' 'avalue']
      '(defn assoc_cj [a k v]
         (do (aset a k v) a)))
  "
  ([njs-code]
   (c/let [args (c/get njs-code :args)
         jsargs-v (c/get njs-code :jsargs-v)
         bodies-str (clojure.string/join " " (c/get njs-code :bodies))
         final-call (c/str "return " (c/get njs-code :call))
         body (c/str "function " "(" (c/apply c/str (c/interpose "," jsargs-v)) ") "
                   "{"
                   bodies-str
                   final-call ";"
                   "}")]
        {
         "$function" {
                      "args" args
                      "body" body
                      "lang" "js"
                      }
         }))
  ([f-name f-args & f-code]
   (c/let [[args body] (js-args-body f-name f-args f-code)]
        {
         "$function" {
                      "args" args
                      "body" body
                      "lang" "js"
                      }
         })))

;;TODO if slow, here only clojure symbols others with :use
;;expected to be ok even all here sometime check again
(def operators-mappings
  '[abs squery-mongo-core.operators.operators/abs
    + squery-mongo-core.operators.operators/+
    inc squery-mongo-core.operators.operators/inc
    - squery-mongo-core.operators.operators/-
    dec squery-mongo-core.operators.operators/dec
    * squery-mongo-core.operators.operators/*
    mod squery-mongo-core.operators.operators/mod
    = squery-mongo-core.operators.operators/=
    not= squery-mongo-core.operators.operators/not=
    > squery-mongo-core.operators.operators/>
    >= squery-mongo-core.operators.operators/>=
    < squery-mongo-core.operators.operators/<
    <= squery-mongo-core.operators.operators/<=
    and squery-mongo-core.operators.operators/and
    or squery-mongo-core.operators.operators/or
    not squery-mongo-core.operators.operators/not
    nor squery-mongo-core.operators.operators/nor
    if-not squery-mongo-core.operators.operators/if-not
    cond squery-mongo-core.operators.operators/cond
    into squery-mongo-core.operators.operators/into
    type squery-mongo-core.operators.operators/type
    boolean squery-mongo-core.operators.operators/boolean
    double squery-mongo-core.operators.operators/double
    int squery-mongo-core.operators.operators/int
    long squery-mongo-core.operators.operators/long
    string squery-mongo-core.operators.operators/string
    exists? squery-mongo-core.operators.operators/exists?
    nil? squery-mongo-core.operators.operators/nil?
    not-value? squery-mongo-core.operators.operators/not-value?
    some? squery-mongo-core.operators.operators/some?
    value? squery-mongo-core.operators.operators/value?
    true? squery-mongo-core.operators.operators/true?
    false? squery-mongo-core.operators.operators/false?
    array? squery-mongo-core.operators.operators/array?
    object? squery-mongo-core.operators.operators/object?
    regex? squery-mongo-core.operators.operators/regex?
    string? squery-mongo-core.operators.operators/string?
    int? squery-mongo-core.operators.operators/int?
    long? squery-mongo-core.operators.operators/long?
    decimal? squery-mongo-core.operators.operators/decimal?
    double? squery-mongo-core.operators.operators/double?
    boolean? squery-mongo-core.operators.operators/boolean?
    number? squery-mongo-core.operators.operators/number?
    rand squery-mongo-core.operators.operators/rand
    let squery-mongo-core.operators.operators/let
    identity squery-mongo-core.operators.operators/identity
    get squery-mongo-core.operators.operators/get
    get-in squery-mongo-core.operators.operators/get-in
    assoc squery-mongo-core.operators.operators/assoc
    assoc-in squery-mongo-core.operators.operators/assoc-in
    dissoc  squery-mongo-core.operators.operators/dissoc
    dissoc-in  squery-mongo-core.operators.operators/dissoc-in
    concat squery-mongo-core.operators.operators/concat
    conj squery-mongo-core.operators.operators/conj
    contains? squery-mongo-core.operators.operators/contains?
    range squery-mongo-core.operators.operators/range
    reverse squery-mongo-core.operators.operators/reverse
    count squery-mongo-core.operators.operators/count

    subvec squery-mongo-core.operators.operators/subvec
    empty? squery-mongo-core.operators.operators/empty?
    conj-distinct squery-mongo-core.operators.operators/conj-distinct
    sort-array squery-mongo-core.operators.operators/sort-array
    fn squery-mongo-core.operators.operators/fn
    map squery-mongo-core.operators.operators/map
    filter squery-mongo-core.operators.operators/filter
    reduce squery-mongo-core.operators.operators/reduce
    bson-size squery-mongo-core.operators.operators/bson-size

    merge squery-mongo-core.operators.operators/merge
    max squery-mongo-core.operators.operators/max
    min squery-mongo-core.operators.operators/min
    str squery-mongo-core.operators.operators/str
    subs squery-mongo-core.operators.operators/subs
    take-str squery-mongo-core.operators.operators/take-str
    re-find squery-mongo-core.operators.operators/re-find
    re-find? squery-mongo-core.operators.operators/re-find?
    re-seq squery-mongo-core.operators.operators/re-seq
    replace squery-mongo-core.operators.operators/replace
    replace-all squery-mongo-core.operators.operators/replace-all

    ;;accumulators
    sort-first squery-mongo-core.operators.operators/sort-first
    sort-take  squery-mongo-core.operators.operators/sort-take
    sort-last squery-mongo-core.operators.operators/sort-last
    sort-take-last squery-mongo-core.operators.operators/sort-take-last
    ;;count/conj/conj-distinct 1 arg call combined with 2 arg call for arrays

    ;;accumulators+arrays
    first squery-mongo-core.operators.operators/first
    take squery-mongo-core.operators.operators/take
    last squery-mongo-core.operators.operators/last
    take-last squery-mongo-core.operators.operators/take-last
    avg squery-mongo-core.operators.operators/avg
    sum squery-mongo-core.operators.operators/sum

    ;;Not clojure overides

    pow squery-mongo-core.operators.operators/pow
    exp squery-mongo-core.operators.operators/exp
    ln  squery-mongo-core.operators.operators/ln
    log squery-mongo-core.operators.operators/log
    ceil squery-mongo-core.operators.operators/ceil
    floor squery-mongo-core.operators.operators/floor
    round squery-mongo-core.operators.operators/round
    trunc squery-mongo-core.operators.operators/trunc
    sqrt squery-mongo-core.operators.operators/sqrt
    mod squery-mongo-core.operators.operators/mod
    div squery-mongo-core.operators.operators/div
    cmp squery-mongo-core.operators.operators/cmp
    if- squery-mongo-core.operators.operators/if-
    if-not-value squery-mongo-core.operators.operators/if-not-value
    literal squery-mongo-core.operators.operators/literal
    expr squery-mongo-core.operators.operators/expr
    convert squery-mongo-core.operators.operators/convert
    boolean squery-mongo-core.operators.operators/boolean
    date squery-mongo-core.operators.operators/date
    decimal squery-mongo-core.operators.operators/decimal
    object-id squery-mongo-core.operators.operators/object-id
    string squery-mongo-core.operators.operators/string
    exists? squery-mongo-core.operators.operators/exists?
    not-exists? squery-mongo-core.operators.operators/not-exists?
    array? squery-mongo-core.operators.operators/array?
    object? squery-mongo-core.operators.operators/object?
    object-id? squery-mongo-core.operators.operators/object-id?
    date? squery-mongo-core.operators.operators/date?
    leti squery-mongo-core.operators.operators/leti
    set-field squery-mongo-core.operators.operators/set-field
    get-field squery-mongo-core.operators.operators/get-field
    unset-field squery-mongo-core.operators.operators/unset-field
    conj-at squery-mongo-core.operators.operators/conj-at
    index-of squery-mongo-core.operators.operators/index-of
    ziparray squery-mongo-core.operators.operators/ziparray
    not-empty? squery-mongo-core.operators.operators/not-empty?
    conj-distinct squery-mongo-core.operators.operators/conj-distinct
    all-true? squery-mongo-core.operators.operators/all-true?
    any-true? squery-mongo-core.operators.operators/any-true?
    subset? squery-mongo-core.operators.operators/subset?
    =sets squery-mongo-core.operators.operators/=sets
    union squery-mongo-core.operators.operators/union
    intersection squery-mongo-core.operators.operators/intersection
    difference squery-mongo-core.operators.operators/difference
    disjoint? squery-mongo-core.operators.operators/disjoint?
    not-disjoint? squery-mongo-core.operators.operators/not-disjoint?
    bson-size squery-mongo-core.operators.operators/bson-size

    rank squery-mongo-core.operators.operators/rank
    dense-rank squery-mongo-core.operators.operators/dense-rank
    stdDevPop squery-mongo-core.operators.operators/stdDevPop
    stdDevSamp squery-mongo-core.operators.operators/stdDevSamp
    lower-case squery-mongo-core.operators.operators/lower-case
    upper-case squery-mongo-core.operators.operators/upper-case
    index-of-in-bytes squery-mongo-core.operators.operators/index-of-in-bytes
    index-of-str squery-mongo-core.operators.operators/index-of-str
    count-str squery-mongo-core.operators.operators/count-str
    count-str-bytes squery-mongo-core.operators.operators/count-str-bytes
    subs-bytes squery-mongo-core.operators.operators/subs-bytes
    split squery-mongo-core.operators.operators/split
    cmp-str-icase squery-mongo-core.operators.operators/cmp-str-icase
    triml squery-mongo-core.operators.operators/triml
    trimr squery-mongo-core.operators.operators/trimr
    trim squery-mongo-core.operators.operators/trim
    replace-all squery-mongo-core.operators.operators/replace-all

    ;;dates
    inc-date squery-mongo-core.operators.operators/inc-date
    date-diff squery-mongo-core.operators.operators/date-diff
    date-from-parts squery-mongo-core.operators.operators/date-from-parts
    date-from-string squery-mongo-core.operators.operators/date-from-string
    dec-date squery-mongo-core.operators.operators/dec-date
    date-to-parts squery-mongo-core.operators.operators/date-to-parts
    date-to-string squery-mongo-core.operators.operators/date-to-string
    day-of-month squery-mongo-core.operators.operators/day-of-month
    day-of-week squery-mongo-core.operators.operators/day-of-week
    day-of-year squery-mongo-core.operators.operators/day-of-year
    iso-day-of-week squery-mongo-core.operators.operators/iso-day-of-week
    iso-week squery-mongo-core.operators.operators/iso-week
    iso-week-year squery-mongo-core.operators.operators/iso-week-year
    date-millisecond squery-mongo-core.operators.operators/date-millisecond
    date-second squery-mongo-core.operators.operators/date-second
    date-minute squery-mongo-core.operators.operators/date-minute
    date-hour squery-mongo-core.operators.operators/date-hour
    date-week squery-mongo-core.operators.operators/date-week
    date-month squery-mongo-core.operators.operators/date-month
    date-year squery-mongo-core.operators.operators/date-year
    date-trunc squery-mongo-core.operators.operators/date-trunc

    ;;javascript
    njs squery-mongo-core.operators.operators/njs
    ejs squery-mongo-core.operators.operators/ejs

    ;;stages
    pipeline squery-mongo-core.operators.stages/pipeline
    match squery-mongo-core.operators.stages/match
    limit squery-mongo-core.operators.stages/limit
    skip squery-mongo-core.operators.stages/skip
    redact squery-mongo-core.operators.stages/redact
    add squery-mongo-core.operators.stages/add
    set-s squery-mongo-core.operators.stages/set-s
    unset squery-mongo-core.operators.stages/unset
    facet squery-mongo-core.operators.stages/facet
    project squery-mongo-core.operators.stages/project
    unwind squery-mongo-core.operators.stages/unwind
    replace-root squery-mongo-core.operators.stages/replace-root
    replace-with squery-mongo-core.operators.stages/replace-with
    add-to-root squery-mongo-core.operators.stages/add-to-root
    move-to-root squery-mongo-core.operators.stages/move-to-root
    unwind-replace-root squery-mongo-core.operators.stages/unwind-replace-root
    unwind-add-to-root squery-mongo-core.operators.stages/unwind-add-to-root
    unwind-move-to-root squery-mongo-core.operators.stages/unwind-move-to-root
    group squery-mongo-core.operators.stages/group
    group-array squery-mongo-core.operators.stages/group-array
    reduce-array squery-mongo-core.operators.stages/reduce-array
    bucket squery-mongo-core.operators.stages/bucket
    bucket-auto squery-mongo-core.operators.stages/bucket-auto
    sort squery-mongo-core.operators.stages/sort
    group-count-sort squery-mongo-core.operators.stages/group-count-sort
    lookup squery-mongo-core.operators.stages/lookup
    plookup squery-mongo-core.operators.stages/plookup
    glookup squery-mongo-core.operators.stages/glookup
    join squery-mongo-core.operators.stages/join
    out squery-mongo-core.operators.stages/out
    if-match squery-mongo-core.operators.stages/if-match
    merge-s squery-mongo-core.operators.stages/merge-s
    union-s squery-mongo-core.operators.stages/union-s
    count-s squery-mongo-core.operators.stages/count-s
    group-count squery-mongo-core.operators.stages/group-count
    coll-stats-s squery-mongo-core.operators.stages/coll-stats-s
    current-op-s squery-mongo-core.operators.stages/current-op-s
    sample squery-mongo-core.operators.stages/sample
    wfields squery-mongo-core.operators.stages/wfields
    list-local-sessions squery-mongo-core.operators.stages/list-local-sessions

    ;;query operators

    ;;qcompare
    =? squery-mongo-core.operators.qoperators/=?
    >? squery-mongo-core.operators.qoperators/>?
    >=? squery-mongo-core.operators.qoperators/>=?
    <? squery-mongo-core.operators.qoperators/<?
    <=? squery-mongo-core.operators.qoperators/<=?

    ;;qlogical
    not=? squery-mongo-core.operators.qoperators/not=?
    in? squery-mongo-core.operators.qoperators/in?
    not-in? squery-mongo-core.operators.qoperators/not-in?
    not? squery-mongo-core.operators.qoperators/not?
    and? squery-mongo-core.operators.qoperators/and?
    nor? squery-mongo-core.operators.qoperators/nor?
    or? squery-mongo-core.operators.qoperators/or?

    ;;
    exists?? squery-mongo-core.operators.qoperators/exists??
    not-exists?? squery-mongo-core.operators.qoperators/not-exists??
    type? squery-mongo-core.operators.qoperators/type?

    ;;qevaluation
    mod? squery-mongo-core.operators.qoperators/mod?
    re-find?? squery-mongo-core.operators.qoperators/re-find??
    json-schema? squery-mongo-core.operators.qoperators/json-schema?
    text? squery-mongo-core.operators.qoperators/text?
    where? squery-mongo-core.operators.qoperators/where?
    superset? squery-mongo-core.operators.qoperators/superset?
    elem-match? squery-mongo-core.operators.qoperators/elem-match?
    count? squery-mongo-core.operators.qoperators/count?

    ;;update operators
    now-date! squery-mongo-core.operators.uoperators/now-date!
    +! squery-mongo-core.operators.uoperators/+!
    *! squery-mongo-core.operators.uoperators/*!
    min! squery-mongo-core.operators.uoperators/min!
    max! squery-mongo-core.operators.uoperators/max!
    rename! squery-mongo-core.operators.uoperators/rename!
    set!- squery-mongo-core.operators.uoperators/set!-
    set-on-insert! squery-mongo-core.operators.uoperators/set-on-insert!
    unset! squery-mongo-core.operators.uoperators/unset!
    conj-distinct! squery-mongo-core.operators.uoperators/conj-distinct!
    conj! squery-mongo-core.operators.uoperators/conj!
    pop! squery-mongo-core.operators.uoperators/pop!
    remove! squery-mongo-core.operators.uoperators/remove!
    remove-all! squery-mongo-core.operators.uoperators/remove-all!
    each! squery-mongo-core.operators.uoperators/each!
    position! squery-mongo-core.operators.uoperators/position!
    slice! squery-mongo-core.operators.uoperators/slice!
    sort! squery-mongo-core.operators.uoperators/sort!
    take! squery-mongo-core.operators.uoperators/take!

    ;;options
    upsert squery-mongo-core.operators.options/upsert
    array-filters squery-mongo-core.operators.options/array-filters
    ])
