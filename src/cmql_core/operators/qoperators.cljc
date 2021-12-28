(ns cmql-core.operators.qoperators
  (:require [cmql-core.internal.convert.qoperators :refer [remove-q-combine-fields]]
            [clojure.core :as c]))

;;geo bitwise TODO

;;each query operator gets a ___q___, if i combine them i remove the __q__ and add in the external
;;those reach the final match that decides if expr is needed or not

;;To remove the pattern way i need to use (f field value), and also use $and, instead of 1 document with
;; implicit $and


;;bad idea makes things complicated, i dont want pattern matching, and 1 documents with impllicit $and
(defn qf
  "Converts a document pattern query, to a cmql filter
   For example {:_id (q= 1) :a (q> 2} would be made as [(q= :_id 1) (q> :a 2)]
   And auto-unested later from cmql, to become 2 filters (q= :_id 1) (q> :a 2)"
  [m]
  (reduce (fn [filters field]
            (let [op-doc (get (get m field) "$__q__")]
              (if op-doc
                (let [op (first (keys op-doc))
                      vl (first (vals op-doc))]
                  (conj filters {"$__q__" {field {op vl}}}))
                (conj filters {"$__q__" {field (get m field)}}))))
          []
          (keys m)))

;;----------------------------------------------Comparison--------------------------------------------------------------

(defn =-
  ([field value]
   {"$__q__" {(name field) {"$eq" value}}})
  ([value]
   {"$__q__" {"$eq" value}}))

(defn >-
  ([field value]
   {"$__q__" {(name field) {"$gt" value}}})
  ([value]
   {"$__q__" {"$gt" value}}))

(defn >=-
  ([field value]
   {"$__q__" {(name field) {"$gte" value}}})
  ([value]
   {"$__q__" {"$gte" value}}))

(defn <-
  ([field value]
   {"$__q__" {(name field) {"$lt" value}}})
  ([value]
   {"$__q__" {"$lt" value}}))


(defn <=-
  ([field value]
   {"$__q__" {(name field) {"$lte" value}}})
  ([value]
   {"$__q__" {"$lte" value}}))

(defn not=-
  ([field value]
   {"$__q__" {(name field) {"$ne" value}}})
  ([value]
   {"$__q__" {"$ne" value}}))

(defn member?-
  "if field single value => check if array contains that value
   if field array => check if array contains any of those value (at least 1 not all)"
  ([field ar-value]
   {"$__q__" {(name field) {"$in" ar-value}}})
  ([ar-value]
   {"$__q__" {"$in" ar-value}}))

(defn not-member?-
  ([field ar-value]
   {"$__q__" {(name field) {"$nin" ar-value}}})
  ([ar-value]
   {"$__q__" {"$nin" ar-value}}))


;;------------------------------------------------Logical---------------------------------------------------------------

(defn not-
  ([field value]
   {"$__q__" {(name field) {"$not" value}}})
  ([value]
   {"$__q__" {"$not" value}}))

(defn and- [& es]
  {"$__q__" {"$and" (remove-q-combine-fields es)}})

(defn nor- [& es]
  {"$__q__" {"$nor" (remove-q-combine-fields es)}})

(defn or- [& es]
  {"$__q__" {"$or" (remove-q-combine-fields es)}})

;;--------------------------------------------Element query operators---------------------------------------------------

(defn exists?- [field]
  {"$__q__" {(name field) {"$exists" true}}})

(defn not-exists?- [field]
  {"$__q__" {(name field) {"$exists" false}}})

(defn type- [field & types]
  (if (c/= (c/count types) 1)
    {"$__q__" {(name field) {"$type" (c/first types)}}}
    {"$__q__" {(name field) {"$type" (c/into [] types)}}}))

;;-------------------------------------------Evaluation-----------------------------------------------------------------

(defn mod-
  "checks if field/divisor has the remainder"
  ([field divisor remainder]
   {"$__q__" { (name field) { "$mod" [ divisor remainder]}}})
  ([divisor remainder]
   {"$__q__" { "$mod" [ divisor remainder]}}))


(defn regex-
  ([field pattern-options-vec]
   (c/let [m { "$regex" (c/first pattern-options-vec) } ]
     (if (c/= (c/count pattern-options-vec) 1)
       {"$__q__" {(name field) m}}
       {"$__q__" {(name field) (c/assoc m "$options" (c/second pattern-options-vec))}})))
  ([pattern-options-vec]
   (c/let [m { "$regex" (c/first pattern-options-vec) } ]
     (if (c/= (c/count pattern-options-vec) 1)
       {"$__q__" m}
       {"$__q__" (c/assoc m "$options" (c/second pattern-options-vec))}))))

(defn jsonSchema- [schema-doc]
  {"$__q__" {"$jsonSchema" schema-doc}})

(defn text- [search-str & options]
  (let [options-map (apply (partial c/merge {}) options)
        m {"$text" {"$search"  search-str}}
        m (if (get options-map "$language")
            (assoc m "$language"  (get options-map "$language"))
            m)
        m (if (get options-map "$caseSensitive")
            (assoc m "$caseSensitive" (get options-map "$caseSensitive"))
            m)
        m (if (get options-map "$diacriticSensitive")
            (assoc m "$diacriticSensitive" (get options-map "$diacriticSensitive"))
            m)]
    {"$__q__" m}))

(defn where- [js-code]
  {"$__q__" { "$where" js-code }})

;;----------------------------------------Array-------------------------------------------------------------------------

(defn contains-all?-
  ([field arr-value]
   {"$__q__" {(name field) { "$all" arr-value}}})
  ([arr-value]
   {"$__q__" { "$all" arr-value}}))

(defn elem-match-
  ([& qs]
   (if (map? (first qs))
     {"$__q__" {"$elemMatch" (apply (partial c/merge {}) (remove-q-combine-fields qs))}}
     {"$__q__" {(name (first qs)) {"$elemMatch" (apply (partial c/merge {}) (remove-q-combine-fields (rest qs)))}}})))

(defn count-
  ([field size]
   {"$__q__" {(name field) { "$size" size } }})
  ([size]
   {"$__q__" { "$size" size }}))


;;--------------------------------------Project-------------------------------------------------------------------------

(defn take-
  ([field n] { (name field) { "$slice" [n] }})
  ([field start-index n] {(name field) { "$slice" [ start-index n ] } }))
