(ns squery-mongo-core.operators.qoperators
  (:require [squery-mongo-core.internal.convert.qoperators :refer [remove-q-combine-fields]]
            [clojure.core :as c]))

;;geo bitwise TODO

;;----------------------------------------------Comparison--------------------------------------------------------------

(defn =?
  ([field value]
   {"$__q__" {(name field) {"$eq" value}}})
  ([value]
   {"$__q__" {"$eq" value}}))

(defn >?
  ([field value]
   {"$__q__" {(name field) {"$gt" value}}})
  ([value]
   {"$__q__" {"$gt" value}}))

(defn >=?
  ([field value]
   {"$__q__" {(name field) {"$gte" value}}})
  ([value]
   {"$__q__" {"$gte" value}}))

(defn <?
  ([field value]
   {"$__q__" {(name field) {"$lt" value}}})
  ([value]
   {"$__q__" {"$lt" value}}))


(defn <=?
  ([field value]
   {"$__q__" {(name field) {"$lte" value}}})
  ([value]
   {"$__q__" {"$lte" value}}))

(defn not=?
  ([field value]
   {"$__q__" {(name field) {"$ne" value}}})
  ([value]
   {"$__q__" {"$ne" value}}))

(defn in?
  "if field single value => check if array contains that value
   if field array => check if array contains any of those value (at least 1 not all)"
  ([field ar-value]
   {"$__q__" {(name field) {"$in" ar-value}}})
  ([ar-value]
   {"$__q__" {"$in" ar-value}}))

(defn not-in?
  ([field ar-value]
   {"$__q__" {(name field) {"$nin" ar-value}}})
  ([ar-value]
   {"$__q__" {"$nin" ar-value}}))


;;------------------------------------------------Logical---------------------------------------------------------------

(defn not?
  "example call
     (not? :myfield (<? 5))"
  ([field q-operation]
   {"$__q__" {(name field) {"$not" (get q-operation "$__q__" q-operation)}}})
  ([q-operation]
   {"$__q__" {"$not" (get q-operation "$__q__" q-operation)}}))

(defn and? [& es]
  {"$__q__" {"$and" (remove-q-combine-fields es)}})

(defn nor? [& es]
  {"$__q__" {"$nor" (remove-q-combine-fields es)}})

(defn or? [& es]
  {"$__q__" {"$or" (remove-q-combine-fields es)}})

;;--------------------------------------------Element query operators---------------------------------------------------

(defn exists?? [field]
  {"$__q__" {(name field) {"$exists" true}}})

(defn not-exists?? [field]
  {"$__q__" {(name field) {"$exists" false}}})

(defn type? [field & types]
  (if (c/= (c/count types) 1)
    {"$__q__" {(name field) {"$type" (c/first types)}}}
    {"$__q__" {(name field) {"$type" (c/into [] types)}}}))

;;-------------------------------------------Evaluation-----------------------------------------------------------------

(defn mod?
  "checks if field/divisor has the remainder"
  ([field divisor remainder]
   {"$__q__" { (name field) { "$mod" [ divisor remainder]}}})
  ([divisor remainder]
   {"$__q__" { "$mod" [ divisor remainder]}}))


(defn re-find??
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

(defn json-schema? [schema-doc]
  {"$__q__" {"$jsonSchema" schema-doc}})

(defn text? [search-str & options]
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

(defn where? [js-code]
  {"$__q__" { "$where" js-code }})

;;----------------------------------------Array-------------------------------------------------------------------------

(defn superset?
  ([field arr-value]
   {"$__q__" {(name field) { "$all" arr-value}}})
  ([arr-value]
   {"$__q__" { "$all" arr-value}}))

(defn elem-match?
  "Match member 
    (elem-match? :ar1 (=? avalue))
   Match embeded
    (elem-match? :ar1 (=? :aField avalue))
   Match nested arrays, i can nest them 
    (elem-match? :ar1 (elem-match? :ar2 :avalue))
   Multiple qs can be used for more creteria
   Project operator (doesn't work on aggregation projects)
    [:array.$]"
  ([& qs]
   {"$__q__" {(name (first qs)) {"$elemMatch" (apply (partial c/merge {}) (remove-q-combine-fields (rest qs)))}}}))

(defn count?
  ([field size]
   {"$__q__" {(name field) { "$size" size } }})
  ([size]
   {"$__q__" { "$size" size }}))
