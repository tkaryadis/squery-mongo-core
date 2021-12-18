(ns cmql-core.operators.qoperators
  (:require [cmql-core.internal.convert.qoperators :refer [remove-q-combine-fields]]
            [clojure.core :as c]))

;;geo bitwise TODO

;;each query operator gets a ___q___, if i combine them i remove the __q__ and add in the external
;;those reach the final match that decides if expr is needed or not


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

(defn q=
  ([field value]
   {"$__q__" {field {"$eq" value}}})
  ([value]
   {"$__q__" {"$eq" value}}))

(defn q>
  ([field value]
   {"$__q__" {field {"$gt" value}}})
  ([value]
   {"$__q__" {"$gt" value}}))

(defn q>=
  ([field value]
   {"$__q__" {field {"$gte" value}}})
  ([value]
   {"$__q__" {"$gte" value}}))

(defn q<
  ([field value]
   {"$__q__" {field {"$lt" value}}})
  ([value]
   {"$__q__" {"$lt" value}}))


(defn q<=
  ([field value]
   {"$__q__" {field {"$lte" value}}})
  ([value]
   {"$__q__" {"$lte" value}}))

(defn qnot=
  ([field value]
   {"$__q__" {field {"$ne" value}}})
  ([value]
   {"$__q__" {"$ne" value}}))

(defn qcontains?
  ([ar-value field]
   {"$__q__" {field {"$in" ar-value}}})
  ([ar-value]
   {"$__q__" {"$in" ar-value}}))

(defn qnot-contains?
  ([ar-value field]
   {"$__q__" {field {"$nin" ar-value}}})
  ([ar-value]
   {"$__q__" {"$nin" ar-value}}))


;;------------------------------------------------Logical---------------------------------------------------------------

(defn qnot
  ([field value]
   {"$__q__" {field {"$not" value}}})
  ([value]
   {"$__q__" {"$not" value}}))

(defn qand [& es]
  {"$__q__" {"$and" (remove-q-combine-fields es)}})

(defn qnor [& es]
  {"$__q__" {"$nor" (remove-q-combine-fields es)}})

(defn qor [& es]
  {"$__q__" {"$or" (remove-q-combine-fields es)}})

;;--------------------------------------------Element query operators---------------------------------------------------

(defn qexists? [field]
  {"$__q__" {field {"$exists" true}}})

(defn qnot-exists? [field]
  {"$__q__" {field {"$exists" false}}})

(defn qtype [field & types]
  (if (c/= (c/count types) 1)
    {"$__q__" {field {"$type" (c/first types)}}}
    {"$__q__" {field {"$type" (c/into [] types)}}}))

;;-------------------------------------------Evaluation-----------------------------------------------------------------

(defn qmod
  "checks if field/divisor has the remainder"
  ([field divisor remainder]
   {"$__q__" { field { "$mod" [ divisor remainder]}}})
  ([divisor remainder]
   {"$__q__" { "$mod" [ divisor remainder]}}))


(defn qregex
  ([field pattern options]
   (c/let [m { "$regex" pattern } ]
     (if (c/not (c/empty? options))
       {"$__q__" {field (c/assoc m "$options" options)}}
       {"$__q__" {field m}})))
  ([pattern options]
   (c/let [m { "$regex" pattern } ]
     (if (c/not (c/empty? options))
       {"$__q__" (c/assoc m "$options" options)}
       {"$__q__" m}))))

(defn jsonSchema [schema-doc]
  {"$__q__" {"$jsonSchema" schema-doc}})

(defn text [search-str & options]
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

(defn where [js-code]
  {"$__q__" { "$where" js-code }})

;;----------------------------------------Array-------------------------------------------------------------------------

(defn qcontains-all?
  ([field arr-value]
   {"$__q__" {field { "$all" arr-value}}})
  ([arr-value]
   {"$__q__" { "$all" arr-value}}))

(defn elem-match
  ([& qs]
   (if (map? (first qs))
     {"$__q__" {"$elemMatch" (apply (partial c/merge {}) (remove-q-combine-fields qs))}}
     {"$__q__" {(first qs) {"$elemMatch" (apply (partial c/merge {}) (remove-q-combine-fields (rest qs)))}}})))

(defn qcount
  ([field size]
   {"$__q__" {field { "$size" size } }})
  ([size]
   {"$__q__" { "$size" size }}))


;;--------------------------------------Project-------------------------------------------------------------------------

(defn qtake
  ([field n] { field { "$slice" [n] }})
  ([field start-index n] {field { "$slice" [ start-index n ] } }))