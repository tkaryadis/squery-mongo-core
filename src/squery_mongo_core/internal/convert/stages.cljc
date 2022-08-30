(ns squery-mongo-core.internal.convert.stages
  (:require [squery-mongo-core.internal.convert.common :refer [single-maps]]
            [squery-mongo-core.utils :refer [ordered-map]]))


;;;------------------------------------------stages---------------------------------------------------------------------
;;;---------------------------------------------------------------------------------------------------------------------

;;helper used in Sort/Project/Index
(defn squery-vector->squery-map
  "Used in Sort/Project/Index
   makes the members maps(if they are :k or :!k),and merge all to 1 map
   squery-vector = [:k1 :!k2 :k3 {:k4 ''}] => {:k1 1 :k2 v :k3 1 {:k4 ''}}
   Used in sort/project/index etc, in sort v=-1,in project v=0
   Adds literal,can't be used as normal mongo project {:field 1} means here {:field (literal- 1)}"
  [fields replace-v]
  (reduce (fn [m field]
            (if (keyword? field)
              (let [arg-name (name field)]
                (if (clojure.string/starts-with? arg-name "!")
                  (assoc m (keyword (subs arg-name 1)) replace-v)
                  (assoc m field 1)))
              (merge m field)))  ;; if not keyword keep the orinal value
          (ordered-map)
          fields))


;;-----------------------------------Project----------------------------------------------------------------------------

(defn replaced-fields [fields]
  (reduce (fn [replace-fields field]
            (if (map? field)
              (let [cur-replaced-keys (filter #(clojure.string/starts-with? % "!") (map name (keys field)))]
                (concat replace-fields cur-replaced-keys))
              replace-fields))
          []
          fields))

(defn renamed-fields [fields]
  (reduce (fn [rename-fields-map field]
            (assoc rename-fields-map (subs (name field) 1) (keyword field)))
          {}
          fields))

(defn squery-project->mql-project
  "squery-project
     [:a :!_id {:c ''} {:!d ''}]
     keep a
     dont keep _id
     add :c
     replace :d (its always replace by default except add document to array place)
     "
  [& fields]
  (let [fields (single-maps fields)
        replaced-fields (replaced-fields fields)
        replaced-fields-parents (map (fn [f] (first (clojure.string/split f #"\.")))
                                     replaced-fields)
        project-doc (squery-vector->squery-map fields 0)
        ]
    (if (not (empty? replaced-fields))
      [
       ;;add with the !names
       {"$project" project-doc}
       ;;remove the old ones
       {"$unset" (vec (map #(subs (name %) 1) replaced-fields))}
       ;;rename the new ones from  !name to name
       {"$set" (renamed-fields replaced-fields)}
       ;;remove the temp ones
       {"$unset" (vec replaced-fields-parents)}
       ]
      {"$project" project-doc})))

(defn squery-addFields->mql-addFields
  "fields = [{:c ''} {:!d ''}]
   add :c
   replace :d (its always replace by default except add document to array place)
  "
  [& fields]
  (let [fields (single-maps fields)
        replaced-fields (replaced-fields fields)
        ;;TODO works ok but for nested fields,no need to add temp fields as nested
        ;;here i add temp fields as nested,and i remove their parents => all
        ;;solution = change it so temp fields are never nested
        replaced-fields-parents (map (fn [f] (first (clojure.string/split f #"\.")))
                                    replaced-fields)
        add-doc (squery-vector->squery-map fields nil)]                 ;;make 1 map,no keyword replacement
    (if (not (empty? replaced-fields))
      [
       ;;add with the !names
       {"$set" add-doc}
       ;;remove the old ones
       {"$unset" (vec (map #(subs (name %) 1) replaced-fields))}
       ;;rename the new ones from  !name to name
       {"$set" (renamed-fields replaced-fields)}
       ;;remove the temp ones
       {"$unset" (vec replaced-fields-parents)}
       ]
      {"$set" add-doc})))
