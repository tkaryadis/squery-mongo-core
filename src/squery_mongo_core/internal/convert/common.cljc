(ns squery-mongo-core.internal.convert.common
  (:require [squery-mongo-core.utils :refer [keyword-map]]
            clojure.set))


;;;;----------------------------------------smongo->mongo---------------------------------------------------------------
;;;;-----------------convert operators/stage operators/read-write commands to valid mongo queries-----------------------
;;;;--------------------------------------------------------------------------------------------------------------------
;;; order     general-helpers/operators/stage operators/pipeline/read-write/cursor/valid command(squery-map->mql-map)

;;------------------------------------------general-helpers-------------------------------------------------------------

(defn squery-var?
  "squery var =  :myvar.  :myvar.afield.  OR  :.myvar :.myvar.afield
  The second is used mostly in paths to avoid forgeting the .
  "
  [e]
  (and (keyword? e)
       (or (clojure.string/starts-with? (name e) ".")
           (clojure.string/ends-with? (name e) "."))))

(defn squery-var-path->squery-var [squery-var-path]
  (let [var-path-str (name squery-var-path)
        dot-index (clojure.string/index-of var-path-str ".")]
    (keyword (subs var-path-str 0 (inc dot-index)))))

(defn squery-var->mql-var [e]
  (let [var-name (name e)
        var-name (if (clojure.string/starts-with? var-name ".")
                   (subs var-name 1)
                   (subs var-name 0 (dec (count var-name))))
        var-parts (clojure.string/split var-name #"\.")
        var-parts (assoc var-parts 0 (clojure.string/replace (get var-parts 0) #"-" "_")) ;;- is not allowed in var names
        mql-var (str "$$" (clojure.string/join "." var-parts))
        ]
    mql-var))

(defn squery-var-ref->mql-var-ref [e]
  (cond

    (squery-var? e)
    (squery-var->mql-var e)

    (keyword? e)                                            ;;reference
    (str "$" (name e))                                      ; mql-ref

    :else
    e))

(defn args->nested-2args
  "Helper used to make variadic functions,when corresponding mongo operator takes 2 args
   makes (op args) to many nested 2 arg (op (op arg1 arg2) arg3) ..."
  [op args]
  (loop [nested-op {op [(first args) (second args)]}
         args (rest (rest args))]
    (if (empty? args)
      nested-op
      (recur {op [nested-op (first args)]} (rest args)))))

(defn single-maps
  "Makes all map members to have max 1 pair,and key to be keyword(if not starts with $) on those single maps.
   [{:a 1 :b 2} 20 {'c' 3} [1 2 3]] => [{:a 1} {:b 2} 20 {:c 3} [1 2 3]]
   It is used from read-write/project/add-fields
   In commands its needed ONLY when i want to seperate command options from extra command args.
   (if i only need command to have keywords i use command-keywords function)"
  ([ms keys-to-seperate]
   (loop [ms ms
          m  {}
          single-ms []]
     (if (and (empty? ms)
              (or (nil? m)                                  ; last element was not a map
                  (and (map? m) (empty? m))))               ; last element was a map that emptied
       single-ms
       (cond

         (not (map? m))
         (recur (rest ms) (first ms) (conj single-ms m))

         (empty? m)
         (recur (rest ms) (first ms) single-ms)

         ; if keys-to-seperate
         ;   and map doesnt have any key that needs seperation,keep it as is
         (and (not (empty? keys-to-seperate))
              (empty? (clojure.set/intersection (set (map (fn [k]
                                                            (if (string? k)
                                                              (keyword k)
                                                              k))
                                                          (keys m)))
                                                keys-to-seperate)))
         (recur (rest ms) (first ms) (conj single-ms (keyword-map m)))

         :else
         (let [[k v] (first m)]
           (recur ms (dissoc m k) (conj single-ms (keyword-map {k v}))))))))
  ([ms]
   (single-maps ms #{})))

(defn not-fref
  ":afield => :!afield"
  [fref]
  (keyword (str "!" (name fref))))