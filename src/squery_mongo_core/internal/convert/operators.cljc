(ns squery-mongo-core.internal.convert.operators
  (:require [squery-mongo-core.internal.convert.common :refer [squery-var? squery-var->mql-var squery-var-ref->mql-var-ref squery-var-path->squery-var]]
            [squery-mongo-core.utils :refer [ordered-map]]
            [clojure.walk :refer [postwalk]]))


;;;-----------------------------------------operators-------------------------------------------------------------------
;;;---------------------------------------------------------------------------------------------------------------------

;;------------------------------------------fn- helpers-----------------------------------------------------------------

(defn squery-var-name
  "Mongovar= :prefixNAMEsuffix  here i get 'NAME' only
   Its used fn operator"                    ; TODO REVIEW: needed?or final convert is enough?
  [squery-var]
  (if (squery-var? squery-var)
    (subs (squery-var->mql-var (name squery-var)) 2)                   ; remove the "$$" , fn- squery-vars are never squery-var-paths
    squery-var))

;;---------------------------------------let- helpers-------------------------------------------------------------------

(defn squery-var-name-keyword
  "Mongovar= :prefixNAMEsuffix  here i get :NAME only
   Its used at let operator"               ; TODO REVIEW: needed?or final convert is enough?
  [e]
  (if (squery-var? e)
    (keyword (squery-var-name e))
    e))

(defn let-squery-vars->map [vars-values]
  (let [vars-values (first (reduce (fn [[vars-values index] v]
                                     (if (even? index)
                                       [(conj vars-values (squery-var-name-keyword v)) (inc index)]
                                       [(conj vars-values (squery-var-ref->mql-var-ref v)) (inc index)]))
                                   [[] 0]
                                   vars-values))]
    (into {} (into [] (map vec (partition 2 vars-values))))))


(defn leti
  "The let like mongos let,works only for independent vars"
  [vars-values result-expression]
  {"$let"
   {:vars (let-squery-vars->map vars-values)  ;;{ <var1>: <expression>, ... }
    :in   result-expression}})

;;--------------------------------let with dependent vars helpers-------------------------------------------------------
;;--------------------------------1 let becomes many nested lets(fewer as possible)-------------------------------------


;;TODO BUG , with  + it worked, with get it didnt (it didnt make the nested let)
;;(let [:curtag. (get :field-pair. 1)
;       :songs.  (+ :curtag. 1)      ;(get :curtag. "songs")

;;i think the reason is that get hidded the :curtag. var in the end result

(defn recur-deps [v-dep deps]
  (reduce (fn [recur-dep v]
            (clojure.set/union recur-dep (get deps v)))
          v-dep
          v-dep))


(defn squery-variables [form]
  (let [members (atom [])]
    (postwalk (fn [m]
                (if (squery-var? m)
                  (swap! members conj (squery-var-path->squery-var m))))
              form)
    @members))

(defn var-deps
  "Returns ordered map [[var #{dep1 dep2...}] ....]
   #ordered/map ([:a. #{}] [:ab. #{:a.}] [:abc. #{:ab}])     #{} the depenencies"
  [pairs]
  (loop [pairs pairs
         v-names []
         deps (ordered-map)
         first-var? true]
    (if (empty? pairs)
      deps
      (if first-var?
        (recur (rest pairs) (conj v-names (first (first pairs))) (assoc deps (first (first pairs)) #{}) false)
        (let [pair (first pairs)
              v-name (first pair)
              v-value (second pair)

              ;;new-code(postwalk)
              v-dep (squery-variables v-value)
              v-names-set (into #{} v-names)
              v-dep (into [] (filter (fn [dep] (contains? v-names-set dep)) v-dep))

              ;;old-code(regex bad)
              ;;v-dep (re-seq (re-pattern (clojure.string/join "|" v-names)) (str v-value))
              ;;v-dep (mapv #(keyword (subs % 1)) v-dep)

              v-dep (recur-deps (into #{} v-dep) deps)]
          (recur (rest pairs) (conj v-names v-name) (assoc deps v-name v-dep) false))))))

;;seperate them in independent groups    dep1 dep2 dep3 dep4 .....
;;first let will have all the first members
;;second the second members
;;.... until empty all

(defn indepedent-lets [deps pair-maps lets]
  (loop [deps deps
         deps-keys (keys deps)
         cur-let []
         cur-let-vars #{}]
    (if (empty? deps-keys)
      (if (empty? deps)
        (concat lets [cur-let])
        (concat lets [cur-let] (indepedent-lets deps pair-maps [])))
      (let [v (first deps-keys)
            v-dep (get deps v)]
        (if
          ;;if no dep,or cur-let doesnt have its deps => some previous have them,so i can add them
          (or (empty? v-dep) (empty? (clojure.set/intersection v-dep cur-let-vars)))
          (recur (dissoc deps v)
                 (rest deps-keys)
                 (conj cur-let v (get pair-maps v))
                 (conj cur-let-vars v))

          ;;if dep are in the let ,ignore this var
          (recur deps
                 (rest deps-keys)
                 cur-let
                 cur-let-vars))))))

(defn get-lets [vars]
  (let [pairs (map vec (partition 2 vars))
        pairs-map (into {} pairs)
        deps (var-deps pairs)]
    (indepedent-lets deps pairs-map [])))

(defn get-nested-lets [lets body]
  (loop [lets lets
         nested-lets nil
         first-let? true]
    (if (empty? lets)
      nested-lets
      (let [cur-let (first lets)]
        (if first-let?
          (recur (rest lets) (leti cur-let body) false)
          (recur (rest lets) (leti cur-let nested-lets) false))))))