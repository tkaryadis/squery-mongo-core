(ns cmql-core.internal.convert.commands
  (:require [cmql-core.internal.convert.stages :refer [cmql-addFields->mql-addFields cmql-project->mql-project]]
            [cmql-core.internal.convert.common :refer [cmql-var-ref->mql-var-ref]]
            [cmql-core.utils :refer [ordered-map]]
            clojure.string
            clojure.set))

;;those are for all commands,
(def cmql-specific-options #?(:clj #{:session :command}
                              :cljs #{:session :command :client :decode}))

;;;----------------------------------cmql-pipeline->mql-pipeline---------------------------------------------------------------
;;;---------------------------------------------------------------------------------------------------------------------

(defn add-stage? [m]
  (and (map? m)
       (every? (fn [k]
                 (and (or (keyword? k) (string? k))         ; String is ok also {"a" ".."}
                      (not (clojure.string/starts-with? (name k) "$"))))
               (keys m))))

(defn project-field? [field]
  (or (keyword? field)
      (and (map? field)
           ;;(= (count field) 1)    ; TODO: REVIEW: project can be [{:a "" :b ""} :c]
           (not (clojure.string/starts-with? (name (first (keys field))) "$")))))

(defn project-stage? [m]
  (and (vector? m) (not (empty? m)) (every? project-field? m)))

;;only stage operators can appear in the pipeline
(def stage-operators
  #{"$addFields" "$bucket" "$bucketAuto" "$collStats" "$count" "$currentOp" "$facet"
    "$geoNear" "$graphLookup" "$group" "$indexStats" "$limit" "$listLocalSessions"
    "$listSessions" "$lookup" "$match" "$merge" "$out" "$planCacheStats" "$project" "$redact"
    "$replaceRoot" "$replaceWith" "$sample" "$set" "$skip" "$sort" "$sortByCount"
    "$unionWith" "$unset" "$unwind" "$setWindowFields"})

(defn stage-operator? [stage]
  (and (map? stage)
       (= (count stage) 1)
       (let [k (first (keys stage))]
         (and (clojure.string/starts-with? k "$")
              (contains? stage-operators (name k))))))

(defn qfilter-stage? [stage]
  (and (map? stage) (= (count stage) 1) (contains? stage "$__qfilter__")))

(defn cmql-filters->match-stage
  "Many filters(aggregate operators) combined to a match $exprs using $and operators"
  [filters qfilters?]
  (cond
    (= (count filters) 1)
    (if qfilters?
      {"$match" (get (first filters) "$__qfilter__")}
      {"$match" {"$expr" (first filters)}})

    (> (count filters) 1)
    (if qfilters?
      {"$match" {"$and" (mapv #(get % "$__qfilter__") filters)}}
      {"$match" {"$expr" {"$and" (vec filters)}}})

    :else  ; i cant get here,i never call with empty filters
    {"$match" {}}))

;;options are seperated,before this,here comes only stages never options
;;TODO there is no need to seperate qfilters from $expr filters,into seperate matches
;;but Mongodb optimizes it anyways so its not a problem also
(defn cmql-pipeline->mql-pipeline
  "Converts a cmql-pipeline to a mongo pipeline (1 vector with members stage operators)
   [],{}../nil  => empty stages or nil stages are removed
   [[] []] => [] []   flatten of stages (used when one stage produces more than 1 stages)
   cmql-filters combined =>  $match stage with $and
   [] projects  => $project"
  [cmql-pipeline]
  (loop [cmql-pipeline cmql-pipeline
         cmql-qfilters []
         cmql-filters []
         mql-pipeline []]
    (if (empty? cmql-pipeline)
      (cond
        (and (empty? cmql-filters) (empty? cmql-qfilters))
        mql-pipeline

        (not (empty? cmql-qfilters))
        (conj mql-pipeline (cmql-filters->match-stage cmql-qfilters true))

        :else
        (conj mql-pipeline (cmql-filters->match-stage cmql-filters false)))
      (let [stage (first cmql-pipeline)]
        (cond

          (or (= stage []) (nil? stage))                  ; ignore [] or nil stages
          (recur (rest cmql-pipeline) cmql-qfilters cmql-filters mql-pipeline)

          (qfilter-stage? stage)
          (if (empty? cmql-filters)
            (recur (rest cmql-pipeline) (conj cmql-qfilters stage) [] mql-pipeline)
            (recur (rest cmql-pipeline) (conj cmql-qfilters stage) [] (conj mql-pipeline (cmql-filters->match-stage cmql-filters false))))

          (add-stage? stage)                             ; {:a ".." :!b ".."}
          (let [stage (apply cmql-addFields->mql-addFields [stage])]
            (if (vector? stage)                 ; 1 project stage might produce nested stages,put the nested and recur
              (recur (concat stage (rest cmql-pipeline)) cmql-qfilters cmql-filters mql-pipeline) ; do what is done for nested stages(see below)
              (cond
                (and (empty? cmql-filters) (empty? cmql-qfilters))
                (recur (rest cmql-pipeline) [] [] (conj mql-pipeline stage))

                (not (empty? cmql-qfilters))
                (recur (rest cmql-pipeline) [] cmql-filters (conj mql-pipeline (cmql-filters->match-stage cmql-qfilters true) stage))

                :else
                (recur (rest cmql-pipeline) cmql-qfilters [] (conj mql-pipeline (cmql-filters->match-stage cmql-filters false) stage)))))

          (project-stage? stage)                             ; [:a ....]
          (let [stage (apply cmql-project->mql-project stage)]
            (if (vector? stage)                 ; 1 project stage might produce nested stages,put the nested and recur
              (recur (concat stage (rest cmql-pipeline)) cmql-qfilters cmql-filters mql-pipeline) ; do what is done for nested stages(see below)
              (cond
                (and (empty? cmql-filters) (empty? cmql-qfilters))
                (recur (rest cmql-pipeline) [] [] (conj mql-pipeline stage))

                (not (empty? cmql-qfilters))
                (recur (rest cmql-pipeline) [] cmql-filters (conj mql-pipeline (cmql-filters->match-stage cmql-qfilters true) stage))

                :else
                (recur (rest cmql-pipeline) cmql-qfilters [] (conj mql-pipeline (cmql-filters->match-stage cmql-filters false) stage)))))

          (vector? stage)      ; vector but no project = nested stage,add the members as stages and recur     ; TODO: REVIEW:
          (recur (concat stage (rest cmql-pipeline)) cmql-qfilters cmql-filters mql-pipeline)

          (stage-operator? stage)                                    ; normal stage operator {}
          (cond
            (and (empty? cmql-filters) (empty? cmql-qfilters))
            (recur (rest cmql-pipeline) [] [] (conj mql-pipeline stage))

            (not (empty? cmql-qfilters))
            (recur (rest cmql-pipeline) [] cmql-filters (conj mql-pipeline (cmql-filters->match-stage cmql-qfilters true) stage))

            :else
            (recur (rest cmql-pipeline) cmql-qfilters [] (conj mql-pipeline (cmql-filters->match-stage cmql-filters false) stage)))

          :else                        ; filter stage (not qfilter,they are collected above)
          (if (empty? cmql-qfilters)
            (recur (rest cmql-pipeline) [] (conj cmql-filters stage) mql-pipeline)
            (recur (rest cmql-pipeline) [] (conj cmql-filters stage) (conj mql-pipeline (cmql-filters->match-stage cmql-qfilters true)))))))))


;;;---------------------------------------read-write--------------------------------------------------------------------
;;;---------------------------------------------------------------------------------------------------------------------

(defn command-keys [command-def]
  (clojure.set/union (into #{} (map (fn [k]
                                      (if (string? k)
                                        (keyword k)
                                        k))
                                    (keys command-def)))
                     cmql-specific-options))

(defn update-pipeline-stage? [stage]
  (or (project-stage? stage)
      (add-stage? stage)
      (and (map? stage) (contains?                          ;;#{"$addFields" "$set" "$project" "$unset" "$replaceRoot" "$replaceWith"}
                          ; only the above are allowed in update,but here all allowed and removed late
                          ; it is done to allow options to look as stages,when writting the query
                          stage-operators
                          (name (first (keys stage)))))))

(defn command-option? [option command-keys]
  (and (map? option) (contains? command-keys
                                (let [k (first (keys option))]   ; no need,options should be already as keywords
                                  (if (string? k)
                                    (keyword k)
                                    k)))))

(defn upsert-query [args]
  (let [upsert? (contains? (into #{} args) {:upsert true})
        [upsert-query args] (if upsert? [(first args) (rest args)] [nil args]) ; if upsert,the first argument is the q doc
        ]
    [upsert-query args]))

(defn args->query-updatePipeline-options
  "Seperates update arguments to [query update-pipeline options]
   Its used from update command,and from others like delete(dq) , that dont have pipeline just query and options"
  [args command-keys]
  (let [[query update-pipeline args]
        (reduce (fn [[query update-pipeline args] arg]
                  (cond

                    (command-option? arg command-keys)         ; position is important,the rest are addFields
                    [query update-pipeline (conj args arg)]

                    (update-pipeline-stage? arg)
                    [query (conj update-pipeline arg) args]

                    :else                                   ;;query form
                    [(conj query arg) update-pipeline args]
                    ))
                [[] [] []]
                args)

        query-with-expr (map (fn [q-form]
                               (if (contains? q-form "$__qfilter__")
                                 (get q-form "$__qfilter__")
                                 {"$expr" q-form}))
                             query)
        query (cond
                (= (count query-with-expr) 1)
                (first query-with-expr)

                (> (count query-with-expr) 1)
                {"$and" (vec query-with-expr)}

                :else
                {})
        ]
    [query (cmql-pipeline->mql-pipeline update-pipeline) args]))

(defn seperate-bulk
  "Used in bulk deletes/updates seperate the bulk queries from the args
   ({:dq ...} {:dq ...} arg1 arg2 ...) => [ [{:dq ...} {:dq ...}] [arg1 arg2 ...] ]"
  [bulk-key args]
  (reduce (fn [[bulk-queries options] arg]
            (if (and (map? arg) (contains? arg bulk-key))
              [(conj bulk-queries arg) options]
              [bulk-queries (conj options arg)]))
          [[] []]
          args))

;; TODO : FIXME : doesnt work for {:allowDiskUse	true :maxTimeMS 10000} i need to make all the command-keys,maps of 1 size
;; also check all commands if this happening there also
(defn get-pipeline-options [args command-keys]
  (let [[pipeline options]
        (reduce (fn [[pipeline options] arg]
                  (if (and (map? arg)
                           (contains? command-keys (first (keys arg)))) ;; options are always single maps,with keyword
                    [pipeline (conj options arg)]
                    [(conj pipeline arg) options]))
                [[] []]
                args)]
    [pipeline options]))


;;-----------------------------------Final process of commmand-and Run Command------------------------------------------

;;;----------------------------------scommand->mcommand(cmql-map->mql-map (command is a map))----------------------------------
;;; cmql-map->mql-map (to make a valid mongo command) , that will be converted to Document(if java driver) and runCommand()

(declare cmql-map->mql-map)

(defn cmql-vector->mql-vector [v]
  (loop [v v
         mql-vector []]
    (if (empty? v)
      mql-vector
      (let [mb (first v)]
        (cond
          (map? mb)
          (let [mb (cmql-map->mql-map mb )]
            (recur (rest v) (conj mql-vector mb)))

          (vector? mb)
          (let [mb (cmql-vector->mql-vector mb )]
            (recur (rest v) (conj mql-vector mb)))

          :else
          (recur (rest v) (conj mql-vector (cmql-var-ref->mql-var-ref mb))))))))

(defn cmql-map->mql-map
  "Converts a smongo query a valid mongo query (removes the smongo symbols)

   Variables
     :aname-        =>   '$$aname'
     :aname-.afield =>   '$$aname.afield'
     :-            => anonymous variable converts to '$$__m__'

   Keyword in key poistion => field name
     {:f ''}  => {'f' ''}

   Keyword not in key position => field reference
     :v => '$v'   (can be as value in a map,a value in a vector,or out of a collection)

   else
     no change
  "
  [m]
  (loop [ks (keys m)
         mql-map (ordered-map)]
    (if (empty? ks)
      mql-map
      (let [k (first ks)
            vl (get m k)
            k (if (keyword? k) (name k) k)]
        (cond
          (map? vl)
          (let [vl (cmql-map->mql-map vl)]
            (recur (rest ks) (assoc mql-map k vl)))

          (vector? vl)
          (let [vl (cmql-vector->mql-vector vl)]
            (recur (rest ks) (assoc mql-map k vl)))

          :else
          (recur (rest ks) (assoc mql-map k (cmql-var-ref->mql-var-ref vl))))))))


;;----------------------------db-namespaces--------------------------------------------------------------------

(defn split-db-namespace
  "Arguments can be
   :db-name.coll-name
   :db-name
   :.coll-name     ;;db-name will be '',the default db will be added later in the command
   Seperator can be the . or /
   "
  [db-namespace]
  (let [seperator (if (clojure.string/includes? (name db-namespace) "/") #"/" #"\.")
        [db-name coll-name] (clojure.string/split (name db-namespace) seperator)
        db-name (if (= db-name "") nil db-name)]
    [db-name coll-name]))

      
