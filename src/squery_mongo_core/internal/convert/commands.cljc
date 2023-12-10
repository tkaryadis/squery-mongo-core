(ns squery-mongo-core.internal.convert.commands
  (:require [squery-mongo-core.internal.convert.stages :refer [squery-addFields->mql-addFields squery-project->mql-project]]
            [squery-mongo-core.internal.convert.common :refer [squery-var-ref->mql-var-ref]]
            [squery-mongo-core.utils :refer [ordered-map]]
            clojure.string
            clojure.set
            [squery-mongo-core.internal.convert.qoperators :refer [remove-q-combine-fields]]))

;;those are for all commands,
(def squery-specific-options #?(:clj #{:session :command}
                              :cljs #{:session :command :client :decode}))

;;;----------------------------------squery-pipeline->mql-pipeline---------------------------------------------------------------
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
    "$unionWith" "$unset" "$unwind" "$setWindowFields" "$densify" "$documents" "$fill"})

(defn stage-operator? [stage]
  (and (map? stage)
       (= (count stage) 1)
       (let [k (first (keys stage))]
         (and (clojure.string/starts-with? k "$")
              (contains? stage-operators (name k))))))

(defn qfilter-stage? [stage]
  (and (map? stage) (= (count stage) 1) (contains? stage "$__q__")))

(defn add-and-filters [filters]
  (if (= (count filters) 1)
    {"$expr" (first filters)}
    {"$expr" {"$and" filters}}))

(defn add-and-qfilters [filters]
  (let [filters (remove-q-combine-fields filters)]
    (if (= (count filters) 1)
      (first filters)
      {"$and" filters})))

(defn group-add-and-to-filters [squery-filters]
  (loop [squery-filters squery-filters
         qfilters []
         filters []
         grouped-filters []]
    (if (empty? squery-filters)
      (let [grouped-filters (cond
                              (and (empty? qfilters) (empty? filters))
                              grouped-filters

                              (empty? qfilters)
                              (conj grouped-filters (add-and-filters filters))

                              :else
                              (conj grouped-filters (add-and-qfilters qfilters)))]
        (if (= (count grouped-filters) 1)
          (first grouped-filters)
          {"$and" grouped-filters}))
      (let [cur-filter (first squery-filters)]
        (if (qfilter-stage? cur-filter)
          (if (empty? filters)
            (recur (rest squery-filters) (conj qfilters cur-filter) [] grouped-filters)
            (recur (rest squery-filters) (conj qfilters cur-filter) [] (conj grouped-filters (add-and-filters filters))))
          (if (empty? qfilters)
            (recur (rest squery-filters) [] (conj filters cur-filter) grouped-filters)
            (recur (rest squery-filters) [] (conj filters cur-filter) (conj grouped-filters (add-and-qfilters qfilters)))))))))

(defn squery-filters->match-stage
  "Many filters(aggregate operators) combined to a match $exprs using $and operators"
  [filters]
  {"$match" (group-add-and-to-filters filters)})

;;options are seperated,before this,here comes only stages never options
;;TODO there is no need to seperate qfilters from $expr filters,into seperate matches
;;but Mongodb optimizes it anyways so its not a problem also
(defn squery-pipeline->mql-pipeline
  "Converts a squery-pipeline to a mongo pipeline (1 vector with members stage operators)
   [],{}../nil  => empty stages or nil stages are removed
   [[] []] => [] []   flatten of stages (used when one stage produces more than 1 stages)
   squery-filters combined =>  $match stage with $and
   [] projects  => $project"
  [squery-pipeline]
  (loop [squery-pipeline squery-pipeline
         ;;squery-qfilters []
         ;;squery-filters []
         filters []
         mql-pipeline []]
    (if (empty? squery-pipeline)
      (if (empty? filters)
        mql-pipeline
        (conj mql-pipeline (squery-filters->match-stage filters)))
      (let [stage (first squery-pipeline)]
        (cond

          (or (= stage []) (nil? stage))                  ; ignore [] or nil stages
          (recur (rest squery-pipeline) filters mql-pipeline)

          (qfilter-stage? stage)
          (recur (rest squery-pipeline) (conj filters stage) mql-pipeline)

          (add-stage? stage)                             ; {:a ".." :!b ".."}
          (let [stage (apply squery-addFields->mql-addFields [stage])]
            (if (vector? stage)                 ; 1 project stage might produce nested stages,put the nested and recur
              (recur (concat stage (rest squery-pipeline)) filters mql-pipeline) ; do what is done for nested stages(see below)
              (if (empty? filters)
                (recur (rest squery-pipeline) [] (conj mql-pipeline stage))
                (recur (rest squery-pipeline) [] (conj mql-pipeline (squery-filters->match-stage filters) stage)))))

          (project-stage? stage)                             ; [:a ....]
          (let [stage (apply squery-project->mql-project stage)]
            (if (vector? stage)                 ; 1 project stage might produce nested stages,put the nested and recur
              (recur (concat stage (rest squery-pipeline)) filters mql-pipeline) ; do what is done for nested stages(see below)
              (if (empty? filters)
                (recur (rest squery-pipeline) [] (conj mql-pipeline stage))
                (recur (rest squery-pipeline) [] (conj mql-pipeline (squery-filters->match-stage filters) stage)))))

          (vector? stage)      ; vector but no project = nested stage,add the members as stages and recur     ; TODO: REVIEW:
          (recur (concat stage (rest squery-pipeline)) filters mql-pipeline)

          (stage-operator? stage)                                    ; normal stage operator {}
          (if (empty? filters)
            (recur (rest squery-pipeline) [] (conj mql-pipeline stage))
            (recur (rest squery-pipeline) [] (conj mql-pipeline (squery-filters->match-stage filters) stage)))

          :else                        ; filter stage (not qfilter,they are collected above)
          (recur (rest squery-pipeline) (conj filters stage) mql-pipeline))))))


;;;---------------------------------------read-write--------------------------------------------------------------------
;;;---------------------------------------------------------------------------------------------------------------------

(defn command-keys [command-def]
  (clojure.set/union (into #{} (map (fn [k]
                                      (if (string? k)
                                        (keyword k)
                                        k))
                                    (keys command-def)))
                     squery-specific-options))

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

(defn upsert-doc [args]
  (reduce (fn [[upsert-doc args] arg]
            (if (contains? arg :upsert)
              [(get arg :upsert) (conj args {"upsert" true})]
              [upsert-doc (conj args arg)]))
          [nil []]
          args))

(defn args->query-updatePipeline-options
  "Seperates update arguments to [query update-pipeline options]
   Its used from update command,and from others like delete(dq) , that dont have pipeline just query and options"
  [args command-keys]
  (let [[filters update-pipeline args]
        (reduce (fn [[query update-pipeline args] arg]
                  (cond

                    (command-option? arg command-keys)      ; position is important,the rest are addFields
                    [query update-pipeline (conj args arg)]

                    (update-pipeline-stage? arg)
                    [query (conj update-pipeline arg) args]

                    :else                                   ;;query form
                    [(conj query arg) update-pipeline args]
                    ))
                [[] [] []]
                args)

        ;;filters,update-pipeline will be converted using the aggregation common functions
        ;;to do all the necessary processing squery does
        ]
    [(if (empty? filters) {} (get (first (squery-pipeline->mql-pipeline filters)) "$match"))
     (squery-pipeline->mql-pipeline update-pipeline)
     args]))

(defn args->query-updateOperators-options
  "Seperates update arguments to [query update-pipeline options]
   Its used from update command,and from others like delete(dq) , that dont have pipeline just query and options"
  [args command-keys]
  (let [[filters updateOperators args]
        (reduce (fn [[filters updateOperators args] arg]
                  (cond

                    (command-option? arg command-keys)      ; position is important,the rest are addFields
                    (if (contains? arg :arrayFilters)
                      (let [array-filters (get arg :arrayFilters)
                            array-filters (mapv (fn [f] (if (contains? f "$__q__")
                                                          (get f "$__q__")
                                                          f))
                                                array-filters)]
                        [filters updateOperators (conj args {:arrayFilters array-filters})])
                      [filters updateOperators (conj args arg)])

                    (contains? arg "$__u__")
                    [filters (conj updateOperators (get arg "$__u__")) args]

                    (contains? arg "$__us__")
                    [filters (apply (partial conj updateOperators) (get arg "$__us__")) args]

                    :else                                   ;;query form
                    [(conj filters arg) updateOperators args]
                    ))
                [[] [] []]
                args)

        ]
    [(if (empty? filters) {} (get (first (squery-pipeline->mql-pipeline filters)) "$match"))
     updateOperators
     args]))


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

;;;----------------------------------scommand->mcommand(squery-map->mql-map (command is a map))----------------------------------
;;; squery-map->mql-map (to make a valid mongo command) , that will be converted to Document(if java driver) and runCommand()

(declare squery-map->mql-map)

(defn squery-vector->mql-vector [v]
  (loop [v v
         mql-vector []]
    (if (empty? v)
      mql-vector
      (let [mb (first v)]
        (cond
          (map? mb)
          (let [mb (squery-map->mql-map mb )]
            (recur (rest v) (conj mql-vector mb)))

          (vector? mb)
          (let [mb (squery-vector->mql-vector mb )]
            (recur (rest v) (conj mql-vector mb)))

          :else
          (recur (rest v) (conj mql-vector (squery-var-ref->mql-var-ref mb))))))))

(defn squery-map->mql-map
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
          (let [vl (squery-map->mql-map vl)]
            (recur (rest ks) (assoc mql-map k vl)))

          (vector? vl)
          (let [vl (squery-vector->mql-vector vl)]
            (recur (rest ks) (assoc mql-map k vl)))

          :else
          (recur (rest ks) (assoc mql-map k (squery-var-ref->mql-var-ref vl))))))))


;;----------------------------db-namespaces--------------------------------------------------------------------

;;TODO in case db name has .  doesnt work
(defn split-db-namespace
  "Arguments can be
   :db-name.coll-name
   :db-name
   :.coll-name     ;;db-name will be '',the default db will be added later in the command
   Seperator can be the . or /
   "
  [db-namespace]
  (let [seperator (if (clojure.string/includes? (name db-namespace) "/") #"/" #"\.")
        parts (clojure.string/split (name db-namespace) seperator)
        db-name (first parts)
        coll-name (clojure.string/join "." (rest parts))
        db-name (if (= db-name "") nil db-name)]
    [db-name coll-name]))
