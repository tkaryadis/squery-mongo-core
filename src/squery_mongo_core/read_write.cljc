(ns squery-mongo-core.read-write
  (:require [squery-mongo-core.utils :refer [ordered-map]]
            [squery-mongo-core.internal.convert.common :refer [single-maps]]
            [squery-mongo-core.internal.convert.commands :refer
             [split-db-namespace
              command-keys get-pipeline-options squery-pipeline->mql-pipeline
              args->query-updatePipeline-options args->query-updateOperators-options upsert-doc seperate-bulk
              squery-map->mql-map]]
            squery-mongo-core.operators.operators))

;;also i want to be able to take a raw-mongo-command(only the necessary args and the rest in a map) and run it

;;;----------------------------------------------insert-----------------------------------------------------------------
;;;---------------------------------------------------------------------------------------------------------------------

(def insert-def
  {
   :insert                   "collection"
   :documents                ["document" "document" "document" "..."]
   :ordered                  "boolean"    ;Defaults to true.if one fails => remaining are not inserted
   :writeConcern             "document"
   :bypassDocumentValidation "boolean"
   :comment                  "any"
   })

(defn insert
  "Call
  (insert :testdb.testcoll doc option1 option2 ...)
  (insert :testdb.testcoll [doc1 doc2 ...] option1 option2})"
  [db-namespace documents & args]
  (let [[db-name coll-name] (split-db-namespace db-namespace)
        command-keys (command-keys insert-def)
        args (single-maps args command-keys)
        documents (if (map? documents) [documents] documents)
        mql-map (squery-map->mql-map (apply (partial merge {}) args))

        command-head {"insert" coll-name}
        command-body (merge {"documents" documents} mql-map)
        ;;_ (clojure.pprint/pprint (into {} command-map))
        ]
    {:db db-name
     :coll coll-name
     :command-head command-head
     :command-body command-body}))

;;;----------------------------------------------delete-----------------------------------------------------------------
;;;---------------------------------------------------------------------------------------------------------------------

(def delete-def
  {
   :delete "collection",
   :deletes [{ :q  "doc-query", :limit  "integer", :collation "document" :hint "document/string" :comment ""}, ; (dq ..)

             ],                                               ;;many for bulk operations
   :ordered "boolean",
   :writeConcern "document"
   :comment "any"
   })

(defn dq-f
  "Delete query,one delete command can take one or many dqs
  Each dq will be a member of :deletes [],see delete command
  Call
   (dq filter1 filter2)
   (dq (>_ age 25) {:comment '...'})"
  [& args]
  (let [command-keys (command-keys (get-in delete-def [:deletes 0]))       ; q limit etc
        args (single-maps args command-keys)
        [q _ options] (args->query-updatePipeline-options args command-keys) ; only q and options can be found here
        options (apply (partial merge {})options)
        options (assoc options :limit (get options :limit 0))]   ; delete all is the default,else user :limit 1
    {:dq (merge {:q q} options)}))

(defn delete
  "Delete documents using one or more delete queries (dq ..)
  Call
  (delete :testdb.testcoll (dq ...) (dq ..) opt1 opt2 ...)
  (delete :testdb.testcoll [(dq ...) (dq ..)] {:ordered true} ...)
  Delete all matching is the default {:limit 0}"
  [db-namespace & args]
  (let [
        [db-name coll-name] (split-db-namespace db-namespace)
        command-keys (command-keys delete-def)
        args (single-maps args command-keys)
        args (flatten args)                          ;; bulk deletes ([dq1 dq2] arg1 arg2) => (dq1 dq2 arg1 arg2)
        [deletes options] (seperate-bulk :dq args)
        deletes (mapv (fn [m] (get m :dq)) deletes)

        command-body (apply (partial merge {}) (conj options {:deletes deletes}))
        command-body (squery-map->mql-map command-body)
        command-head  {"delete" coll-name}

        ;- (clojure.pprint/pprint command-body)
        ]
    {:db db-name
     :coll coll-name
     :command-head command-head
     :command-body command-body}))


;;;--------------------------------------find(not used unless reason to not aggregate)----------------------------------
;;;---------------------------------------------------------------------------------------------------------------------

(def find-def
  #?(:clj {
           :find "string",

           ;;those are not to be used by the user
           ;;the user only writes a pipeline and they are auto-generated
           :filter "document",
           :projection "document",
           :sort "document",
           :skip "int",
           :limit "int",

           ;;options
           :max "document",
           :min "document",
           :comment "string",
           :batchSize "int",
           :awaitData "bool",
           :noCursorTimeout "bool",
           :oplogReplay "bool",
           :returnKey "bool",
           :showRecordId "bool",
           :allowDiskUse "bool"

           ;;Different name same value
           :allowPartialResults "bool"


           ;;Same name,different value
           :hint "document|string"     ;;java only document
           :collation "Document|Collation_class"      ;;java only Collation_Class

           ;;Different name,maybe value

           ;;Command only
           :maxTimeMS "int"     ;;alias for maxTime milliseconds


           ;;Java only
           :hintString "string"  ;;hint with only string value
           :partial "bool"       ;;allowPartialResults
           :cursorType "CursorType_class"
           :maxAwaitTime "[number,TimeUnit_class]"
           :maxTime "[number,TimeUnit_class]"


           ;;Command specific (no option in method)
           :readConcern "document"    ;;argument in find() call
           :singleBatch "bool"        ;;part of cursorType
           :tailable "bool"           ;;part of cursorType
           }

     :cljs {
            :find "string",

            ;;those are not to be used by the user
            ;;the user only writes a pipeline and they are auto-generated
            :filter "document",
            :projection "document",
            :sort "document",
            :skip "int",
            :limit "int",

            ;;options
            :hint "document"
            :snapshot "boolean"  ;;depre
            :timeout "boolean"
            :tailable "boolean"
            :awaitData "boolean"
            :batchSize "int"
            :returnKey "bool"
            :maxScan "number"       ;;depre
            :min "number"
            :max "number"
            :showDiskLoc "boolean"
            :comment "string"
            :raw "boolean"
            ;:promotelongs
            :promoteValues "boolean"
            :promoteBuffers "boolean"
            :readPreference "readPreference|string"
            :partial "boolean"
            :maxTimeMS "number"
            :maxAwaitTime "number"
            :noCursorTimeout "bool"
            :collation "document"
            :allowDiskUse "bool"
            :explain "queryPlanner
                      |queryPlannerExtended
                      |executionStats
                      |allPlansExecution
                      |boolean"
            }))

;;find cannot exists as command,because cursor implementation was removed
;;for simplicity,so it will be always a method call,called like a command
(defn fq-f
  "Find query
  filters/project/sort/skip/limit as if it was pipeline stages
  (they are auto converted to find command options)
  The rest are options
  Call
  (fq :testdb.testcoll
      (>_ :spent 150)
      (sort- :!spent)
      [:!_id :spent {:aspent (+_ :spent 20)}]
      (skip 5)
      (limit 1)
      {:singleBatch true})"
  [db-namespace & args]
  (let [
        [db-name coll-name] (split-db-namespace db-namespace)
        command-keys (command-keys find-def)
        args (single-maps args command-keys)
        [query pipeline args] (args->query-updatePipeline-options args command-keys)
        args (conj args (if (empty? query) {} {:filter query}))

        stages-to-options (map (fn [stage]
                                 (cond

                                   (= (first (keys stage)) "$project")
                                   {:projection (first (vals stage))}

                                   (= (first (keys stage)) "$sort")
                                   {:sort (first (vals stage))}

                                   (= (first (keys stage)) "$limit")
                                   {:limit (first (vals stage))}

                                   (= (first (keys stage)) "$skip")
                                   {:skip (first (vals stage))}

                                   :else
                                   stage))
                               pipeline)

        args (concat args stages-to-options)
        squery-map (apply (partial merge {}) args)
        squery-map (if (contains? squery-map :maxTimeMS)
                       squery-map
                      (assoc squery-map :maxTimeMS 0))

        command-head {"find" coll-name}
        command-body (squery-map->mql-map squery-map)

        ;- (clojure.pprint/pprint command-doc)
        ]
    {:db db-name
     :coll coll-name
     :command-head command-head
     :command-body command-body}))



;;------------------------------update-----------------------------------------------------
;;-----------------------------------------------------------------------------------------
;;-----------------------------------------------------------------------------------------

(def update-def
  {
   :update "collection-string"
   :updates [
             {
              :q "query-document"
              :u "document or pipeline"       ;;$addFields=$set/$project=$unset/$replaceRoot=$replaceWith
              :upsert "bool"
              :multi "bool"
              :collation "document"
              :arrayFilters "array"
              :hint "document|string"
              }
             "...."
             ]
   :ordered "boolean"
   :writeConcern "document"
   :bypassDocumentValidation "boolean"
   })

;;"$__u__"

(defn uq-f
  "Update query,one update command can take one or many uqs
  Each uq will be a member of :updates [].see update command
  uq works only with pipeline updates
  query is always filters,document is only when {:upsert true}
  Call
  (uq (=_ :_id 2)
      {:grade (+_ :grade 5)}
      [:!_id :name])

  Upsert (if upsert true => document always as first argument)
  (uq {:_id 4}
      {:grade (if- (exist?- :grade)
                (+_ :grade 5)
                10)}
      {:upsert true})"
  [& args]
  (let [not-pipeline? (reduce (fn [not-pipeline? arg] (or not-pipeline?
                                                          (contains? arg "$__u__")
                                                          (contains? arg "$__us__"))) false args)
        command-keys (command-keys (get-in update-def [:updates 0]))
        args (single-maps args command-keys)
        [upsert-query args] (upsert-doc args)]
    (if not-pipeline?
      (let [[query update-operators options] (args->query-updateOperators-options args command-keys)
            query (if (some? upsert-query) upsert-query query)
            update-operators (apply (partial merge {}) update-operators)
            options (apply (partial merge {}) options)
            options (assoc options :multi (get options :multi true))]
        {:uq (merge {:q query} {:u update-operators} options)})
      (let [[query update-pipeline options] (args->query-updatePipeline-options args command-keys)
            query (if (some? upsert-query) upsert-query query)
            options (apply (partial merge {}) options)
            options (assoc options :multi (get options :multi true))]
        {:uq (merge {:q query} {:u update-pipeline} options)}))))

(defn update-
  "Update documents using one or more update queries uqs
  Update many is the default {:limit 0}
  Call
  (update- :testdb.testcoll (uq ...) (uq ..) opt1 opt2 ...)
  (update- :testdb.testcoll [(uq ...) (uq ..)] opt1 opt2 ...)"
  [db-namespace & args]
  (let [[db-name coll-name] (split-db-namespace db-namespace)
        command-keys (command-keys update-def)
        args (single-maps args command-keys)
        args (flatten args) ;; bulk updates ([uq1 uq2] arg1 arg2) => (uq1 uq2 arg1 arg2)
        [updates options] (seperate-bulk :uq args)
        updates (mapv (fn [m] (get m :uq)) updates)

        squery-map (apply (partial merge {}) (conj options {:updates updates}))

        command-head {"update" coll-name}

        command-body (squery-map->mql-map squery-map)
        ]
    {:db db-name
     :coll coll-name
     :command-head command-head
     :command-body command-body}))

;;------------------------------find and modify--------------------------------------------
;;-----------------------------------------------------------------------------------------
;;-----------------------------------------------------------------------------------------

(def find-and-modify-def
  {
   :findAndModify "collection-name"
   :query "document"       ;;which to modify
   :update "pipeline or document"
   :sort "document"

   :remove "boolean"
   :new "boolean"
   :fields "document"
   :upsert "boolean"
   :bypassDocumentValidation "boolean"
   :writeConcern "document"
   :collation "document"
   :arrayFilters "array"
   })

(defn find-and-modify-f
  "Find and Modify as one step
  query/update/sort as if it was pipeline stages
  The rest are options
  If many match the first document is choosen
  Call
  Find-Modify (no upsert)
  (find-and-modify :testdb.testcoll
                   (=_ :name 'MongoDB')
                   {:name (str- :name '-with-id=1')}
                   (sort- :_id)
                   (fields-o :!_id :name)
                   (new-o))

  Upsert (when upsert true => first argument doc(only then))
  (find-and-modify :testdb.testcoll
                   {:_id 4}
                   {:name (if- (exist?- :name)
                            (str- :name '-with-id=4')
                            'new-name')}
                   {:upsert true} ;; upsert => first argument document
                   (fields-o :!_id :name)
                   (new-o))"
  [db-namespace & args]
  (let [not-pipeline? (reduce (fn [not-pipeline? arg] (or not-pipeline? (contains? arg "$__u__"))) false args)
        [db-name coll-name] (split-db-namespace db-namespace)
        command-keys (command-keys find-and-modify-def)
        args (single-maps args command-keys)
        [upsert-query args] (upsert-doc args)

        [query update-operators args]
        (if not-pipeline?
          (let [[query update-operators options] (args->query-updateOperators-options args command-keys)
                update-operators (apply (partial merge {}) update-operators)
                options (apply (partial merge {}) options)]
            [query update-operators options])
          (let [[query update-pipeline args] (args->query-updatePipeline-options args command-keys)
                [stage-options update-pipeline]  (reduce (fn [[options stages] stage]
                                                           (if (= (first (keys stage)) "$sort")
                                                             [(conj options {:sort (first (vals stage))}) stages]
                                                             [options (conj stages stage)]))
                                                         [[] []]
                                                         update-pipeline)
                args (concat args stage-options)]
            [query update-pipeline args]))

        query (if (some? upsert-query) upsert-query query)

        ;;update-pipeline can only have 1 or more projects,that will combined to one
        args (conj args (if (empty? query) {} {:query query}))
        args (conj args (if (empty? update-operators) {} {:update update-operators}))

        squery-map (apply (partial merge {}) args)

        command-head {"findAndModify" coll-name}
        command-body (squery-map->mql-map squery-map)

        ;- (clojure.pprint/pprint command-map)
        ]

    {:db db-name
     :coll coll-name
     :command-head command-head
     :command-body command-body}))


;;------------------------------aggregate--------------------------------------------------
;;-----------------------------------------------------------------------------------------
;;-----------------------------------------------------------------------------------------

(def aggregate-def
  #?(:clj {
          :aggregate "collection || 1",
          :pipeline ["stage" "..."],
          :hint "string or document",
          :collation "document",
          :cursor "document",
          :readConcern "document",
          :writeConcern "document",
          :maxTimeMS "int",
          :allowDiskUse "boolean",
          :bypassDocumentValidation "boolean",
          :explain "boolean"
          :comment "any"


          }

     :cljs {
            :aggregate "collection || 1",
            :pipeline ["stage" "..."],
            :hint "string or document",
            :collation "document",
            :cursor "document",
            :readConcern "document",
            :writeConcern "document",
            :maxTimeMS "int",
            :allowDiskUse "boolean",
            :bypassDocumentValidation "boolean",
            :explain "boolean"
            :comment "any"

            ;;cljs extra driver specific,for commands that will run as method
            :readPreference "ReadPreference | string"
            :batchSize "number"
            :maxAwaitTimeMS "number"
            :raw "boolean"
            ;:promoteLongs
            :promoteValues "boolean"
            :promoteBuffers "boolean"

            }))

(defn q-f
  "()->filters {}->addfields []->project
  Pipeline stages can be nil or nested

  Call example
  (q :testdb.testcoll
      (> :age 25)
      (= :gender 'female')           ; filter, both will be 1 match stage with $and and $expr
      {:pass (> :grade 5)}           ; addFields
      (group :hairColor               ; :hairColor :count  ,here :_id will be auto-replaced by :hairColor
             {:count (sum-a 1)})
      [:count]                        ; project,keep only count
      (> :count 20)                  ; another filter alone => 1 match stage
      (sort :!count)                 ; sort descending
      (skip 2)
      (limit 1))"
  [db-namespace & args]
  (let [[db-name coll-name] (split-db-namespace db-namespace)
        command-keys (command-keys aggregate-def)
        args (single-maps args command-keys)
        [pipeline args] (get-pipeline-options args command-keys)
        pipeline (squery-pipeline->mql-pipeline pipeline)
        args (conj args {:pipeline (into [] pipeline)})
        squery-map (apply (partial merge {}) args)

        squery-map (if (contains? squery-map :cursor)
                   squery-map
                   (assoc squery-map :cursor {}))

        squery-map (if (contains? squery-map :maxTimeMS)
                   squery-map
                   (assoc squery-map :maxTimeMS (* 20 60 1000))) ;;5 minute alive max

        command-head {"aggregate" coll-name}
        command-body (squery-map->mql-map squery-map)
        ]
    {:db db-name
     :coll coll-name
     :command-head command-head
     :command-body command-body}))

;;no count implementation,if count needed,add (count-documents) "stage" at
;;the pipeline its implemented as  (this works on sharded clusters,transactions also)
;;its like the shell command count-documents,but here like extra stage only
;;(defn count-documents []
;  [{ "$group" { :_id nil :count { "$sum" 1}}}
;   { "$project" { :_id 0}}])

(def count-def
  {
   :count "collection/view"

   ;use as aggregation stages
   :query "document"
   :skip  "integer"
   :limit "integer"

   :hint  "hint"
   :readConcern "document"
   :collation "document"
   :comment "any"
   })

(defn q-count-f
  "Count documents with optional query on them
  Can be done with aggregation easily
  query/limit/skip use as aggregate stages
  Call
  (q-count  :testdb.testcoll
            (= :dept 'A')      ; filters like aggregation
            (skip 1)
            (limit 1))"
  [db-namespace & args]
  (let [
        [db-name coll-name] (split-db-namespace db-namespace)
        command-keys (command-keys count-def)
        args (single-maps args command-keys)
        [query pipeline args] (args->query-updatePipeline-options args command-keys)

        stages-to-options (map (fn [stage]
                                 (cond

                                   (= (first (keys stage)) "$limit")
                                   {:limit (first (vals stage))}

                                   (= (first (keys stage)) "$skip")
                                   {:skip (first (vals stage))}

                                   :else
                                   stage))
                               pipeline)

        args (concat args stages-to-options)
        args (conj args (if (empty? query) {} {:query query}))

        squery-map (apply (partial merge {}) args)

        command-head {"count" coll-name}
        command-body (squery-map->mql-map squery-map)

        ;- (clojure.pprint/pprint command-map)
        ]
    {:db db-name
     :coll coll-name
     :command-head command-head
     :command-body command-body}))



;;------------------------------distinct(aggregate)----------------------------------------
;;-----------------------------------------------------------------------------------------
;;-----------------------------------------------------------------------------------------

(def distinct-def
  {
   :distinct "<collection>",
   :query "query" ,
   :key "<field>",
   :readConcern "<read concern document>,"
   :collation "<collation document>"

   })

(defn q-distinct-f
  "Distinct a field,make it an array,with optinal query first
  Can be done with aggregation easily
  Call
  (q-distinct :testdb.testcoll
              (not= :dept 'B')
              {:key 'dept'})"
  [db-namespace & args]
  (let [
        [db-name coll-name] (split-db-namespace db-namespace)
        command-keys (command-keys distinct-def)
        args (single-maps args command-keys)
        [query _ args] (args->query-updatePipeline-options args command-keys)
        args (conj args (if (empty? query) {} {:query query}))

        squery-map (apply (partial merge {}) args)

        command-head {"distinct" coll-name}

        command-body (squery-map->mql-map squery-map)

        ;- (clojure.pprint/pprint command-map)
        ]
    {:db db-name
     :coll coll-name
     :command-head command-head
     :command-body command-body}))
