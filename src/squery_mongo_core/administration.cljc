(ns squery-mongo-core.administration
  (:require [squery-mongo-core.internal.convert.common :refer [single-maps ]]
            [squery-mongo-core.internal.convert.commands :refer [command-keys get-pipeline-options squery-pipeline->mql-pipeline
                                                   squery-map->mql-map split-db-namespace]]
            [squery-mongo-core.internal.convert.stages :refer [squery-vector->squery-map]]
            [squery-mongo-core.utils :refer [keyword-map]]))

;;-------------------------------------Collection/Indexes/Views---------------------------------------------------------
;;----------------------------------------------------------------------------------------------------------------------
;;----------------------------------------------------------------------------------------------------------------------

(def create-def
  {
   :create  "collection or view name"

   ;;views only
   :viewOn   "collection source"   ;;only for views,db always the same
   ;;the view definition (it is included as view info,after view is created)
   :pipeline  "pipeline"   ;;except $out or the $merge,on nested pipelines also

   ;;collections
   ;;validation
   :validator     "document"
   :validationLevel "string"
   :validationAction "string"
   ;;capped
   :capped  "bool"         ;;default=false,if capped i have to set :size also
   :size "max-size"        ;;only for capped collections ,max size in bytes(requeired if capped)
   :max   "max_documents"  ;;only for capped max size in documents(optional even if capped)

   :autoIndexId "bool"     ;;i dont use this,default=true=>index on _id
   :indexOptionDefaults "document"   ;;default options when i create an index in that collection

   ;;only for wiredTiger,configure the wireTiger
   ;;{ "wiredTiger" <options> }
   :storageEngine "document"  ;;

   :collation    "document"
   :writeConcern "document"

   })

(defn create-view
  "Creates a view
  View=predefined pipeline to run on the source collection
  They are not saved in memory or in disk,they are re-computed each time
  Can be used when many queries have a common starting part
  Takes the pipeline and 'creates' the view in the database of the source
  Pipeline stages are args without []"
  [db-namespace new-view-name & args]
  (let [
        [db-name coll-name] (split-db-namespace db-namespace)
        command-keys (command-keys create-def)
        args (single-maps args command-keys)
        
        [pipeline args] (get-pipeline-options args command-keys)
        pipeline (squery-pipeline->mql-pipeline pipeline)
        args (conj args {:viewOn (name coll-name)} {:pipeline pipeline})

        squery-map (apply (partial merge {}) args)

        command-head {"create" (name new-view-name)}
        command-body (squery-map->mql-map squery-map)


        ;- (clojure.pprint/pprint command-map)
        ]
    {:db db-name
     :coll coll-name
     :command-head command-head
     :command-body command-body}))


(defn create-collection [db-namespace & args]
  (let [
        [db-name coll-name] (split-db-namespace db-namespace)
        squery-map (apply (partial merge {})args)

        command-head {"create" coll-name}
        command-body (squery-map->mql-map squery-map)
        ;- (clojure.pprint/pprint command-map)
        ]
    {:db db-name
     :coll coll-name
     :command-head command-head
     :command-body command-body}))

(def rename-collection-def
  {
   :renameCollection "source_namespace"
   :to "<target_namespace>",
   :dropTarget "<true|false>"
   :writeConcern "<document>"
   :comment "<any>"
   })

;;{ renameCollection: "<source_namespace>", to: "<target_namespace>", dropTarget: <true|false>  writeConcern: <document> }
;;dropTarget is for cases when the new-name already exist as a collection,and i want to replace it
;;  (default is false) => rename will fail
(defn rename-collection [source-db-namespace target-db-namespace & args]
  (let [db-name "admin"
        args (conj args {:to (name target-db-namespace)})
        squery-map (apply (partial merge {}) args)
        command-head {"renameCollection" (name source-db-namespace)}
        command-body (squery-map->mql-map squery-map)]
    {:db db-name
     :coll nil
     :command-head command-head
     :command-body command-body}))


(def drop-def
  {
   :drop "<collection_name>"
   :writeConcern "<document>"
   :comment  "<any>"
   })

(defn drop-collection
  "Drops the collection and its indexes and its associated zone/tag ranges"
  [db-namespace & args]
  (let [
        [db-name coll-name] (split-db-namespace db-namespace)
        squery-map (apply (partial merge {}) args)

        command-head {"drop" coll-name}
        command-body (squery-map->mql-map squery-map)]
    {:db db-name
     :coll coll-name
     :command-head command-head
     :command-body command-body}))

(def list-collections-def
  {
   :listCollections 1,
   :filter "<document>",
   :nameOnly "<boolean>",
   :authorizedCollections "<boolean>",
   :comment "<any> "
   })

(defn list-collections
  "Returns cursor with collection info"
  [db-name & args]
  (let [db-name (name db-name)
        squery-map (if (contains? args :filter)
                  (assoc args :filter {"$expr" (get args :filter)})
                  args)

        command-head {"listCollections" 1}
        command-body (squery-map->mql-map squery-map)]
    {:db db-name
     :coll nil
     :command-head command-head
     :command-body command-body}))


;;------------------------------------------------Indexes---------------------------------------------------------------
;;----------------------------------------------------------------------------------------------------------------------
;;----------------------------------------------------------------------------------------------------------------------

(def create-indexes-def
    {
     :createIndexes "string-collectionName"
     :indexes [
               ;;index1
               {
                :key  {
                       "field1" "0/1/text..."
                       "field2" "0/1/text..."
                       ;;....
                       }
                :name "string-indexName"
                :background "boolean"
                :unique "boolean"
                :partialFilterExpression "document"
                :sparse "boolean"
                :expireAfterSeconds "integer"
                :hidden "boolean"
                :storageEngine "document"
                :weights "document"
                :default_language "string"
                :language_override "string"
                :textIndexVersion "integer"
                :2dsphereIndexVersion "integer"
                :bits "integer(1-32,26=default)"
                :min "number"
                :max "number"
                :bucketSize "number"
                :collation "document"
                :wildcardProjection "document"
                }
               ;;index2
               {
                ;;...
                }
               ]
     :commitQuorum "<int|string>"
     :comment      "<any>"
     })

(defn get-index-name [index-sorted-map]
  (clojure.string/join "_" (map (fn [m]
                                  (if (keyword? m)
                                    (name m)
                                    m))
                                (flatten (into [] index-sorted-map)))))

(defn index
  "Index definition,one create-index command can take one or many indexes
  Each index will be a member of :indexes [],see create-index command
  keys-vec = [:field1 :!field2 [:field3 1] [:field4 -1]...]
  keys-vec = [[:field1 'text']]  ; for text index
  Index name if not given = 'fieldName_type_fieldName_type'
  Call
  (index [:field1 :!field2 ..]) "
  [keys-vec & args]
  (let [index-sorted-map (squery-vector->squery-map (mapv (fn [k]
                                                  (if (vector? k) (into {} [k]) k))
                                                keys-vec)
                                          -1)
        index-name (get-index-name index-sorted-map)
        options-map (apply (partial merge {})args)
        options-map (if (contains? options-map :name)
                      options-map
                      (assoc options-map :name index-name))]
    {:index (merge {:key index-sorted-map} options-map)}))

(defn create-indexes
  "Creates one or more indexes
  Index name if not given = 'fieldName_type_fieldName_type'
  Call
  (create-indexes (index keys-vec option1 ...) (index ...)  option1)"
  [db-namespace & args]
  (let [[db-name coll-name] (split-db-namespace db-namespace)
        [indexes options] (reduce (fn [[indexes options] m]
                                    (if (contains? m :index)
                                      [(conj indexes (get m :index)) options]
                                      [indexes (conj options m)]))
                                  [[] []]
                                  args)
        options-map (apply (partial merge {}) options)
        squery-map (merge {:indexes indexes} options-map)

        command-head {"createIndexes" coll-name}
        command-body (squery-map->mql-map squery-map)

        ;_ (prn command-head)
        ;_ (clojure.pprint/pprint command-body)
        ]
    {:db db-name
     :coll coll-name
     :command-head command-head
     :command-body command-body}))

(defn create-index [coll-namespace index & options]
  (apply (partial create-indexes coll-namespace index) options))

(def drop-indexes-def
  {
   :dropIndexes "<string>",
   :index "<string|document|arrayofstrings>",
   :writeConcern "<document>",
   :comment "<any> "
   })

(defn drop-indexes
  "Drop specified indexes from the collection(except the index on _id)
  Index name the manual or the auto-created name or use key-vec
  Index name if not given = 'fieldName_type_fieldName_type'
  To drop some =  [indexname-or-key-vec indexname-or-key-vec ...]
  To drop all  =  '*'
  Call
  (drop-indexes ['myindex' [:afield :!bfield] 'afield_1_bfield_-1'])
  (drop-indexes '*')
   "
  [db-namespace indexnames-key-vecs & options]
  (let [[db-name coll-name] (split-db-namespace db-namespace)
        index-names (if (= indexnames-key-vecs "*") ;;delete all
                        "*"
                        (mapv (fn [index-name]
                                (if (vector? index-name)
                                  (let [index-sorted-map (squery-vector->squery-map (mapv (fn [k]
                                                                                  (if (vector? k) (into {} [k]) k))
                                                                                index-name)
                                                                          -1)
                                        index-name (get-index-name index-sorted-map)]
                                    index-name)
                                  index-name))
                              indexnames-key-vecs))

        options-map (apply (partial merge {}) options)
        squery-map (merge {:index index-names} options-map)

        command-head {"dropIndexes" coll-name}
        command-body (squery-map->mql-map squery-map)

        ;- (clojure.pprint/pprint command-map)
        ]
    {:db db-name
     :coll coll-name
     :command-head command-head
     :command-body command-body}))

(defn drop-index
  "To drop one index
  Call
  (drop-index indexname-or-key-vec)"
  [coll-namespace indexname-key-vec & options]
  (apply (partial drop-indexes coll-namespace [indexname-key-vec]) options))

;;{ "listIndexes": "<collection-name>" }
(defn list-indexes
  "Returns a cursor with the indexes(version name key(doc-definition) ns(db.collection))"
  [db-namespace & args]
  (let [
        [db-name coll-name] (split-db-namespace db-namespace)

        command-head {"listIndexes" coll-name}
        command-body {}]
    {:db db-name
     :coll coll-name
     :command-head command-head
     :command-body command-body}))

(def kill-cursors-def
  {
   :killCursors "<collection>",
   :cursors "[ <cursor id1>, ... ]"
   :comment "any"
   })

(defn kill-cursors
  "arg = id
         cursor
         [id1 id2 cursor3 id4 ...]
  If some cursor is exchausted(cursor-id 0) it is excluded
  If all are exchausted ,command isn't sended at all
  Call
  (kill-cursors id1 cursor2 [cursor3 id4] id5 [cursor6]"
  [db-namespace & args]
  (let [[db-name coll-name] (split-db-namespace db-namespace)
        cursor-ids (reduce (fn [cursor-ids c]
                             (let [c-id (if (map? c) (get c :id) c)]
                               (if (= c-id 0)
                                 cursor-ids
                                 (conj cursor-ids c-id))))
                           []
                           (flatten args))]
    (if-not (empty? cursor-ids)
      (let [squery-map {:cursors cursor-ids}
            command-head {"killCursors" coll-name}
            command-body (squery-map->mql-map squery-map)]
        {:db db-name
         :coll coll-name
         :command-head command-head
         :command-body command-body}))))



;;-----------------------------------Database--------------------------------------------------------------------------

;;exclusive(X) database lock only.
;;TODO
;;This command does not delete the users associated with the current database.
;;To drop the associated users, run the dropAllUsersFromDatabase command in the database you are deleting.
;;creates an invalidate Event for any Change Streams opened on the dropped database or opened on the collections in the dropped database.
;;TODO cluster and sharded

(def drop-database-def
  {
   :dropDatabase 1,
   :writeConcern "<document>",
   :comment "<any>"
   })

(defn drop-database [db-name & args]
  (let [db-name (name db-name)
        squery-map (apply (partial merge {}) args)

        command-head {"dropDatabase" 1}
        command-body (squery-map->mql-map squery-map)]
    {:db db-name
     :coll nil
     :command-head command-head
     :command-body command-body}))

(def list-databases-def
  {

   :listDatabases 1, ;;auto-added here
   :filter "optional filter with aggregation only,by default expr",
   :nameOnly "boolean , optional , if true only name else name+size (size=>lock database to get it)"
   :authorizedDatabases "boolean"

   })

(defn list-databases
  "Returns 1 document with array that contains all the databases info"
  [& args]
  (let [db-name "admin"
        args (map keyword-map args)          ; i need it to safe check :filter bellow
        squery-map (if (some? (first args))
                  (first args)
                  {})
        squery-map (if (contains? squery-map :filter)
                      (assoc squery-map :filter {"$expr" (get squery-map :filter)})
                      squery-map)

        command-head {"listDatabases" 1}
        command-body (squery-map->mql-map squery-map)]
    {:db db-name
     :coll nil
     :command-head command-head
     :command-body command-body}))

(def shutdown-def
  {
   :shutdown 1,
   :force  "<boolean>"
   :timeoutSecs "<int>",
   :comment "<any>"
   })

(defn shutdown-database [& args]
  (let [command-keys (command-keys shutdown-def)
        args (single-maps args command-keys)
        db-name "admin"
        options-map (apply (partial merge {})args)
        squery-map options-map

        command-head {"shutdown" 1}
        command-body (squery-map->mql-map squery-map)]
    {:db db-name
     :coll nil
     :command-head command-head
     :command-body command-body}))

;;-------------------------------general--------------------------------------------------------------------------------

(defn current-op
  "Like original command,just allows many maps as arguments
  that will be merged in 1 map
  option = $ownOps,$all,filter-operator,comment"
  [& args]
  (let [
        db-name "admin"
        squery-map (apply (partial merge {}) args)

        command-head {"currentOp" 1}
        command-body (squery-map->mql-map squery-map)]
    {:db db-name
     :coll nil
     :command-head command-head
     :command-body command-body}))

(defn kill-op [opid-number & args]
  (let [db-name "admin"
        squery-map (if (empty? args)
                  {:op opid-number}
                  {:op opid-number :comment args})
        command-head {"killOp" 1}
        command-body (squery-map->mql-map squery-map)]
    {:db db-name
     :coll nil
     :command-head command-head
     :command-body command-body}))
