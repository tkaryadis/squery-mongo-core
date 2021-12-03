(ns cmql-core.operators.stages
  (:refer-clojure :exclude [sort])
  (:require [cmql-core.internal.convert.common :refer [not-fref ]]
            [cmql-core.internal.convert.stages :refer [cmql-vector->cmql-map cmql-addFields->mql-addFields cmql-project->mql-project]]
            [cmql-core.internal.convert.commands :refer [cmql-pipeline->mql-pipeline split-db-namespace]]
            [cmql-core.internal.convert.operators :refer [let-cmql-vars->map]]
            cmql-core.operators.operators
            [cmql-core.utils :refer [ordered-map]]))

(defn pipeline
  "(pipeline stage1 stage2 ..) = [stage1 stage2 ...]
  Used optionally to avoid confusion"
  [& args]
  (vec args))

;;--------------------------------------remove-documents----------------------------------------------------------------
(defn match
  "$match
  No need to use it in cmql unless you want to use Query operators
  cmql auto-generates a match stage from filters if one after another,
  auto-combine them with $expr $and
  Call
  (q ....
     (=_ :age 25)
     (>_ :weight 50))
  Call (if we want to use a query operator)
  (match { views: { '$gte' 1000 }})"
  [e-doc]
  (if (and (map? e-doc) (= (count e-doc) 1) (contains? e-doc "$__qfilter__"))
    {"$match" (get e-doc "$__qfilter__")}
    {"$match" e-doc}))

(defn limit [n]
  {"$limit" n})


(defn skip [n]
  {"$skip" n})

(defn redact
  "$redact
  I keep or delete root document,or embeded documents based on condition
  instead of doing it by hand and paths,like auto-find all embeded documents
  stage => argument = 1 doc from the pipeline
  i start at level 0 {field0 {field1 {field2 ..}}}
  if condition $$DESCEND/$$PRUNE/$$KEEP
  else $$DESCEND/$$PRUNE/$$KEEP

  if $$PRUNE i delete that document and everything inside
  if $$KEEP i keep that documents and everything inside
  if $$DESCEND i keep the document,but i re-run the condition
    in the next level,in my case i repeat on {field1 {field2 ..}}

  $$DESCEND allows to check all levels so when done,i know that
  all the documents remained satisfy the condition
  (no matter the embed level they are)

  Arg
  any expression that evaluates to the 3 system variables,
  normaly its a condition using field references
  (if i use references and descend i have to make sure they that
   they exist in all embeded documents or check inside the condition
   what to do when they dont exist)
  $$DESCEND (keep embeded document,but search seperatly the embeded ones?)
  $$PRUNE   (remove embeded document,dont search more at this level?)
  $$KEEP    (keep embeded document,dont search embeded ones?)"
  [condition-DESCEND-PRUNE-KEEP]
  {"$redact" condition-DESCEND-PRUNE-KEEP})

;;-------------------------------------------add/remove fields----------------------------------------------------------
;;----------------------------------------------------------------------------------------------------------------------

;;Addfields
;; Replace always except
;; array-> add document =>array with all members that document
;; document->add document => merge documents

(defn add
  "$addFields
  No need to type add in cmql
  A Map literal as a pipeline stage means add
  The only situation that is useful is if the new field has the name
  of an option in the command.
  For example
  {:allowDiskUse true} will be interpeted as option not as add-field
  Dont use a variable that was added in the same add,use separate add
  Call
  {:field1 .. :field2 .. :!field3 ...}
  The last one !field3 means replace the field3 if it existed
  This happens anyways but not always (for example add doc on doc => merge)
  Using :!field3 i know that i will replace it"
  [& fields]
  (apply cmql-addFields->mql-addFields fields))

(defn set-s
  "$set
  Set is an alias of add,used in update
  Like add-fields set is not used,map literas means add-fields"
  [& fields]
  (apply add fields))

(defn unset
  "like project using only :!field
   not useful, :! is better and only project"
  [ & fields]
  { "$unset" (mapv name fields)})

(defn facet
  "$facet
  Run many pipelines in serial using as source the same pipeline
  Call
  (q ...
      ...
      (facet {:f1 pipeline1
              :f2 pipeline2})
  The result is one document with ONLY the fields :f1 :f2
  f1,f2 will be arrays with the document results of each pipeline
  Restrictions
   I cant use those stages in facet pipelines
   collStats/facet/geoNear/indexStats/out/merge/planCacheStats"
  [& fields]
  (let [fields (apply (partial merge {})fields)
        fields-keys (keys fields)
        fields-values (map cmql-pipeline->mql-pipeline (vals fields))
        fields (zipmap fields-keys fields-values)]
    {"$facet" fields}))

;;Project
;; Replace always except
;; array-> add document =>array with all members that document

;;When i add fields with project and the value is constant number
;;(literal- number) even its nested
;;For example
;; {:d (literal- 1)}
;; {:d {"i" (literal- 1)}} ;;needs also literal
(defn project
  "$project
  In cmql [...] inside a pipeline is a project stage(except nested stages)
  {:f 1} means {:f (literal- 1)} so don't use it for project inside []
  If you want to use this notation use MQL directly {'$project' ....}
  Call
  1)add those that i want to keep(and optionally :!_id to remove it
    [:!_id :f1 {:f3 (+_ :a 1)} {:!f4 (*_ :a 1))}] (all others wil be removed)
    {:!f4 ..} means replace the old f4
    (replace happens anyways(without {:! ..}) but not always)
  2)add those that i want to remove
    [:!a :!b]         (all others will be kept)
  *i never mix keep/remove except :!_id"
  [& fields]
  (apply cmql-project->mql-project fields))

;;-------------------------------------------arrays---------------------------------------------------------------------
;;----------------------------------------------------------------------------------------------------------------------

(defn unwind
  "$unwind
  1 document with array of n documents becomes
  those n documents are like the old 1 document
  with one extra field with the array member
  Example
  { :field1  value1 :field2 [1 2]}
  ->
  { :field1  value1  :field2 1}
  { :field1  value2  :field2 2}
  Normally  1 document with an array of N members => N documents
  Options
   Include one field to keep the index the member had in the
   initial array (default no index)
   {:includeArrayIndex  string}

   Used in special case2 (default false)
   {:preserveNullAndEmptyArrays true/false}
  Array field special cases
   unwind to itself(1 document)
   (if i used includeArrayIndex,the index will have value null)
   1)a single value(not array,not null) =>
   Dissapear if {:preserveNullAndEmptyArrays: false}(default)
   unwind to itself(1 document) if {:preserveNullAndEmptyArrays: true}
   2)null/empty array/missing field"
  [field-reference & options]
  (if (empty? options)
    {"$unwind" field-reference}
    (let [;;includeArrayIndex preserveNullAndEmptyArrays
        unwind-map (conj options {:path field-reference})
        unwind-map (apply (partial merge {}) unwind-map)]
      {"$unwind" unwind-map})))

;;-------------------------------------------root-----------------------------------------------------------------------
;;----------------------------------------------------------------------------------------------------------------------

(defn replace-root
  "$replaceRoot
  Embeded document fully replaces the document including the :_id
  Call
  doc={:field1 {:field2 value2}}
  (replace-root :field1)
  outdoc={:field2 value2}"
  [e-doc]
  {"$replaceRoot" {:newRoot e-doc}})

(defn replace-with
  "$replaceRoot
  Alias of replace-root"
  [e-doc]
  (replace-root e-doc))

(defn add-to-root
  "newRoot = merge(root+doc)+doc_field
  Embeded document is added to the document
  and remains embeded.
  {:field1 {:field2 value2}}
  (add-to-root :field1)->
  {:field1 {:field2 value2}
   :field2 value2}"
  [e-doc]
  {"$replaceRoot" {:newRoot {"$mergeObjects" [e-doc "$$ROOT"]}}})

(defn move-to-root
  "newRoot=merge(root+doc)-doc_field
  Add to root,and remove the embeded doc =>as if it moved to root"
  [e-doc]
  [(add-to-root e-doc)
   (project (not-fref e-doc))])

(defn unwind-replace-root
  "newRoot=doc-member
  1 array with n members => n documents as roots
  Like replace collection with the array members
  {:a 'b'
   :myarray [doc1 doc2]}
  Replaces from the 2 docs
  doc1
  doc2"
  [doc-e]
  [(unwind doc-e)
   (replace-root doc-e)])


(defn unwind-add-to-root
  "newRoot=doc+member
  1 array with n members => n documents added to root
  Keeps the unwinded field also
  {'a' 'b'
   :myarray [doc1 doc2]}
  Replaced from the 2 docs
  {'a' 'b' :myarray doc1}
  {'a' 'b' :myarray doc2}"
  [doc-e]
  [(unwind doc-e)
   (add-to-root doc-e)])

(defn unwind-move-to-root
  "newRoot=doc+member_fields
  1 array with n members => n documents added to root
  Keeps the unwinded field also
  {'a' 'b'
   :myarray [doc1 doc2]}
  Replaced from the 2 docs,like doc moved to root
  (merge {'a' 'b'} doc1)
  (merge {'a' 'b'} doc2)"
  [doc-e]
  [(unwind doc-e)
   (move-to-root doc-e)])

;;------------------------------------------grouping-documents---------------------------------------------------------
;;----------------------------------------------------------------------------------------------------------------------

(defn group
  "$group
  Groups in 1 document,using accumulators
   e = nil      meaning {:_id nil} +remove the :_id field after
     = :field   meaning {:_id :field} + rename after the :_id to :field
        For more than 1 fields use edoc like
          {:_id {:fiedl1 .. :field2 .. ...}}
     = {:_id edoc/field/nil} like mongo original group
     = {:field edoc/field/nil} like mongo original group + rename :_id to :field
  Acumulators one or many like (sum- :field) or (avg- :field) .... "
  [e & accumulators]
  (let [[group-field group-value rename? rename-field remove?]
        (cond

          (nil? e)
          [:_id nil false nil true]

          (not (map? e))
          [:_id e true e false]

          (or (contains? e :_id) (contains? e "_id"))
          [:_id (first (vals e)) false nil false]

          :else
          [:_id (first (vals e)) true (first (keys e)) false])
        accumulators (cmql-vector->cmql-map accumulators nil)    ; make it 1 map,no keywords to to replace
        group-by-map (ordered-map group-field group-value)
        group-by-map (merge group-by-map accumulators)]
    (cond

      rename?
      [{"$group" group-by-map}                         ;;group-by field/{:field ....}
       (add {rename-field :_id
             "_id" "$$REMOVE"})]


      remove?                                               ;;group-by nil
      [{"$group" group-by-map}
       [:!_id]]

      :else
      {"$group" group-by-map})))

;;{
;   $setWindowFields: {
;      partitionBy: <expression>,   ;;optional , if missing => all collection 1 group
;      sortBy: {                 ;;required if Rank and order window operators.Bounded windows (either a documents window or a range window).
;         <sort field 1>: <sort order>,
;         <sort field 2>: <sort order>,
;         ...,
;         <sort field n>: <sort order>
;      },
;      output: {  ;;Required , Specifies the field(s) to append to the documents
;         <output field 1>: {   ;;the field i want to add
;            <window operator>: <window operator parameters>,  ;;the accumulator on the group, or { $rank: { } } or ?
;            window: {
                ;;window operator works in those boundaries (for example $sum from "unbounded"(first doc of group) to current)

;               documents: [ <lower boundary>, <upper boundary> ],
                ;;
;               range: [ <lower boundary>, <upper boundary> ],
;               unit: <time unit>
;            }
;         },
;         <output field 2>: { ... },
;         ...
;         <output field n>: { ... }
;      }
;   }
;}

;;The "current" string for the current document position in the output.
;The "unbounded" string for the first or last document position in the partition.
;An integer for a position relative to the current document. Use a negative integer for a position before the current document.
; Use a positive integer for a position after the current document. 0 is the current document position.

;; its like $set but adds to each document the result of a group (if missing all collection 1 group)
;;   (in past we could do similar thing with group and unwind after)
;; sort optionally inside the group
;; output = the fields to append
;; window operator = the accumulator on the group, or { $rank: { } } or ?
;; documents (based on sort order)
;;   ["unbounded","current"] (accumulator from first of group since the current cdocument)
;;   [-1 0] means current and previous document only
;; range (based on value of the field, like range -10 +10 days)
;;   again unbount/current or numbers
;;   range can take a unit also


(defn wfields [& args]
  (let [op-map (reduce (fn [op-map arg]
                         (cond
                           (not (map? arg))
                           (if arg {:partitionBy arg} {})

                           (and (map? arg) (contains? arg "$sort"))
                           (assoc op-map :sortBy (get arg "$sort"))

                           :else
                           (update op-map :output (fn [o] (conj o arg)))))
                       {:output []}
                       args)]
    (loop [output {:output {}}
           args (get op-map :output)]
      (if (empty? args)
        {"$setWindowFields" (merge op-map output)}
        (let [arg (first args)
              w-op-options #{"documents" "range" "unit" :documents :range :unit}
              [output-field window-op] (first (filter (fn [pair] (not (contains? w-op-options (first pair)))) (into [] arg)))
              arg (dissoc arg output-field)
              window-map (if (empty? arg) {} {:window arg})]
          (recur (assoc-in output [:output output-field] (merge window-op window-map))
                 (rest args)))))))


(declare lookup-p)

(defn group-array
  "Used to reduce an array to another array,because conj- is very slow
  Very fast uses lookup with pipepline facet,and group
  Requires a dummy collection with 1 document set in settings
  Stage operator ,results are added to the root document
  For nested arrays,results must be moved to that position manually
  Call
  (group-array :myarray  ;; the ref of the array i want to group
               {:myagg1 (conj-each- :myarray) ;; reuse of the :myarray name
                :myagg2 (sum- :myarray)}
               :mygroups)
  Result
  {
   ...old_fields...
   :mygroups [{:myarray id1 :myagg1 ... :myagg2 ...} {:myarray id2 :myagg1 ... :myagg2 ...}]
  }"
  ([array-ref group-field accumulators results-field]
   (let [one-document-coll :cmql
         initial-group-field (if (keyword? group-field)
                               (name group-field)
                               group-field)
         group-field-root "a"
         group-field-path (if (nil? group-field)
                            (keyword (str group-field-root ""))
                            (keyword (str group-field-root "." initial-group-field)))
         add-fields-doc {results-field (cmql-core.operators.operators/let
                                         [:v. (cmql-core.operators.operators/get :joined 0)] :v.aggr.)}]
     [(lookup-p one-document-coll                           ;; TODO make it global
                [:myarray. array-ref]
                (pipeline
                  ;;a check that that property we want to use exists
                  ;;not enough to check first document,slow to check all => we dont check
                  #_(if (some? initial-group-field)
                    (cmql-core.operators.operators/let
                      [:v. (cmql-core.operators.operators/get :myarray. 0)]
                      (cmql-core.operators.operators/exists? (str "$$v" "." initial-group-field))))
                  (facet {:aggr [{:a :myarray.}
                                 (unwind :a)
                                 (group {:_id group-field-path} accumulators)]}))
                :joined)
      add-fields-doc
      (unset :joined)]))
  ;;this is used when i group an array by its members,not with a field of its members(for arrays that dont contain docs)
  ([array-ref accumulators results-field]
   (group-array array-ref nil accumulators results-field)))

(defn reduce-array
  "Used to reduce an array to another array,because conj- is very slow
  Very fast uses lookup with pipepline facet,and group
  Requires a dummy collection with 1 document set in settings
  Stage operator ,results are added to the root document
  For nested arrays,results must be moved to that position manually
  Call
  (group-array :myarray   ;; the ref of the array i want to reduce
               {:myagg1 (conj-each- :myarray) ;; reuse of the :myarray ref
               :myagg2 (sum- :myarray)})
  Result(i can add group-field and be like below,but not useful if no group)
  {
   ...old_fields...
   :myagg1 .....
   :myagg2 .....
   }"
  [array-ref accumulators]
  (let [one-document-coll :cmql
        add-fields-doc (reduce (fn [add-fields-doc k]
                                 (assoc add-fields-doc k (cmql-core.operators.operators/let
                                                           [:v. (cmql-core.operators.operators/get :joined 0)
                                                            :v1. (cmql-core.operators.operators/get :v.aggr. 0)]
                                                           (keyword (str "v1." (name k) ".")))))
                               {}
                               (keys accumulators))]
    [(lookup-p one-document-coll
               [:myarray. array-ref]
               (pipeline
                 (facet {:aggr [{:a :myarray.}
                                (unwind :a)
                                (group nil accumulators)]}))
               :joined)
     add-fields-doc
     (unset :joined)]))

(defn bucket
  "$bucket
  group => all members of the group have the same 1 value
            on the grouping field/fields
  bucket => all members of the group have value in a range
            of allowed values
  bucket allows you to define groups on range of values
  (same range=>same group) (bucket groups)
  [0,18,30,40,60]  buckets =>  [0,18) [18,30) [30,40) [40,60)
  & args = default(optional,bucket name for those out of range,
                   if not provided,and out of range => error)
  accumulators(optional) 1 doc with all the acculumators inside"
  [group-id-field boundaries & args]
  (let [[default accumulators] (reduce (fn [[default accumulators] arg]
                                         (if (map? arg)
                                           [default arg]
                                           [arg accumulators]))
                                       [nil nil]
                                       args)
        bucket-map {
                    "$bucket"
                    {
                     "groupBy" group-id-field
                     "boundaries"  boundaries
                     }
                    }

        bucket-map (if (some? default)
                     (assoc bucket-map :default default)
                     bucket-map)

        bucket-map (if (some? accumulators)
                     (assoc bucket-map :output accumulators)
                     bucket-map)]
    bucket-map))

(defn bucket-auto
  "$bucketAuto
  same as bucket,but now i give just the number of buckets
  that i want and mongo tries to find the right ranges,
  so each bucket has the same number of members as possible
  group => all members of the group have the same 1 value
           on the grouping field/fields
  bucket => the number of buckets that mongo will auto-make
  & args = granularity(optional,a string,picking the way to make the ranges,
           for example granularity='POWERSOF2' see docs)
  Accumulators(optional) 1 doc with all the acculumators inside"
  [group-id-field buckets-number & args]
  (let [[granularity accumulators] (reduce (fn [[default accumulators] arg]
                                         (if (map? arg)
                                           [default arg]
                                           [arg accumulators]))
                                       [nil nil]
                                       args)
        bucket-map {
                    "$bucketAuto"
                    {
                     "groupBy" group-id-field
                     "buckets"  buckets-number
                     }
                    }

        bucket-map (if (some? granularity)
                     (assoc bucket-map :granularity granularity)
                     bucket-map)

        bucket-map (if-not (empty? accumulators)
                     (assoc bucket-map :output accumulators)
                     bucket-map)]
    bucket-map))

;;-------------------------------------------sort-----------------------------------------------------------------------
;;----------------------------------------------------------------------------------------------------------------------

(defn sort
  "$sort
  Call
  (sort- :a :!b)"
  [& fields]
  (let [sort-doc (cmql-vector->cmql-map fields -1)]
    {"$sort" sort-doc}))

(defn group-count-sort
  "group by e
  {:count {$sum 1}}
  sort by :!count(desc?true) or :count
  "
  [e desc?]
  [(group e {:count {"$sum" 1}})
   (if desc?
     (sort :!count)
     (sort :count))])

;;-------------------------------------------combining collections------------------------------------------------------
;;----------------------------------------------------------------------------------------------------------------------
;;----------------------------------------------------------------------------------------------------------------------

(defn lookup
  "$lookup
  Left equality join
  doc= {'a' 'b' 'c' 'd'}
  doc result (the one from the left,the doc i had in pipeline)
  {'a' 'b' 'c' 'd' :joined [joined_doc1 joined_doc2 ...]}
  The joined_doc has is like merge of the 2 joined docs
  The joined field is created even if empty array (zero joined)
  The joined fields can be an array, in this case we get an array with the joined. (like join each member)
  Call
  (lookip :a :coll2.e :joined) ; join if :a==:e"
  [this-field other-coll-field-path join-result-field]
  (let [[other-coll other-field-path] (split-db-namespace other-coll-field-path)]
    {"$lookup" {:from         other-coll
                :localField   (name this-field)
                :foreignField other-field-path
                :as           (name join-result-field)}}))

(defn lookup-p
  "$lookup
  lookup with pipeline to allow more join creteria(not just equality on 2 field)
  also the pipeline allows the joined doc to have any shape (not just merge)
  Returns like lookup
  {'a' 'b' 'c' 'd' :joined [joined_doc1 joined_doc2 ...]}
  :joined is an array with the result of the pipeline
  inside the pipeline references refer to the right doc
  to refer to the left doc from pipeline use variables
  Using variables and coll2 references i make complex join creteria
  and withe the pipeline i can make the joined docs to have any shape
  Call
  (lookup-p :coll2 or [this-field :coll2.other-field-path]
            [:v1- :afield ...] ; optional
            [stage1
             stage2]
            :joined)"
  ([join-info let-vars pipeline join-result-field]
   (if-not (coll? join-info)
     (let [m {:from (name join-info)
              :pipeline (cmql-pipeline->mql-pipeline pipeline)
              :as (name join-result-field)}]
       {"$lookup" (if let-vars (assoc m :let (let-cmql-vars->map let-vars)))})
     (let [this-field (name (first join-info))
           other-coll-field-path (name (second join-info))
           [other-coll other-field-path] (split-db-namespace other-coll-field-path)
           m {:from other-coll
              :localField   this-field
              :foreignField other-field-path
              :pipeline (cmql-pipeline->mql-pipeline pipeline)
              :as (name join-result-field)}]
       {"$lookup" (if let-vars (assoc m :let (let-cmql-vars->map let-vars)))})))
  ([join-info pipeline join-result-field]
   (lookup-p join-info nil pipeline join-result-field)))

(defn join
  "$sql_join
  Like sql join,join when equal on field,replace left document with
  the merged document
  Call
  (join :localfield :foreignTable.foreingfField)
  (join :foreignTable.foreingfField)
    assumes localfield name to be foreingField name "
  ([localfield foreign-table-field]
   [(lookup localfield foreign-table-field :joined__)
    (unwind-move-to-root :joined__)])
  ([foreign-table-field]
   (let [f (second (clojure.string/split (name foreign-table-field) #"\."))]
     [(lookup f foreign-table-field :joined__)
      (unwind-move-to-root :joined__)])))


(defn graphlookup [collection
                   startWith
                   connectFromField
                   connectToField
                   as
                   maxDepth
                   depthField
                   restrictSearchWithMatch]
  {
   "$graphLookup"
{
    "from" collection
    "startWith"  startWith   ;;expression
    "connectFromField" connectFromField   ;;string
    "connectToField" connectToField       ;;string
    "as"  as  ;;string
    "maxDepth" maxDepth     ;;number
    "depthField" depthField ;;string
    "restrictSearchWithMatch" restrictSearchWithMatch       ;;doc
    }
   })



;;----------------------------------pipeline->disk (not combine,just save pipeline)-------------------------------------

(defn out [db-namespace]
  "$out
  Writes the pipeline to a collection,replacing it if it existed
  Its faster than merge if i want just replace"
  (let [[db coll] (clojure.string/split (name db-namespace) #"\.")]
    { "$out" { "db" db , "coll" coll } }))


;;----------------------------------pipiline-Combine->disk--------------------------------------------------------------
;;----------------------------------------------------------------------------------------------------------------------

(defn if-match
  "$merge
  Helper used only as argument in merge"
  [fields let-or-when-matched when-not-matched]
  (if (and (map? let-or-when-matched)                       ;;if its let
           (contains? let-or-when-matched "$let"))
    {:fields (mapv name fields)
     :let (get-in let-or-when-matched ["$let" :vars])
     :whenMatched (get-in let-or-when-matched ["$let" :in])
     :whenNotMatched when-not-matched}
    {:fields (mapv name fields)
     :whenMatched let-or-when-matched
     :whenNotMatched when-not-matched}))

(defn merge-s
  "$merge
  Update/upsert one collection,using what its coming from the pipeline.
  Merge in document level and collection level.
  Join when i want mathcing  parts from other collection.
  Merge when i want from both collections ,even if not match.
  collection -> pipeline -> collection   (normal updates)
  any_pipeline -> collection             (merge)
  For example keepExisting means keep what collection had
  Updates that collection,and returns nothing(an empty cursor)
  Requeries a unique index in the right collection,on the merged fields

  3 call ways
  (merge :mydb.mycoll)

  (merge :mydb.mycoll                ;;no variables used
         (if-match [field1 fied2]    ;;becomes  :on [field1 field2]
           whenMatched            ;;can also be pipeline
           whenNoMatched))

  (merge :mydb.mycoll
         (if-match [field1 fied2]
           (let- [:v1- :f1 :v2- :f2 ...] ; to refer pipeline doc fields
             whenMatched     ;;can also be pipeline
             whenNoMatced)))

  whenMatched
   replace      (keep pipelines)
   keepExisting (keep collections)
   merge   (merge old+new document ,like mergeObjects)
   fail    (stops in the middle if happen,no rollback)
   pipeline(used like update pipepline =>,i can use only
            $addFields=$set  $project=$unset $replaceRoot=$replaceWith)

  whenNotMatched
   insert  (insert pipelines)
   discard (ignore pipelines)
   fail (if pipeline has ane not match fail,but no rollback)"
  ([db-namespace if-match-e]
   (let [into (cond

                (map? db-namespace)                       ;;user gave {db .. coll ..}
                db-namespace

                (clojure.string/includes? (name db-namespace) ".")
                (let [[db coll] (clojure.string/split (name db-namespace) #"\.")]
                  {"db"   db
                   "coll" coll})

                :else
                (name db-namespace))

         on (get if-match-e :fields)
         let (get if-match-e :let)
         whenMatched (get if-match-e :whenMatched)
         whenMatched (if (vector? whenMatched)              ;;in case its pipeline
                       (cmql-pipeline->mql-pipeline whenMatched)
                       whenMatched)
         whenNoMatched (get if-match-e :whenNotMatched)
         merge-map {:into into}
         merge-map (if on (assoc merge-map :on on) merge-map)
         merge-map (if let (assoc merge-map :let let) merge-map)
         merge-map (if whenMatched (assoc merge-map :whenMatched whenMatched) merge-map)
         merge-map (if whenNoMatched (assoc merge-map :whenNotMatched whenNoMatched) merge-map)
         ;_ (prn merge-map)
         ]
     {"$merge" merge-map}))
  ([coll-namespace]
   (merge-s coll-namespace {})))


;;---------------pipeline<-disk (join is also pipeline<-disk but union doesnt do any processing or merge)---------------
;;----------------------------------------------------------------------------------------------------------------------

(defn union-s
  "$unionWith
  Reads from the collection and add documents in the pipeline
  no processing is made,duplicates can be added also"
  [coll-name & stages]
  (if (empty? stages)
    {"$unionWith" {:coll (name coll-name)}}
    {"$unionWith" {:coll (name coll-name)
                   :pipeline (cmql-pipeline->mql-pipeline stages)}}))

;;-------------------------------------------------------count----------------------------------------------------------
;;----------------------------------------------------------------------------------------------------------------------


(defn count-s
  "$count
  Counts all documents in collection its equivalent with
  (group nil {:count (sum- 1)})"
  ([ref-e]
   {"$count" ref-e})
  ([]
   {"$count" "count"}))

(defn group-count
  "$group
  (group e {'count' {'$sum' 1}})
  used for simplicity because common"
  [e]
  (group e {"count" {"$sum" 1}}))

;;-----------------------------------------------------diagnostic-------------------------------------------------------
;;i use those inside pipeline,because its common to want to process the info they return

;;TODO
(defn coll-stats-s [options-map]
  {"$collStats"  options-map})

(defn sample
  "Usefull for very big collections
  if sample=first stage in pipeline
            number<5% collection size
            collection>100 documents
  i will get number random documents
  else random sort,full collection scan,and select number documents"
  [number]
  {"$sample" {:size number}})


;;--------------------------------------------------database-aggregate-stages-------------------------------------------
;;i aggregate on the db instance for stages like $currentOp , $listLocalSessions

(defn current-op-s
  "{ $currentOp: { allUsers: <boolean>, idleConnections: <boolean>, idleCursors: <boolean>, idleSessions: <boolean>, localOps: <boolean> } }"
  [options-map]
  {"$currentOp" options-map})


(defn list-local-sessions
  "users-map = {} or { allUsers: true } or { users: [ { user: <user>, db: <db> }, ... ] } "
  ([users-map]
   {"$listLocalSessions" users-map})
  ([]
   {"$listLocalSessions" {}}))

#_(get-ns-documentation *ns* nil)
