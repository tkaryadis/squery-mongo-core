(ns cmql-core.diagnostic
  (:require [cmql-core.internal.convert.commands :refer [cmql-map->mql-map split-db-namespace]]
            [cmql-core.internal.convert.stages :refer [cmql-vector->cmql-map]]))

(def coll-stats-def
  {
   :collStats "<string>",
   :scale "<int>"
   })

;;shell has a wrapper for it   db.read-write.stats
;;db.read-write.totalIndexSize
;;db.read-write.totalSize
(defn coll-stats
  "command = collStats
  Takes 1 argument = {'scale' number} optional ,sizes are returned in bytes default=1 => bytes
                     or
                     :b :kb :mg :gb  (to avoid writing {'scale' 1024} etc)"
  ([db-namespace & args]
   (let [
         [db-name coll-name] (split-db-namespace db-namespace)
         options (if (empty? args)
                   ["b"]
                   args)

         cmql-map (let [option (first options)]
                       (if (map? option)
                         option
                         (cond (= (clojure.string/lower-case (name option)) "b")
                               {:scale 1}

                               (= (clojure.string/lower-case (name option)) "kb")
                               {:scale 1024}

                               (= (clojure.string/lower-case (name option)) "mb")
                               {:scale 1048576}

                               (= (clojure.string/lower-case (name option)) "gb")
                               {:scale 1073741824}

                               :else
                               {:scale 1})))

         command-head {"collStats" coll-name}
         command-body (cmql-map->mql-map cmql-map)]
     {:db db-name
      :coll coll-name
      :command-head command-head
      :command-body command-body})))

(defn coll-data-size
  "Runs the coll-stats command,and from the returned results keeps only the size field"
  [db-namespace & options]
  (get (apply (partial coll-stats db-namespace) options) "size"))

(defn caped?
  "Runs the coll-stats command,and from the returned results keeps only the capped field"
  [db-namespace]
  (get (coll-stats db-namespace ) "capped"))

(defn storage-size
  "Runs the coll-stats command,and from the returned results keeps only the storageSize field"
  [db-namespace]
  (get (coll-stats db-namespace ) "storageSize"))

(def validate-def
  {
   :validate "<string>",
   :full  "<boolean>"
   })

(defn validate [db-namespace & args]
  (let [[db-name coll-name] (split-db-namespace db-namespace)
        cmql-map (cond
                  (or (empty? args) (= (first args) false))
                  {:full false}

                  (first args)
                  {:full true}

                  :else
                  {:full false})

        command-head {"validate" coll-name}
        command-body (cmql-map->mql-map cmql-map)]
    {:db db-name
     :coll coll-name
     :command-head command-head
     :command-body command-body}))


;;---------------------------------general-------------------------------------------------------------------------


(defn server-status
  "for fields excluded by default use :field    or {:field 1}
  for fields included by default use  :!field  or {:field 0}
  information about what is happening/now or in past, in the server
  alot of this information is for internal usage from mongodb processes
  Info topics
  instance information
  asserts/connection/network/locking
  operation stats/security/replication stats/storage engine stats/metrics
  see serverStatuc command in docs for details"
  [& args]
  (let [
        db-name "admin"
        fields-map (cmql-vector->cmql-map args 0)
        cmql-map fields-map

        command-head {"serverStatus" 1}
        command-body (cmql-map->mql-map cmql-map)]
    {:db db-name
     :coll nil
     :command-head command-head
     :command-body command-body}))