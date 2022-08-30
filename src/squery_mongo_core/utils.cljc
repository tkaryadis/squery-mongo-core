(ns squery-mongo-core.utils
  #?(:clj (:require clojure.repl
                    flatland.ordered.map
                    [tupelo.misc :refer [shell-cmd]]
                    me.raynes.fs)
     :cljs (:require linked.core
                     cljs.reader))
  #?(:clj (:import (java.time Instant)
                   (java.sql Date)
                   (java.io File))))

(defn dissoc-vec  [v pos]
  (if (>= pos (count v))
    (subvec v 0 pos)
    (reduce (fn [new-v m]
              (conj new-v m))
            (subvec v 0 pos)
            (subvec v (inc pos)))))

;;--------------------------------make string keys to keywords----------------------------------------------------------

(defn keyword-map
  "Makes string keys that dont start with $,to keywords ($ keys are MQL operators)"
  [m]
  (if (map? m)
    (reduce (fn [m-k k]
              (assoc m-k (if (and (string? k) (not (clojure.string/starts-with? k "$")))
                           (keyword k)
                           k)
                         (get m k)))
            {}
            (keys m))
    m))


(defn string-map
  "Makes keyword keys to strings"
  [m]
  (if (map? m)
    (reduce (fn [m-k k]
              (assoc m-k (if (keyword? k)
                           (name k)
                           k)
                         (get m k)))
            {}
            (keys m))
    m))

;;-----------------------------------ordered-map-----------------------------------------------
;;(used in Commands,index definition etc)

;;TODO replace flatland with linked.core,requires changes on Java Codec
#?(:clj  (defn ordered-map
           ([] (flatland.ordered.map/ordered-map))
           ([other-map] (into (flatland.ordered.map/ordered-map) other-map))
           ([k1 v1 & keyvals] (apply flatland.ordered.map/ordered-map (cons k1 (cons v1 keyvals)))))

   :cljs (defn ordered-map
               ([] (linked.core/map))
               ([other-map] (into (linked.core/map) other-map))
               ([k1 v1 & keyvals] (apply linked.core/map (cons k1 (cons v1 keyvals))))))


;;--------------------------------------dates----------------------------------------------------------
(defn days-to-ms [ndays]
  (* ndays 24 60 60000))

;;For example   {mydate (date "2019-01-01T00:00:00Z")}
;; "2019-07-02" doesnt work
(defn date [date-s]
  #?(:clj (Date/from (Instant/parse date-s))
     :cljs (js/Date. date-s)))

;;------------------------------------files-------------------------------------------------------------
#?(:cljs (def fs (js/require "fs")))
#?(:cljs (def child-process (js/require "child_process")))
#?(:cljs (def js-path (js/require "path")))

(defn path-str [& strs]
  #?(:clj (clojure.string/join File/separator strs)
     :cljs (apply (.-join js-path) strs)))

(defn make-dir [path-str]
  #?(:clj (me.raynes.fs/mkdir path-str)
     :cljs (.mkdirSync fs path-str)))

(defn delete-dir [path-str]
  #?(:clj (me.raynes.fs/delete-dir path-str)
     :cljs (.rmSync fs path-str)))

(defn file-exist? [path-str]
  #?(:clj (.exists (File. ^String path-str))
     :cljs (.existsSync fs path-str)))

;;----------------------------------shell command-------------------------------------------------------

;;see
;;https://stackoverflow.com/questions/20643470/execute-a-command-line-binary-with-node-js

(defn run-shell-command [command]
  #?(:clj (shell-cmd command)
     :cljs (.execSync child-process command)))

;;--------------------------------cljc read/write to file-----------------------------------------------

(defn write-file [path-str file-content]
  #?(:clj (spit path-str file-content)
     :cljs (.writeFileSync fs path-str file-content)))

(defn read-file [path-str]
  #?(:clj (slurp path-str)
     :cljs (.readFileSync fs path-str "utf8")))

(defn read-file-read-string [path-str]
  #?(:clj (read-string (slurp path-str))
     :cljs (cljs.reader/read-string (.readFileSync fs path-str "utf8"))))

(defn get-cwd []
  #?(:clj (System/getProperty "user.dir")
     :cljs (.cwd js/process)))

;;--------------------------------documentation related-----------------------------------------

#_(:clj
    (defn get-ns-documentation [ns-name keep-names]
      (let [ns-m (ns-publics ns-name)]
        (loop [public-names (clojure.repl/dir-fn ns-name)]
          (if-not (empty? public-names)
            (let [k (first public-names)
                  v (get ns-m k)
                  md (meta v)
                  doc (:doc md)
                  doc (if (nil? doc) "" doc)
                  first-line (first (clojure.string/split doc #"\n"))
                  mongo-op (if (and (string? first-line) (clojure.string/starts-with? doc "$")) first-line "")
                  arg-list (clojure.string/join " " (map str (:arglists md)))                                       ;(first (:arglists md))
                  _ (if (and (or (empty? keep-names)
                                 (contains? keep-names (str k)))
                             (not (re-find #"def$" (str k))))
                      (println "###" k "&nbsp;&nbsp;" mongo-op "\n```txt\n" arg-list "\n" doc "\n```"))]
              (recur (rest public-names))))))))