(ns squery-mongo-core.internal.convert.js-functions
  (:require  clojure.string
             [squery-mongo-core.utils :refer [make-dir delete-dir path-str file-exist? run-shell-command
                                         read-file read-file-read-string write-file get-cwd]]))

(defn js-body [& strings]
  (apply str (interpose " " strings)))

(def fnames (atom 0))
(defn get-fname []
  (swap! fnames inc))

(def vnames (atom 0))
(defn get-vname []
  (swap! vnames inc))

(defn js-arg? [arg]
  (and (map? arg) (contains? arg :call)))

;(vec (filter #(not (js-arg? %)) ))
(defn update-args [args]
  (reduce (fn [args arg]
            (if (js-arg? arg)
              (vec (concat args (get arg :args)))
              (conj args arg)))
          []
          args))

(defn update-jsargs [args]
  (reduce (fn [[jsargs jsargs-v] arg]
            (if (js-arg? arg)
              [(conj jsargs (get arg :call))
               (vec (concat jsargs-v (get arg :jsargs-v)))]
              (let [new-var (str "v" (get-vname))]
                [(conj jsargs new-var) (conj jsargs-v new-var)])))
          [[] []]
          args))


(defn update-bodies [args fname body new?]
  (let [prv-bodies (vec (reduce (fn [prv-bodies arg]
                                  (if (js-arg? arg)
                                    (concat prv-bodies (get arg :bodies))
                                    prv-bodies))
                                []
                                args))]
    (if new?
      (conj prv-bodies (str "var " fname "=" body "; "))
      prv-bodies)))

(defn update-call [fname jsargs]
  (let [jsargs-str (clojure.string/join "," jsargs)]
    (str fname "(" jsargs-str ")")))

(defn update-fnames [args body]
  (let [prv-fnames (reduce (fn [prv-fnames arg]
                             (if (js-arg? arg)
                               (merge prv-fnames (get arg :fnames))
                               prv-fnames))
                           {}
                           args)
        fname (reduce (fn [fname prv-fname]
                        (let [prv-body (get prv-fnames prv-fname)]
                          (if (= prv-body body)
                            (reduced prv-fname)
                            nil)))
                      nil
                      (keys prv-fnames))

        new? (nil? fname)

        [fnames fname] (if new?
                         (let [fname (str "f" (get-fname))]
                           [(assoc prv-fnames fname body) fname])
                         [prv-fnames fname])]
    [fnames fname new?]))

(defn js-info [initial-args body]
  (let [args (update-args initial-args)
        [jsargs jsargs-v] (update-jsargs initial-args)
        [fnames fname new?] (update-fnames initial-args body)
        bodies (update-bodies initial-args fname body new?)
        call (update-call fname jsargs)]
    {:args args
     :jsargs jsargs
     :jsargs-v jsargs-v
     :fnames fnames
     :bodies bodies
     :call call
     }))

;;--------------------------------------convert-js----------------------------------------------------------------------

(declare compile-library)

(defn js-args-body [f-name f-args f-code]
  (let [f-name (name f-name)
        f-code (cond

                 (empty? f-code)
                 nil

                 (string? (first f-code))
                 (clojure.string/join "\n" f-code)

                 :else                                      ;;clojurescript code
                 (first f-code))

        cwd  (get-cwd)

        _ (if (some? f-code)
            (if (string? f-code)
              (write-file (path-str cwd "js" (str f-name ".js")) f-code)
              (compile-library f-name)               ;;TODO was (compile-library f-name f-code)
              ))

        body (read-file (path-str cwd "js" (str f-name ".js")))
        args (vec f-args)]
    [args body]))

;;------------------------------------compile---------------------------------------------------------------------------

(defn compile-dep [f-name]
  (let [f-name (name f-name)
        cwd (get-cwd)
        lib-folder (path-str cwd "js" "lib")
        wisp-filename (str f-name ".wisp")
        js-filename (str f-name ".js")

        _ (if (not (file-exist? lib-folder))
            (make-dir lib-folder))
        ;_
        #_(write-file  (path-str wisp-folder wisp-filename) (str f-code))


        wisp-filename-path (path-str lib-folder wisp-filename)
        js-filename-path (path-str lib-folder js-filename)

        ;cd-command (path-str (str " cd " cwd))
        compile-command (str "wisp < " wisp-filename-path " > " js-filename-path ";")
        exit-command (str " exit;")
        command (str compile-command exit-command)
        _ (println "Compliling  : wisp function ---> js function (" f-name ")")
        _ (run-shell-command command)
        _ (println "Compiled to : " js-filename-path)
        mainjs-str (read-file (path-str js-filename-path))
        index (clojure.string/index-of mainjs-str " = function")
        mainjs-str (subs mainjs-str (+ index 3))]
    (write-file (path-str js-filename-path) mainjs-str)))

;;-------------------------------------------deps-----------------------------------------------------------------------

(defn get-deps [f-name]
  (let [f-name (name f-name)
        cwd (get-cwd)
        js-deps-filename (str f-name ".deps")
        js-deps-path (path-str cwd "js" "lib" js-deps-filename)
        deps (if (not (file-exist? js-deps-path))
               []
               (read-file-read-string js-deps-path))
        deps (mapv str deps)]
    deps))

(defn keep-unchecked-deps [checked-deps deps next-deps]
  (let [all-checked-deps (clojure.set/union (into #{} checked-deps) (into #{} (map #(get % :f-name)
                                                                                   (filter #(get % :checked) deps))))]
    (into [] (filter #(not (contains? (into #{} all-checked-deps) %)) next-deps))))

(defn checked-dep? [dep]
  (true? (get dep :checked)))

(defn combine-deps [f-name]
  (loop [deps [{:f-name (name f-name)
                :checked false}]
         checked-deps []]
    (if (empty? deps)
      {:call-fn (last checked-deps)
       :deps (into [] (drop-last checked-deps))}
      (let [dep (first deps)]
        (if (checked-dep? dep)
          (recur (rest deps) (conj checked-deps (get dep :f-name)))
          (let [f-name (get dep :f-name)
                next-deps (get-deps f-name)
                next-deps (keep-unchecked-deps checked-deps deps next-deps)
                next-deps (mapv (fn [dep] {:f-name dep :checked false}) next-deps)]
            (recur (concat next-deps
                           (list {:f-name f-name
                                  :checked true})
                           (rest deps))
                   checked-deps)))))))

(defn dep-to-code [f-name]
  (let [f-name (name f-name)
        cwd (get-cwd)
        js-filename (str f-name ".js")
        js-path (path-str cwd "js" "lib" js-filename)
        code (read-file js-path)
        code (str "var " f-name " = " code)]
    code))

(defn call-fn-code [call-fn deps-code include-wisp-core?]
  (let [f-name (name call-fn)
        cwd (get-cwd)
        js-filename (str f-name ".js")
        js-path (path-str cwd "js" "lib" js-filename)
        call-code (read-file js-path)
        index-of-bracket (clojure.string/index-of call-code "{")
        before-bracket (subs call-code 0 (+ index-of-bracket 1))
        after-bracket (subs call-code (+ index-of-bracket 1))
        ;;the core is entered in all functions
        core-code (if include-wisp-core?
                    (let [core-path (path-str cwd "js" "core" "core.js")]
                      (str "\n" (read-file core-path)))
                    "")]
    (str before-bracket core-code deps-code after-bracket)))

(defn deps-to-code [deps-map include-wisp-core?]
  (let [call-fn (get deps-map :call-fn)
        deps-codes (clojure.string/join "\n" (mapv dep-to-code (get deps-map :deps)))
        deps-codes (if (not= deps-codes "") (str "\n" deps-codes) deps-codes)
        call-code (call-fn-code call-fn deps-codes include-wisp-core?)
        cwd (get-cwd)
        js-filename (str call-fn ".js")
        js-path (path-str cwd "js" js-filename)]
    (write-file js-path call-code)))


;;----------------------------------------------wisp-and-deps-----------------------------------------------------------
;;----------------------------------------------------------------------------------------------------------------------

(defn compile-library [f-name]
  (let [f-name (name f-name)
        cwd (get-cwd)
        js-filename (str f-name ".js")]
    ;;if exists in the not-library folder,i do nothing,its standalone+compiled
    (if-not (file-exist? (path-str cwd "js" js-filename))
      (let [deps-map (combine-deps f-name)
            callfn-and-deps (conj (get deps-map :deps) f-name)
            ;;compile all dependencies and the function i want to call, if i dont have a js file for them
            _ (dorun (map (fn [dep]
                            (if-not (file-exist? (path-str cwd "js" "lib" (str dep ".js")))
                              (compile-dep dep)))
                          callfn-and-deps))
            include-wisp-core? (not (empty? (filter (fn [dep]
                                                      (file-exist? (path-str cwd "js" "lib" (str dep ".wisp"))))
                                                    callfn-and-deps)))]
        (deps-to-code deps-map include-wisp-core?)))))
