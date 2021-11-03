(defproject cmql/cmql-core "0.1.0-SNAPSHOT"
  :description "Query MongoDB with up to 3x less code (cmql-core, used from both cmql-j,cmql-js)"
  :url "https://github.com/tkaryadis/cmql-core"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 ;;js/java (used for js only)
                 [frankiesardo/linked "1.3.0"]
                 ;;Java
                 ;;ordered map
                 [org.flatland/ordered "1.5.9"]
                 ;;file utils
                 [me.raynes/fs "1.4.6"]
                 ;;run shell-command,compile to clojurescript
                 ;;for java
                 [tupelo/tupelo "20.08.27"]
                 ;;convert clj <-> json for java
                 [cheshire "5.10.0"]]
  :plugins [[lein-codox "0.10.7"]]
  )
