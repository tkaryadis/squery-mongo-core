(defproject org.squery/squery-mongo-core "0.2.0-SNAPSHOT"
  :description "The SQuery core, used from both java and nodejs drivers"
  :url "https://github.com/tkaryadis/squery-mongo-core"
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
                 [cheshire "5.10.0"]
                 ]
  :plugins [[lein-codox "0.10.7"]])
