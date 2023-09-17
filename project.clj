(defproject igel "0.1.0-SNAPSHOT"
  :description "Simple Key-Value Store"
  :url "https://github.com/yito88/igel"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.clojure/core.async "1.6.681"]
                 [org.clojure/data.fressian "1.0.0"]
                 [org.clojure/tools.logging "1.2.4"]
                 [clj-commons/clj-yaml "1.0.27"]
                 [blossom "1.1.0"]]
  :profiles {:dev {:plugins [[lein-cloverage "1.2.4"]]}
             :uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})
