(defproject juxt/crux-redis "0.1.0-alpha"
  :description "Crux Redis"
  :url "https://github.com/crux-labs/crux-redis"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [juxt/crux-core "20.09-1.12.0-beta"]
                 [com.taoensso/carmine "3.0.0" :exclusions [com.taoensso/nippy org.clojure/tools.reader]]]
  :profiles {:test {:dependencies [[org.clojure/test.check "0.10.0"]
                                   [org.signal/embedded-redis "0.8.0"]]}}
  :pedantic? :warn)
