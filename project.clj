(defproject com.clojurewerkz/cascading-cassandra "0.0.1-SNAPSHOT"
  :description ""
  :min-lein-version "2.0.0"
  :license {:name "Apache License 2.0"}

  :dependencies [[org.clojure/clojure "1.4.0"]
                 [cascading/cascading-hadoop "2.0.0"
                    :exclusions [org.codehaus.janino/janino
                                 org.apache.hadoop/hadoop-core]]
                 [org.apache.hadoop/hadoop-core "0.20.2-dev"]

                 [clojurewerkz/cassaforte "1.0.0-SNAPSHOT"]

                 [com.twitter/maple "0.2.3"]
                 [mysql/mysql-connector-java "5.1.18"]

                 [cascalog "1.10.0"]

                 [midje "1.3.0" :exclude [org.clojure/clojure]]
                 [midje-cascalog "0.4.0" :exclude [org.clojure/clojure]]]

  :main com.clojurewerkz.cascading.cassandra.core
  :source-paths   ["src/main/clj"]
  :java-source-paths ["src/main/java"]
  :test-paths        ["src/test"]
  :test-selectors {:all     (constantly true)
                   :focus   :focus
                   :default (constantly true)}
  :aot [com.clojurewerkz.cascading.cassandra.tap]
  :javac-options     ["-target" "1.7" "-source" "1.7"]
  :repositories {
                 "conjars" "http://conjars.org/repo/"
                 "sonatype" {:url "http://oss.sonatype.org/content/repositories/releases"
                             :snapshots false
                             :releases {:checksum :fail :update :always}}
                 "sonatype-snapshots" {:url "http://oss.sonatype.org/content/repositories/snapshots"
                                       :snapshots true
                                       :releases {:checksum :fail :update :always}}})
