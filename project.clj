(defproject com.ifesdjeen/cascading-cassandra "0.0.4-SNAPSHOT"
  :description ""
  :min-lein-version "2.0.0"
  :license {:name "Apache License 2.0"}

  :dependencies [[org.apache.cassandra/cassandra-all "1.1.5"]]

  :main com.ifesdjeen.cascading.cassandra.core
  :java-source-paths ["src/main/java"]
  :test-paths        ["src/test"]
  :profiles {:dev {:resource-paths     ["src/resources"]
                   :dependencies [[org.clojure/clojure "1.4.0"]
                                  [clojurewerkz/cassaforte "1.0.0-beta11-SNAPSHOT"]
                                  [cascalog "1.10.0"]
                                  [midje "1.3.0" :exclude [org.clojure/clojure]]
                                  [midje-cascalog "0.4.0" :exclude [org.clojure/clojure]]
                                  [org.apache.hadoop/hadoop-core "0.20.205.0"]]}
             }
  :test-selectors {:all     (constantly true)
                   :focus   :focus
                   :default (constantly true)}
  :repositories {"conjars" "http://conjars.org/repo/"
                 "sonatype" {:url "http://oss.sonatype.org/content/repositories/releases"
                             :snapshots false
                             :releases {:checksum :fail :update :always}}
                 "sonatype-snapshots" {:url "http://oss.sonatype.org/content/repositories/snapshots"
                                       :snapshots true
                                       :releases {:checksum :fail :update :always}}}
  :jvm-opts ["-Xmx768m" "-server" "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n"]
  :pedantic :warn)
