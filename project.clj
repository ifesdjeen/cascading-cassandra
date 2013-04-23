(defproject com.ifesdjeen/cascading-cassandra "0.0.6-SNAPSHOT"
  :description ""
  :min-lein-version "2.0.0"
  :license {:name "Apache License 2.0"}

  :dependencies [[org.clojure/clojure "1.5.1"]

                 [log4j "1.2.16"]
                 [org.slf4j/slf4j-log4j12 "1.6.6"]

                 [cascalog "1.10.1"]
                 [cascading/cascading-hadoop "2.0.8"
                  :exclusions [org.codehaus.janino/janino
                               org.apache.hadoop/hadoop-core]]
                 [org.apache.hadoop/hadoop-core "1.0.4"
                   :exclusions [org.codehaus.jackson/jackson-mapper-asl]]
                 ;; [org.apache.hadoop/hadoop-core "0.20.2"
                 ;;  :exclusions [org.codehaus.jackson/jackson-mapper-asl]]
                 ]

  :aot [com.ifesdjeen.cascading.cassandra.core-test]
  :java-source-paths ["src/main/java"]
  :test-paths        ["src/test"]
  :exclusions [log4j/log4j org.slf4j/slf4j-log4j12]
  :resource-paths ["src/resources"]
  :profiles {:dev {:resource-paths     ["src/resources"]
                   :dependencies [[org.xerial.snappy/snappy-java "1.0.5-M3"]
                                  [clojurewerkz/cassaforte "1.0.0-beta13"
                                   :exclusions [org.apache.thrift/libthrift]]
                                  [commons-lang/commons-lang "2.6"]
                                  [org.apache.cassandra/cassandra-all "1.2.4"
                                   :exclusions [org.apache.hadoop
                                                org.apache.thrift/libthrift
                                                org.apache.httpcomponents/httpclient]]]
                   }
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
  :jvm-opts ["-server" "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n"
             "-Xmx768m"]
  :pedantic :warn)
