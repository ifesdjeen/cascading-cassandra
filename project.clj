(defproject com.ifesdjeen/cascading-cassandra "1.0.0-beta2"
  :description ""
  :min-lein-version "2.0.0"
  :license {:name "Apache License 2.0"}

  :dependencies [[org.clojure/clojure "1.5.1"]
                 [cascalog "1.10.1"]]
  :aot               [com.ifesdjeen.cascading.cassandra.core-test]
  :java-source-paths ["src/main/java"]
  :test-paths        ["src/test"]
  :resource-paths    ["src/resources"]
  :profiles {:provided {:dependencies   [[org.apache.cassandra/cassandra-all "1.2.4"
                                          :exclusions [org.apache.hadoop
                                                       org.apache.thrift/libthrift
                                                       org.apache.httpcomponents/httpclient]]
                                         ;; [org.apache.hadoop/hadoop-core "1.0.4"
                                         ;;   :exclusions [org.codehaus.jackson/jackson-mapper-asl]]
                                         [org.apache.hadoop/hadoop-core "0.20.2"
                                          :exclusions [org.codehaus.jackson/jackson-mapper-asl]]
                                         ]}
             :dev      {:resource-paths ["src/resources"]
                        :jvm-opts       ["-server" "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n"
                                         "-Xmx768m"]
                        :dependencies   [[org.xerial.snappy/snappy-java "1.0.5-M3"]
                                         [clojurewerkz/cassaforte "1.0.0-beta13"
                                          :exclusions [org.apache.thrift/libthrift]]
                                         [commons-lang/commons-lang "2.6"]]}}
  :test-selectors {:all     (constantly true)
                   :focus   :focus
                   :default (constantly true)}
  :repositories {"conjars" "http://conjars.org/repo/"
                 "sonatype"           {:url "http://oss.sonatype.org/content/repositories/releases"
                                       :snapshots false
                                       :releases {:checksum :fail :update :always}}
                 "sonatype-snapshots" {:url "http://oss.sonatype.org/content/repositories/snapshots"
                                       :snapshots true
                                       :releases {:checksum :fail :update :always}}}

  :pedantic :warn)
