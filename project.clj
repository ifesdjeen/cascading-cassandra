(defproject cascading-cassandra "1.0.0-rc6"
  :description "Modern Cassandra tap for Cascading. Actually works with Cascading 2.0 and Cascalog 1.10."
  :url "https://github.com/ifesdjeen/cascading-cassandra"
  :min-lein-version "2.0.0"
  :license {:name "Double licensed under the Eclipse Public License (the same as Clojure) or the Apache Public License 2.0."}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [cascalog "2.0.0"]]
  :aot               [com.ifesdjeen.cascading.cassandra.core-test]
  :plugins [[lein-idea "1.0.1"]]
  :javac-options ["-target" "1.6" "-source" "1.6" "-Xlint:-options"]
  :java-source-paths ["src/main/java"]
  :test-paths        ["src/test"]
  :resource-paths    ["src/resources"]
  :profiles {:provided {:dependencies   [[mccraigmccraig/cassandra-all "1.2.8-SNAPSHOT"
                                          :exclusions [org.apache.hadoop
                                                       ;; org.apache.thrift/libthrift
                                                       org.apache.httpcomponents/httpclient]]
                                         ;; [org.apache.hadoop/hadoop-core "1.0.4"
                                         ;;   :exclusions [org.codehaus.jackson/jackson-mapper-asl]]
                                         [org.apache.hadoop/hadoop-core "1.1.2"
                                          :exclusions [org.codehaus.jackson/jackson-mapper-asl]]
                                         ]}
             :dev      {:resource-paths ["src/resources"]
                        :jvm-opts       ["-server"
                                         "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=51240"
                                         "-javaagent:lib/jamm-0.2.5.jar"
                                         "-Xmx768m"]
                        :dependencies   [[org.xerial.snappy/snappy-java "1.0.5-M3"]
                                         [clojurewerkz/cassaforte "1.2.0-SNAPSHOT"
                                          :exclusions [org.apache.thrift/libthrift
                                                       org.apache.cassandra/cassandra-all]]
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
