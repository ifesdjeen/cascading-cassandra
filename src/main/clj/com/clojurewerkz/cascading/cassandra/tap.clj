(ns com.clojurewerkz.cascading.cassandra.tap
  (:import [clojure.lang PersistentHashMap]
           [cascading.flow.FlowProcess]
           [cascading.scheme SinkCall SourceCall]
           [java.lang String]))


(gen-class
 :name com.clojurewerkz.cascading.cassandra.CassandraSink
 :methods [#^{:static true} [sink [cascading.flow.FlowProcess cascading.scheme.SinkCall] boolean]]
 :prefix tap-)

(gen-class
 :name com.clojurewerkz.cascading.cassandra.CassandraSource
 :methods [#^{:static true} [source [cascading.flow.FlowProcess cascading.scheme.SourceCall] boolean]]
 :prefix tap-)

(defn tap-sink
  [flow sink]
  (println "I AM SINK")
  true)

(defn tap-source
  [flow sink]
  (println "I AM SOURCE")
  (println flow sink)
  true)