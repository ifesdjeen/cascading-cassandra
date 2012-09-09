(ns com.clojurewerkz.cascading.cassandra.tap
  (:import [clojure.lang PersistentHashMap]
           [cascading.flow.FlowProcess]
           [cascading.tuple Tuple]
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
  true)

(defn tap-source
  [flow source-call]
  (let [tuple (Tuple.)
        context (.getContext source-call)
        key (get context 0)
        value (get context 1)
        input (.getInput source-call)
        result (.next input key value)]
    (if result
      false
      (do
        (println "====================================================================================================")
        (println key)
        (println value)
        (println "====================================================================================================")
        true))
    )
)