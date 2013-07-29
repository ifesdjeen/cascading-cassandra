(ns com.ifesdjeen.cascading.cassandra.cql3-test
  (:require [clojurewerkz.cassaforte.client :as client])
  (:use cascalog.api
        clojure.test
        clojurewerkz.cassaforte.cql
        clojurewerkz.cassaforte.query
        [midje sweet cascalog])
  (:require [com.ifesdjeen.cascading.cassandra.test-helpers :as th]
            [cascalog.io :as io]
            [cascalog.ops :as c])
  (:import [cascading.tuple Fields]
           [cascading.scheme Scheme]
           [com.ifesdjeen.cascading.cassandra CassandraTap]
           [com.ifesdjeen.cascading.cassandra.cql3 CassandraCQL3Scheme]
           [org.apache.cassandra.utils ByteBufferUtil]
           [org.apache.cassandra.thrift Column]))

(use-fixtures :each th/initialize!)

(defn create-tap
  [conf]
  (let [defaults      {"db.host" "127.0.0.1"
                       "db.port" "19160"
                       "db.keyspace" "cascading_cassandra"
                       "db.inputPartitioner" "org.apache.cassandra.dht.Murmur3Partitioner"
                       "db.outputPartitioner" "org.apache.cassandra.dht.Murmur3Partitioner"}
        scheme        (CassandraCQL3Scheme. (merge defaults conf))
        tap           (CassandraTap. scheme)]
    tap))

(deftest t-cassandra-tap-as-source
  (dotimes [counter 100]
    (client/prepared
     (insert :libraries_cql_3
             {:name (str "Cassaforte" counter)
              :language (str "Clojure" counter)
              :schmotes (int counter)
              :votes (int (* 2 counter))})))
  (let [tap (create-tap {"db.columnFamily" "libraries_cql_3"
                         "types" {"name"      "UTF8Type"
                                  "language"  "UTF8Type"
                                  "schmotes"  "Int32Type"
                                  "votes"     "Int32Type"}})
        query (<- [?count ?sum3 ?sum4]
                  (tap ?value1 ?value2 ?value3 ?value4)
                  (c/count ?count)
                  (c/sum ?value3 :> ?sum3)
                  (c/sum ?value4 :> ?sum4))]

    (fact query => (produces [[100 4950 9900]]))))
