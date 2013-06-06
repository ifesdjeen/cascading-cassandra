(ns com.ifesdjeen.cascading.cassandra.core-test
  (:require [clojurewerkz.cassaforte.embedded :as e])
  (:use cascalog.api
        clojure.test
        clojurewerkz.cassaforte.cql
        cascalog.playground
        clojurewerkz.cassaforte.query
        [clojurewerkz.cassaforte.bytes :as b]
        [midje sweet cascalog])
  (:require [cascalog.io :as io]
            [cascalog.ops :as c])
  (:import [cascading.tuple Fields]
           [cascading.scheme Scheme]
           [com.ifesdjeen.cascading.cassandra CassandraTap CassandraScheme]
           [org.apache.cassandra.utils ByteBufferUtil]
           [org.apache.cassandra.thrift Column]))

(bootstrap-emacs)

(declare connected?)
(defn create-test-column-family
  []
  (e/start-server!)
  (alter-var-root (var *debug-output* ) (constantly false))
  (when (not (bound? (var *client*)))
    (connect! ["127.0.0.1"]))
  (drop-keyspace :cascading_cassandra)
  (create-keyspace :cascading_cassandra
                   (with {:replication
                          {:class "SimpleStrategy"
                           :replication_factor 1}}))
  (use-keyspace :cascading_cassandra)
  (create-table :libraries
                (with {:compact-storage true})
                (column-definitions {:name :varchar
                                     :language :varchar
                                     :schmotes :int
                                     :votes :int
                                     :primary-key [:name]}))
  (create-table :libraries_wide
                (with {:compact-storage true})
                (column-definitions {:name :varchar
                                     :language :varchar
                                     :votes :int
                                     :primary-key [:name :language]})))

(defn create-tap
  [conf]
  (let [defaults      {"sink.keyColumnName" "name"
                       "db.host" "127.0.0.1"
                       "db.port" "9160"
                       "db.keyspace" "cascading_cassandra"
                       "db.inputPartitioner" "org.apache.cassandra.dht.Murmur3Partitioner"
                       "db.outputPartitioner" "org.apache.cassandra.dht.Murmur3Partitioner"}
        scheme        (CassandraScheme. (merge defaults conf))
        tap           (CassandraTap. scheme)]
    tap))


(deftest t-cassandra-tap-as-source
  (create-test-column-family)
  (dotimes [counter 100]
    (prepared
     (insert :libraries
             (values {:name (str "Cassaforte" counter)
                      :language (str "Clojure" counter)
                      :schmotes (int counter)
                      :votes (int counter)}))))
  (let [tap (create-tap {"source.types" {"language"    "UTF8Type"
                                         "schmotes"    "Int32Type"
                                         "votes"       "Int32Type"}
                         "source.useWideRows" false
                         "db.columnFamily" "libraries"})]
    (fact "Handles simple calculations"
          (<-
           [?count ?sum]
           (tap ?value1 ?value2 ?value3 ?value4)
           (c/count ?count)
           (c/sum ?value3 :> ?sum))
          => (produces [[100 4950]]))))


(deftest t-cassandra-tap-as-sink
  (create-test-column-family)
  (let [test-data [["Riak" "Erlang"]
                   ["Cassaforte" "Clojure"]]]

    (?<- (create-tap {"source.columns" ["name" "language"]
                      "sink.outputMappings" {"name"     "?value1"
                                             "language" "?value2"}
                      "sink.keyColumnName" "name"
                      "db.columnFamily" "libraries"})
         [?value1 ?value2]
         (test-data ?value1 ?value2))

    (let [res (select :libraries)]
      (is (= "Riak" (:name (first res))))
      (is (= "Erlang" (:language (first res))))
      (is (= "Cassaforte" (:name (second res))))
      (is (= "Clojure" (:language (second res)))))))

(deftest t-cassandra-tap-as-source-wide
  (create-test-column-family)
  (dotimes [counter 100]
    (prepared
     (insert :libraries_wide
             (values {:name (str "Cassaforte" counter)
                      :language (str "Clojure" counter)
                      :votes (int counter)}))))

  (let [tap (create-tap {"db.columnFamily" "libraries_wide"
                         "source.useWideRows" true
                         "source.types" {"key"   "UTF8Type"
                                         "value" "Int32Type"}})]
    (fact "Handles simple calculations"
          (<-
           [?count ?sum]
           (tap ?value1 ?value2 ?value3)
           (c/count ?count)
           (c/sum ?value3 :> ?sum))
          => (produces [[100 4950]]))))

(deftest t-cassandra-tap-as-sink-wide
  (create-test-column-family)
  (let [test-data [["Riak" "Erlang" (int 100)]
                   ["Cassaforte" "Clojure" (int 150)]]]

    (?<- (create-tap {"sink.sinkImpl" "com.ifesdjeen.cascading.cassandra.DynamicRowSink"
                      "sink.keyColumnName" "name"
                      "sink.outputMappings" {"name" "?value1"}
                      "sink.outputWideMappings" {"columnName" "?value2"
                                                 "columnValue" "?value3"}
                      "db.columnFamily" "libraries_wide"})
         [?value1 ?value2 ?value3]
         (test-data ?value1 ?value2 ?value3))

    (let [res (select :libraries_wide)]
      (is (= "Riak" (:name (first res))))
      (is (= "Erlang" (:language (first res))))
      (is (= "Cassaforte" (:name (second res))))
      (is (= "Clojure" (:language (second res))))))


  )
