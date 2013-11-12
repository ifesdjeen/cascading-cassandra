(ns com.ifesdjeen.cascading.cassandra.core-test
  (:require [clojurewerkz.cassaforte.client :as client])
  (:use cascalog.api
        clojure.test
        clojurewerkz.cassaforte.cql
        clojurewerkz.cassaforte.query
        [midje sweet cascalog])
  (:require [com.ifesdjeen.cascading.cassandra.test-helpers :as th]
            [cascalog.logic.ops :as c])
  (:import [cascading.tuple Fields]
           [cascading.scheme Scheme]
           [com.ifesdjeen.cascading.cassandra CassandraTap CassandraScheme]
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
        scheme        (CassandraScheme. (merge defaults conf))
        tap           (CassandraTap. scheme)]
    tap))


(deftest t-cassandra-tap-as-source
  (dotimes [counter 100]
    (client/prepared
     (insert :libraries
             {:name (str "Cassaforte" counter)
              :language (str "Clojure" counter)
              :schmotes (int counter)
              :votes (int (* 2 counter))})))
  (let [tap (create-tap {"db.columnFamily" "libraries"
                         "types" {"name"      "UTF8Type"
                                  "language"  "UTF8Type"
                                  "schmotes"  "Int32Type"
                                  "votes"     "Int32Type"}
                         "mappings.source" ["language" "schmotes" "votes"]})
        query (<- [?count ?sum3 ?sum4]
                  (tap ?value1 ?value2 ?value3 ?value4)
                  (c/count ?count)
                  (c/sum ?value3 :> ?sum3)
                  (c/sum ?value4 :> ?sum4))]

    (fact query => (produces [[100 4950 9900]]))))

(deftest t-cassandra-tap-as-source-different-mapping
  (dotimes [counter 100]
    (client/prepared
     (insert :libraries
             {:name (str "Cassaforte" counter)
              :language (str "Clojure" counter)
              :schmotes (int counter)
              :votes (int (* 2 counter))})))
  (let [tap (create-tap {"db.columnFamily" "libraries"
                         "types" {"name"      "UTF8Type"
                                  "language"  "UTF8Type"
                                  "schmotes"  "Int32Type"
                                  "votes"     "Int32Type"}
                         "mappings.source" ["votes" "language"]})
        query (<- [?count ?votes-sum]
                  (tap ?name-field ?votes-field ?language-field)
                  (c/count ?count)
                  (c/sum ?votes-field :> ?votes-sum))]

    (fact query => (produces [[100 9900]]))))

(deftest t-cassandra-tap-as-sink
  (let [test-data [["Riak" "Erlang" (int 100) (int 200)]
                   ["Cassaforte" "Clojure" (int 300) (int 400)]]]

    (?<- (create-tap {"db.columnFamily" "libraries"
                      "types" {"name"      "UTF8Type"
                               "language"  "UTF8Type"
                               "schmotes"  "Int32Type"
                               "votes"     "Int32Type"}
                      "mappings.rowKey" "name"
                      "mappings.sink" {"name"     "?value1"
                                       "language" "?value2"
                                       "schmotes" "?value3"
                                       "votes"    "?value4"}})
         [?value1 ?value2 ?value3 ?value4]
         (test-data ?value1 ?value2 ?value3 ?value4))

    (let [res (select :libraries)]
      (is (= "Riak" (:name (first res))))
      (is (= "Erlang" (:language (first res))))
      (is (= 100 (:schmotes (first res))))
      (is (= 200 (:votes (first res))))

      (is (= "Cassaforte" (:name (second res))))
      (is (= "Clojure" (:language (second res))))
      (is (= 300 (:schmotes (second res))))
      (is (= 400 (:votes (second res)))))))

(deftest t-cassandra-tap-as-source-wide
  (dotimes [counter 100]
    (client/prepared
     (insert :libraries_wide
             {:name (str "Cassaforte" counter)
              :language (str "Clojure" counter)
              :votes (int counter)})))

  (let [tap (create-tap {"db.columnFamily" "libraries_wide"
                         "types.dynamic" {"rowKey"      "UTF8Type"
                                          "columnName"  "UTF8Type"
                                          "columnValue" "Int32Type"}})
        query (<- [?count ?sum]
                  (tap ?value1 ?value2 ?value3)
                  (c/count ?count)
                  (c/sum ?value3 :> ?sum))]
    (fact "Handles simple calculations"
          query
          => (produces [[100 4950]]))))

(deftest t-cassandra-tap-as-sink-wide
  (let [test-data [["Riak" "Erlang" (int 100)]
                   ["Cassaforte" "Clojure" (int 150)]]]

    (?<- (create-tap {"db.columnFamily" "libraries_wide"
                      "types.dynamic" {"rowKey"      "UTF8Type"
                                       "columnName"  "UTF8Type"
                                       "columnValue" "Int32Type"}

                      "mappings.dynamic" {"rowKey"      "?value1"
                                          "columnName"  "?value2"
                                          "columnValue" "?value3"}})
         [?value1 ?value2 ?value3]
         (test-data ?value1 ?value2 ?value3))

    (let [res (select :libraries_wide)]
      (is (= "Riak" (:name (first res))))
      (is (= "Erlang" (:language (first res))))
      (is (= "Cassaforte" (:name (second res))))
      (is (= "Clojure" (:language (second res)))))))

(deftest t-cassandra-tap-as-source-wide-empty-value
  (dotimes [counter 100]
    (client/prepared
     (insert :libraries_wide_empty_value
             {:name (str "Cassaforte" counter)
              :votes (int counter)})))

  (let [tap (create-tap {"db.columnFamily" "libraries_wide_empty_value"
                         "types.dynamic" {"rowKey"      "UTF8Type"
                                          "columnName"  "Int32Type"
                                          "columnValue" nil}})
        query (<- [?count ?sum]
                  (tap ?value1 ?value2)
                  (c/count ?count)
                  (c/sum ?value2 :> ?sum))]
    (fact "Handles simple calculations"
      query
      => (produces [[100 4950]]))))

(deftest t-cassandra-tap-as-sink-wide-empty-value
  (let [test-data [["Riak" (int 100)]
                   ["Cassaforte" (int 150)]]]

    (?<- (create-tap {"db.columnFamily" "libraries_wide_empty_value"
                      "types.dynamic" {"rowKey"      "UTF8Type"
                                       "columnName"  "Int32Type"
                                       "columnValue" nil}

                      "mappings.dynamic" {"rowKey"      "?value1"
                                          "columnName"  "?value2"
                                          "columnValue" nil}})
         [?value1 ?value2]
         (test-data ?value1 ?value2))

    (let [res (select :libraries_wide_empty_value)]
      (is (= "Riak" (:name (first res))))
      (is (= 100 (:votes (first res))))
      (is (= "Cassaforte" (:name (second res))))
      (is (= 150 (:votes (second res)))))))

(deftest t-cassandra-tap-as-source-wide-composite
  (dotimes [counter 100]
    (client/prepared
     (insert :libraries_wide_composite
             {:name (str "Cassaforte" counter)
              :language (str "Clojure" counter)
              :version (int 5)
              :votes (int counter)})))

  (let [tap (create-tap {"db.columnFamily" "libraries_wide_composite"

                         "types.dynamic" {"rowKey"      "UTF8Type"
                                          "columnName"  "CompositeType(UTF8Type, Int32Type)"
                                          "columnValue" "Int32Type"}})
        query (<- [?count ?version-sum ?votes-sum]
                  (tap ?value1 ?value2 ?value3 ?value4)
                  (c/count ?count)
                  (c/sum ?value3 :> ?version-sum)
                  (c/sum ?value4 :> ?votes-sum))]
    (fact "Handles simple calculations"
      query
      => (produces [[100 500 4950]]))))

(deftest t-cassandra-tap-as-sink-wide-composite
  (let [test-data [["Riak" "Erlang" (int 5) (int 100)]
                   ["Cassaforte" "Clojure" (int 1) (int 150)]]]

    (?<- (create-tap {"db.columnFamily" "libraries_wide_composite"

                      "types.dynamic" {"rowKey"      "UTF8Type"
                                       "columnName"  "CompositeType(UTF8Type, Int32Type)"
                                       "columnValue" "Int32Type"}

                      "mappings.dynamic" {"rowKey"      "?value1"
                                          "columnName"  ["?value2" "?value3"]
                                          "columnValue" "?value4"}})
         [?value1 ?value2 ?value3 ?value4]
         (test-data ?value1 ?value2 ?value3 ?value4))

    (let [res (select :libraries_wide_composite)]
      (is (= "Riak" (:name (first res))))
      (is (= "Erlang" (:language (first res))))
      (is (= 5 (:version (first res))))

      (is (= "Cassaforte" (:name (second res))))
      (is (= "Clojure" (:language (second res))))
      (is (= 1 (:version (second res)))))))

(deftest t-cassandra-tap-as-source-wide-composite-empty-value

  (dotimes [counter 100]
    (client/prepared
     (insert :libraries_wide_composite_empty_value
             {:name (str "Cassaforte" counter)
              :language (str "Clojure" counter)
              :votes (int counter)})))

  (let [tap (create-tap {"db.columnFamily" "libraries_wide_composite_empty_value"

                         "types.dynamic" {"rowKey"      "UTF8Type"
                                          "columnName"  "CompositeType(UTF8Type, Int32Type)"
                                          "columnValue" nil}})
        query (<- [?count ?votes-sum]
                  (tap ?value1 ?value2 ?value3)
                  (c/count ?count)
                  (c/sum ?value3 :> ?votes-sum))]
    (fact "Handles simple calculations"
      query
      => (produces [[100 4950]]))))



(deftest t-cassandra-tap-as-sink-wide-composite-empty-value
  (let [test-data [["Riak" "Erlang" (int 5)]
                   ["Cassaforte" "Clojure" (int 1)]]]

    (?<- (create-tap {"db.columnFamily" "libraries_wide_composite_empty_value"

                      "types.dynamic" {"rowKey"      "UTF8Type"
                                       "columnName"  "CompositeType(UTF8Type, Int32Type)"
                                       "columnValue" nil}

                      "mappings.dynamic" {"rowKey"      "?value1"
                                          "columnName"  ["?value2" "?value3"]
                                          "columnValue" nil}})
         [?value1 ?value2 ?value3]
         (test-data ?value1 ?value2 ?value3))

    (let [res (select :libraries_wide_composite_empty_value)]
      (is (= "Riak" (:name (first res))))
      (is (= "Erlang" (:language (first res))))
      (is (= 5 (:votes (first res))))

      (is (= "Cassaforte" (:name (second res))))
      (is (= "Clojure" (:language (second res))))
      (is (= 1 (:votes (second res)))))))
