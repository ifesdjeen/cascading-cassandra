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

(deftest t-tap-as-source-compact-storage
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

(deftest t-cassandra-tap-compact-storage-with-columns
  (dotimes [counter 100]
    (client/prepared
     (insert :libraries
             {:name (str "Cassaforte" counter)
              :language (str "Clojure" counter)
              :votes (int counter)})))

  (let [tap (create-tap {"db.columnFamily" "libraries"
                         "source.columns" "votes"
                         "types" {"name"      "UTF8Type"
                                  "votes"     "Int32Type"}})
        query (<- [?count ?sum]
                   (tap _ ?value1)
                   (c/count ?count)
                   (c/sum ?value1 :> ?sum))]

    (fact "Handles simple calculations"
          query
          => (produces [[100 4950]]))))

(deftest t-tap-as-source-normal
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

(defn orf
  [& args]
  (reduce #(or %1 %2) args))

(deftest t-tap-as-source-normal-null-value
  (dotimes [counter 100]
    (client/prepared
     (insert :libraries_cql_3
             {:name (str "Cassaforte" counter)
              :language (str "Clojure" counter)
              :schmotes (int counter)
              :votes nil})))
  (let [tap (create-tap {"db.columnFamily" "libraries_cql_3"
                         "types" {"name"      "UTF8Type"
                                  "language"  "UTF8Type"
                                  "schmotes"  "Int32Type"
                                  "votes"     "Int32Type"}})
        query (<- [?count ?sum3 ?sum4]
                  (tap ?value1 ?value2 ?value3 !value4)
                  (c/count ?count)
                  (c/sum ?value3 :> ?sum3)
                  (orf !value4 0 :> ?value4)
                  (c/sum ?value4 :> ?sum4))]

    ;;(fact (??- query) => [[[100 4950 0]]])
    (fact query => (produces [[100 4950 0]]))
    ))

(deftest t-tap-as-source-normal-composite-key
  (dotimes [counter 100]
    (client/prepared
     (insert :libraries_cql_3_composite_key
             {:name (str "Cassaforte" counter)
              :language (str "Clojure" counter)
              :schmotes (int counter)
              :votes (int (* 2 counter))})))
  (let [tap (create-tap {"db.columnFamily" "libraries_cql_3_composite_key"
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

(deftest t-tap-as-source-normal-empty-value
  (dotimes [counter 100]
    (client/prepared
     (insert :libraries_cql_3_empty_value
             {:name (str "Cassaforte" counter)
              :language (str "Clojure" counter)
              :schmotes (int counter)
              :votes (int (* 2 counter))})))
  (let [tap (create-tap {"db.columnFamily" "libraries_cql_3_empty_value"
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

(deftest t-cassandra-tap-as-source-with-where-statement
  (create-index :libraries_cql_3 :schmotes)
  (dotimes [counter 100]
    (client/prepared
     (insert :libraries_cql_3
             {:name (str "Cassaforte" counter)
              :language (str "Clojure" counter)
              :schmotes (int counter)
              :votes (int (* 2 counter))})))

  (let [tap (create-tap {"db.columnFamily" "libraries_cql_3"
                         "source.whereClauses" "schmotes = 50"
                         "source.CQLPageRowSize" "50"
                         "types" {"name"      "UTF8Type"
                                  "language"  "UTF8Type"
                                  "schmotes"  "Int32Type"
                                  "votes"     "Int32Type"}})
        query (<- [?count ?sum3 ?sum4]
                  (tap ?value1 ?value2 ?value3 ?value4)
                  (c/count ?count)
                  (c/sum ?value3 :> ?sum3)
                  (c/sum ?value4 :> ?sum4))]

    (fact query => (produces [[1 50 100]]))))

(deftest t-cassandra-tap-as-sink-compact-sorage
  (let [test-data [["Riak" "Erlang" (int 100)]
                   ["Cassaforte" "Clojure" (int 150)]]]

    (?<- (create-tap {"db.columnFamily" "libraries"
                      "sink.outputCQL" "UPDATE libraries SET votes = ?, language = ?"
                      "mappings.cqlKeys" ["name"]
                      "mappings.cqlValues" ["votes" "language"]
                      "mappings.cql" {"name"     "?value1"
                                      "language" "?value2"
                                      "votes"    "?value3" }})
         [?value1 ?value2 ?value3]
         (test-data ?value1 ?value2 ?value3))

    (let [res (select :libraries)]
      (is (= "Riak" (:name (first res))))
      (is (= "Erlang" (:language (first res))))
      (is (= "Cassaforte" (:name (second res))))
      (is (= "Clojure" (:language (second res)))))))

(deftest t-cassandra-tap-as-sink-normal
  (let [test-data [["Riak" "Erlang" (int 100)]
                   ["Cassaforte" "Clojure" (int 150)]]]

    (?<- (create-tap {"db.columnFamily" "libraries_cql_3"
                      "sink.outputCQL" "UPDATE libraries_cql_3 SET votes = ?, language = ?"
                      "mappings.cqlKeys" ["name"]
                      "mappings.cqlValues" ["votes" "language"]
                      "mappings.cql" {"name"     "?value1"
                                      "language" "?value2"
                                      "votes"    "?value3" }})
         [?value1 ?value2 ?value3]
         (test-data ?value1 ?value2 ?value3))

    (let [res (select :libraries_cql_3)]
      (is (= "Riak" (:name (first res))))
      (is (= "Erlang" (:language (first res))))
      (is (= "Cassaforte" (:name (second res))))
      (is (= "Clojure" (:language (second res)))))))

(deftest t-cassandra-tap-as-sink-normal-composite-key
  (let [test-data [["Riak" "Erlang" (int 100)]
                   ["Cassaforte" "Clojure" (int 150)]]]

    (?<- (create-tap {"db.columnFamily" "libraries_cql_3_composite_key"
                      "sink.outputCQL" "UPDATE libraries_cql_3_composite_key SET votes = ?"
                      "mappings.cqlKeys" ["name" "language"]
                      "mappings.cqlValues" ["votes"]
                      "mappings.cql" {"name"     "?value1"
                                      "language" "?value2"
                                      "votes"    "?value3" }})
         [?value1 ?value2 ?value3]
         (test-data ?value1 ?value2 ?value3))

    (let [res (select :libraries_cql_3_composite_key)]
      (is (= "Riak" (:name (first res))))
      (is (= "Erlang" (:language (first res))))
      (is (= 100 (:votes (first res))))
      (is (= "Cassaforte" (:name (second res))))
      (is (= "Clojure" (:language (second res))))
      (is (= 150 (:votes (second res)))))))

(deftest t-cassandra-tap-as-sink-normal-composite-partition-key
  (let [test-data [["Riak" "Erlang" (int 100)]
                   ["Cassaforte" "Clojure" (int 150)]]]

    (?<- (create-tap {"db.columnFamily" "libraries_cql_3_composite_partition_key"
                      "sink.outputCQL" "UPDATE libraries_cql_3_composite_partition_key SET votes = ?"
                      "mappings.cqlKeys" ["name" "language"]
                      "mappings.cqlValues" ["votes"]
                      "mappings.cql" {"name"     "?value1"
                                      "language" "?value2"
                                      "votes"    "?value3" }})
         [?value1 ?value2 ?value3]
         (test-data ?value1 ?value2 ?value3))

    (let [res (select :libraries_cql_3_composite_partition_key)]
      (is (= "Riak" (:name (first res))))
      (is (= "Erlang" (:language (first res))))
      (is (= 100 (:votes (first res))))
      (is (= "Cassaforte" (:name (second res))))
      (is (= "Clojure" (:language (second res))))
      (is (= 150 (:votes (second res)))))))

(deftest t-cassandra-tap-as-source-wide-compact-storage
  (dotimes [counter 100]
    (client/prepared
     (insert :libraries_wide
             {:name (str "Cassaforte" counter)
              :language (str "Clojure" counter)
              :votes (int counter)})))

  (let [tap (create-tap {"db.columnFamily" "libraries_wide"
                         "types" {"name"      "UTF8Type"
                                  "language"  "UTF8Type"
                                  "votes"     "Int32Type"}})
        query (<- [?count ?sum]
                  (tap ?value1 ?value2 ?value3)
                  (c/count ?count)
                  (c/sum ?value3 :> ?sum))]
    (fact "Handles simple calculations"
          query
          => (produces [[100 4950]]))))
