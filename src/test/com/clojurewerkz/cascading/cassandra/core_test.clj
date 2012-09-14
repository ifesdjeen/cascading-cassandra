(ns com.clojurewerkz.cascading.cassandra.core-test
  (:require [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.schema :as sch]
            [clojurewerkz.cassaforte.conversion :as cconv]
            [clojurewerkz.cassaforte.cql    :as cql])
  (:use cascalog.api
        clojure.test
        [midje sweet cascalog])
  (:require [cascalog.io :as io]
            [cascalog.ops :as c])
  (:import [cascading.tuple Fields]
           [cascading.scheme Scheme]
           [com.clojurewerkz.cascading.cassandra CassandraTap CassandraScheme]))

(defmacro with-thrift-exception-handling
[& forms]
`(try
   (do ~@forms)
   (catch org.apache.cassandra.thrift.InvalidRequestException ire#
     (println (.getWhy ire#)))))

(cc/connect! "127.0.0.1" "CascadingCassandra")

(defn create-test-column-family
  []
  (with-thrift-exception-handling
    (cql/drop-column-family "libraries"))
  (cql/create-column-family "libraries"
                            {:name      "varchar"
                             :language  "varchar"
                             :votes     "bigint"}
                            :primary-key :name))

(defn create-tap
  []
  (let [keyspace      "CascadingCassandra"
        column-family "libraries"
        scheme        (CassandraScheme. "127.0.0.1"
                                        "9160"
                                        keyspace
                                        column-family
                                        "name"
                                        (java.util.ArrayList. ["name" "language" "votes"])
                                        (java.util.HashMap. {"name" "?value1"
                                                             "language" "?value2"
                                                             "votes" "?value3"}))
        tap           (CassandraTap. scheme)]
    tap))

(deftest t-cassandra-tap-as-source
  (create-test-column-family)
  (dotimes [counter 100]
    (cql/insert "libraries" {:name (str "Cassaforte" counter) :language (str "Clojure" counter) :votes counter}))

  (fact "Handles simple calculations"
        (<-
         [?count ?sum]
         ((create-tap) ?value1 ?value2 ?value3)
         (c/count ?count)
         (c/sum ?value3 :> ?sum))
        => (produces [[100 4950]])))

(deftest t-cassandra-tap-as-source-2
  (create-test-column-family)

  (cql/insert "libraries" {:name "Riak" :language "Erlang" :votes 5})
  (cql/insert "libraries" {:name "Cassaforte" :language "Clojure" :votes 3})

  (fact "Retrieves data"
        (<-
         [?value1 ?value3]
         ((create-tap) ?value1 ?value2 ?value3))
        => (produces [["Cassaforte" 3] ["Riak" 5]])))

(deftest t-cassandra-tap-as-sink
  (create-test-column-family)

  (let [test-data [["Riak" "Erlang" 5]
                   ["Cassaforte" "Clojure" 3]]]

    (?<- (create-tap)
         [?value1 ?value2 ?value3]
         (test-data ?value1 ?value2 ?value3))
    (Thread/sleep 5000)

    (let [res (cconv/to-plain-hash (:rows (cql/execute "SELECT * FROM libraries")))]
      (is (= {:name "Cassaforte" :language "Clojure" :votes 3}
             (get res "Cassaforte")))
      (is (= {:name "Riak" :language "Erlang" :votes 5}{:name "Riak" :language "Erlang" :votes 5}
             (get res "Riak"))))))