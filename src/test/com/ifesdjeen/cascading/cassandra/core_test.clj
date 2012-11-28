(ns com.ifesdjeen.cascading.cassandra.core-test
  (:require [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.schema :as sch]
            [clojurewerkz.cassaforte.conversion :as cconv]
            [clojurewerkz.cassaforte.cql    :as cql]
            [clojurewerkz.cassaforte.bytes  :as bytes])
  (:use cascalog.api
        clojure.test
        [midje sweet cascalog])
  (:require [cascalog.io :as io]
            [cascalog.ops :as c])
  (:import [cascading.tuple Fields]
           [cascading.scheme Scheme]
           [com.ifesdjeen.cascading.cassandra CassandraTap CassandraScheme]
           [org.apache.cassandra.utils ByteBufferUtil]
           [org.apache.cassandra.thrift Column]))

(defmacro with-thrift-exception-handling
[& forms]
`(try
   (do ~@forms)
   (catch org.apache.cassandra.thrift.InvalidRequestException ire#
     (println (.getWhy ire#)))))

(cc/connect! "127.0.0.1")
(sch/set-keyspace "CascadingCassandra")

(defn create-test-column-family
  []
  (with-thrift-exception-handling
    (cql/drop-column-family "libraries"))
  (cql/create-column-family "libraries"
                            {:name      "varchar"
                             :language  "varchar"
                             :votes     "int"}
                            :primary-key :name))

(defn create-tap
  ([]
     (create-tap ["name" "language" "votes"] {"name"     "?value1"
                                              "language" "?value2"
                                              "votes"    "?value3"}))
  ([columns mappings]
      (let [keyspace      "CascadingCassandra"
            column-family "libraries"
            scheme        (CassandraScheme. "127.0.0.1"
                                            "9160"
                                            keyspace
                                            column-family
                                            "name"
                                            columns
                                            mappings
                                            {"cassandra.inputPartitioner" "org.apache.cassandra.dht.RandomPartitioner"
                                             "cassandra.outputPartitioner" "org.apache.cassandra.dht.RandomPartitioner"})
            tap           (CassandraTap. scheme)]
        tap)))

(deftest t-cassandra-tap-as-source
  (create-test-column-family)
  (dotimes [counter 100]
    (cql/insert "libraries" {:name (str "Cassaforte" counter) :language (str "Clojure" counter) :votes (int counter)}))

  (fact "Handles simple calculations"
        (<-
         [?count ?sum]
         ((create-tap) ?value1 ?value2 ?value3)
         (c/count ?count)
         (c/sum ?value3 :> ?sum))
        => (produces [[100 4950]])))

(deftest t-cassandra-tap-as-source-2
  (create-test-column-family)

  (cql/insert "libraries" {:name "Riak" :language "Erlang" :votes (int 5)})
  (cql/insert "libraries" {:name "Cassaforte" :language "Clojure" :votes (int 3)})

  (fact "Retrieves data"
        (<-
         [?value1 ?value3]
         ((create-tap) ?value1 ?value2 ?value3))
        => (produces [["Cassaforte" 3] ["Riak" 5]])))


(deftest t-cassandra-tap-as-sink
  (create-test-column-family)
  (let [test-data [["Riak" "Erlang"]
                   ["Cassaforte" "Clojure"]]]

    (?<- (create-tap ["name" "language"] {"name"     "?value1"
                                          "language" "?value2"})
         [?value1 ?value2]
         (test-data ?value1 ?value2))

    (let [res (cconv/to-plain-hash (:rows (cql/execute "SELECT * FROM libraries")))]
      (is (= {:name "Cassaforte" :language "Clojure" :votes nil}
             (get res "Cassaforte")))
      (is (= {:name "Riak" :language "Erlang" :votes nil}
             (get res "Riak"))))))

;;
;;  To whom it may concern, same serialization code, but in Java:
;;
;;  for (IColumn column : columns.values()) {
;;    String name  = ByteBufferUtil.string(column.name());
;;    System.out.print("Name:");
;;    System.out.println(name);
;;
;;    String v = null;
;;
;;    if (name.contains("votes"))
;;      v = String.valueOf(ByteBufferUtil.toInt(column.value()));
;;    else
;;      v = ByteBufferUtil.string(column.value());
;;
;;    System.out.print("Value:");
;;    System.out.println(v);
;;  }

(defmapop deserialize-values
  [columns]
  (into []
        (for [column (.values columns)]
          (let [name (ByteBufferUtil/string (.name column))
                raw-value (.value column)]
            (cond
              (= "votes" name) (ByteBufferUtil/toInt raw-value)
              :else (ByteBufferUtil/string raw-value))))))

(deftest t-cassandra-tap-as-source-3
  (create-test-column-family)

  (cql/insert "libraries" {:name "Riak" :language "Erlang" :votes (int 5)})
  (cql/insert "libraries" {:name "Cassaforte" :language "Clojure" :votes (int 3)})

  (fact "Retrieves data"
        (<-
         [?name ?language ?votes]
         (deserialize-values ?columns :> ?language ?votes)
         ((create-tap [] {}) ?name ?columns))
        => (produces [["Cassaforte" "Clojure" 3] ["Riak" "Erlang" 5]])))