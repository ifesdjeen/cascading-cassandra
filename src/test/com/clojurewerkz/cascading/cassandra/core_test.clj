(ns com.clojurewerkz.cascading.cassandra.core-test
  (:use cascalog.api
        clojure.test
        [midje sweet cascalog])
  (:require [cascalog.io :as io]
            [cascalog.ops :as c])
  (:import [cascading.tuple Fields]
           [com.twitter.maple.jdbc JDBCTap JDBCScheme TableDesc]
           [com.clojurewerkz.cascading.cassandra CassandraTap CassandraScheme]))

(deftest test-get
  (testing ""
    (let [scheme (CassandraScheme. (Fields. (into-array String ["field1" "field2" "field3"]))
                                   "ColumnFamily1")
          tap    (CassandraTap. "CassaforteTest1" "ColumnFamily1" scheme)

          test-data [["field1-1" "field1-2" "field1-3"]
                     ["field2-1" "field2-2" "field2-3"]
                     ["field3-1" "field3-2" "field3-3"]]]

      (fact ""
            (<- [?field1 ?field2 ?field3]
                (tap :> ?field1 ?field2 ?field3))
            => (produces [["field1-1" "field1-2" "field1-3"]
                          ["field2-1" "field2-2" "field2-3"]
                          ["field3-1" "field3-2" "field3-3"]])
            ))))

(comment (defn mysql-tap []
           (let [scheme (JDBCScheme. (Fields. (into-array String ["timeval" "value1" "value2" "value3"])) (into-array String ["timeval" "value1" "value2" "value3"]))
                 table-desc (TableDesc. "raw_values")
                 tap (JDBCTap. "jdbc:mysql://localhost:3306/eventoverse?user=root&password=root" "com.mysql.jdbc.Driver" table-desc scheme)]
             tap))

         (deftest test-get-2
           (fact ""
                 (?<-
                  (stdout)
                  [?count]
                  ((mysql-tap) ?timeval ?value1 ?value2 ?value3)
                  (c/count ?count))
                 (produces [["field1-1" "field1-2" "field1-3"]
                            ["field2-1" "field2-2" "field2-3"]
                            ["field3-1" "field3-2" "field3-3"]]))))