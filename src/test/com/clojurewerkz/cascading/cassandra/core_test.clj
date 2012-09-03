(ns com.clojurewerkz.cascading.cassandra.core-test
  (:use cascalog.api
        clojure.test
        [midje sweet cascalog])
  (:require [cascalog.io :as io])
  (:import [cascading.tuple Fields]
           [cascading.tap Tap]
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
