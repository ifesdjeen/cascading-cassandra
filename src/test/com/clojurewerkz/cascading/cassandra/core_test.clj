(ns com.clojurewerkz.cascading.cassandra.core-test
  (:use [midje sweet cascalog])
  (:require [cascalog.io :as io])
  (:import [cascading.tuple Fields]
           [cascading.tap Tap]
           [com.clojurewerkz.cascading.cassandra CassandraTap CassandraScheme]))

(fact ""
  (io/with-log-level :fatal
    (let [scheme (CassandraScheme. (Fields. (into-array String ["field1" "field2" "field3"]))
                                   "ColumnFamily1")
          tap    (CassandraTap. "CassaforteTest1" "ColumnFamily1" scheme)]

      (println tap))


  ))
