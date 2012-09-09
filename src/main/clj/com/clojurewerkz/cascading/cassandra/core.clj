(ns com.clojurewerkz.cascading.cassandra.core
  (:require [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.schema :as sch]
            [clojurewerkz.cassaforte.cql    :as cql])
  (:use cascalog.api
        clojure.test)
  (:require [cascalog.io :as io]
            [cascalog.ops :as c])
  (:import [cascading.tuple Fields]
           [cascading.scheme Scheme]
           [com.twitter.maple.jdbc JDBCTap JDBCScheme TableDesc]
           [com.clojurewerkz.cascading.cassandra CassandraTap CassandraScheme]))


(defmacro with-thrift-exception-handling
[& forms]
`(try
   (do ~@forms)
   (catch org.apache.cassandra.thrift.InvalidRequestException ire#
     (println (.getWhy ire#)))))

(cc/connect! "127.0.0.1" "CassaforteTest1")

(defn yo
  []
  (with-thrift-exception-handling
    (cql/drop-column-family "libraries"))

  (cql/create-column-family "libraries"
                            {:name      "varchar"
                             :language  "varchar"}
                            :primary-key :name)

  (sch/create-index "libraries" :language)
  ;; (dotimes [n 10000]
  ;;   (cql/insert "libraries" {:name (str "Cassaforte" n) :language (str "Clojure" n)}))

  (let [scheme (CassandraScheme. "127.0.0.1" "9160" "CassaforteTest1" "libraries" (java.util.ArrayList. ["?value1" "?value2"]))
        tap    (CassandraTap. scheme)

        test-data [["field1-1" "field1-2" "field1-2"]
                   ["field2-1" "field2-2" "field1-2"]
                   ["field3-1" "field3-2" "field1-2"]]]


    (?<- tap
         [?value1 ?value2]
         (test-data ?value1 ?value2 ?value3)
         ))

  )

(comment (defn yo
           []
           (with-thrift-exception-handling
             (cql/drop-column-family "libraries"))

           (cql/create-column-family "libraries"
                                     {:name      "varchar"
                                      :language  "varchar"}
                                     :primary-key :name)

           (sch/create-index "libraries" :language)
           (dotimes [n 10000]
             (cql/insert "libraries" {:name (str "Cassaforte" n) :language (str "Clojure" n)}))

           (let [scheme (CassandraScheme. "127.0.0.1" "9160" "CassaforteTest1" "libraries" (java.util.ArrayList. ["name" "language"]))
                 tap    (CassandraTap. scheme)

                 test-data [["field1-1" "field1-2" "field1-3"]
                            ["field2-1" "field2-2" "field2-3"]
                            ["field3-1" "field3-2" "field3-3"]]]


             (comment (?<-
                       (stdout)
                       ;; (hfs-textline (str "/tmp/results/my_results"))
                       [?count]
                       (tap ?value1 ?value2 ?value3)
                       (c/count ?count)
                       )))

           ;; (let [scheme (JDBCScheme. (Fields. (into-array String ["timeval" "value1" "value2" "value3"])) (into-array String ["timeval" "value1" "value2" "value3"]))
           ;;       table-desc (TableDesc. "raw_values")
           ;;       tap (JDBCTap. "jdbc:mysql://localhost:3306/eventoverse?user=root&password=root" "com.mysql.jdbc.Driver" table-desc scheme)]
           ;;   (?<-
           ;;      (stdout)
           ;;      [?count]
           ;;      (tap ?timeval ?value1 ?value2 ?value3)
           ;;      (c/count ?count)))



           ))

(defn -main
  []
  (yo))