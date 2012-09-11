(ns com.clojurewerkz.cascading.cassandra.core
  (:require [clojurewerkz.cassaforte.client :as cc]
           [clojurewerkz.cassaforte.schema :as sch]
           [clojurewerkz.cassaforte.cql    :as cql])
  (:use cascalog.api
        clojure.test)
  (:require [cascalog.io :as io]
            [cascalog.ops :as c])
  (:import [java.util.concurrent Executors])
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

(defn create-test-column-family
  [settings]
  (println "Connecting to: " (:host settings) ", "(:keyspace settings))
  (cc/connect! (:host settings) (:keyspace settings))
  (with-thrift-exception-handling
    (cql/drop-column-family "libraries"))
  (cql/create-column-family "libraries"
                            {:name      "varchar"
                             :language  "varchar"
                             :votes "int"}
                             :primary-key :name))

(defn make-cassandra-tap
  [settings]
  (let [scheme (CassandraScheme. (:host settings) "9160" (:keyspace settings) "libraries" "name" (java.util.ArrayList. ["name" "language" "votes"]))
        tap    (CassandraTap. scheme)]
    tap))


(defn populate-test-column-family
  [settings factor]
  (let [executors (Executors/newFixedThreadPool 256)
        tasks (map
               (fn [i]
                 (fn []
                   (binding [cc/*cassandra-client* (cc/connect (:host settings) (:keyspace settings))]
                     (dotimes [j factor]
                       (let [counter (+ (* i factor) j)]
                         (cql/insert "libraries" {:name (str "Cassaforte" counter) :language (str "Clojure" counter) :votes counter}))))))
               (range factor))]
    (doseq [future (.invokeAll executors tasks)]
      (.get future))
    (.shutdown executors))

  (let [res (cql/execute-raw "SELECT COUNT(*) FROM libraries")
        n   (cql/count-value res)]
    (println "Inserted " n " records")))

(defn use-test-db-as-sink
  [])
(defn use-test-db-as-source
  [settings]
  (let [tap (make-cassandra-tap settings)]
    (?<-
     (stdout)
     [?count]
     (tap ?value1 ?value2 ?value3)
     (c/sum ?value3 :> ?sum)
     (c/count ?count))))

(def local-config {:host "127.0.0.1" :keyspace "CassaforteTest1"})
(def staging-config {:host "10.1.12.13" :keyspace "CassaforteTest1"})


(comment (defn -main
           []
           (let [settings local-config]
             ;; (create-test-column-family settings)
             ;; (populate-test-column-family settings 1000)
             (use-test-db-as-source)
             )))

(defmain WritePoemFreq []
  (let [settings local-config]
    (use-test-db-as-source settings)))