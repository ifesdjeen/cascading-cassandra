(ns com.ifesdjeen.cascading.cassandra.test-helpers
  (:use cascalog.playground
        clojurewerkz.cassaforte.cql
        clojurewerkz.cassaforte.query
        [midje sweet cascalog])
  (:require [clojurewerkz.cassaforte.embedded :as e]
            [clojurewerkz.cassaforte.client :as client]))

(bootstrap-emacs)

(declare session)

(defn initialize!
  [f]
  (when (not (bound? (var session)))
    (e/start-server! :cleanup true)
    (def session (client/connect! ["127.0.0.1"]
                                  :port 19042)))

  (try
    (drop-keyspace :cascading_cassandra)
    (catch Exception _ nil))

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

  (create-table :libraries_cql_3
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
                                     :primary-key [:name :language]}))

  (create-table :libraries_wide_empty_value
                (with {:compact-storage true})
                (column-definitions {:name :varchar
                                     :votes :int
                                     :primary-key [:name :votes]}))

  (create-table :libraries_wide_composite
                (with {:compact-storage true})
                (column-definitions {:name :varchar
                                     :language :varchar
                                     :version :int
                                     :votes :int
                                     :primary-key [:name :language :version]}))

  (create-table :libraries_wide_composite_empty_value
                (with {:compact-storage true})
                (column-definitions {:name :varchar
                                     :language :varchar
                                     :votes :int
                                     :primary-key [:name :language :votes]}))

  (f))
