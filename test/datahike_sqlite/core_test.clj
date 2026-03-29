;; Copyright © 2025 Casey Link <casey@outskirtslabs.com>
;; SPDX-License-Identifier: MIT
(ns datahike-sqlite.core-test
  (:require
   [clojure.java.io :as io]
   [clojure.test :as t :refer [deftest is testing use-fixtures]]
   [datahike-sqlite.core]
   [datahike-sqlite.konserve :as dsk]
   [datahike.api :as d]
   [datahike.store :as ds]
   [konserve.core :as k]
   [sqlite4clj.core :as sq]))

(defn test-data-fixture [f]
  (let [test-dir (io/file "test-data")]
    (when (.exists test-dir)
      (doseq [file (reverse (file-seq test-dir))]
        (.delete file)))
    (.mkdirs test-dir)
    (f)))

(use-fixtures :each test-data-fixture)

(deftest ^:integration test-basic-operation
  (let [config {:store {:backend :sqlite
                        :dbname "test-data/config-test.sqlite"}
                :schema-flexibility :read
                :keep-history? false}
        _ (d/delete-database config)]
    (is (not (d/database-exists? config)))
    (let [_ (d/create-database config)
          conn (d/connect config)]

      (d/transact conn [{:db/id 1, :name "Ivan", :age 15}
                        {:db/id 2, :name "Petr", :age 37}
                        {:db/id 3, :name "Ivan", :age 37}
                        {:db/id 4, :age 15}])
      (is (= (d/q '[:find ?e :where [?e :name]] @conn)
             #{[3] [2] [1]}))

      (d/release conn)
      (is (d/database-exists? config))
      (d/delete-database config)
      (is (not (d/database-exists? config))))))

(deftest ^:integration test-env
  (let [_ (d/delete-database)]
    (is (not (d/database-exists?)))
    (let [_ (d/create-database)
          conn (d/connect)]

      (d/transact conn [{:db/ident :name
                         :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one}
                        {:db/ident :age
                         :db/valueType :db.type/long
                         :db/cardinality :db.cardinality/one}])
      (d/transact conn [{:db/id 1, :name "Ivan", :age 15}
                        {:db/id 2, :name "Petr", :age 37}
                        {:db/id 3, :name "Ivan", :age 37}
                        {:db/id 4, :age 15}])
      (is (= (d/q '[:find ?e :where [?e :name]] @conn)
             #{[3] [2] [1]}))

      (d/release conn)
      (is (d/database-exists?))
      (d/delete-database)
      (is (not (d/database-exists?))))))

(defn has-table? [db table-name]
  (let [tables (sq/q (:reader db) ["SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"])]
    (println "tables" tables table-name (some #(= table-name %) tables))
    (some? (some #(= table-name %) tables))))

(defn count-rows [db table-name]
  (first (sq/q (:reader db) [(str "SELECT COUNT(*) as cnt FROM " table-name)])))

(deftest ^:integration test-custom-table
  (let [config {:store {:backend :sqlite
                        :dbname "test-data/config-test.sqlite"}
                :schema-flexibility :write
                :keep-history? false}
        config2 {:store {:backend :sqlite
                         :dbname "test-data/config-test.sqlite"
                         :table "tea_party"}
                 :schema-flexibility :write
                 :keep-history? false}
        _ (d/delete-database config)
        _ (d/delete-database config2)
        db (sq/init-db! "test-data/config-test.sqlite" {:pool-size 1})]
    (is (not (d/database-exists? config)))
    (is (not (d/database-exists? config2)))
    (let [_ (d/create-database config)
          _ (d/create-database config2)
          conn (d/connect config)
          conn2 (d/connect config2)]
      (d/transact conn [{:db/ident :name
                         :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one}
                        {:db/ident :age
                         :db/valueType :db.type/long
                         :db/cardinality :db.cardinality/one}])
      (d/transact conn [{:db/id 1, :name "Ivan", :age 15}
                        {:db/id 2, :name "Petr", :age 37}
                        {:db/id 3, :name "Ivan", :age 37}
                        {:db/id 4, :age 15}])
      (is (= (d/q '[:find ?e :where [?e :name]] @conn)
             #{[3] [2] [1]}))
      (is (empty? (d/q '[:find ?e :where [?e :name]] @conn2)))

      (testing "SQLite tables are correctly created"
        (is (has-table? db "konserve") "Default konserve table should exist")
        (is (has-table? db "tea_party") "Default tea_party table should exist")
        (is (not= (count-rows db "tea_party") (count-rows db "konserve"))))

      (d/release conn)
      (d/release conn2)
      (d/delete-database config)
      (d/delete-database config2)
      (is (not (d/database-exists? config)))
      (is (not (d/database-exists? config2))))))

(deftest test-store-identity
  (let [base-config {:backend :sqlite
                     :dbname "test-data/config-test.sqlite"}
        explicit-id #uuid "71de0715-0000-0000-0000-000000000071"]
    (is (uuid? (ds/store-identity base-config)))
    (is (= (ds/store-identity base-config)
           (ds/store-identity (assoc base-config :table "konserve"))))
    (is (not= (ds/store-identity base-config)
              (ds/store-identity (assoc base-config :table "tea_party"))))
    (is (= explicit-id
           (ds/store-identity (assoc base-config :id explicit-id))))))

(deftest ^:integration test-batch-writes
  (testing "PMultiWriteBackingStore batch operations"
    (let [db-spec {:dbname "test-data/batch-test.sqlite" :dbtype "sqlite"}
          _ (try (dsk/delete-store db-spec) (catch Exception _ nil))
          store (dsk/connect-store db-spec :opts {:sync? true})
          ;; Helper function to generate test data
          generate-test-data (fn [n size]
                               (into {}
                                     (map (fn [i]
                                            [(keyword (str "key-" i))
                                             {:id i
                                              :data (apply str (repeat size "x"))
                                              :timestamp (System/currentTimeMillis)}])
                                          (range n))))]

      (testing "Small batch writes succeed"
        (let [batch-data {:key1 {:data "value1"}
                          :key2 {:data "value2"}
                          :key3 {:data "value3"}}]
          (k/multi-assoc store batch-data {:sync? true})
          (is (= batch-data
                 (k/multi-get store [:key1 :key2 :key3] {:sync? true})))

          (is (= (k/get store :key1 nil {:sync? true}) {:data "value1"}))
          (is (= (k/get store :key2 nil {:sync? true}) {:data "value2"}))
          (is (= (k/get store :key3 nil {:sync? true}) {:data "value3"}))
          (is (= {:key1 true :missing false}
                 (k/multi-dissoc store [:key1 :missing] {:sync? true})))
          (is (= {:key2 {:data "value2"} :key3 {:data "value3"}}
                 (k/multi-get store [:key1 :key2 :key3] {:sync? true})))))

      (testing "Large batch operations with 500+ writes"
        (let [test-data (generate-test-data 500 100)]
          (k/multi-assoc store test-data {:sync? true})
          (doseq [i (range 500)]
            (let [key (keyword (str "key-" i))
                  value (k/get store key nil {:sync? true})]
              (is (= (:id value) i)
                  (str "Entry " i " should have correct id"))))))

      (dsk/release store {:sync? true})
      (dsk/delete-store db-spec))))
