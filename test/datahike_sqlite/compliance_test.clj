;; Copyright © 2025 Christian Weilbach
;; SPDX-License-Identifier: EPL-2.0
(ns datahike-sqlite.compliance-test
  (:require
   [clojure.java.io :as io]
   [clojure.test :refer [deftest use-fixtures]]
   [datahike-sqlite.konserve :as sqlite]
   [konserve.compliance-test :refer [compliance-test]])
  (:import
   [java.util UUID]))

(def ^:dynamic *test-store* nil)
(def ^:dynamic *test-db-spec* nil)

(defn with-store [f]
  (let [db-file (io/file "test-data" (str "compliance-" (UUID/randomUUID) ".sqlite"))
        db-spec {:dbname (.getPath db-file)
                 :dbtype "sqlite"
                 :id (UUID/randomUUID)}]
    (.mkdirs (.getParentFile db-file))
    (try
      (let [store (sqlite/connect-store db-spec :opts {:sync? true})]
        (binding [*test-store* store
                  *test-db-spec* db-spec]
          (f)))
      (finally
        (when *test-store*
          (sqlite/release *test-store* {:sync? true}))
        (sqlite/delete-store db-spec)
        (.delete db-file)))))

(use-fixtures :each with-store)

(deftest sqlite-compliance-test
  (compliance-test *test-store*))
