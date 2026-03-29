;; Copyright © 2025 Casey Link <casey@outskirtslabs.com>
;; SPDX-License-Identifier: MIT
(ns datahike-sqlite.core
  (:require
   [datahike-sqlite.konserve :as sqlite]
   [konserve.store :as ks]
   [konserve.utils :refer [async+sync *default-sync-translation*]]
   [superv.async :refer [<?- go-try-]]))

(defmethod ks/-create-store :sqlite
  [config opts]
  (sqlite/connect-store config :opts opts))

(defmethod ks/-connect-store :sqlite
  [config opts]
  (async+sync (:sync? opts) *default-sync-translation*
              (go-try-
               (when-not (<?- (sqlite/store-exists? config :opts opts))
                 (throw (ex-info (str "SQLite store does not exist at dbname: " (:dbname config))
                                 {:config config})))
               (<?- (sqlite/connect-store config :opts opts)))))

(defmethod ks/-store-exists? :sqlite
  [config opts]
  (sqlite/store-exists? config :opts opts))

(defmethod ks/-delete-store :sqlite
  [config opts]
  (sqlite/delete-store config :opts opts))

(defmethod ks/-release-store :sqlite
  [_config store opts]
  (sqlite/release store opts))
