;; Copyright © 2025 Casey Link <casey@outskirtslabs.com>
;; SPDX-License-Identifier: MIT
(ns datahike-sqlite.core
  (:require
   [clojure.spec.alpha :as s]
   [datahike-sqlite.konserve :as k]
   [datahike-sqlite.store-config :as store-config]
   [datahike.config :refer [map-from-env]]
   [datahike.store :refer [config-spec connect-store default-config
                           delete-store empty-store release-store
                           store-identity]]))

(defmethod store-identity :sqlite [store-config]
  (store-config/store-id store-config))

(defmethod empty-store :sqlite [store-config]
  (k/connect-store store-config))

(defmethod delete-store :sqlite [store-config]
  (k/delete-store store-config))

(defmethod connect-store :sqlite [store-config]
  (k/connect-store store-config))

(defmethod default-config :sqlite [config]
  (let [env-config (map-from-env :datahike-store-config {})
        passed-config config]
    (store-config/ensure-store-id
     (merge env-config passed-config))))

(s/def :datahike.store.sqlite/dbname string?)
(s/def :datahike.store.sqlite/id uuid?)
(s/def :datahike.store.sqlite/table string?)
(s/def :datahike.store.sqlite/sqlite-opts map?)

(s/def ::sqlite (s/keys :req-un [:datahike.store.sqlite/dbname]
                        :opt-un [:datahike.store.sqlite/id
                                 :datahike.store.sqlite/table
                                 :datahike.store.sqlite/sqlite-opts]))

(defmethod config-spec :sqlite [_] ::sqlite)

(defmethod release-store :sqlite [_ store]
  (k/release store {:sync? true}))
