;; Copyright © 2025 Casey Link <casey@outskirtslabs.com>
;; SPDX-License-Identifier: MIT
(ns datahike-sqlite.store-config
  (:import
   [java.util UUID]))

(def ^:const default-table "konserve")

(defn derive-store-id
  [{:keys [dbname table]}]
  (when dbname
    (UUID/nameUUIDFromBytes
     (.getBytes (str "sqlite:" dbname ":" (or table default-table)) "UTF-8"))))

(defn store-id
  [store-config]
  (or (:id store-config)
      (derive-store-id store-config)))

(defn ensure-store-id
  [store-config]
  (if-let [id (store-id store-config)]
    (assoc store-config :id id)
    store-config))
