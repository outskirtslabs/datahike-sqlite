;; Copyright © 2025 Casey Link <casey@outskirtslabs.com>
;; SPDX-License-Identifier: MIT
(ns datahike-sqlite.konserve
  (:require
   [datahike-sqlite.store-config :as store-config]
   [clojure.string :as str]
   [konserve.compressor :refer [null-compressor]]
   [konserve.encryptor :refer [null-encryptor]]
   [konserve.impl.defaults :refer [connect-default-store]]
   [konserve.impl.storage-layout :refer [PBackingBlob PBackingLock PBackingStore PMultiReadBackingStore PMultiWriteBackingStore -delete-store]]
   [konserve.utils :refer [*default-sync-translation* async+sync]]
   [sqlite4clj.core :as d]
   [superv.async :refer [go-try-]])
  (:import
   [java.io ByteArrayInputStream]))

(set! *warn-on-reflection* 1)

(def ^:const default-table "konserve")

(def ^:dynamic *batch-insert-strategy*
  "DO NOT USE! For testing/dev only!
  Strategy for batch inserts: :sequential, :multi-row
   :sequential - Execute individual INSERT statements for each key-value pair
   :multi-row - Use multi-row INSERT VALUES syntax
  Preliminary testing shows that :sequential is always faster than :multi-row"
  :sequential)

(defn with-write-tx
  "Wrapper around the with-write-tx macro, for use in situations where we cannot use macros directly."
  [db f]
  (d/with-write-tx [tx (:writer db)]
    (f tx)))

(defn init-db [db-spec]
  (let [dbname (:dbname db-spec)
        ;; Pass through any sqlite4clj options the user provided
        sqlite-opts (or (:sqlite-opts db-spec)
                        {:pool-size 4})
        db (d/init-db! dbname sqlite-opts)]
    db))

(defn create-statement [table]
  [(str "CREATE TABLE IF NOT EXISTS " table " (id varchar(100) primary key, header bytea, meta bytea, val bytea)")])

(defn upsert-statement [table id header meta value]
  [(str "INSERT INTO " table " (id, header, meta, val) VALUES (?, ?, ?, ?) "
        "ON CONFLICT (id) DO UPDATE "
        "SET header = excluded.header, meta = excluded.meta, val = excluded.val;")
   id header meta value])

(defn select-exists-statement [table id]
  [(str "SELECT 1 FROM " table " WHERE id = ?") id])

(defn delete-row-statement [table store-key]
  [(str "DELETE FROM " table " WHERE id = ?") store-key])

(defn select-row-statement [table id]
  [(str "SELECT id, header, meta, val FROM " table " WHERE id = ?") id])

(defn select-rows-statement [table ids]
  (let [placeholders (str/join ", " (repeat (count ids) "?"))]
    (into [(str "SELECT id, header, meta, val FROM " table " WHERE id IN (" placeholders ")")]
          ids)))

(defn select-ids-statement [table ids]
  (let [placeholders (str/join ", " (repeat (count ids) "?"))]
    (into [(str "SELECT id FROM " table " WHERE id IN (" placeholders ")")]
          ids)))

(defn select-table-exists-statement [table]
  [(str "SELECT 1 FROM " table " LIMIT 1")])

(defn select-all-ids-statement [table]
  [(str "SELECT id FROM " table)])

(defn delete-rows-statement [table store-keys]
  (let [placeholders (str/join ", " (repeat (count store-keys) "?"))]
    (into [(str "DELETE FROM " table " WHERE id IN (" placeholders ")")]
          store-keys)))

(defn update-id-statement [table from to]
  [(str "UPDATE " table " SET id = ? WHERE id = ?") from to])

(defn multi-insert-statement [table batch-size]
  (let [values-clause (str/join ", " (repeat batch-size "(?, ?, ?, ?)"))]
    (str "INSERT INTO " table
         " (id, header, meta, val) VALUES "
         values-clause
         " ON CONFLICT (id) DO UPDATE "
         "SET header = excluded.header, "
         "meta = excluded.meta, "
         "val = excluded.val;")))

(defn copy-row-statement [table to from]
  [(str "INSERT INTO " table " (id, header, meta, val) "
        "SELECT ?, header, meta, val FROM " table " WHERE id = ? "
        "ON CONFLICT (id) DO UPDATE "
        "SET header = excluded.header, meta = excluded.meta, val = excluded.val") to from])

(defn delete-store-statement [table]
  [(str "DROP TABLE IF EXISTS " table)])

(defn change-row-id [db table from to]
  (with-write-tx db
    (fn [tx]
      (d/q tx
           (update-id-statement table to from)))))

(defn read-all [db table id]
  (let [res (d/q (:reader db)
                 (select-row-statement table id))]
    (when res
      (let [[_ header meta val] (first res)]
        {:header header
         :meta meta
         :value val}))))

(extend-protocol PBackingLock
  Boolean
  (-release [_ env]
    (if (:sync? env) nil (go-try- nil))))

(defrecord SQLiteRow [db key data cache]
  PBackingBlob
  (-sync [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (let [{:keys [header meta value]} @data]
                           (if (and header meta value)
                             (let [ps (upsert-statement (:table db) key header meta value)]
                               (d/q (:writer (:db db)) ps))
                             (throw (ex-info "Updating a row is only possible if header, meta and value are set." {:data @data})))
                           (reset! data {})))))
  (-close [_ env]
    (if (:sync? env) nil (go-try- nil)))
  (-get-lock [_ env]
    ;; Must not return nil, otherwise it retries forever
    (if (:sync? env) true (go-try- true)))
  (-read-header [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (when-not @cache
                   (reset! cache (read-all (:db db) (:table db) key)))
                 (-> @cache :header))))
  (-read-meta [_ _meta-size env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (-> @cache :meta))))
  (-read-value [_ _meta-size env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (-> @cache :value))))
  (-read-binary [_ _meta-size locked-cb env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (locked-cb {:input-stream (when (-> @cache :value) (ByteArrayInputStream. (-> @cache :value)))
                                     :size nil}))))
  (-write-header [_ header env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :header header))))
  (-write-meta [_ meta env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :meta meta))))
  (-write-value [_ value _meta-size env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :value value))))
  (-write-binary [_ _meta-size blob env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :value blob)))))

(defn- batch-insert-sequential
  "Execute individual INSERT statements for each key-value pair.
  Returns a map of store-key -> success/failure status."
  [tx table store-key-values]
  (let [exec-update (fn [[store-key data]]
                      (let [{:keys [header meta value]} data
                            ps (upsert-statement table store-key header meta value)
                            exec-result (d/q tx ps)]
                        [store-key (some? exec-result)]))]
    (into {} (map exec-update store-key-values))))

(defn- batch-insert-multi-row
  "Execute a single INSERT statement with multiple VALUES clauses."
  [tx table store-key-values]
  (let [entries (vec store-key-values)
        batch-size (count entries)
        ;; Safety limit to avoid hitting SQLITE_MAX_VARIABLE_NUMBER
        max-batch 1000
        process-batch (fn [batch]
                        (if (empty? batch)
                          {}
                          (let [sql (multi-insert-statement table (count batch))
                                params (mapcat (fn [[store-key data]]
                                                 (let [{:keys [header meta value]} data]
                                                   [store-key
                                                    header
                                                    meta
                                                    value]))
                                               batch)
                                query (into [sql] params)]
                            (d/q tx query)
                            (into {} (map (fn [[k _]] [k true]) batch)))))]

    (if (<= batch-size max-batch)
      (process-batch entries)
      (apply merge (map process-batch (partition-all max-batch entries))))))

(defrecord SQLiteTable [db-spec db table]
  PBackingStore
  (-create-blob [this store-key env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (SQLiteRow. this store-key (atom {}) (atom nil)))))
  (-delete-blob [_ store-key env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (d/q (:writer db)
                              (delete-row-statement table store-key)))))
  (-blob-exists? [_ store-key env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (let [res (d/q (:reader db)
                                        (select-exists-statement table store-key))]
                           (not (nil? res))))))
  (-copy [_ from to env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (d/q (:writer db) (copy-row-statement table to from)))))
  (-atomic-move [_ from to env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (change-row-id db table from to))))
  (-migratable [_ _key _store-key env]
    (if (:sync? env) nil (go-try- nil)))
  (-migrate [_ _migration-key _key-vec _serializer _read-handlers _write-handlers env]
    (if (:sync? env) nil (go-try- nil)))
  (-create-store [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (d/q (:writer db) (create-statement table)))))
  (-store-exists? [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (try
                   (let [res (d/q (:reader db) (select-table-exists-statement table))]
                     (not (nil? res)))
                   (catch Exception _e
                     false)))))
  (-sync-store [_ env]
    (if (:sync? env) nil (go-try- nil)))
  (-delete-store [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (d/q (:writer db) (delete-store-statement table)))))
  (-handle-foreign-key [_ _migration-key _serializer _read-handlers _write-handlers env]
    (if (:sync? env) nil (go-try- nil)))
  (-keys [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (d/q (:reader db)
                              (select-all-ids-statement table)))))

  PMultiWriteBackingStore
  (-multi-write-blobs [_ store-key-values env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (with-write-tx db
                   (fn [tx]
                     (case *batch-insert-strategy*
                       :sequential (batch-insert-sequential tx table store-key-values)
                       :multi-row (batch-insert-multi-row tx table store-key-values)))))))
  (-multi-delete-blobs [_ store-keys env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (if (empty? store-keys)
                   {}
                   (let [existing-ids (set (d/q (:reader db) (select-ids-statement table store-keys)))]
                     (with-write-tx db
                       (fn [tx]
                         (d/q tx (delete-rows-statement table store-keys))
                         (into {}
                               (map (fn [store-key]
                                      [store-key (contains? existing-ids store-key)]))
                               store-keys))))))))

  PMultiReadBackingStore
  (-multi-read-blobs [this store-keys env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (if (empty? store-keys)
                   {}
                   (let [rows (d/q (:reader db) (select-rows-statement table store-keys))]
                     (into {}
                           (map (fn [[store-key header meta value]]
                                  [store-key
                                   (SQLiteRow. this store-key (atom {}) (atom {:header header
                                                                               :meta meta
                                                                               :value value}))]))
                           rows)))))))

(defn prepare-spec [db-spec opts-table]
  (-> db-spec
      (assoc :dbtype "sqlite")
      (assoc :table (or opts-table (:table db-spec) default-table))
      (store-config/ensure-store-id)))

(defn connect-store [db-spec & {:keys [opts]
                                :as params}]
  (let [complete-opts (merge {:sync? true} opts)
        db-spec (-> db-spec (prepare-spec (:table params)) (assoc :sync? (:sync? complete-opts)))
        db (init-db db-spec)
        _ (assert (:table db-spec))
        backing (SQLiteTable. db-spec db (:table db-spec))
        config (merge {:opts complete-opts
                       :config {:sync-blob? true
                                :in-place? true
                                :no-backup? true
                                :lock-blob? true}
                       :default-serializer :FressianSerializer
                       :compressor null-compressor
                       :encryptor null-encryptor
                       :buffer-size (* 1024 1024)}
                      (dissoc params :opts :config))]
    (connect-default-store backing config)))

(defn release
  "Closes the SQLite database connection pools."
  [store env]
  (async+sync (:sync? env) *default-sync-translation*
              (go-try-
               ((get-in store [:backing :db :reader :close]))
               ((get-in store [:backing :db :writer :close]))
               nil)))

(defn delete-store [db-spec & {:keys [table opts]}]
  (let [complete-opts (merge {:sync? true} opts)
        db-spec (prepare-spec db-spec table)
        _ (assert (:table db-spec))
        db (init-db db-spec)]
    (-delete-store (SQLiteTable. db-spec db (:table db-spec)) complete-opts)))

(comment

  (require '[konserve.core :as k])

  (def db-spec {:dbname "test-data/dhsqlite.sqlite" :dbtype "sqlite"})
  (def store (connect-store db-spec :opts {:sync? true}))
  (def store2 (connect-store db-spec :opts {:sync? true}))

  (time (k/assoc-in store ["foo"] {:foo "baz"} {:sync? true}))
  (time (k/assoc-in store ["foo2"] {:foo2 "baz"} {:sync? true}))
  (time (k/assoc-in store2 ["foo"] {:foo "baz"} {:sync? true}))
  (k/get-in store ["foo"] nil {:sync? true})
  (k/exists? store "foo" {:sync? true})

  (k/get store :db nil {:sync? true})

  (k/exists? store2 "foo2" {:sync? true})
  (release store {:sync? true})

  ;;
  )
