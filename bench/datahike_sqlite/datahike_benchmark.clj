;; Copyright © 2026 Casey Link <casey@outskirtslabs.com>
;; SPDX-License-Identifier: MIT
(ns datahike-sqlite.datahike-benchmark
  "SQLite Datahike benchmark entry point aligned with the datahike-lmdb benchmark.

   Comparable rows:
   - SQLite-backed Datahike
   - Datalevin

   Run with:
     clojure -M:dev:bench -m datahike-sqlite.datahike-benchmark
     clojure -M:dev:bench -m datahike-sqlite.datahike-benchmark '{:mode :smoke}'
     clojure -M:dev:bench -m datahike-sqlite.datahike-benchmark '{:sizes [100000 500000] :output \"bench/results/sqlite-datahike.txt\"}'"
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [datahike-sqlite.core]
            [datahike.api :as d]
            [datalevin.core :as dl])
  (:import [java.io File]
           [java.util UUID]))

(set! *warn-on-reflection* true)

(def default-sizes [100000 500000 1000000])
(def smoke-sizes [1000])
(def warmup-size 1000)
(def base-path "bench/tmp/sqlite-datahike")

(def schema
  [{:db/ident :person/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :person/age
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :person/email
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}])

(def datalevin-schema
  {:person/name {:db/valueType :db.type/string}
   :person/age {:db/valueType :db.type/long}
   :person/email {:db/valueType :db.type/string}})

(def benchmark-query
  '[:find ?n ?a
    :where
    [?e :person/name ?n]
    [?e :person/age ?a]
    [(> ?a 40)]])

(defn generate-entities [n]
  (mapv (fn [i]
          {:person/name (str "Person-" i)
           :person/age (+ 20 (mod i 50))
           :person/email (str "person" i "@example.com")})
        (range n)))

(defn delete-path [^File path]
  (when (.exists path)
    (doseq [^File child (reverse (file-seq path))]
      (.delete child))))

(defn delete-sqlite-files! [path]
  (doseq [suffix ["" "-shm" "-wal"]]
    (delete-path (io/file (str path suffix)))))

(defn stable-id [label path]
  (UUID/nameUUIDFromBytes (.getBytes (str "benchmark:" label ":" path) "UTF-8")))

(defn ensure-parent-dir! [path]
  (when-let [parent (.getParentFile (io/file path))]
    (.mkdirs parent)))

(defn sqlite-config [path label extra-config]
  (merge {:store {:backend :sqlite
                  :dbname path
                  :id (stable-id label path)}
          :schema-flexibility :write
          :keep-history? false}
         extra-config))

(defn datahike-row
  [backend operation n elapsed & {:as extras}]
  (merge {:benchmark :sqlite-datahike
          :backend backend
          :operation operation
          :entities n
          :time-sec elapsed}
         extras))

(defn cleanup-sqlite-db! [config]
  (let [path (get-in config [:store :dbname])]
    (ensure-parent-dir! path)
    (delete-sqlite-files! path))
  (try
    (when (d/database-exists? config)
      (d/delete-database config))
    (catch Exception _))
  (delete-sqlite-files! (get-in config [:store :dbname])))

(defn bench-sqlite-write [n path]
  (let [config (sqlite-config path :write {:attribute-refs? true})
        entities (generate-entities n)]
    (ensure-parent-dir! path)
    (cleanup-sqlite-db! config)
    (try
      (d/create-database config)
      (let [conn (d/connect config)]
        (try
          (d/transact conn schema)
          (let [start (System/nanoTime)]
            (d/transact conn entities)
            (let [elapsed (/ (- (System/nanoTime) start) 1e9)]
              (datahike-row :sqlite-datahike :write n elapsed
                            :entities-per-sec (/ n elapsed))))
          (finally
            (d/release conn))))
      (finally
        (cleanup-sqlite-db! config)))))

(defn bench-sqlite-query [n path]
  (let [config (sqlite-config path :query {})
        entities (generate-entities n)]
    (ensure-parent-dir! path)
    (cleanup-sqlite-db! config)
    (try
      (d/create-database config)
      (let [conn (d/connect config)]
        (try
          (d/transact conn schema)
          (d/transact conn entities)
          (let [start (System/nanoTime)
                result (d/q benchmark-query @conn)
                elapsed (/ (- (System/nanoTime) start) 1e9)]
            (datahike-row :sqlite-datahike :query n elapsed
                          :results (count result)))
          (finally
            (d/release conn))))
      (finally
        (cleanup-sqlite-db! config)))))

(defn cleanup-dir! [path]
  (.mkdirs (io/file path))
  (delete-path (io/file path)))

(defn bench-datalevin-write [n path]
  (cleanup-dir! path)
  (let [conn (dl/get-conn path datalevin-schema)
        entities (generate-entities n)]
    (try
      (let [start (System/nanoTime)]
        (dl/transact! conn entities)
        (let [elapsed (/ (- (System/nanoTime) start) 1e9)]
          (datahike-row :datalevin :write n elapsed
                        :entities-per-sec (/ n elapsed))))
      (finally
        (dl/close conn)
        (cleanup-dir! path)))))

(defn bench-datalevin-query [n path]
  (cleanup-dir! path)
  (let [conn (dl/get-conn path datalevin-schema)
        entities (generate-entities n)]
    (try
      (dl/transact! conn entities)
      (let [db (dl/db conn)
            start (System/nanoTime)
            result (dl/q benchmark-query db)
            elapsed (/ (- (System/nanoTime) start) 1e9)]
        (datahike-row :datalevin :query n elapsed
                      :results (count result)))
      (finally
        (dl/close conn)
        (cleanup-dir! path)))))

(defn format-throughput [n elapsed]
  (format "%.0f" (/ n elapsed)))

(defn print-row [row]
  (let [{:keys [backend operation entities time-sec results entities-per-sec]} row]
    (println
     (format "| %-15s | %-6s | %8d | %10.3f | %8s | %12s |"
             (name backend)
             (name operation)
             entities
             time-sec
             (or results "-")
             (if entities-per-sec
               (format-throughput entities time-sec)
               "-")))))

(defn warmup! []
  (println "Warming up SQLite Datahike benchmark family...")
  (bench-sqlite-write warmup-size (str base-path "/warmup.sqlite"))
  (bench-datalevin-write warmup-size (str base-path "/warmup-datalevin"))
  (println "Warmup complete.")
  (println))

(defn collect-results
  [& {:keys [sizes mode]
      :or {sizes default-sizes
           mode :full}}]
  (let [sizes (if (= mode :smoke) smoke-sizes sizes)]
    (warmup!)
    {:benchmark :sqlite-datahike
     :mode mode
     :sizes sizes
     :rows (vec
            (mapcat
             (fn [n]
               [(bench-datalevin-write n (str base-path "/datalevin-write-" n))
                (bench-sqlite-write n (str base-path "/sqlite-write-" n ".sqlite"))
                (bench-datalevin-query n (str base-path "/datalevin-query-" n))
                (bench-sqlite-query n (str base-path "/sqlite-query-" n ".sqlite"))])
             sizes))}))

(defn render-results [{:keys [mode rows sizes]}]
  (println)
  (println "╔════════════════════════════════════════════════════════════╗")
  (println "║              SQLite Datahike Benchmark Suite             ║")
  (println "╚════════════════════════════════════════════════════════════╝")
  (println)
  (println (format "Sizes: %s" sizes))
  (println (format "Mode: %s" (name mode)))
  (println "Comparable rows: SQLite Datahike and Datalevin")
  (println)
  (doseq [n sizes]
    (println (format "--- %d entities ---" n)))
  (println)
  (println "| Backend         | Op     | Entities | Time (sec) | Results  | Entities/sec |")
  (println "|-----------------|--------|----------|------------|----------|--------------|")
  (doseq [row rows]
    (print-row row))
  (println)
  (println "Result rows (EDN)")
  (doseq [row rows]
    (prn row)))

(defn parse-args [args]
  (cond
    (empty? args) {}
    (= 1 (count args)) (edn/read-string (first args))
    :else (edn/read-string (apply str args))))

(defn write-output! [path text]
  (when path
    (let [file (io/file path)]
      (when-let [parent (.getParentFile file)]
        (.mkdirs parent))
      (spit file text))))

(defn -main [& args]
  (let [{:keys [output] :as opts} (parse-args args)
        results (apply collect-results (mapcat identity (dissoc opts :output)))
        report (with-out-str (render-results results))]
    (print report)
    (write-output! output report)
    (shutdown-agents)))
