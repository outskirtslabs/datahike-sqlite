;; Copyright © 2026 Casey Link <casey@outskirtslabs.com>
;; SPDX-License-Identifier: MIT
(ns datahike-sqlite.kv-benchmark
  "SQLite benchmark entry point aligned with the konserve-lmdb benchmark shape.

   Headline overlap rows:
   - SQLite Konserve API
   - Datalevin raw KV API

   Additional local reference rows:
   - Konserve file
   - Konserve JDBC over SQLite

   Run with:
     clojure -M:dev:bench -m datahike-sqlite.kv-benchmark
     clojure -M:dev:bench -m datahike-sqlite.kv-benchmark 1000
     clojure -M:dev:bench -m datahike-sqlite.kv-benchmark '{:mode :smoke}'
     clojure -M:dev:bench -m datahike-sqlite.kv-benchmark '{:n 1000 :output \"bench/results/sqlite-kv.txt\"}'"
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [criterium.core :as crit]
            [datahike-sqlite.konserve :as sqlite]
            [datalevin.core :as d]
            [datalevin.binding.cpp]
            [konserve-jdbc.core]
            [konserve.core :as k])
  (:import [java.io File]
           [java.util UUID]))

(set! *warn-on-reflection* true)

(def default-entry-count 1000)
(def smoke-entry-count 25)
(def warmup-entry-count 50)
(def base-path "bench/tmp/sqlite-kv")

(defn delete-path [^File path]
  (when (.exists path)
    (doseq [^File child (reverse (file-seq path))]
      (.delete child))))

(defn delete-sqlite-files! [path]
  (doseq [suffix ["" "-shm" "-wal"]]
    (delete-path (io/file (str path suffix)))))

(defn ensure-parent-dir! [path]
  (when-let [parent (.getParentFile (io/file path))]
    (.mkdirs parent)))

(defn fresh-dir [path]
  (let [dir (io/file path)]
    (delete-path dir)
    (.mkdirs dir)
    path))

(defn fresh-file [path]
  (let [file (io/file path)]
    (when-let [parent (.getParentFile file)]
      (.mkdirs parent))
    (delete-sqlite-files! path)
    path))

(defn fresh-store-path [path]
  (ensure-parent-dir! path)
  (delete-path (io/file path))
  path)

(defn gen-value
  "Generate the same benchmark value structure as the LMDB reference benchmark."
  [i]
  {:id (UUID/randomUUID)
   :ts (System/currentTimeMillis)
   :n (long i)
   :tag :benchmark})

(defn gen-test-data [n]
  (into {} (map (fn [i] [(keyword (str "k" i)) (gen-value i)]) (range n))))

(defn bench-and-report [label f]
  (println (str "  Benchmarking " label "..."))
  (let [results (crit/quick-benchmark* f {})
        mean-ns (* 1e9 (first (:mean results)))
        mean-us (/ mean-ns 1000.0)]
    {:label label
     :mean-us mean-us}))

(defn ops-per-sec [entry-count mean-us]
  (/ entry-count (/ mean-us 1e6)))

(defn format-ops [ops]
  (cond
    (>= ops 1e6) (format "%.2fM" (/ ops 1e6))
    (>= ops 1e3) (format "%.1fK" (/ ops 1e3))
    :else (format "%.0f" ops)))

(defn kv-row [layer operation entry-count mean-us]
  {:benchmark :sqlite-kv
   :layer layer
   :operation operation
   :entries entry-count
   :mean-us mean-us
   :ops-per-sec (ops-per-sec entry-count mean-us)
   :comparability :headline})

(defn sqlite-db-spec [path]
  {:dbname path
   :dbtype "sqlite"
   :sqlite-opts {:pragma sqlite/default-sqlite-pragmas}})

(defn jdbc-store-config [path]
  {:backend :jdbc
   :dbtype "sqlite"
   :dbname path
   :properties {:busy_timeout 5000
                :cache_size 15625
                :foreign_keys "OFF"
                :journal_mode "WAL"
                :page_size 4096
                :synchronous "NORMAL"
                :temp_store "MEMORY"}
   :id (UUID/randomUUID)})

(defn file-store-config [path]
  {:backend :file
   :path path
   :id (UUID/randomUUID)})

(defn bench-konserve-put [layer store cleanup! kvs]
  (try
    (let [{:keys [mean-us]} (bench-and-report (str (name layer) " put")
                                              #(doseq [[k v] kvs]
                                                 (k/assoc store k v {:sync? true})))]
      (kv-row layer :single-put (count kvs) mean-us))
    (finally
      (cleanup!))))

(defn bench-konserve-get [layer store cleanup! kvs]
  (try
    (doseq [[k v] kvs]
      (k/assoc store k v {:sync? true}))
    (let [ks (keys kvs)
          {:keys [mean-us]} (bench-and-report (str (name layer) " get")
                                              #(doseq [key ks]
                                                 (k/get store key nil {:sync? true})))]
      (kv-row layer :single-get (count kvs) mean-us))
    (finally
      (cleanup!))))

(defn bench-konserve-batch [layer store cleanup! kvs]
  (try
    (let [{:keys [mean-us]} (bench-and-report (str (name layer) " multi-assoc")
                                              #(k/multi-assoc store kvs {:sync? true}))]
      (kv-row layer :batch-put (count kvs) mean-us))
    (finally
      (cleanup!))))

(defn bench-konserve-multi-get [layer store cleanup! kvs]
  (try
    (k/multi-assoc store kvs {:sync? true})
    (let [ks (vec (keys kvs))
          {:keys [mean-us]} (bench-and-report (str (name layer) " multi-get")
                                              #(k/multi-get store ks {:sync? true}))]
      (kv-row layer :multi-get (count kvs) mean-us))
    (finally
      (cleanup!))))

(defn bench-konserve-multi-dissoc [layer store cleanup! kvs]
  (try
    (let [ks (vec (keys kvs))
          {:keys [mean-us]}
          (bench-and-report
           (str (name layer) " multi-dissoc")
           #(do
              (k/multi-assoc store kvs {:sync? true})
              (k/multi-dissoc store ks {:sync? true})))]
      (kv-row layer :multi-dissoc (count kvs) mean-us))
    (finally
      (cleanup!))))

(defn open-sqlite-konserve-store [path]
  (let [store (sqlite/connect-store (sqlite-db-spec path) :opts {:sync? true})]
    {:store store
     :cleanup! #(do
                  (sqlite/release store {:sync? true})
                  (delete-sqlite-files! path))}))

(defn open-file-konserve-store [path]
  (let [config (file-store-config path)
        store (k/create-store config {:sync? true})]
    {:store store
     :cleanup! #(do
                  (k/release-store config store {:sync? true})
                  (try
                    (k/delete-store config {:sync? true})
                    (catch Exception _))
                  (delete-path (io/file path)))}))

(defn open-jdbc-konserve-store [path]
  (let [config (jdbc-store-config path)
        store (k/create-store config {:sync? true})]
    {:store store
     :cleanup! #(do
                  (k/release-store config store {:sync? true})
                  (try
                    (k/delete-store config {:sync? true})
                    (catch Exception _))
                  (delete-sqlite-files! path))}))

(defn bench-sqlite-konserve-put [path kvs]
  (let [{:keys [store cleanup!]} (open-sqlite-konserve-store path)]
    (bench-konserve-put :sqlite-konserve store cleanup! kvs)))

(defn bench-sqlite-konserve-get [path kvs]
  (let [{:keys [store cleanup!]} (open-sqlite-konserve-store path)]
    (bench-konserve-get :sqlite-konserve store cleanup! kvs)))

(defn bench-sqlite-konserve-batch [path kvs]
  (let [{:keys [store cleanup!]} (open-sqlite-konserve-store path)]
    (bench-konserve-batch :sqlite-konserve store cleanup! kvs)))

(defn bench-sqlite-konserve-multi-get [path kvs]
  (let [{:keys [store cleanup!]} (open-sqlite-konserve-store path)]
    (bench-konserve-multi-get :sqlite-konserve store cleanup! kvs)))

(defn bench-sqlite-konserve-multi-dissoc [path kvs]
  (let [{:keys [store cleanup!]} (open-sqlite-konserve-store path)]
    (bench-konserve-multi-dissoc :sqlite-konserve store cleanup! kvs)))

(defn bench-file-konserve-put [path kvs]
  (let [{:keys [store cleanup!]} (open-file-konserve-store path)]
    (bench-konserve-put :konserve-file store cleanup! kvs)))

(defn bench-file-konserve-get [path kvs]
  (let [{:keys [store cleanup!]} (open-file-konserve-store path)]
    (bench-konserve-get :konserve-file store cleanup! kvs)))

(defn bench-jdbc-konserve-put [path kvs]
  (let [{:keys [store cleanup!]} (open-jdbc-konserve-store path)]
    (bench-konserve-put :konserve-jdbc-sqlite store cleanup! kvs)))

(defn bench-jdbc-konserve-get [path kvs]
  (let [{:keys [store cleanup!]} (open-jdbc-konserve-store path)]
    (bench-konserve-get :konserve-jdbc-sqlite store cleanup! kvs)))

(defn bench-jdbc-konserve-batch [path kvs]
  (let [{:keys [store cleanup!]} (open-jdbc-konserve-store path)]
    (bench-konserve-batch :konserve-jdbc-sqlite store cleanup! kvs)))

(defn bench-jdbc-konserve-multi-get [path kvs]
  (let [{:keys [store cleanup!]} (open-jdbc-konserve-store path)]
    (bench-konserve-multi-get :konserve-jdbc-sqlite store cleanup! kvs)))

(defn bench-jdbc-konserve-multi-dissoc [path kvs]
  (let [{:keys [store cleanup!]} (open-jdbc-konserve-store path)]
    (bench-konserve-multi-dissoc :konserve-jdbc-sqlite store cleanup! kvs)))

(defn bench-datalevin-put [path kvs]
  (let [kv (d/open-kv path {:mapsize 512})]
    (try
      (d/open-dbi kv "bench")
      (let [{:keys [mean-us]} (bench-and-report "datalevin put"
                                                #(doseq [[k v] kvs]
                                                   (d/transact-kv kv [[:put "bench" (name k) v :string :data]])))]
        (kv-row :datalevin :single-put (count kvs) mean-us))
      (finally
        (d/close-kv kv)
        (delete-path (io/file path))))))

(defn bench-datalevin-get [path kvs]
  (let [kv (d/open-kv path {:mapsize 512})]
    (try
      (d/open-dbi kv "bench")
      (doseq [[k v] kvs]
        (d/transact-kv kv [[:put "bench" (name k) v :string :data]]))
      (let [ks (keys kvs)
            {:keys [mean-us]} (bench-and-report "datalevin get"
                                                #(doseq [key ks]
                                                   (d/get-value kv "bench" (name key) :string :data)))]
        (kv-row :datalevin :single-get (count kvs) mean-us))
      (finally
        (d/close-kv kv)
        (delete-path (io/file path))))))

(defn bench-datalevin-batch [path kvs]
  (let [kv (d/open-kv path {:mapsize 512})]
    (try
      (d/open-dbi kv "bench")
      (let [txs (mapv (fn [[k v]] [:put "bench" (name k) v :string :data]) kvs)
            {:keys [mean-us]} (bench-and-report "datalevin batch"
                                                #(d/transact-kv kv txs))]
        (kv-row :datalevin :batch-put (count kvs) mean-us))
      (finally
        (d/close-kv kv)
        (delete-path (io/file path))))))

(defn print-summary-table [rows]
  (println "| Operation   | Layer                 | Mean (us) | Ops/sec |")
  (println "|-------------|-----------------------|-----------|---------|")
  (doseq [{:keys [operation layer mean-us ops-per-sec]} rows]
    (println (format "| %-11s | %-21s | %9.2f | %7s |"
                     (name operation)
                     (name layer)
                     mean-us
                     (format-ops ops-per-sec)))))

(defn warmup! []
  (println "Warming up SQLite KV benchmark family...")
  (let [warmup-data (gen-test-data warmup-entry-count)]
    (bench-sqlite-konserve-put (fresh-file (str base-path "/warmup-konserve.sqlite")) warmup-data)
    (bench-file-konserve-put (fresh-store-path (str base-path "/warmup-filestore")) warmup-data)
    (bench-jdbc-konserve-put (fresh-file (str base-path "/warmup-jdbc.sqlite")) warmup-data)
    (bench-datalevin-put (fresh-dir (str base-path "/warmup-datalevin")) warmup-data))
  (println "Warmup complete.")
  (println))

(defn run-benchmark
  [& {:keys [n mode]
      :or {n default-entry-count
           mode :full}}]
  (let [entry-count (if (= mode :smoke) smoke-entry-count n)
        kvs (gen-test-data entry-count)]
    (println)
    (println "╔════════════════════════════════════════════════════════════╗")
    (println "║                 SQLite KV Benchmark Suite                 ║")
    (println "╚════════════════════════════════════════════════════════════╝")
    (println)
    (println (format "Entries: %d" entry-count))
    (println (format "Mode: %s" (name mode)))
    (println "Headline overlap rows: SQLite Konserve API and Datalevin raw KV API")
    (println "Additional local reference rows: Konserve file and Konserve JDBC over SQLite")
    (println "Note: Konserve file does not expose multi-key operations, so no file multi-key rows are recorded")
    (println "Note: multi-dissoc rows benchmark re-populate the store inside each timed iteration")
    (println)
    (warmup!)
    (println "Running benchmarks...")
    (println)
    (let [rows [(bench-sqlite-konserve-put (fresh-file (str base-path "/konserve-put.sqlite")) kvs)
                (bench-file-konserve-put (fresh-store-path (str base-path "/file-put")) kvs)
                (bench-jdbc-konserve-put (fresh-file (str base-path "/jdbc-put.sqlite")) kvs)
                (bench-datalevin-put (fresh-dir (str base-path "/datalevin-put")) kvs)
                (bench-sqlite-konserve-get (fresh-file (str base-path "/konserve-get.sqlite")) kvs)
                (bench-file-konserve-get (fresh-store-path (str base-path "/file-get")) kvs)
                (bench-jdbc-konserve-get (fresh-file (str base-path "/jdbc-get.sqlite")) kvs)
                (bench-datalevin-get (fresh-dir (str base-path "/datalevin-get")) kvs)
                (bench-sqlite-konserve-batch (fresh-file (str base-path "/konserve-batch.sqlite")) kvs)
                (bench-jdbc-konserve-batch (fresh-file (str base-path "/jdbc-batch.sqlite")) kvs)
                (bench-sqlite-konserve-multi-get (fresh-file (str base-path "/konserve-multi-get.sqlite")) kvs)
                (bench-jdbc-konserve-multi-get (fresh-file (str base-path "/jdbc-multi-get.sqlite")) kvs)
                (bench-sqlite-konserve-multi-dissoc (fresh-file (str base-path "/konserve-multi-dissoc.sqlite")) kvs)
                (bench-jdbc-konserve-multi-dissoc (fresh-file (str base-path "/jdbc-multi-dissoc.sqlite")) kvs)
                (bench-datalevin-batch (fresh-dir (str base-path "/datalevin-batch")) kvs)]
          results {:benchmark :sqlite-kv
                   :mode mode
                   :entries entry-count
                   :rows rows}]
      (println)
      (println "Comparable results")
      (println)
      (print-summary-table rows)
      (println)
      (println "Result rows (EDN)")
      (doseq [row rows]
        (prn row))
      results)))

(defn parse-args [args]
  (cond
    (empty? args) {}
    (= 1 (count args))
    (let [arg (first args)]
      (if (re-matches #"\d+" arg)
        {:n (parse-long arg)}
        (edn/read-string arg)))
    :else
    (edn/read-string (apply str args))))

(defn write-output! [path text]
  (when path
    (let [file (io/file path)]
      (when-let [parent (.getParentFile file)]
        (.mkdirs parent))
      (spit file text))))

(defn -main [& args]
  (let [{:keys [output] :as opts} (parse-args args)
        report (with-out-str
                 (apply run-benchmark (mapcat identity (dissoc opts :output))))]
    (print report)
    (write-output! output report)
    (shutdown-agents)))
