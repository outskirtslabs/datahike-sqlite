;; Copyright © 2026 Casey Link <casey@outskirtslabs.com>
;; SPDX-License-Identifier: MIT
(ns datahike-sqlite.lmdb-benchmark
  "SQLite benchmark entry point aligned with the konserve-lmdb benchmark shape.

   Comparable rows:
   - SQLite Konserve API
   - Datalevin raw KV API

   Run with:
     clojure -M:dev:bench -m datahike-sqlite.lmdb-benchmark
     clojure -M:dev:bench -m datahike-sqlite.lmdb-benchmark 1000
     clojure -M:dev:bench -m datahike-sqlite.lmdb-benchmark '{:mode :smoke}'
     clojure -M:dev:bench -m datahike-sqlite.lmdb-benchmark '{:n 1000 :output \"bench/results/sqlite-kv.txt\"}'"
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [criterium.core :as crit]
            [datahike-sqlite.konserve :as sqlite]
            [datalevin.binding.cpp]
            [datalevin.lmdb :as dl]
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
   :dbtype "sqlite"})

(defn bench-sqlite-konserve-put [path kvs]
  (let [store (sqlite/connect-store (sqlite-db-spec path) :opts {:sync? true})]
    (try
      (let [{:keys [mean-us]} (bench-and-report "sqlite konserve put"
                                                #(doseq [[k v] kvs]
                                                   (k/assoc store k v {:sync? true})))]
        (kv-row :sqlite-konserve :single-put (count kvs) mean-us))
      (finally
        (sqlite/release store {:sync? true})
        (delete-sqlite-files! path)))))

(defn bench-sqlite-konserve-get [path kvs]
  (let [store (sqlite/connect-store (sqlite-db-spec path) :opts {:sync? true})]
    (try
      (doseq [[k v] kvs]
        (k/assoc store k v {:sync? true}))
      (let [ks (keys kvs)
            {:keys [mean-us]} (bench-and-report "sqlite konserve get"
                                                #(doseq [key ks]
                                                   (k/get store key nil {:sync? true})))]
        (kv-row :sqlite-konserve :single-get (count kvs) mean-us))
      (finally
        (sqlite/release store {:sync? true})
        (delete-sqlite-files! path)))))

(defn bench-sqlite-konserve-batch [path kvs]
  (let [store (sqlite/connect-store (sqlite-db-spec path) :opts {:sync? true})]
    (try
      (let [{:keys [mean-us]} (bench-and-report "sqlite konserve multi-assoc"
                                                #(k/multi-assoc store kvs {:sync? true}))]
        (kv-row :sqlite-konserve :batch-put (count kvs) mean-us))
      (finally
        (sqlite/release store {:sync? true})
        (delete-sqlite-files! path)))))

(defn bench-datalevin-put [path kvs]
  (let [kv (dl/open-kv path {:mapsize 512})]
    (try
      (dl/open-dbi kv "bench")
      (let [{:keys [mean-us]} (bench-and-report "datalevin put"
                                                #(doseq [[k v] kvs]
                                                   (dl/transact-kv kv [[:put "bench" (name k) v :string :nippy]])))]
        (kv-row :datalevin :single-put (count kvs) mean-us))
      (finally
        (dl/close-kv kv)
        (delete-path (io/file path))))))

(defn bench-datalevin-get [path kvs]
  (let [kv (dl/open-kv path {:mapsize 512})]
    (try
      (dl/open-dbi kv "bench")
      (doseq [[k v] kvs]
        (dl/transact-kv kv [[:put "bench" (name k) v :string :nippy]]))
      (let [ks (keys kvs)
            {:keys [mean-us]} (bench-and-report "datalevin get"
                                                #(doseq [key ks]
                                                   (dl/get-value kv "bench" (name key) :string :nippy)))]
        (kv-row :datalevin :single-get (count kvs) mean-us))
      (finally
        (dl/close-kv kv)
        (delete-path (io/file path))))))

(defn bench-datalevin-batch [path kvs]
  (let [kv (dl/open-kv path {:mapsize 512})]
    (try
      (dl/open-dbi kv "bench")
      (let [txs (mapv (fn [[k v]] [:put "bench" (name k) v :string :nippy]) kvs)
            {:keys [mean-us]} (bench-and-report "datalevin batch"
                                                #(dl/transact-kv kv txs))]
        (kv-row :datalevin :batch-put (count kvs) mean-us))
      (finally
        (dl/close-kv kv)
        (delete-path (io/file path))))))

(defn print-summary-table [rows]
  (println "| Operation   | Layer            | Mean (us) | Ops/sec |")
  (println "|-------------|------------------|-----------|---------|")
  (doseq [{:keys [operation layer mean-us ops-per-sec]} rows]
    (println (format "| %-11s | %-16s | %9.2f | %7s |"
                     (name operation)
                     (name layer)
                     mean-us
                     (format-ops ops-per-sec)))))

(defn warmup! []
  (println "Warming up SQLite KV benchmark family...")
  (let [warmup-data (gen-test-data warmup-entry-count)]
    (bench-sqlite-konserve-put (fresh-file (str base-path "/warmup-konserve.sqlite")) warmup-data)
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
    (println "Comparable rows: SQLite Konserve API and Datalevin raw KV API")
    (println "SQLite-only diagnostics: not recorded in this run")
    (println)
    (warmup!)
    (println "Running benchmarks...")
    (println)
    (let [rows [(bench-sqlite-konserve-put (fresh-file (str base-path "/konserve-put.sqlite")) kvs)
                (bench-sqlite-konserve-get (fresh-file (str base-path "/konserve-get.sqlite")) kvs)
                (bench-sqlite-konserve-batch (fresh-file (str base-path "/konserve-batch.sqlite")) kvs)
                (bench-datalevin-put (fresh-dir (str base-path "/datalevin-put")) kvs)
                (bench-datalevin-get (fresh-dir (str base-path "/datalevin-get")) kvs)
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
