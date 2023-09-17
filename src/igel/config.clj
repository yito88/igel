(ns igel.config
  (:require [clojure.java.io :refer [reader]]
            [clj-yaml.core :as yaml]))

(def ^:private ^:const DEFAULT_MEMTABLE_SIZE 1024)
(def ^:private ^:const DEFAULT_SYNC_WINDOW_TIME 200)
(def ^:private ^:const DEFAULT_WRITE_RETRIES 10)
(def ^:private ^:const DEFAULT_BLOOM_FILTER {:size 10240})

(defn- read-config
  [config-path]
  (with-open [stream (reader config-path)]
    (yaml/parse-stream stream)))

(defn load-config
  "Load the KVS config from config.yaml"
  [config-path]
  (let [config (read-config config-path)
        default {:memtable-size DEFAULT_MEMTABLE_SIZE
                 :sync-window-time DEFAULT_SYNC_WINDOW_TIME
                 :write-retries DEFAULT_WRITE_RETRIES
                 :bloom-filter DEFAULT_BLOOM_FILTER}]
    (when (nil? (:sstable-dir config))
      (throw (ex-info "Need to set `sstable-dir` in the config" config)))
    (when (nil? (:wal-dir config))
      (throw (ex-info "Need to set `wal-dir` in the config" config)))
    (reduce
     (fn [ret [k v]] (if (nil? (get config k)) (assoc ret k v) ret))
     config
     default)))
