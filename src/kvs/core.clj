(ns kvs.core
  (:require [clojure.tools.logging :as logging]
            [kvs.io :as io]
            [kvs.memtable :refer [create-memtable]]
            [kvs.sstable :refer [get-sstable-path
                                 restore-tree-store
                                 update-tree]]
            [kvs.store :refer [IStore select scan write! delete!]])
  (:gen-class))

(defrecord KVS [config memtable tree sstable-id]
  IStore
  (select
    [_ k]
    (or (select @memtable k) (select @tree k)))
  (scan
    [_ from-key to-key]
    (merge (scan @tree from-key to-key) (scan @memtable from-key to-key)))
  (write!
    [_ k v]
    (when (> (write! @memtable k v) (:memtable-size config))
      (let [[old _] (reset-vals! memtable (create-memtable))
            new-id (swap! sstable-id inc)
            ;; TODO: async flush
            bf (io/flush! old (get-sstable-path (:sstable-dir config) new-id))]
        (reset! tree (update-tree tree new-id bf)))))
  (delete!
    [_ k]
    (when (> (delete! @memtable k) (:memtable-size config))
      (let [[old _] (reset-vals! memtable (create-memtable))
            new-id (swap! sstable-id inc)
            ;; TODO: async flush
            bf (io/flush! old (get-sstable-path (:sstable-dir config) new-id))]
        (reset! tree (update-tree tree new-id bf))))))

(defn load-config
  "Load the KVS config from config.toml"
  [_config-path]
  ; TODO: load from the file
  {:sstable-dir "./data"
   :memtable-size 1024})

(defn gen-kvs
  [config-path]
  (let [config (load-config config-path)
        memtable (create-memtable)
        [treestore last-index]  (restore-tree-store config)]
    (->KVS config (atom memtable) (atom treestore) (atom last-index))))

(defn -main
  "I don't do a whole lot ... yet."
  [& config-path]
  ; sequencial write
  (let [kvs (gen-kvs config-path)]
    (dorun
     (doseq [i (range 0 128)]
       (let [k (.getBytes (str "key" i))
             v (.getBytes (str "val" i))]
         (write! kvs k v))))
    (doseq [i (range 0 128)]
      (let [k (.getBytes (str "key" i))
            v (.getBytes (str "val" i))
            actual (select kvs k)]
        (if (java.util.Arrays/equals actual v)
          (println "OK: key:" (String. k) "value:" (String. actual))
          (println "ERROR: key:" (String. k) "value:" (String. actual) "expect:" (String. v)))))))
