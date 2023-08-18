(ns igel.core
  (:require [clojure.tools.logging :as logging]
            [igel.io :as io]
            [igel.memtable :refer [create-memtable]]
            [igel.sstable :refer [get-sstable-path
                                  restore-tree-store
                                  update-tree]]
            [igel.store :refer [IStore IFlush flush! select scan write! delete!]])
  (:gen-class))

(defrecord KVS [config memtable tree sstable-id]
  IStore
  (select
    [_ k]
    (let [data (or (select @memtable k) (select @tree k))]
      (if (:deleted? data) nil (:value data))))
  (scan
    [_ from-key to-key]
    (->> (merge (scan @tree from-key to-key) (scan @memtable from-key to-key))
         (filter (fn [[_ data]] (:deleted? data)))
         (map (fn [[k data]] [k (:value data)]))
         (into {})))
  (write!
    [this k v]
    ;; TODO: wait for WAL
    (when (> (write! @memtable k v) (:memtable-size config))
      (flush! this)))
  (delete!
    [this k]
    (when (> (delete! @memtable k) (:memtable-size config))
      (flush! this)))

  IFlush
  (flush!
    [_]
    (let [[old _] (reset-vals! memtable (create-memtable))
          new-id (swap! sstable-id inc)
          ;; TODO: async flush
          bf (io/flush! old (get-sstable-path (:sstable-dir config) new-id))]
      (reset! tree (update-tree tree new-id bf)))))

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

;; TODO: Move to test
(def NUM_ITEMS 128)
(defn -main
  "I don't do a whole lot ... yet."
  [& config-path]
  ; sequencial write
  (let [kvs (gen-kvs config-path)]
    (dorun
     (doseq [i (range 0 NUM_ITEMS)]
       (let [k (.getBytes (str "key" i))
             v (.getBytes (str "val" i))]
         (write! kvs k v))))
    (doseq [i (range 0 NUM_ITEMS)]
      (when (zero? (mod i 32))
        (let [k (.getBytes (str "key" i))]
          (delete! kvs k))))
    (doseq [i (range 0 NUM_ITEMS)]
      (let [k (.getBytes (str "key" i))
            v (if (zero? (mod i 32)) nil (.getBytes (str "val" i)))
            actual (select kvs k)]
        (cond
          (and (nil? v) (nil? actual))
          (println "OK: key:" (String. k) "value was deleted")

          (and (nil? v) (seq actual))
          (println "ERROR: key:" (String. k) "the value was not deleted unexpectedly."
                   "actual:" (String. actual))

          (and (seq v) (nil? actual))
          (println "ERROR: key:" (String. k) "the value was deleted unexpectedly."
                   "expected:" (String. v))

          :else
          (if (java.util.Arrays/equals actual v)
            (println "OK: key:" (String. k) "value:" (String. actual))
            (println "ERROR: key:" (String. k)
                     "value:" (String. actual)
                     "expect:" (String. v))))))))
