(ns igel.sstable
  (:require [blossom.core :as blossom]
            [igel.data :as data]
            [igel.io :as io]
            [igel.store :as store]))

(defn get-sstable-path
  [id dir]
  (str dir "/" id ".sst"))

(defrecord TableInfo [bloom-filter head-key tail-key])

(defrecord TreeStore [dir sstables]
  store/IStoreRead
  (select
    [_ k]
    (loop [tables (reverse sstables)]
      (let [[id table] (first tables)
            hit? (blossom/hit? (:bloom-filter table) k)
            sstable-path (get-sstable-path id dir)
            v (io/read-value sstable-path k)]
        (if (and table hit? v)
          v
          (when id
            (recur (next tables)))))))
  (scan
    [_ from-key to-key]
    (loop [pairs (new java.util.TreeMap (data/byte-array-comparator))
           tables sstables]
      (if (empty? tables)
        (->> pairs .entrySet (map (fn [e] [(.getKey e) (.getValue e)])))
        (let [[id table] (first tables)
              head-key (:head-key table)
              tail-key (:tail-key table)
              sstable-path (get-sstable-path id dir)]
          (if (empty? tables)
            (->> pairs .entrySet (map (fn [e] [(.getKey e) (.getValue e)])))
            (recur
             (if (and (data/byte-array-smaller? head-key to-key)
                      (data/byte-array-smaller-or-equal? from-key tail-key))
               (reduce
                (fn [tree-map [k d]]
                  (.put tree-map k d)
                  tree-map)
                pairs
                (io/scan-pairs sstable-path from-key to-key))
               pairs)
             (rest tables))))))))

(defn restore-tree-store
  [{:keys [sstable-dir]}]
  (io/make-dir sstable-dir)
  ; TODO: restore from the exiting sstables
  [(->TreeStore sstable-dir []) 0])

(defn update-tree
  [tree new-id new-table-info]
  (->TreeStore (:dir tree)
               (conj (:sstables tree) [new-id new-table-info])))
