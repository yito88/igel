(ns kvs.sstable
  (:require [blossom.core :as blossom]
            [kvs.io :as io]
            [kvs.store :refer [IStore]]))

(defn get-sstable-path
  [sstable-dir id]
  (str sstable-dir "/" id ".sst"))

(defrecord TreeStore [dir ids bloom-filters]
  IStore
  (select
    [_ k]
    (loop [filters (map-indexed vector bloom-filters)]
      (let [[index bf] (first filters)
            hit? (blossom/hit? bf k)
            v (io/read-value (get-sstable-path dir (nth ids index)) k)]
        (if (and hit? v)
          v
          (when index
            (recur (next filters)))))))
  (scan [_ from-key to-key] nil)
  (write! [_ k v] nil)
  (delete! [_ k] nil))

(defn restore-tree-store
  [{:keys [sstable-dir]}]
  (io/make-sstable-dir sstable-dir)
  ; TODO: restore from the exiting sstables
  [(->TreeStore sstable-dir '() nil) 0])

(defn update-tree
  [tree new-id new-filter]
  (->TreeStore (:dir @tree)
               (cons new-id (:ids @tree))
               (cons new-filter (:bloom-filters @tree))))
