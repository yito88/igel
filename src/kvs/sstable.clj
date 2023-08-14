(ns kvs.sstable
  (:require [blossom.core :as blossom]
            [kvs.io :as io]
            [kvs.store :refer [IStore]])
  )

(defn get-sstable-path
  [sstable-dir id]
  (str sstable-dir "/" id ".sst"))

(defrecord TreeStore [dir ids bloom-filters]
  IStore
  (select
    [_ k]
    (loop [[index bf] (map-indexed vector bloom-filters)]
      (if (blossom/hit? bf k)
        (nth ids index)
        )))
  (scan [_ from-key to-key] nil)
  (write! [_ k v] nil)
  (delete! [_ k] nil))


(defn restore-tree-store
  [{:keys [sstable-dir]}]
  (io/make-sstable-dir sstable-dir)
  ; TODO: restore from the exiting sstables
  [(->TreeStore '() nil) 0])

(defn update-tree!
  [tree new-id new-filter]
  (reset! tree (->TreeStore (cons new-id (:sstables @tree))
                            (cons new-filter (:bloom-filters @tree)))))
