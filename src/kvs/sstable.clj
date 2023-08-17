(ns kvs.sstable
  (:require [blossom.core :as blossom]
            [kvs.data :as data]
            [kvs.io :as io]
            [kvs.store :refer [IStore]]))

(defn get-sstable-path
  [sstable-dir id]
  (str sstable-dir "/" id ".sst"))

(defrecord TreeStore [dir sstables]
  IStore
  (select
    [_ k]
    (loop [tables sstables]
      (let [[id bf] (first tables)
            v (io/read-value (get-sstable-path dir id) k)]
        (if (and bf (blossom/hit? bf k) v)
          v
          (when id
            (recur (next tables)))))))
  (scan [_ from-key to-key] nil)
  ;; TODO throw an exception
  (write! [_ _k _v] nil)
  ;; TODO throw an exception
  (delete! [_ _k] nil))

(defn restore-tree-store
  [{:keys [sstable-dir]}]
  (io/make-sstable-dir sstable-dir)
  ; TODO: restore from the exiting sstables
  [(->TreeStore sstable-dir {}) 0])

(defn update-tree
  [tree new-id new-filter]
  (->TreeStore (:dir @tree)
               (assoc (:sstables @tree) new-id new-filter)))
