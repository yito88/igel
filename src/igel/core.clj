(ns igel.core
  (:require [clojure.core.async :as async]
            [igel.config :as config]
            [igel.data :as data]
            [igel.io :as io]
            [igel.memtable :refer [create-memtable]]
            [igel.sstable :refer [->TableInfo
                                  get-sstable-path
                                  restore-tree-store
                                  update-tree]]
            [igel.store :as store]
            [igel.wal :as wal])
  (:gen-class))

(defn- merge-scan-results
  [mem-ret tree-ret]
  (loop [pairs (transient [])
         m-pairs mem-ret
         t-pairs tree-ret]
    (cond
      (and (empty? m-pairs) (empty? t-pairs))
      (persistent! pairs)
      (and (seq m-pairs) (empty? t-pairs))
      (reduce #(conj %1 %2) (persistent! pairs) m-pairs)
      (and (empty? m-pairs) (seq t-pairs))
      (reduce #(conj %1 %2) (persistent! pairs) t-pairs)
      :else
      (let [[m-key m-data] (first m-pairs)
            [t-key t-data] (first t-pairs)
            [updated m-rest t-rest] (cond
                                      (data/byte-array-equals? m-key t-key)
                                      [(conj! pairs [m-key m-data])
                                       (rest m-pairs) (rest t-pairs)]
                                      (data/byte-array-smaller? m-key t-key)
                                      [(conj! pairs [m-key m-data])
                                       (rest m-pairs) t-pairs]
                                      (data/byte-array-smaller? t-key m-key)
                                      [(conj! pairs [t-key t-data])
                                       m-pairs (rest t-pairs)])]
        (recur updated m-rest t-rest)))))

(defrecord KVS [config memtable tree sstable-id wal-writer wal-chan]
  store/IStoreRead
  (select
    [_ k]
    (let [data (or (store/select @memtable k) (store/select @tree k))]
      (when (data/is-valid? data) (:value data))))
  (scan
    [_ from-key to-key]
    (->> (merge-scan-results (store/scan @memtable from-key to-key)
                             (store/scan @tree from-key to-key))
         (filter (fn [[_ data]] (data/is-valid? data)))
         (map (fn [[k data]] [k (:value data)]))))

  store/IStoreMutate
  (write!
    [this k v]
    (let [comp-chan (async/chan)]
      (async/>!! wal-chan [k (data/new-data v) comp-chan])
      (async/<!! comp-chan))
    (when (> (store/write! @memtable k v) (:memtable-size config))
      (store/flush! this)))
  (delete!
    [this k]
    (let [comp-chan (async/chan)]
      (async/>!! wal-chan [k (data/deleted-data) comp-chan])
      (async/<!! comp-chan))
    (when (> (store/delete! @memtable k) (:memtable-size config))
      (store/flush! this)))

  store/IFlush
  (flush!
    [_]
    (let [[old _] (reset-vals! memtable (create-memtable))
          new-id (swap! sstable-id inc)
          ;; TODO: async flush
          [bf head-key tail-key] (io/flush! old
                                            (get-sstable-path
                                             (:sstable-dir config)
                                             new-id))]
      (swap! tree #(update-tree % new-id (->TableInfo bf head-key tail-key))))))

;; ==== Main APIs ====

(defn gen-kvs
  [config-path]
  (let [config (config/load-config config-path)
        memtable (create-memtable)
        [treestore last-index]  (restore-tree-store config)
        wal-chan (async/chan)
        wal-writer (wal/spawn-wal-writer wal-chan config)]
    (->KVS config (atom memtable) (atom treestore) (atom last-index)
           wal-writer wal-chan)))

(defn select
  "Read the value corresponding to the given key.
  If the key doesn't exist, it returns nil."
  [^KVS kvs ^bytes k]
  (store/select kvs k))

(defn scan
  "Read the key-value pairs between the `from-key` and the `to-key`.
  This range should include `from-key` and not include `to-key`.
  It returns key-value pair vectors like [[k0 v0] [k1 v1]].
  The keys should be ordered by ascending."
  [^KVS kvs ^bytes from-key ^bytes to-key]
  (store/scan kvs from-key to-key))

(defn write!
  "Write the new value correponding to the given key."
  [^KVS kvs ^bytes k ^bytes v]
  (store/write! kvs k v))

(defn delete!
  "Delete the given key from the key-value store."
  [^KVS kvs ^bytes k]
  (store/delete! kvs k))
