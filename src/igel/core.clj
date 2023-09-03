(ns igel.core
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :refer [info]]
            [igel.config :as config]
            [igel.data :as data]
            [igel.flush :as f]
            [igel.memtable :refer [create-memtable]]
            [igel.sstable :refer [restore-tree-store]]
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

(defrecord KVS [config memtable tree coordinator]
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
    [_ k v]
    (store/write! @memtable k v))
  (delete!
    [_ k]
    (store/delete! @memtable k)))

(defn spawn-bg-coordinator
  [memtable tree sstable-id config]
  (let [flush-writer-end-chan (async/chan)]
    (f/spawn-flush-writer memtable tree sstable-id flush-writer-end-chan config)
    ;; check and rerun a wal writer
    (async/go-loop [wal-data-chan (:wal-chan @memtable)
                    wal-end-chan (async/chan)
                    _wal-writer (wal/spawn-wal-writer wal-data-chan wal-end-chan config)]
      (if (nil? (async/<!! wal-end-chan))
        (let [wal-data-chan (async/chan)
              wal-end-chan (async/chan)]
          ;; run a new wal writer
          (recur wal-data-chan
                 wal-end-chan
                 (wal/spawn-wal-writer wal-data-chan wal-end-chan config)))
        (throw (ex-info "unreachable" {:type :coordinator}))))
    ;; shutdown
    (info "Shutting down...")
    (async/>!! flush-writer-end-chan :close)))

;; ==== Main APIs ====

(defn gen-kvs
  [config-path]
  (let [config (config/load-config config-path)
        [tree sstable-id] (mapv atom (restore-tree-store config))
        memtable (atom (create-memtable (async/chan)))
        coordinator (spawn-bg-coordinator memtable tree sstable-id config)]
    (->KVS config memtable tree coordinator)))

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
