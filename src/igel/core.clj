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

(defrecord BgWorkers [wal-handler flush-writer flush-req-chan])

(defn spawn-bg-workers
  [memtable tree sstable-id config]
  (let [flush-req-chan (async/chan)
        flush-wal-chan (async/chan)]
    (->BgWorkers
     (wal/spawn-wal-writer @sstable-id
                           (:wal-chan @memtable)
                           flush-req-chan
                           flush-wal-chan
                           config)
     (f/spawn-flush-writer memtable tree sstable-id
                           flush-req-chan
                           flush-wal-chan
                           config)
     flush-req-chan)))

(defn- terminate-flush-writer
  [coordinator]
  (async/close! (:flush-req-chan coordinator)))

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

(defrecord KVS [config memtable tree workers]
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
    (loop [retries (:write-retries config)]
      (if (try
            (store/write! @memtable k v)
            false ;; break the loop when it succeeded
            (catch Exception e
              (when-not (-> e ex-data :retriable)
                (throw (ex-info "Write failed" {:retriable false})))
              (pos? retries)))
        (do
          (Thread/sleep 100)
          (recur (dec retries)))
        (when (zero? retries)
          (throw (ex-info "Write failed repeatedly" {:retriable false}))))))
  (delete!
    [_ k]
    (loop [retries (:write-retries config)]
      (if (try
            (store/delete! @memtable k)
            false ;; break the loop when it succeeded
            (catch Exception e
              (when-not (-> e ex-data :retriable)
                (throw (ex-info "Delete failed" {:retriable false})))
              (pos? retries)))
        (do
          (Thread/sleep 100)
          (recur (dec retries)))
        (when (zero? retries)
          (throw (ex-info "Delete failed repeatedly" {:retriable false}))))))

  Object
  (finalize [_]
    (info "KVS is shutting down...")
    (terminate-flush-writer workers)))

;; ==== Main APIs ====

(defn gen-kvs
  [config-path]
  (let [config (config/load-config config-path)
        [tree sstable-id] (mapv atom (restore-tree-store config))
        memtable (atom (create-memtable (async/chan)))
        workers (spawn-bg-workers memtable tree sstable-id config)]
    (->KVS config memtable tree workers)))

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
