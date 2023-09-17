(ns igel.memtable
  (:require [clojure.core.async :as async]
            [igel.data :as data]
            [igel.store :as store :refer [select scan write! delete!]]
            [igel.wal :as wal]))

(defrecord MemStore [^java.util.TreeMap mem]
  store/IStoreRead
  (select
    [_ k]
    (let [data (.get mem k)]
      (when (seq data)
        (if (data/is-valid? data) data (data/deleted-data)))))
  (scan
    [_ from-key to-key]
    (some->> (.subMap mem from-key true to-key false)
             .entrySet
             (map
              (fn [e]
                (let [k (.getKey e)
                      data (.getValue e)]
                  (if (data/is-valid? data)
                    [k data]
                    [k (data/deleted-data)]))))))

  store/IStoreMutate
  (write!
    [_ k v]
    (.put mem k (data/new-data v)))
  (delete!
    [_ k]
    (.put mem k (data/deleted-data))))

(defrecord Memtable [mem wal-chan size]
  store/IStoreRead
  ;; TODO: need concurrent BTreeMap
  (select
    [_ k]
    (locking mem
      (select mem k)))
  (scan
    [_ from-key to-key]
    (locking mem
      (scan mem from-key to-key)))

  store/IStoreMutate
  (write!
    [_ k v]
    (locking mem
      (let [comp-chan (async/chan)]
        (when-not (async/>!! wal-chan [k (data/new-data v) comp-chan])
          (throw (ex-info "Write failed due to memtable switching"
                          {:retriable true})))
        (write! mem k v)
        (case (async/<!! comp-chan)
          :done (swap! size (partial + (count k) (count v)))
          nil (throw (ex-info "Write failed due to memtable switching"
                              {:retriable true}))
          (throw (ex-info "Write failed" {:retriable false}))))))
  (delete!
    [_ k]
    (locking mem
      (let [comp-chan (async/chan)]
        (when-not (async/>!! wal-chan [k (data/deleted-data) comp-chan])
          (throw (ex-info "Delete failed due to memtable switching"
                          {:retriable true})))
        (delete! mem k)
        (case (async/<!! comp-chan)
          :done (swap! size (partial + (count k)))
          nil (throw (ex-info "Delete failed due to memtable switching"
                              {:retriable true}))
          (throw (ex-info "Delete failed" {:retriable false})))))))

(defn create-memtable
  "Create the new memtable"
  [wal-chan]
  (->Memtable (->MemStore (new java.util.TreeMap (data/byte-array-comparator)))
              wal-chan
              (atom 0)))

(defn init-memtable
  "Initialize the memtable. Restore data from WAL to the new memtable."
  [wal-chan config]
  (let [store (->MemStore (new java.util.TreeMap (data/byte-array-comparator)))
        wal-pairs (wal/load-existing-wal config)
        memtable-size (if (empty? wal-pairs) 0 (:memtable-size config))]
    (mapv #(apply write! store %) wal-pairs)
    (->Memtable store wal-chan (atom memtable-size))))

(defn entry-set
  [^Memtable memtable]
  (->> memtable :mem :mem .entrySet
       (map (fn [e] [(.getKey e) (.getValue e)]))))
