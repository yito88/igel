(ns igel.memtable
  (:require [igel.data :as data]
            [igel.store :as store :refer [select scan write! delete!]]))

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

(defrecord Memtable [mem size]
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
      (write! mem k v)
      (swap! size (partial + (count k) (count v)))))
  (delete!
    [_ k]
    (locking mem
      (delete! mem k)
      (swap! size (partial + (count k))))))

(defn create-memtable
  "Create the memtable handler"
  []
  (->Memtable (->MemStore (new java.util.TreeMap (data/byte-array-comparator)))
              (atom 0)))

(defn entry-set
  [^Memtable memtable]
  (->> memtable :mem :mem .entrySet
       (map (fn [e] [(.getKey e) (.getValue e)]))))
