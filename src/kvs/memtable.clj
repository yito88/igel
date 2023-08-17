(ns kvs.memtable
  (:require [kvs.data :as data]
            [kvs.store :refer [IStore select scan write! delete!]]))

;; for TreeMap
(defn byte-array-comparator
  []
  (reify java.util.Comparator
    (compare [_ a b]
      (loop [i 0]
        (if (< i (min (count a) (count b)))
          (let [cmp (compare (aget a i) (aget b i))]
            (if (zero? cmp)
              (recur (inc i))
              cmp))
          (compare (count a) (count b)))))))

(defrecord MemStore [^java.util.TreeMap mem]
  IStore
  (select [_ k] (.get mem k))
  (scan
    [_ from-key to-key]
    (.subMap mem from-key true to-key false))
  (write!
    [_ k v]
    (.put mem k (data/new-data v)))
  (delete!
    [_ k]
    (.put mem k (data/deleted-data))))

(defrecord Memtable [mem size]
  IStore
  ;; TODO: need concurrent BTreeMap
  (select
    [_ k]
    (locking mem
      (select mem k)))
  (scan
    [_ from-key to-key]
    (locking mem
      (scan mem from-key to-key)))
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
  (->Memtable (->MemStore (new java.util.TreeMap (byte-array-comparator)))
              (atom 0)))

(defn entry-set
  [^Memtable memtable]
  (-> memtable :mem :mem .entrySet seq))
