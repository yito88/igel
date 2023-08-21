(ns igel.core
  (:require [clojure.tools.logging :as logging]
            [igel.data :as data]
            [igel.io :as io]
            [igel.memtable :refer [create-memtable]]
            [igel.sstable :refer [->TableInfo
                                  get-sstable-path
                                  restore-tree-store
                                  update-tree]]
            [igel.store :as store
             :refer [flush! select scan write! delete!]])
  (:gen-class))

(defn- merge-scan-results
  [mem-ret tree-ret]
    (println "DEBUG: m-pairs" mem-ret)
    (println "DEBUG: t-pairs" tree-ret)
  (loop [pairs (transient [])
         mem-pairs mem-ret
         tree-pairs tree-ret]
    (if (and (empty? mem-pairs) (empty? tree-pairs))
      (persistent! pairs)
      (cond
        (and (seq mem-pairs) (empty? tree-pairs))
        (persistent! (reduce #(conj! %1 %2) pairs mem-pairs))
        (and (empty? mem-pairs) (seq tree-pairs))
        (persistent! (reduce #(conj! %1 %2) pairs tree-pairs))
        :else
        (let [[m-key m-data] (first mem-pairs)
              [t-key t-data] (first tree-pairs)
              [m-next t-next] (cond
                                (data/byte-array-equals? m-key t-key)
                                (do
                                  (conj! pairs [m-key m-data])
                                  [(rest mem-pairs) (rest tree-pairs)])
                                (data/byte-array-smaller? m-key t-key)
                                (do
                                  (conj! pairs [m-key m-data])
                                  [(rest mem-pairs) tree-pairs])
                                (data/byte-array-smaller? t-key m-key)
                                (do
                                  (conj! pairs [t-key t-data])
                                  [mem-pairs (rest tree-pairs)]))]
          (recur pairs m-next t-next))))))

(defrecord KVS [config memtable tree sstable-id]
  store/IStoreRead
  (select
    [_ k]
    (let [data (or (select @memtable k) (select @tree k))]
      (if (:deleted? data) nil (:value data))))
  (scan
    [_ from-key to-key]
    (->> (merge-scan-results (scan @memtable from-key to-key)
                             (scan @tree from-key to-key))
         (filter (fn [[_ data]] (:deleted? data)))
         (map (fn [[k data]] [k (:value data)]))
         (into {})))

  store/IStoreMutate
  (write!
    [this k v]
    ;; TODO: wait for WAL
    (when (> (write! @memtable k v) (:memtable-size config))
      (flush! this)))
  (delete!
    [this k]
    (when (> (delete! @memtable k) (:memtable-size config))
      (flush! this)))

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

(defn load-config
  "Load the KVS config from config.toml"
  [_config-path]
  ; TODO: load from the file
  {:sstable-dir "./data"
   :memtable-size 1024})

(defn gen-kvs
  [config-path]
  (let [config (load-config config-path)
        memtable (create-memtable)
        [treestore last-index]  (restore-tree-store config)]
    (->KVS config (atom memtable) (atom treestore) (atom last-index))))

;; TODO: Move to test
(def NUM_ITEMS 128)
(defn -main
  "I don't do a whole lot ... yet."
  [& config-path]
  ; sequencial crud test
  (let [kvs (gen-kvs config-path)]
    ;; insert
    (doseq [i (range 0 NUM_ITEMS)]
      (let [k (.getBytes (str "key" i))
            v (.getBytes (str "val" i))]
        (write! kvs k v)))
    ;; delete
    (doseq [i (range 0 NUM_ITEMS)]
      (when (zero? (mod i 16))
        (let [k (.getBytes (str "key" i))]
          (delete! kvs k))))
    ;; update
    (doseq [i (range 0 NUM_ITEMS)]
      (when (zero? (mod i 32))
        (let [k (.getBytes (str "key" i))
              v (.getBytes (str "overwritten-val" i))]
          (write! kvs k v))))
    ;; select all
    (doseq [i (range 0 NUM_ITEMS)]
      (let [k (.getBytes (str "key" i))
            v (cond
                (zero? (mod i 32)) (.getBytes (str "overwritten-val" i))
                (zero? (mod i 16)) nil
                :else (.getBytes (str "val" i)))
            actual (select kvs k)]
        (cond
          (and (nil? v) (nil? actual))
          (println "OK: key:" (String. k) "value was deleted")

          (and (nil? v) (seq actual))
          (println "ERROR: key:" (String. k) "the value was not deleted unexpectedly."
                   "actual:" (String. actual))

          (and (seq v) (nil? actual))
          (println "ERROR: key:" (String. k) "the value was deleted unexpectedly."
                   "expected:" (String. v))

          :else
          (if (java.util.Arrays/equals actual v)
            (println "OK: key:" (String. k) "value:" (String. actual))
            (println "ERROR: key:" (String. k)
                     "value:" (String. actual)
                     "expect:" (String. v))))))
    ;; scan
    (doseq [group (partition 16 (sort-by str (range 0 NUM_ITEMS)))]
      (let [from-key (.getBytes (str "key" (first group)))
            to-key (.getBytes (str "key" (last group) 0))
            expect (filter #(not (nil? %))
                           (for [i group]
                             (cond
                               (zero? (mod i 32)) (.getBytes
                                                   (str "overwritten-val" i))
                               (zero? (mod i 16)) nil
                               :else (.getBytes (str "val" i)))))
            actual (scan kvs from-key to-key)]
        (println "expect: " expect)
        (println "actual: " actual)
        (= (count expect) (count actual))))))
