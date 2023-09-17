(ns igel.sstable
  (:require [blossom.core :as blossom]
            [clojure.java.io :as java-io]
            [clojure.tools.logging :as logging]
            [igel.data :as data]
            [igel.io :as io]
            [igel.store :as store])
  (:import (java.io FileOutputStream BufferedOutputStream)))

(defn get-sstable-path
  [id dir]
  (str dir "/" id ".sst"))

(defn get-info-path
  [id dir]
  (str dir "/" id ".info"))

(defrecord TableInfo [bloom-filter head-key tail-key])

(defn write-table-info
  [file-path {:keys [bloom-filter head-key tail-key]}]
  (with-open [file-stream (FileOutputStream. file-path)]
    (with-open [out-stream (BufferedOutputStream. file-stream)]
      (io/write-bytes! out-stream (blossom/serialize-filter bloom-filter))
      (io/write-bytes! out-stream head-key)
      (io/write-bytes! out-stream tail-key)
      (.flush out-stream)
      (-> file-stream .getFD .sync))))

(defn read-table-info
  [file-path {:keys [bloom-filter]}]
  (with-open [in-stream (java-io/input-stream file-path)]
    (->TableInfo
     (blossom/deserialize-filter (io/read-data! in-stream) bloom-filter)
     (io/read-data! in-stream)
     (io/read-data! in-stream))))

(defrecord TreeStore [dir sstables]
  store/IStoreRead
  (select
    [_ k]
    (loop [tables (reverse sstables)]
      (let [[id table] (first tables)
            hit? (blossom/hit? (:bloom-filter table) k)
            sstable-path (get-sstable-path id dir)
            v (io/read-value sstable-path k)]
        (if (and table hit? v)
          v
          (when id
            (recur (next tables)))))))
  (scan
    [_ from-key to-key]
    (loop [pairs (new java.util.TreeMap (data/byte-array-comparator))
           tables sstables]
      (if (empty? tables)
        (->> pairs .entrySet (map (fn [e] [(.getKey e) (.getValue e)])))
        (let [[id table] (first tables)
              head-key (:head-key table)
              tail-key (:tail-key table)
              sstable-path (get-sstable-path id dir)]
          (if (empty? tables)
            (->> pairs .entrySet (map (fn [e] [(.getKey e) (.getValue e)])))
            (recur
             (if (and (data/byte-array-smaller? head-key to-key)
                      (data/byte-array-smaller-or-equal? from-key tail-key))
               (reduce
                (fn [tree-map [k d]]
                  (.put tree-map k d)
                  tree-map)
                pairs
                (io/scan-pairs sstable-path from-key to-key))
               pairs)
             (rest tables))))))))

(defn restore-tree-store
  [{:keys [sstable-dir] :as config}]
  (io/make-dir sstable-dir)
  (let [indexes (->> (io/list-files sstable-dir)
                     (filter #(and (.isFile %)
                                   (.endsWith (.getName %) ".sst")))
                     (map #(Integer. (re-find #"\d+" (.getName %))))
                     (remove nil?)
                     sort)
        tables (mapv (fn [index]
                       [index
                        (read-table-info (get-info-path index sstable-dir)
                                         config)])
                     indexes)
        sstable-id (if (empty? indexes)
                     0
                     (-> (last indexes) (quot 2) inc (* 2)))]
    (logging/info "Restoring SSTables:" indexes)
    [(->TreeStore sstable-dir tables) sstable-id]))

(defn update-tree
  [tree new-id new-table-info]
  (->TreeStore (:dir tree)
               (conj (:sstables tree) [new-id new-table-info])))
