(ns igel.flush
  (:require  [blossom.core :as blossom]
             [clojure.core.async :as async]
             [clojure.tools.logging :as logging]
             [igel.io :as io]
             [igel.memtable :as memtable]
             [igel.sstable :as sstable]
             [igel.wal :as wal])
  (:import (java.io FileOutputStream BufferedOutputStream)))

(defn- switch-memtable!
  [memtable wal-chan]
  (let [[old _] (reset-vals! memtable (memtable/create-memtable wal-chan))]
    old))

(defn- flush-memtable!
  [memtable new-id {:keys [sstable-dir bloom-filter]}]
  (let [sstable-path (sstable/get-sstable-path new-id sstable-dir)
        info-path (sstable/get-info-path new-id sstable-dir)
        bf (blossom/make-filter bloom-filter)
        entry-set (memtable/entry-set memtable)
        head-key (-> entry-set first first)
        tail-key (-> entry-set last first)]
    (logging/info "Starting flush to SSTable" sstable-path)
    (with-open [file-stream (FileOutputStream. sstable-path)]
      (with-open [out-stream (BufferedOutputStream. file-stream 16384)]
        (doseq [entry entry-set]
          (let [k (first entry)
                data (second entry)
                value (:value data)]
            ;; write the key
            (io/write-bytes! out-stream k)
            ;; write the value
            ;; if it's deleted, write only the length 0
            (if (:deleted? data)
              (io/write-tombstone! out-stream)
              (io/write-bytes! out-stream value))
            (blossom/add bf k)))
        (.flush out-stream)
        (-> file-stream .getFD .sync)))
    (let [table-info (sstable/->TableInfo bf head-key tail-key)]
      (sstable/write-table-info info-path table-info)
      table-info)))

(defn- flush!
  [memtable tree sstable-id flush-wal-chan config]
  (if (zero? (-> @memtable :size deref))
    ;; no flush
    (async/go
      (async/>! flush-wal-chan [@sstable-id (:wal-chan @memtable)]))
    ;; flush and update the memtable and the sstables
    (let [new-id @sstable-id
          wal-chan (async/chan)
          table-info (-> (switch-memtable! memtable wal-chan)
                         (flush-memtable! new-id config))]
      ;; Send a new WAL ID and wal-chan to the WAL writer to receive new requests
      (async/>!! flush-wal-chan [(+ @sstable-id 2) wal-chan])
      ;; Update the tree
      (swap! tree #(sstable/update-tree % new-id table-info))
      ;; The previous WAL can be deleted
      (io/delete-file (wal/wal-file-path @sstable-id config))
      ;; Update SSTable ID for the next
      (swap! sstable-id (partial + 2)))))

(defn spawn-flush-writer
  [memtable tree sstable-id req-chan flush-wal-chan config]
  ;; first flush
  (async/go
    (flush! memtable tree sstable-id flush-wal-chan config))
  ;; TODO: error handling
  (let [threshold (:memtable-size config)]
    (async/go-loop [shutdown? false]
      (case (async/<! req-chan)
        :flush (do
                 (flush! memtable tree sstable-id flush-wal-chan config)
                 (when-not shutdown? (recur false)))
        :try-flush (do
                     (when (> (deref (:size @memtable)) threshold)
                       ;; close the data channel not to send data to the WAL thread
                       (async/close! (:wal-chan @memtable)))
                     (recur false))
        ;; when nil
        (do
          ;; close the data channel not to send data to the WAL thread
          (async/close! (:wal-chan @memtable))
          (recur true))))))
