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

(defn flush-memtable!
  [memtable file-path]
  (let [bf (blossom/make-filter {:hash-size "SHA-256" :size 1024})
        entry-set (memtable/entry-set memtable)
        head-key (-> entry-set first first)
        tail-key (-> entry-set last first)
        file-stream (FileOutputStream. file-path)]
    (logging/info "Starting flush to SSTable" file-path)
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
      ;; fsync
      (-> file-stream .getChannel (.force true)))
    [bf head-key tail-key]))

(defn flush!
  [memtable tree sstable-id flush-wal-chan config]
  (let [new-id @sstable-id
        file-path (sstable/get-sstable-path
                   new-id
                   (:sstable-dir config))
        wal-chan (async/chan)
        table-info (apply sstable/->TableInfo
                          (-> (switch-memtable! memtable wal-chan)
                              (flush-memtable! file-path)))]
    ;; Send a new WAL ID and wal-chan to the WAL writer to receive new requests
    (async/>!! flush-wal-chan [(+ @sstable-id 2) wal-chan])
    ;; Update the tree
    (swap! tree #(sstable/update-tree % new-id table-info))
    ;; The previous WAL can be deleted
    (io/delete-file (wal/wal-file-path @sstable-id config))
    ;; Update SSTable ID for the next
    (swap! sstable-id (partial + 2))))

(defn spawn-flush-writer
  [memtable tree sstable-id req-chan flush-wal-chan config]
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
