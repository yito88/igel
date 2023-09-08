(ns igel.flush
  (:require  [blossom.core :as blossom]
             [clojure.core.async :as async]
             [clojure.tools.logging :as logging]
             [igel.io :as io]
             [igel.memtable :as memtable]
             [igel.sstable :as sstable])
  (:import (java.io FileOutputStream BufferedOutputStream)))

(defn- switch-memtable!
  [memtable wal-chan]
  (let [[old _] (reset-vals! memtable (memtable/create-memtable wal-chan))]
    old))

(defn flush!
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

(defn spawn-flush-writer
  [memtable tree sstable-id req-chan flush-wal-chan wal-switch-chan config]
  ;; TODO: error handling
  (let [threshold (:memtable-size config)
        new-id (swap! sstable-id (partial + 2))
        file-path (sstable/get-sstable-path
                   (:sstable-dir config)
                   new-id)]
    (async/go-loop []
      (let [shutdown? (nil? (async/<! req-chan))]
        (if (or shutdown? (> (deref (:size @memtable)) threshold))
          (do
            ;; close the data channel not to send data to the WAL thread
            (async/close! (:wal-chan @memtable))
            ;; notify the memtable switching
            (async/>! wal-switch-chan :switch)
            ;; wait for the current WAL sync
            (if (= :flush (async/<! req-chan))
              (let [wal-chan (async/chan)
                    table-info (-> (switch-memtable! memtable wal-chan)
                                   (flush! file-path)
                                   (apply sstable/->TableInfo))]
              ;; Send a new wal-chan to the WAL writer to receive new requests
                (async/>! flush-wal-chan wal-chan)
                (swap! tree #(sstable/update-tree % new-id table-info))
                (when-not shutdown? (recur)))
              (throw (ex-info "Flush writer received an unexpected request"
                              {:fatal "flush-writer"}))))
          (recur))))))
