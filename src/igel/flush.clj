(ns igel.flush
  (:require  [blossom.core :as blossom]
             [clojure.core.async :as async]
             [clojure.tools.logging :as logging]
             [igel.io :as io]
             [igel.memtable :as memtable]
             [igel.sstable :as sstable])
  (:import (java.io FileOutputStream BufferedOutputStream)))

(def ^:const ^:private CHECK_INTERVAL_TIME 1000)

(defn- switch-memtable!
  [memtable]
  (async/close! (:wal-chan @memtable))
  (let [wal-chan (async/chan)
        [old _] (reset-vals! memtable (memtable/create-memtable wal-chan))]
    old))

(defn flush!
  [memtable file-path]
  (let [bf (blossom/make-filter {:hash-size "SHA-256" :size 1024})
        entry-set (memtable/entry-set memtable)
        head-key (-> entry-set first first)
        tail-key (-> entry-set last first)
        file-stream (FileOutputStream. file-path)]
    (logging/info "Starting flush to SSTable" file-path head-key tail-key)
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
  [memtable tree sstable-id end-chan config]
  (let [threshold (:memtable-size config)
        interval (or (:flush_check_interval config) CHECK_INTERVAL_TIME)]
    ;; TODO: error handling
    (async/go-loop [interval-chan (async/timeout interval)]
      (let [signal (async/alt!
                     end-chan ([] :close)
                     interval-chan ([]
                                    (if (> (:size @memtable) threshold)
                                      :flush
                                      :continue)))]
        (println "DEBUG:" signal)
        (case signal
          :flush (let [old-memtable (switch-memtable! memtable)
                       new-id (swap! sstable-id inc)
                       file-path (sstable/get-sstable-path
                                  (:sstable-dir config)
                                  new-id)
                       [bf head-key tail-key] (flush! old-memtable file-path)
                       table-info (sstable/->TableInfo bf head-key tail-key)]
                   (swap! tree #(sstable/update-tree % new-id table-info))
                   (recur (async/timeout interval)))
          :close (async/close! end-chan)
          (recur (async/timeout interval)))))))
