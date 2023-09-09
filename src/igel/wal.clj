(ns igel.wal
  (:require [clojure.core.async :as async]
            [igel.io :as io])
  (:import (java.io FileOutputStream BufferedOutputStream)))

(def ^:const ^:private DEFAULT_WINDOW_TIME 200)

(defn wal-file-path
  [sstable-id config]
  (str (:wal-dir config) \/ @sstable-id ".wal"))

(defn spawn-wal-writer
  [sstable-id data-chan flush-req-chan flush-wal-chan config]
  (io/make-dir (:wal-dir config))
  (async/go-loop [data-chan data-chan]
    (let [file-stream (FileOutputStream. (wal-file-path sstable-id config))
          out-stream (BufferedOutputStream. file-stream 4096)
          sync-window (or (:sync-window-time config) DEFAULT_WINDOW_TIME)
          window-chan (async/timeout sync-window)]
      ;; WAL loop until a flush is completed
      ;; TODO: error handling
      (loop [comp-channels (transient #{})]
        (let [channels (async/alt!
                         data-chan ([[k d comp-chan]]
                                    (when-not (nil? k)
                                      (io/append-wal! out-stream [k d])
                                      (conj! comp-channels comp-chan)))
                         window-chan ([]
                                      (if (> (count comp-channels) 0)
                                        (do
                                          (-> file-stream .getFD .sync)
                                          (mapv #(async/>!! % :done)
                                                (persistent! comp-channels))
                                          (async/>! flush-req-chan :try-flush)
                                          (transient #{}))
                                        comp-channels)))]
          (if (nil? channels)
            (do
              (-> file-stream .getFD .sync)
              (.close out-stream)
              (mapv #(async/>!! % :done)
                    (persistent! comp-channels))
              (async/>! flush-req-chan :flush))
            (recur channels))))
      ;; The current WAL loop finished
      ;; Wait for the new data-chan from the flush writer
      (recur (async/<! flush-wal-chan)))))
