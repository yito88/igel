(ns igel.wal
  (:require [clojure.core.async :as async]
            [igel.data :as data]
            [igel.io :as io]
            [igel.store :as store]))

(defn spawn-wal-writer
  [wal-path recv-chan config]
  (let [file-stream (FileOutputStream. wal-path)
        sync-window (or (:sync-window-time config) 200)]
    (with-open [out-stream (BufferedOutputStream. file-stream 4096)]
      (async/go-loop [prev-sync-time (System/currentTimeMillis)
                      comp-channels (transient #{})]
        (let [[k d comp-chan] (async/<! recv-chan)]
          (io/append-wal! out-stream [k d])
          (conj! comp-channels comp-chan)
          (recur
           (if (< (+ prev-sync-time sync-window) (System/currentTimeMillis))
             (do
               (-> file-stream .getChannel (.force true))
               (doseq [comp-chan comp-channels]
                 (async/>! comp-chan :done))
               (reset! comp-channels (transient #{}))
               (System/currentTimeMillis))
             prev-sync-time)
           comp-channels))))))
