(ns igel.wal
  (:require [clojure.core.async :as async]
            [igel.data :as data]
            [igel.io :as io]
            [igel.store :as store])
  (:import (java.io FileOutputStream BufferedOutputStream)))

(defn spawn-wal-writer
  [recv-chan config]
  (let [file-stream (FileOutputStream. (str (:wal-dir config) "/igel.wal"))
        sync-window (or (:sync-window-time config) 200)
        out-stream (BufferedOutputStream. file-stream 4096)]
    (async/go-loop [prev-sync-time (System/currentTimeMillis)
                    comp-channels (transient #{})]
      (if-let [[k d comp-chan] (async/<! recv-chan)]
        ;; TODO: error handling
        (do
          (io/append-wal! out-stream [k d])
          (conj! comp-channels comp-chan)
          (recur
           (if (> (- (System/currentTimeMillis) prev-sync-time) sync-window)
             (do
               (-> file-stream .getChannel (.force true))
               (doseq [comp-chan comp-channels]
                 (async/>! comp-chan :done))
               (reset! comp-channels (transient #{}))
               (System/currentTimeMillis))
             prev-sync-time)
           comp-channels))
        (.close out-stream)))))
