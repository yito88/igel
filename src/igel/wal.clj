(ns igel.wal
  (:require [clojure.core.async :as async]
            [igel.data :as data]
            [igel.io :as io]
            [igel.store :as store])
  (:import (java.io FileOutputStream BufferedOutputStream)))

(defn spawn-wal-writer
  [recv-chan config]
  (let [file-stream (FileOutputStream. (str (:wal-dir config) "/igel.wal"))
        out-stream (BufferedOutputStream. file-stream 4096)
        sync-window (or (:sync-window-time config) 200)
        window-chan (async/timeout sync-window)]
    ;; TODO: error handling
    (async/go-loop [comp-channels (transient #{})]
      (let [state (async/alt!
                    recv-chan ([[k d comp-chan] :as ret]
                               (if (nil? ret)
                                 (do
                                   (-> file-stream .getChannel (.force true))
                                   :closed)
                                 (do
                                   (io/append-wal! out-stream [k d])
                                   (conj! comp-channels comp-chan)
                                   :open)))
                    window-chan ([]
                                 (if (> (count comp-channels) 0)
                                   (do
                                     (-> file-stream .getChannel (.force true))
                                     (mapv #(async/>!! % :done)
                                           (persistent! comp-channels))
                                     :sync)
                                   :open)))]
        (condp = state
          :closed (.close out-stream)
          :sync (recur (transient #{}))
          (recur comp-channels))))))
