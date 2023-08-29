(ns igel.wal
  (:require [clojure.core.async :as async]
            [igel.io :as io])
  (:import (java.io FileOutputStream BufferedOutputStream)))

(defn spawn-wal-writer
  [recv-chan config]
  (let [file-stream (FileOutputStream. (str (:wal-dir config) "/igel.wal"))
        out-stream (BufferedOutputStream. file-stream 4096)
        sync-window (or (:sync-window-time config) 200)
        window-chan (async/timeout sync-window)]
    ;; TODO: error handling
    (async/go-loop [comp-channels (transient #{})]
      (let [channels (async/alt!
                       recv-chan ([[k d comp-chan] :as ret]
                                  ;; when nil, this writer will be closed
                                  (when-not (nil? ret)
                                    (io/append-wal! out-stream [k d])
                                    (conj! comp-channels comp-chan)))
                       window-chan ([]
                                    (if (> (count comp-channels) 0)
                                      (do
                                        (-> file-stream
                                            .getChannel
                                            (.force true))
                                        (mapv #(async/>!! % :done)
                                              (persistent! comp-channels))
                                        (transient #{}))
                                      comp-channels)))]
        (if (nil? channels)
          (do
            (-> file-stream .getChannel (.force true))
            (.close out-stream))
          (recur channels))))))
