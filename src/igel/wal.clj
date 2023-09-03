(ns igel.wal
  (:require [clojure.core.async :as async]
            [igel.io :as io])
  (:import (java.io FileOutputStream BufferedOutputStream)))

(def ^:const ^:private DEFAULT_WINDOW_TIME 200)

(defn spawn-wal-writer
  [data-chan end-chan config]
  (let [file-stream (FileOutputStream. (str (:wal-dir config) "/igel.wal"))
        out-stream (BufferedOutputStream. file-stream 4096)
        sync-window (or (:sync-window-time config) DEFAULT_WINDOW_TIME)
        window-chan (async/timeout sync-window)]
    ;; TODO: error handling
    (async/go-loop [comp-channels (transient #{})]
      (let [channels (async/alt!
                       data-chan ([[k d comp-chan] :as ret]
                                  ;; when nil, this writer will be closed
                                  (when-not (nil? ret)
                                    (io/append-wal! out-stream [k d])
                                    (-> file-stream .getChannel (.force true))
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
            (.close out-stream)
            (doseq [ch (persistent! comp-channels)] (async/close! ch))
            (async/close! end-chan))
          (recur channels))))))
