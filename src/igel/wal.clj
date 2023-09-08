(ns igel.wal
  (:require [clojure.core.async :as async]
            [igel.io :as io])
  (:import (java.io FileOutputStream BufferedOutputStream)))

(def ^:const ^:private DEFAULT_WINDOW_TIME 200)

(defn spawn-wal-writer
  [data-chan flush-req-chan flush-wal-chan switch-chan config]
  (async/go-loop [wal-index 0
                  data-chan data-chan]
    (let [file-stream (FileOutputStream. (str (:wal-dir config) \/ wal-index ".wal"))
          out-stream (BufferedOutputStream. file-stream 4096)
          sync-window (or (:sync-window-time config) DEFAULT_WINDOW_TIME)
          window-chan (async/timeout sync-window)]
      ;; WAL loop until a flush is completed
      ;; TODO: error handling
      (loop [comp-channels (transient #{})]
        (let [channels (async/alt!
                         switch-chan ([]
                                          (println "DEBUG: closing wal loop for switching")
                                      (-> file-stream .getFD .sync)
                                      (.close out-stream)
                                      (doseq [ch (persistent! comp-channels)]
                                        (async/>! ch :done))
                                      (async/>! flush-req-chan :flush))
                         data-chan ([[k d comp-chan]]
                                    (println "DEBUG:" (nil? k))
                                    (io/append-wal! out-stream [k d])
                                    (conj! comp-channels comp-chan))
                         window-chan ([]
                                      (if (> (count comp-channels) 0)
                                        (do
                                          (-> file-stream .getFD .sync)
                                          (mapv #(async/>!! % :done)
                                                (persistent! comp-channels))
                                          (println "DEBUG: sync-window")
                                          (async/>! flush-req-chan :try-flush)
                                          (transient #{}))
                                        comp-channels)))]
          (when-not (nil? channels)
            (recur channels)))))
    (let [data-chan (async/<! flush-wal-chan)]
      ;; the memtable has been switched
      (recur (inc wal-index) data-chan))))
