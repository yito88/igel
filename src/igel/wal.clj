(ns igel.wal
  (:require [clojure.core.async :as async]
            [igel.data :as data]
            [igel.io :as io]
            [igel.store :as store]))

(defn spawn-wal-writer
  [wal-path recv-chan signal-chan]
  (let [file-stream (FileOutputStream. wal-path)]
    (with-open [out-stream (BufferedOutputStream. file-stream 4096)]
      (async/go-loop []
        (io/append-wal! out-stream (async/<! recv-chan))
          ;; TODO: wait for the minimum period instead of sync after every write
          ;; fsync
        (-> file-stream .getChannel (.force true))
        (async/>! signal-chan :sync)
        (recur)))))
