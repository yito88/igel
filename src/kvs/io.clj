(ns kvs.io
  (:require [blossom.core :as blossom]
            [clojure.java.io :as io]
            [clojure.data.fressian :as fressian]
            [kvs.memtable :as memtable])
  (:import (java.io FileOutputStream BufferedOutputStream)
           (java.nio ByteBuffer)
           (java.util.zip CRC32)))

(defn serialize-long
  [value]
  (let [buf (ByteBuffer/allocate Long/BYTES)]
    (.putLong buf value)
    (.array buf)))

(defn calc-crc32 [data]
  (let [crc32 (CRC32.)]
    (.update crc32 data)
    (.getValue crc32)))

(defn make-sstable-dir
  [sstable-dir]
  (let [dir (io/file sstable-dir)]
    (when-not (and (.exists dir) (.isDirectory dir))
      (.mkdirs dir))))

; Data format in an SSTable
; | Key0 | Value0 | Key1 | Value1 | ...
; - Each key or value's data format
; | Length (8 bytes) | Data (Length bytes) | CRC (4 byters) |

(def ^:private ^:const LEN_SIZE Long/BYTES)

(defn flush!
  [memtable file-path]
  (let [bf (blossom/make-filter {:hash-size "SHA-256" :size 1024})]
    (with-open [out-stream (BufferedOutputStream. (FileOutputStream. file-path) 16384)]
      (doseq [entry (memtable/entry-set memtable)]
        (let [k (.getKey entry)
              v (.getValue entry)]
          (doto out-stream
            (.write (serialize-long (count k)))
            (.write k)
            (.write (calc-crc32 k))
            (.write (serialize-long (count v)))
            (.write v)
            (.write (calc-crc32 v)))
          (blossom/add bf k))))
    bf))


(defn- read-data [in-stream]
  (let [buf (make-array Byte/TYPE LEN_SIZE)
        read-len (.read in-stream buf 0 LEN_SIZE)]
    (when (= read-len LEN_SIZE)
      (let [data-len (fressian/read buf)
            buf (make-array Byte/TYPE data-len)
            read-len (.read in-stream buf 0 data-len)]
        (when (= read-len data-len)
          buf)))))

(defn- read-kv-pair
  [in-stream]
  [(read-data in-stream) (read-data in-stream)])

(defn read-value
  [file-path target-key]
  (with-open [in-stream (clojure.java.io/input-stream file-path)]
    (loop []
      (let [[k v] (read-kv-pair in-stream)]
        (if (= k target-key)
          v
          (if (nil? k) nil (recur)))))))
