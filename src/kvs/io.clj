(ns kvs.io
  (:require [blossom.core :as blossom]
            [clojure.tools.logging :as logging]
            [clojure.java.io :as io]
            [kvs.data :as data]
            [kvs.memtable :as memtable])
  (:import (java.io FileOutputStream BufferedOutputStream)
           (java.nio ByteBuffer)
           (java.util.zip CRC32)))

(defn- serialize-long
  [value]
  (let [buf (ByteBuffer/allocate Long/BYTES)]
    (.putLong buf value)
    (.array buf)))

(defn- deserialize-long
  [bytes]
  (.getLong (ByteBuffer/wrap bytes)))

(defn- calc-crc32 [data]
  (let [crc32 (CRC32.)]
    (.update crc32 data)
    (.getValue crc32)))

(defn- valid-data? [data crc]
  (let [crc32 (doto (CRC32.) (.update data))]
    (= (.getValue crc32) crc)))

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
(def ^:private ^:const CRC_SIZE Long/BYTES)

(defn flush!
  [memtable file-path]
  (let [bf (blossom/make-filter {:hash-size "SHA-256" :size 1024})]
    (logging/info "Starting flush to SSTable" file-path)
    (with-open [out-stream (BufferedOutputStream. (FileOutputStream. file-path) 16384)]
      (doseq [entry (memtable/entry-set memtable)]
        (let [k (.getKey entry)
              data (.getValue entry)
              value (:value data)]
          ;; write the key
          (doto out-stream
            (.write (serialize-long (count k)))
            (.write k)
            (.write (serialize-long (calc-crc32 k))))
          ;; write the value
          ;; if it's deleted, write only the length 0
          (if (:deleted? data)
            (.write out-stream (serialize-long 0))
            (doto out-stream
              (.write (serialize-long (count value)))
              (.write value)
              (.write (serialize-long (calc-crc32 value)))))
          (blossom/add bf k))))
    bf))

(defn- read-data [in-stream]
  (let [buf (make-array Byte/TYPE LEN_SIZE)
        read-len (.read in-stream buf 0 LEN_SIZE)]
    (when (= read-len LEN_SIZE)
      (let [data-len (deserialize-long buf)
            buf (make-array Byte/TYPE data-len)
            read-len (.read in-stream buf 0 data-len)
            crc-buf (make-array Byte/TYPE CRC_SIZE)
            crc-len (.read in-stream crc-buf 0 CRC_SIZE)]
        (if (zero? data-len)
          nil ;; the data was deleted
          ;; TODO throw an exception when unexpected length
          (when (and (= read-len data-len)
                     (= crc-len CRC_SIZE)
                     (valid-data? buf (deserialize-long crc-buf)))
            buf))))))

(defn- read-kv-pair
  [in-stream]
  [(read-data in-stream) (read-data in-stream)])

(defn read-value
  [file-path target-key]
  (with-open [in-stream (clojure.java.io/input-stream file-path)]
    (loop []
      (let [[k v] (read-kv-pair in-stream)]
        (if (java.util.Arrays/equals k target-key)
          (if (nil? v)
            (data/deleted-data)
            (data/new-data v))
          (if (nil? k) nil (recur)))))))
