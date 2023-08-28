(ns igel.io
  (:require [blossom.core :as blossom]
            [clojure.tools.logging :as logging]
            [clojure.java.io :as io]
            [igel.data :as data]
            [igel.memtable :as memtable])
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

(defn- write-bytes!
  [^BufferedOutputStream out-stream ^bytes b]
  (doto out-stream
    (.write (serialize-long (count b)))
    (.write b)
    (.write (serialize-long (calc-crc32 b)))))

(defn flush!
  [memtable file-path]
  (let [bf (blossom/make-filter {:hash-size "SHA-256" :size 1024})
        entry-set (memtable/entry-set memtable)
        head-key (-> entry-set first first)
        tail-key (-> entry-set last first)
        file-stream (FileOutputStream. file-path)]
    (logging/info "Starting flush to SSTable" file-path head-key tail-key)
    (with-open [out-stream (BufferedOutputStream. file-stream 16384)]
      (doseq [entry entry-set]
        (let [k (first entry)
              data (second entry)
              value (:value data)]
          ;; write the key
          (write-bytes! out-stream k)
          ;; write the value
          ;; if it's deleted, write only the length 0
          (if (:deleted? data)
            (.write out-stream (serialize-long 0))
            (write-bytes! out-stream value))
          (blossom/add bf k)))
      ;; fsync
      (-> file-stream .getChannel (.force true)))
    [bf head-key tail-key]))

(defn append-wal!
  [^BufferedOutputStream out-stream [^bytes k ^data/Data data]]
  (write-bytes! out-stream k)
  (if (:deleted? data)
    (.write out-stream (serialize-long 0))
    (write-bytes! out-stream (:value data))))

(defn- read-data!
  "Return the byte-array of the data segment from the input stream.
  If the data length is zero, it returns nil."
  [in-stream]
  (let [buf (make-array Byte/TYPE LEN_SIZE)
        read-len (.read in-stream buf 0 LEN_SIZE)
        data-len (deserialize-long buf)]
    (when (and (= read-len LEN_SIZE) (> data-len 0))
      (let [buf (make-array Byte/TYPE data-len)
            read-len (.read in-stream buf 0 data-len)
            crc-buf (make-array Byte/TYPE CRC_SIZE)
            crc-len (.read in-stream crc-buf 0 CRC_SIZE)]
        ;; TODO throw an exception when unexpected length or crc error
        (when (and (= read-len data-len)
                   (= crc-len CRC_SIZE)
                   (valid-data? buf (deserialize-long crc-buf)))
          buf)))))

(defn- read-kv-pair!
  [in-stream]
  [(read-data! in-stream) (read-data! in-stream)])

(defn read-value
  [file-path target-key]
  (with-open [in-stream (clojure.java.io/input-stream file-path)]
    (loop []
      (let [[k v] (read-kv-pair! in-stream)]
        (if (data/byte-array-equals? k target-key)
          (if (nil? v)
            (data/deleted-data)
            (data/new-data v))
          (if (nil? k) nil (recur)))))))

(defn scan-pairs
  [file-path from-key to-key]
  (with-open [in-stream (clojure.java.io/input-stream file-path)]
    (loop [pairs (transient [])]
      (let [[k v] (read-kv-pair! in-stream)
            data (cond
                   (and (seq k) (seq v)) (data/new-data v)
                   (and (seq k) (nil? v)) (data/deleted-data)
                   :else nil)]
        (if (nil? k)
          (persistent! pairs)
          (if (data/byte-array-smaller-or-equal? to-key k)
            (persistent! pairs)
            (recur
             (if (data/byte-array-smaller-or-equal? from-key k)
               (conj! pairs [k data])
               pairs))))))))
