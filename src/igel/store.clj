(ns igel.store)

(defprotocol IStoreRead
  "Interface for reading from the data store"
  ;; select could return three value types; the valid data, the deleted data,
  ;; and nil.
  ;; The deleted data is a tombstone, and nil means that the key doesn't exist.
  (select [this ^bytes k])
  (scan [this ^bytes from-key ^bytes to-key]))

(defprotocol IStoreMutate
  "Interface for mutations to the data store"
  (write! [this ^bytes k ^bytes v])
  (delete! [this ^bytes k]))

(defprotocol IFlush
  "Flush the memtable and update KVS state"
  (flush! [this]))
