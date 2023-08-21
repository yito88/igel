(ns igel.store)

(defprotocol IStoreRead
  "Interface for reading from the data store"
  (select [this ^bytes k])
  (scan [this ^bytes from-key ^bytes to-key]))

(defprotocol IStoreMutate
  "Interface for mutations to the data store"
  (write! [this ^bytes k ^bytes v])
  (delete! [this ^bytes k]))

(defprotocol IFlush
  "Flush the memtable and update KVS state"
  (flush! [this]))
