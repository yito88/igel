(ns igel.store)

(defprotocol IStore
  "Interface for the data store"
  (select [this ^bytes k])
  (scan [this ^bytes from-key ^bytes to-key])
  (write! [this ^bytes k ^bytes v])
  (delete! [this ^bytes k]))
