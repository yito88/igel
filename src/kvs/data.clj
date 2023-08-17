(ns kvs.data)

(defrecord Data [value deleted?])

(defn new-data
  [value]
  (->Data value false))

(defn deleted-data
  []
  (->Data nil false))
