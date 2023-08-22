(ns igel.data
  (:import (java.util Comparator)))

(defrecord Data [value deleted?])

(defn new-data
  [^bytes value]
  (->Data value false))

(defn deleted-data
  []
  (->Data nil true))

(defn is-valid?
  [^Data data]
  (not (:deleted? data)))

(defn byte-array-comparator
  []
  (reify Comparator
    (compare [_ a b]
      (loop [i 0]
        (if (< i (min (count a) (count b)))
          (let [cmp (compare (aget a i) (aget b i))]
            (if (zero? cmp)
              (recur (inc i))
              cmp))
          (compare (count a) (count b)))))))

(defn byte-array-smaller?
  "lhs < rhs"
  [^bytes lhs ^bytes rhs]
  (= (.compare (byte-array-comparator) lhs rhs) -1))

(defn byte-array-smaller-or-equal?
  "lhs <= rhs"
  [^bytes lhs ^bytes rhs]
  (<= (.compare (byte-array-comparator) lhs rhs) 0))

(defn byte-array-equals?
  [^bytes lhs ^bytes rhs]
  (= (.compare (byte-array-comparator) lhs rhs) 0))
