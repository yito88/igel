(ns igel.core-test
  (:require [clojure.test :refer :all]
            [igel.core :as igel]
            [igel.data :as data]))

(def NUM_ITEMS 256)

(deftest sequencial-crud-test
  (let [config-path nil
        kvs (igel/gen-kvs config-path)]
    ;; insert
    (doseq [i (range 0 NUM_ITEMS)]
      (let [k (.getBytes (str "key" i))
            v (.getBytes (str "val" i))]
        (igel/write! kvs k v)))
    ;; delete
    (doseq [i (range 0 NUM_ITEMS)]
      (when (zero? (mod i 16))
        (let [k (.getBytes (str "key" i))]
          (igel/delete! kvs k))))
    ;; update
    (doseq [i (range 0 NUM_ITEMS)]
      (when (zero? (mod i 32))
        (let [k (.getBytes (str "key" i))
              v (.getBytes (str "overwritten-val" i))]
          (igel/write! kvs k v))))
    ;; select
    (doseq [i (range 0 NUM_ITEMS)]
      (let [k (.getBytes (str "key" i))
            expected (cond
                       (zero? (mod i 32)) (.getBytes (str "overwritten-val" i))
                       (zero? (mod i 16)) nil
                       :else (.getBytes (str "val" i)))
            actual (igel/select kvs k)]
        (testing
         (str "The result of `select` is wrong: "
              "expected: " (if (nil? expected)
                             "nil"
                             (String. expected))
              "actual: " (if (nil? actual)
                           "nil"
                           (String. actual)))
          (data/byte-array-equals? expected actual))))
    ;; scan
    (doseq [group (partition 16 (sort-by str (range 0 NUM_ITEMS)))]
      (let [from-key (.getBytes (str "key" (first group)))
            to-key (.getBytes (str "key" (last group) 0))
            expect (filter #(not (nil? %))
                           (for [i group]
                             (cond
                               (zero? (mod i 32)) [(.getBytes (str "key" i))
                                                   (.getBytes
                                                    (str "overwritten-val" i))]
                               (zero? (mod i 16)) nil
                               :else [(.getBytes (str "key" i))
                                      (.getBytes (str "val" i))])))
            expect-results (map (fn [[k v]] [(String. k) (String. v)]) expect)
            actual (igel/scan kvs from-key to-key)
            actual-results (map (fn [[k v]] [(String. k) (String. v)]) actual)]
        (testing
         (str "The number of results was wrong:"
              "\n  expected: " expect-results
              "\n  actual: " actual-results)
          (= (count expect) (count actual)))

        (testing
         (str "Some results were wrong:"
              "\n  expected: " expect-results
              "\n  actual: " actual-results)
          (every? true?
                  (map
                   (fn [[k1 v1] [k2 v2]]
                     (and (data/byte-array-equals? k1 k2)
                          (data/byte-array-equals? v1 v2)))
                   expect
                   actual)))))))
