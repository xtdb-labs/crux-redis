(ns crux-redis.kv-test
  (:require [clojure.test :as t]
            [clojure.test.check.clojure-test :as tcct]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [crux.codec :as c]
            [crux.io :as cio]
            [crux.kv :as kv]
            [crux.memory :as mem]
            [crux.system :as sys]
            [crux.api :as crux]
            [clojure.java.io :as io]
            [taoensso.carmine :as car :refer (wcar)])
  (:import java.nio.ByteOrder
           org.agrona.concurrent.UnsafeBuffer
           (crux.api ICruxAPI)
           (java.io Closeable File)
           (redis.embedded RedisServer)))

;; Tests _copied_ and lightly adapted from crux.kv-test...

;; TODO remove tmp files for redis executable else disk gets used up quickly

(defn with-redis [f]
  (let [retry (fn retry
                [retries f & args]
                (let [res (try {:value (apply f args)}
                               (catch Exception e
                                 (if (zero? retries)
                                   (throw e)
                                   {:exception e})))]
                  (if (:exception res)
                    (recur (dec retries) f args)
                    (:value res))))
        kv (RedisServer. 6378)]
    (try
      (do
        (.start ^RedisServer kv)
        (retry 500 (fn [] ;; .start and .stop are not sufficiently synchronous
                    (Thread/sleep 1)
                    (car/wcar {:pool {} :spec {:uri "redis://localhost:6378/"}} (car/ping))))
        #_(car/wcar {:pool {} :spec {:uri "redis://localhost:6378/"}}
                  (car/flushdb))
        (f))
      (finally
        (.stop ^RedisServer kv)))))

(def ^:dynamic *kv*)
(def ^:dynamic *kv-opts* {:crux/module 'crux.redis/->kv-store, :conn {:pool {} :spec {:uri "redis://localhost:6378/"}}})

(defn with-kv-store* [f]
  (with-redis #(let [db-dir (cio/create-tmpdir "kv-store")]
                 (try
                   (with-open [kv (-> (sys/prep-system
                                        {:kv-store (merge (when-let [db-dir-suffix (:db-dir-suffix *kv-opts*)]
                                                            {:db-dir (io/file db-dir db-dir-suffix)})
                                                          *kv-opts*)})
                                      (sys/start-system)
                                      )]
                     (binding [*kv* (:kv-store kv)]
                       (f *kv*)))
                   (finally
                     (cio/delete-dir db-dir))))))

(defmacro with-kv-store [bindings & body]
  `(with-kv-store* (fn [~@bindings] ~@body)))

(defn with-silent-test-check [f]
                        (binding [tcct/*report-completion* false]
                          (f)))

;; TODO: These helpers convert back and forth to bytes, would be good
;; to get rid of this, but that requires changing removing the byte
;; arrays above in the tests. The tested code uses buffers internally.

(defn seek [kvs k]
  (with-open [snapshot (kv/new-snapshot kvs)
              i (kv/new-iterator snapshot)]
    (when-let [k (kv/seek i k)]
      [(mem/->on-heap k) (mem/->on-heap (kv/value i))])))

(defn value [kvs seek-k]
  (with-open [snapshot (kv/new-snapshot kvs)]
    (some-> (kv/get-value snapshot seek-k)
            (mem/->on-heap))))

(defn seek-and-iterate [kvs key-pred seek-k]
  (with-open [snapshot (kv/new-snapshot kvs)
              i (kv/new-iterator snapshot)]
    (loop [acc (transient [])
           k (kv/seek i seek-k)]
      (let [k (when k
                (mem/->on-heap k))]
        (if (and k (key-pred k))
          (recur (conj! acc [k (mem/->on-heap (kv/value i))])
                 (kv/next i))
          (persistent! acc))))))

(defn long->bytes ^bytes [^long l]
  (let [ub (UnsafeBuffer. (byte-array Long/BYTES))]
    (.putLong ub 0 l ByteOrder/BIG_ENDIAN)
    (.byteArray ub)))

(defn bytes->long ^long [^bytes data]
  (let [ub (UnsafeBuffer. data)]
    (.getLong ub 0 ByteOrder/BIG_ENDIAN)))

(defn compare-bytes
  (^long [^bytes a ^bytes b]
   (mem/compare-buffers (mem/as-buffer a) (mem/as-buffer b)))
  (^long [^bytes a ^bytes b max-length]
   (mem/compare-buffers (mem/as-buffer a) (mem/as-buffer b) max-length)))

(defn bytes=?
  ([^bytes a ^bytes b]
   (mem/buffers=? (mem/as-buffer a) (mem/as-buffer b)))
  ([^bytes a ^bytes b ^long max-length]
   (mem/buffers=? (mem/as-buffer a) (mem/as-buffer b) max-length)))

(def server1-conn {:pool {} :spec {:uri "redis://localhost:6378/"}})
(defmacro wcar* [& body] `(car/wcar server1-conn ~@body))

(t/deftest test-store-and-value []
  (with-kv-store [kv-store]
    (t/testing "store, retrieve and seek value"
      (kv/store kv-store [[(long->bytes 1) (.getBytes "Crux")]])
      (t/is (= "Crux" (String. ^bytes (value kv-store (long->bytes 1)))))
      (t/is (= [1 "Crux"] (let [[k v] (seek kv-store (long->bytes 1))]
                            [(bytes->long k) (String. ^bytes v)]))))

    (t/testing "non existing key"
      (t/is (nil? (value kv-store (long->bytes 2)))))))

(t/deftest test-can-store-and-delete-all-116 []
  (with-kv-store [kv-store]
    (let [number-of-entries 500]
      (kv/store kv-store (map (fn [i]
                                [(long->bytes i) (long->bytes (inc i))])
                              (range number-of-entries)))
      (doseq [i (range number-of-entries)]
        (t/is (= (inc i) (bytes->long (value kv-store (long->bytes i))))))

      (t/testing "deleting all keys in random order, including non existent keys"
        (kv/delete kv-store (for [i (shuffle (range (long (* number-of-entries 1.2))))]
                              (long->bytes i)))
        (doseq [i (range number-of-entries)]
          (t/is (nil? (value kv-store (long->bytes i)))))))))

(t/deftest test-seek-and-iterate-range []
  (with-kv-store [kv-store]
    (doseq [[^String k v] {"a" 1 "b" 2 "c" 3 "d" 4}]
      (kv/store kv-store [[(.getBytes k) (long->bytes v)]]))

    (t/testing "seek range is exclusive"
      (t/is (= [["b" 2] ["c" 3]]
               (for [[^bytes k v] (seek-and-iterate kv-store
                                                    #(neg? (compare-bytes % (.getBytes "d")))
                                                    (.getBytes "b"))]
                 [(String. k) (bytes->long v)]))))

    (t/testing "seek range after existing keys returns empty"
      (t/is (= [] (seek-and-iterate kv-store #(neg? (compare-bytes % (.getBytes "d"))) (.getBytes "d"))))
      (t/is (= [] (seek-and-iterate kv-store #(neg? (compare-bytes % (.getBytes "f")%)) (.getBytes "e")))))

    (t/testing "seek range before existing keys returns keys at start"
      (t/is (= [["a" 1]] (for [[^bytes k v] (into [] (seek-and-iterate kv-store #(neg? (compare-bytes % (.getBytes "b"))) (.getBytes "0")))]
                           [(String. k) (bytes->long v)]))))))

(t/deftest test-seek-between-keys []
  (with-kv-store [kv-store]
    (doseq [[^String k v] {"a" 1 "c" 3}]
      (kv/store kv-store [[(.getBytes k) (long->bytes v)]]))

    (t/testing "seek returns next valid key"
      (t/is (= ["c" 3]
               (let [[^bytes k v] (seek kv-store (.getBytes "b"))]
                 [(String. k) (bytes->long v)]))))))

(t/deftest test-seek-and-iterate-prefix []
  (with-kv-store [kv-store]
    (doseq [[^String k v] {"aa" 1 "b" 2 "bb" 3 "bcc" 4 "bd" 5 "dd" 6}]
      (kv/store kv-store [[(.getBytes k) (long->bytes v)]]))

    (t/testing "seek within bounded prefix returns all matching keys"
      (t/is (= [["b" 2] ["bb" 3] ["bcc" 4] ["bd" 5]]
               (for [[^bytes k v] (into [] (seek-and-iterate kv-store #(bytes=? (.getBytes "b") % (alength (.getBytes "b"))) (.getBytes "b")))]
                 [(String. k) (bytes->long v)]))))

    (t/testing "seek within bounded prefix before or after existing keys returns empty"
      (t/is (= [] (into [] (seek-and-iterate kv-store (partial bytes=? (.getBytes "0")) (.getBytes "0")))))
      (t/is (= [] (into [] (seek-and-iterate kv-store (partial bytes=? (.getBytes "e")) (.getBytes "0"))))))))

(t/deftest test-delete-keys []
  (with-kv-store [kv-store]
    (t/testing "store, retrieve and delete value"
      (kv/store kv-store [[(long->bytes 1) (.getBytes "Crux")]])
      (t/is (= "Crux" (String. ^bytes (value kv-store (long->bytes 1)))))
      (kv/delete kv-store [(long->bytes 1)])
      (t/is (nil? (value kv-store (long->bytes 1))))
      (t/testing "deleting non existing key is noop"
        (kv/delete kv-store [(long->bytes 1)])))))

;; TODO discuss, also in light of https://github.com/juxt/crux/pull/1028
#_(t/deftest test-backup-and-restore-db
  (let [backup-dir (cio/create-tmpdir "kv-store-backup")]
    (try
      (kv/store *kv* [[(long->bytes 1) (.getBytes "Crux")]])
      (cio/delete-dir backup-dir)
      (kv/backup *kv* backup-dir)
      (with-open [restored-kv (start-kv-store {:crux.kv/db-dir (str backup-dir)})]
        (t/is (= "Crux" (String. ^bytes (value restored-kv (long->bytes 1)))))

        (t/testing "backup and original are different"
          (kv/store *kv* [[(long->bytes 1) (.getBytes "Original")]])
          (kv/store restored-kv [[(long->bytes 1) (.getBytes "Backup")]])
          (t/is (= "Original" (String. ^bytes (value *kv* (long->bytes 1)))))
          (t/is (= "Backup" (String. ^bytes (value restored-kv (long->bytes 1)))))))
      (finally
        (cio/delete-dir backup-dir)))))

(t/deftest test-compact []
  (with-kv-store [kv-store]
    (t/testing "store, retrieve and delete value"
      (kv/store kv-store [[(.getBytes "key-with-a-long-prefix-1") (.getBytes "Crux")]])
      (kv/store kv-store [[(.getBytes "key-with-a-long-prefix-2") (.getBytes "is")]])
      (kv/store kv-store [[(.getBytes "key-with-a-long-prefix-3") (.getBytes "awesome")]])
      (t/testing "compacting"
        (kv/compact kv-store))
      (t/is (= "Crux" (String. ^bytes (value kv-store (.getBytes "key-with-a-long-prefix-1")))))
      (t/is (= "is" (String. ^bytes (value kv-store (.getBytes "key-with-a-long-prefix-2")))))
      (t/is (= "awesome" (String. ^bytes (value kv-store (.getBytes "key-with-a-long-prefix-3"))))))))

(t/deftest test-sanity-check-can-start-with-sync-enabled
  (binding [*kv-opts* (merge *kv-opts* {:sync? true})]
    (with-kv-store [kv-store]
      (kv/store kv-store [[(long->bytes 1) (.getBytes "Crux")]])
      (t/is (= "Crux" (String. ^bytes (value kv-store (long->bytes 1))))))))

(t/deftest test-sanity-check-can-fsync
  (with-kv-store [kv-store]
    (kv/store kv-store [[(long->bytes 1) (.getBytes "Crux")]])
    (kv/fsync kv-store)
    (t/is (= "Crux" (String. ^bytes (value kv-store (long->bytes 1)))))))

(t/deftest test-can-get-from-snapshot
  (with-kv-store [kv-store]
    (kv/store kv-store [[(long->bytes 1) (.getBytes "Crux")]])
    (with-open [snapshot (kv/new-snapshot kv-store)]
      (t/is (= "Crux" (String. (mem/->on-heap (kv/get-value snapshot (long->bytes 1))))))
      (t/is (nil? (kv/get-value snapshot (long->bytes 2)))))))

(t/deftest test-can-read-write-concurrently
  (with-kv-store [kv-store]
    (let [w-fs (for [_ (range 128)]
                 (future
                   (kv/store kv-store [[(long->bytes 1) (.getBytes "Crux")]])))]
      @(first w-fs)
      (let [r-fs (for [_ (range 128)]
                   (future
                     (String. ^bytes (value kv-store (long->bytes 1)))))]
        (mapv deref w-fs)
        (doseq [r-f r-fs]
          (t/is (= "Crux" @r-f)))))))

(t/deftest test-prev-and-next []
  (with-kv-store [kv-store]
    (doseq [[^String k v] {"a" 1 "c" 3}]
      (kv/store kv-store [[(.getBytes k) (long->bytes v)]]))

    (with-open [snapshot (kv/new-snapshot kv-store)
                i (kv/new-iterator snapshot)]
      (t/testing "seek returns next valid key"
        (let [k (kv/seek i (mem/as-buffer (.getBytes "b")))]
          (t/is (= ["c" 3] [(String. (mem/->on-heap k)) (bytes->long (mem/->on-heap (kv/value i)))]))))
      (t/testing "prev, iterators aren't bidirectional"
        (t/is (= "a" (String. (mem/->on-heap (kv/prev i)))))
        (t/is (nil? (kv/prev i)))))))

(tcct/defspec test-basic-generative-store-and-get-value 20
  (prop/for-all [kvs (gen/not-empty (gen/map
                                     gen/simple-type-printable
                                     gen/int {:num-elements 100}))]
                (let [kvs (->> (for [[k v] kvs]
                                 [(c/->value-buffer k)
                                  (c/->value-buffer v)])
                               (into {}))]
                  (with-kv-store [kv-store]
                    #_(when (= *kv-module* 'crux.kv.redis/kv)
                      (wcar* (car/flushdb)))
                    (kv/store kv-store kvs)
                    (with-open [snapshot (kv/new-snapshot kv-store)]
                      (->> (for [[k v] kvs]
                             (= v (kv/get-value snapshot k)))
                           (every? true?)))))))

(defn bytes=?= [a b]
  (or (= a b) (and (some? a) (some? b) (bytes=? a b))))

(tcct/defspec test-generative-kv-store-commands 20
  (prop/for-all [commands (gen/let [ks (gen/not-empty (gen/vector gen/simple-type-printable))]
                            (gen/not-empty (gen/vector
                                            (gen/one-of
                                             [(gen/tuple
                                               (gen/return :get-value)
                                               (gen/elements ks))
                                              (gen/tuple
                                               (gen/return :seek)
                                               (gen/elements ks))
                                              (gen/tuple
                                               (gen/return :seek+value)
                                               (gen/elements ks))
                                              (gen/tuple
                                               (gen/return :seek+nexts)
                                               (gen/elements ks))
                                              (gen/tuple
                                               (gen/return :seek+prevs)
                                               (gen/elements ks))
                                              (gen/tuple
                                               (gen/return :fsync))
                                              (gen/tuple
                                               (gen/return :delete)
                                               (gen/elements ks))
                                              (gen/tuple
                                               (gen/return :store)
                                               (gen/elements ks)
                                               gen/int)]))))]
                (with-kv-store [kv-store]
                  (let [expected (->> (reductions
                                       (fn [[state] [op k v :as command]]
                                         (case op
                                           :get-value [state (get state (c/->value-buffer k))]
                                           :seek [state (ffirst (subseq state >= (c/->value-buffer k)))]
                                           :seek+value [state (second (first (subseq state >= (c/->value-buffer k))))]
                                           :seek+nexts [state (subseq state >= (c/->value-buffer k))]
                                           :seek+prevs [state (some->> (subseq state >= (c/->value-buffer k))
                                                                       (ffirst)
                                                                       (rsubseq state <= ))]
                                           :fsync [state]
                                           :delete [(dissoc state (c/->value-buffer k))]
                                           :store [(assoc state
                                                          (c/->value-buffer k)
                                                          (c/->value-buffer v))]))
                                       [(sorted-map-by mem/buffer-comparator)]
                                       commands)
                                      (rest)
                                      (map second))]
                    (->> (for [[[op k v :as command] expected] (map vector commands expected)]
                           (= expected
                              (case op
                                :get-value (with-open [snapshot (kv/new-snapshot kv-store)]
                                             (kv/get-value snapshot (c/->value-buffer k)))
                                :seek (with-open [snapshot (kv/new-snapshot kv-store)
                                                  i (kv/new-iterator snapshot)]
                                        (kv/seek i (c/->value-buffer k)))
                                :seek+value (with-open [snapshot (kv/new-snapshot kv-store)
                                                        i (kv/new-iterator snapshot)]
                                              (when (kv/seek i (c/->value-buffer k))
                                                (kv/value i)))
                                :seek+nexts (with-open [snapshot (kv/new-snapshot kv-store)
                                                        i (kv/new-iterator snapshot)]
                                              (when-let [k (kv/seek i (c/->value-buffer k))]
                                                (cons [(mem/copy-to-unpooled-buffer (mem/->off-heap k)) ;; TODO, should what's coming out of seek / next be off-heap automatically?
                                                       (mem/copy-to-unpooled-buffer (mem/->off-heap (kv/value i)))]
                                                      (->> (repeatedly
                                                            (fn []
                                                              (when-let [k (kv/next i)]
                                                                [(mem/copy-to-unpooled-buffer (mem/->off-heap k))
                                                                 (mem/copy-to-unpooled-buffer (mem/->off-heap (kv/value i)))])))
                                                           (take-while identity)
                                                           (vec)))))
                                :seek+prevs (with-open [snapshot (kv/new-snapshot kv-store)
                                                        i (kv/new-iterator snapshot)]
                                              (when-let [k (kv/seek i (c/->value-buffer k))]
                                                (cons [(mem/copy-to-unpooled-buffer (mem/->off-heap k))
                                                       (mem/copy-to-unpooled-buffer (mem/->off-heap (kv/value i)))]
                                                      (->> (repeatedly
                                                            (fn []
                                                              (when-let [k (kv/prev i)]
                                                                [(mem/copy-to-unpooled-buffer (mem/->off-heap k))
                                                                 (mem/copy-to-unpooled-buffer (mem/->off-heap (kv/value i)))])))
                                                           (take-while identity)
                                                           (vec)))))
                                :fsync (kv/fsync kv-store)
                                :delete (kv/delete kv-store [(c/->value-buffer k)])
                                :store (kv/store kv-store
                                                 [[(c/->value-buffer k)
                                                   (c/->value-buffer v)]]))))
                         (every? true?))))))

(comment
(t/deftest test-performance-off-heap
  (if (and (Boolean/parseBoolean (System/getenv "CRUX_KV_PERFORMANCE"))
           (if-let [backend (System/getenv "CRUX_KV_PERFORMANCE_BACKEND")]
             (= backend (str (:crux/module *kv-opts*)))
             true))
    (with-kv-store [kv-store]
      (let [n 1000000
            ks (vec (for [n (range n)]
                      (mem/->off-heap (.getBytes (format "%020x" n)))))]
        (println (:crux/module *kv-opts*) "off-heap")
        (t/is (= n (count ks)))
        (t/is (mem/off-heap? (first ks)))

        (System/gc)
        (println "Writing")
        (time
         (kv/store kv-store (for [k ks]
                              [k k])))

        (System/gc)
        (println "Reading")
        (time
         (do (dotimes [_ 10]
               (time
                ;; TODO: Note, the cached decorator still uses
                ;; bytes, so we grab the underlying kv store.
                (with-open [snapshot (kv/new-snapshot (:kv kv-store))
                            i (kv/new-iterator snapshot)]
                  (dotimes [idx n]
                    (let [idx (- (dec n) idx)
                          k (get ks idx)]
                      (assert (mem/buffers=? k (kv/seek i k)))
                      (assert (mem/buffers=? k (kv/value i))))))))
             (println "Done")))
        (println)))
    (t/is true)))

)

(comment

  (def dev-node-dir "redis-watdiv-bench")
  (def mynode (crux.api/start-node
                {:crux/index-store {:kv-store {:crux/module
                                               'crux.redis/->kv-store
                                               ;; 'crux.rocksdb/->kv-store
                                               :db-dir (str dev-node-dir "-kv")
                                               :disable-wal? true}}
                 :crux/document-store {:kv-store {:crux/module `rocks/->kv-store, :db-dir (io/file dev-node-dir "documents") :disable-wal? true}}
                 :crux/tx-log {:kv-store {:crux/module `rocks/->kv-store, :db-dir (io/file dev-node-dir "tx-log") :disable-wal? true}}}))
  (.close mynode)
  (crux/status mynode)

  (crux.bench.watdiv-crux/run-watdiv-bench mynode {:test-count 30})

  ;; for fast watdiv ingestion, always disable save config
  ;; ➜  ~ redis-cli config get save
  ;; "900 1 300 10 60 10000"
  ;; ➜  ~ redis-cli config set save ""
  ;; OK
  ;; ➜  ~ redis-cli config get save
  ;; ""

  ;; 127.0.0.1:6379> zcount 0 -inf +inf
  ;; (integer) 38731558

  ;; 127.0.0.1:6379> save
  ;; OK
  ;; (624.82s)
  ;; 5.8 GB

  (map
   (fn [a b]
     (if (and (= (:result-count a)
                 (:result-count b))
              (some? (:result-count a)))
       [(< (:time-taken-ms a)
           (:time-taken-ms b)
           30000)
        (:time-taken-ms a)
        (:time-taken-ms b)
        (+ 0.0 (/ (:time-taken-ms b)
                  (:time-taken-ms a)))]
       [:crux-failed
        (:query-idx a)
        (:result-count a) 
        (:result-count b)]))
   (sort-by :query-idx
            (drop 2 rocksperf)
            )
   (sort-by :query-idx
            redisperf
            ))

  )
