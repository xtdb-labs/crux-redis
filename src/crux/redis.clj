(ns ^:no-doc crux.redis
  "Remote Redis KV backend for Crux."
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [crux.kv :as kv]
            [crux.memory :as mem]
            [taoensso.carmine :as car :refer (wcar)]
            [taoensso.nippy :as nippy]
            [crux.codec :as c]
            [crux.system :as sys])
  (:import java.io.Closeable
           java.nio.ByteBuffer
           org.agrona.concurrent.UnsafeBuffer
           ))

(defn- v->buffer [v]
  (when (some? v)
    (mem/as-buffer v)))

(defrecord RedisKvIterator [zrange-batch-size conn cursor]
  kv/KvIterator
  (seek [this k]
    (reset! cursor
            (when (bytes? (mem/->on-heap k))
              (map
               v->buffer
                (wcar
                 conn
                 (car/parse-raw
                  (car/zrangebylex
                   0
                   (car/raw
                    (byte-array
                     (mapcat seq [(byte-array [91])
                                  (mem/->on-heap k)])))
                   "+"
                   :limit 0 zrange-batch-size))))))
    (first @cursor))

  (next [this]
    (if-let [n (second @cursor)]
      (do
        (swap! cursor (partial drop 1))
        n)
      (first
       (swap! cursor
            (fn [curr]
              (map
               v->buffer
               (wcar
                conn
                (car/parse-raw
                 (car/zrangebylex
                  0
                  (car/raw
                   (byte-array
                    (mapcat seq [(byte-array [40])
                                 (mem/->on-heap (first curr))])))
                  "+"
                  :limit 0 zrange-batch-size)))))))))

  (prev [this]
    (swap! cursor
           (fn [curr]
             (map
              v->buffer
               (wcar
                conn
                (car/parse-raw
                 (car/zrevrangebylex
                  0
                  (car/raw
                   (byte-array
                    (mapcat seq [(byte-array [40])
                                 (mem/->on-heap (first curr))])))
                  "-"
                  :limit 0 1))))))
    (first @cursor))

  (value [this]
    (let [k (mem/->on-heap (first @cursor))]
      (when-not (empty? k)
        (v->buffer
         (wcar
          conn
          (car/parse-raw
           (car/get (car/raw k))))))))

  Closeable
  (close [_]))

(defrecord RedisKvSnapshot [zrange-batch-size conn]
  kv/KvSnapshot
  (new-iterator [_]
    (->RedisKvIterator zrange-batch-size conn (atom nil)))

  (get-value [_ k]
    (when (some? k)
      (v->buffer
       (wcar
        conn
        (car/parse-raw
         (car/get (car/raw (mem/->on-heap k))))))))

  Closeable
  (close [_]))

(defrecord RedisKv [zrange-batch-size conn]
  kv/KvStore
  (new-snapshot [_]
    (->RedisKvSnapshot zrange-batch-size conn))

  (store [_ kvs]
    (wcar
     conn
     (apply car/zadd 0
            (reduce
             (fn [p [k v]]
               (let [k (mem/->on-heap k)]
                 (apply conj p
                        (when (bytes? k)
                          (let [kr (car/raw k)]
                            (car/set
                             kr
                             (car/raw (mem/->on-heap v)))
                            [0 kr]
                            )))))
             []
             kvs)))
    nil)

  (delete [_ ks]
    (let [ks (keep #(and (bytes? %) (not-empty %)) (map mem/->on-heap ks))]
        (wcar
         conn
         (apply car/del (doall (for [k ks] (car/raw k))))
         (apply car/zrem 0 (doall (for [k ks] (car/raw k)))))
      nil))

  (compact [_]
    (log/debug "Using `compact` on Redis has no effect."))

  (fsync [_]
    (log/debug "Using `fsync` on Redis has no effect."))

  ;; TODO discuss, also in light of https://github.com/juxt/crux/pull/1028
  #_(backup [_ dir]
    (log/debug "Using `backup` on Redis has no effect."))

  (count-keys [_]
    (wcar
     conn
     (car/zcard 0)))

  (db-dir [_]
    (log/debug "Using `db-dir` on Redis has no effect."))

  (kv-name [this]
    (.getName (class this)))

  Closeable
  (close [_]))

(defn ->kv-store {::sys/deps {}
                  ::sys/args {:conn {:doc "Redis Carmine connection config map"
                                     :spec #(map? %)
                                     :default {:pool {} :spec {:uri "redis://localhost:6379/"}}}
                              :zrange-batch-size {:doc "zrangebylex batch size"
                                                  :spec #(int? %)
                                                  :default 10}}} [options]
  (map->RedisKv options))

;; TODO using something faster than atoms

;; TODO investigate/document consistency under eviction

;; TODO investigate deployment patterns for single writer

;; TODO could ideally skip zadd for stats and last-tx-id (and anything else changing rapidly that doesn't require ordering?)

;; TODO could split the zranges

;; <1B triples limit due to 2^32-1 zset limit and general key limit for a single node, RESP-compatible alternatives (e.g. ardb) promise higher-single node limits

;; TODO investigate behaviour at limits
