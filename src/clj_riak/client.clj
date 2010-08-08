(ns clj-riak.client
  "Clojure Riak client."
  (:refer-clojure :exclude (get))
  (:require [clojure.contrib.json :as json])
  (:import (com.trifork.riak RiakClient RiakObject RiakLink
                             RequestMeta BucketProperties))
  (:import (com.trifork.riak.mapreduce MapReduceResponse))
  (:import (com.google.protobuf ByteString)))

(defn init
  "Initialize a Riak client.
   Required keys: :host, :port."
  [{:keys [#^String host #^Integer port]}]
  (RiakClient. host port))

(defn ping
  "Returns true if the server if reachable."
  [#^RiakClient rc]
  (.ping rc)
  true)

(defn get-server-info
  "Returns a map of information about the server.
   Keys: :node, :server-version."
  [#^RiakClient rc]
  (let [si (.getServerInfo rc)]
    {:node (.get si "node")
     :server-version (.get si "server_version")}))

(defn list-buckets
  "Returns a seq of Strings corresponding to the buckets in the cluster."
  [#^RiakClient rc]
  (map #(.toStringUtf8 #^ByteString %) (.listBuckets rc)))

(defn list-keys
  "Returns a seq of Strings corresponding to the keys in the given bucket."
  [#^RiakClient rc #^String bucket]
  (map #(.toStringUtf8 #^ByteString %)
       (.listKeys rc (ByteString/copyFromUtf8 bucket))))

(defn get-bucket-props
  "Returns a map of properties for the given bucket.
   Keys: :n-value, :allow-mult."
  [#^RiakClient rc #^String bucket]
  (let [bprops (.getBucketProperties rc (ByteString/copyFromUtf8 bucket))]
    {:n-value (.getNValue bprops)
     :allow-mult (.getAllowMult bprops)}))

(defn set-bucket-props
  "Sets properties for a given bucket, returning nil.
   Recognized props: :n-value, :allow-mult."
  [#^RiakClient rc #^String bucket props]
  (let [{:keys [n-value allow-mult]} props
        bprops (doto (BucketProperties.)
                 (.allowMult allow-mult)
                 (.nValue n-value))]
    (.setBucketProperties rc (ByteString/copyFromUtf8 bucket) bprops)))

(defn- parse-link [#^RiakLink rl]
  {:bucket (ByteString/copyFromUtf8 (.getBucket rl))
   :key (ByteString/copyFromUtf8 (.getKey rl))
   :tag (ByteString/copyFromUtf8 (.getTag rl))})

(defn- parse-object [#^RiakObject ro]
  {:vclock (.getVclock ro)
   :value (.toByteArray (.getValue ro))
   :content-type (.getContentType ro)
   :charset (.getCharset ro)
   :content-encoding (.getContentEncoding ro)
   :vtag (.getVtag ro)
   :last-mod (.getLastModified ro)
   :last-mod-usecs (.getLastModifiedUsec ro)
   :user-meta (into {} (seq (.getUserMeta ro)))
   :links (map parse-link (.getLinks ro)) })

(defn unparse-object [#^String bucket #^String key
                      {:keys [#^"[B" value content-type]}]
  (doto (RiakObject. bucket key value)
    (.setContentType content-type)))

(defn unparse-meta [{:keys [w dw return-body content-type]}]
  (let [rm (RequestMeta.)]
    (if w (.w rm w))
    (if dw (.dw rm dw))
    (if return-body (.returnBody rm return-body))
    (if content-type (.contentType rm content-type))
    rm))

(defn- parse-objects [results]
  (cond
    (empty? results)
      nil
    (= 1 (count results))
      (parse-object (first results))
    :else
      (map parse-object results)))

(defn get
  "Returns nil if the key was not found, an object map if an object was found
   without siblings, or a seq of maps if there are siblings.
   Recognized opts: :r.
   Object keys: :vclock, :value, :content-type, :charset, :content-encoding,
                :vtag, :last-mod, :last-mod-usecs, :user-meta, :links.
   Link keys: :bucket, :key, :tag."
  [#^RiakClient rc #^String bucket #^String key & [opts]]
    (parse-objects
      (if-let [#^Integer r (:r opts)]
        (.fetch rc bucket key r)
        (.fetch rc bucket key))))

(defn put
  "Store an object in the given bucket at the given key.
   Returns nil, a map, or a seq as for get.
   Recobnized obj keys: :value, :content-type
   Recognized opts: :w, :dw, :return-body."
  [#^RiakClient rc #^String bucket #^String key obj & [opts]]
  (let [#^RiakObject ro (unparse-object bucket key obj)
        #^RequestMeta rm (unparse-meta opts)]
    (parse-objects (.store rc ro rm))))

(defn delete
  "Deletes the object from the given bucket at the given key, returning nil.
   Recognized opts: :rw."
  [#^RiakClient rc #^String bucket #^String key & [opts]]
  (if-let [#^Integer rw (:rw opts)]
    (.delete rc bucket key rw)
    (.delete rc bucket key)))

(defn- unparse-map-reduce-response [#^MapReduceResponse mr-response]
  (if-let [content (.getContent mr-response)]
    (.toStringUtf8 content)))

(defn map-reduce
  "Returns the results of a JavaScript map-reduce query.
   The query can be expressed as either a JSON string or a Clojure data
   structure corresponding to JSON."
  [#^RiakClient rc query]
  (let [mr-result (.mapReduce rc
                    (if (string? query) query (json/json-str query))
                    (unparse-meta {:content-type "application/json"}))]
    (remove nil? (map unparse-map-reduce-response mr-result))))
