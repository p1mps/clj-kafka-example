(ns clj-kafka-example.consumer
  (:import (org.apache.kafka.clients.consumer ConsumerConfig ConsumerRecords KafkaConsumer)
           (org.apache.kafka.common.serialization StringDeserializer)
           (java.time Duration)
           (java.util UUID)))


(def properties
  {ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG "127.0.0.1:9092"
   ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG (.getName StringDeserializer)
   ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG (.getName StringDeserializer)
   ConsumerConfig/GROUP_ID_CONFIG "consumer-group"
   ConsumerConfig/CLIENT_ID_CONFIG (str (UUID/randomUUID))})

(def consumer (new KafkaConsumer properties))

(defn consume []
  (let [records (.poll consumer (Duration/ofSeconds (long 2)))]
    (doseq [r records]
      (println r))))

(defn poll!
  [topic]
  (.subscribe consumer [topic])
  (doall (repeatedly consume)))
