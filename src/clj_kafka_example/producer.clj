(ns clj-kafka-example.producer
  (:import (org.apache.kafka.clients.producer KafkaProducer ProducerConfig ProducerRecord)
           (org.apache.kafka.common.serialization StringSerializer)
           (org.apache.kafka.clients.admin NewTopic AdminClient AdminClientConfig)
           (org.apache.kafka.common.errors TopicExistsException)
           (java.util UUID)))


(def properties
  {ProducerConfig/BOOTSTRAP_SERVERS_CONFIG "127.0.0.1:9092"
   ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG (.getName StringSerializer)
   ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG (.getName StringSerializer)})

(defn send!
  "this returns a future"
  [topic value]
  (let [producer (new KafkaProducer properties)
        key (str (UUID/randomUUID))
        record (new ProducerRecord topic key value)]
    (.send producer record)))

(defn create-topic!
  [topic partitions replication config]
  (let [client (AdminClient/create config)]
    (try
      (.createTopics client [(NewTopic. topic partitions replication)])
      ;; ignore exception
      (catch TopicExistsException e nil)
      (finally
        (.close client)))))
