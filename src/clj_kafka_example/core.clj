(ns clj-kafka-example.core
  (:require [clj-kafka-example.producer])
  (:gen-class))

(defn -main
  [& args]
  (clj-kafka-example.producer/create-topic-if-not-exist "topic")
  (println "Hello, World!"))
