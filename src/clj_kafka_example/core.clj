(ns clj-kafka-example.core
  (:require [clj-kafka-example.producer :as producer]
            [clj-kafka-example.consumer :as consumer])
  (:gen-class))

(defn -main
  [& args]
  (when (some #{"consumer"} args)
    (consumer/poll! (second args)))
  (when (some #{"producer"} args)
    @(producer/send! (second args) (nth args 2 "value"))))
