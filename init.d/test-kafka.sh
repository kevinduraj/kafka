#!/bin/bash

# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka Broker
bin/kafka-server-start.sh config/server.properties

# Create a Topic
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

# List of topics
bin/kafka-topics.sh --list --zookeeper localhost:2181

# Start producer
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test

# Start Consumer
bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic test --from-beginning

