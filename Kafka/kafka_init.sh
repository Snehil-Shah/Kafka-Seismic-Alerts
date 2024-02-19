#!/bin/bash

# Start the Kafka server
../../../etc/confluent/docker/run &

# Create the topics
../../bin/kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic minor_seismic_events
../../bin/kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic severe_seismic_events

# Keep the script running
tail -f /dev/null