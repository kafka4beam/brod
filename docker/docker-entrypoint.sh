#!/bin/bash -xe

## run something other than zookeeper and kafka
if [ "$1" != "run" ]; then
  exec "$@"
fi

## run zookeeper
if [ "$2" = "zookeeper" ]; then
  /opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties
  exit $?
fi

if [ "$2" != "kafka" ]; then
  echo "unknown target to run: $2"
  exit 1
fi

## run kafka

prop_file="/etc/kafka/server.properties"

if [ ! -z "$BROKER_ID" ]; then
  echo "broker id: $BROKER_ID"
  sed -r -i "s/^(broker.id)=(.*)/\1=$BROKER_ID/g" $prop_file
fi

if [ ! -z "$KAFKA_PORT" ]; then
  echo "port: $KAFKA_PORT"
  sed -r -i "s/^(listeners)=(.*)/\1=PLAINTEXT:\/\/:$KAFKA_PORT/g" $prop_file
fi

/opt/kafka/bin/kafka-server-start.sh $prop_file

