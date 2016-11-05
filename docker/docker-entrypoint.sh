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

ipwithnetmask="$(ip -f inet addr show dev eth0 | awk '/inet / { print $2 }')"
ipaddress="${ipwithnetmask%/*}"

sed -r -i "s/^(advertised.listeners)=(.*)/\1=PLAINTEXT:\/\/$ipaddress:$PLAINTEXT_PORT,SSL:\/\/$ipaddress:$SSL_PORT/g" $prop_file
sed -r -i "s/^(listeners)=(.*)/\1=PLAINTEXT:\/\/:$PLAINTEXT_PORT,SSL:\/\/:$SSL_PORT/g" $prop_file

/opt/kafka/bin/kafka-server-start.sh $prop_file

