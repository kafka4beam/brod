#!/bin/bash -xe

## run something other than zookeeper and kafka
if [ "$1" != "run" ]; then
  exec "$@"
fi

## run zookeeper
if [ "$2" = "zookeeper" ]; then
  exec /opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties
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

if [[ "$KAFKA_VERSION" = 0.9* ]]; then
  sed -r -i "s/^(advertised.listeners)=(.*)/\1=PLAINTEXT:\/\/$ipaddress:$PLAINTEXT_PORT,SSL:\/\/$ipaddress:$SSL_PORT/g" $prop_file
  sed -r -i "s/^(listeners)=(.*)/\1=PLAINTEXT:\/\/:$PLAINTEXT_PORT,SSL:\/\/:$SSL_PORT/g" $prop_file
else
  sed -r -i "s/^(advertised.listeners)=(.*)/\1=PLAINTEXT:\/\/$ipaddress:$PLAINTEXT_PORT,SSL:\/\/$ipaddress:$SSL_PORT,SASL_SSL:\/\/$ipaddress:$SASL_SSL_PORT,SASL_PLAINTEXT:\/\/$ipaddress:$SASL_PLAINTEXT_PORT/g" $prop_file
  sed -r -i "s/^(listeners)=(.*)/\1=PLAINTEXT:\/\/:$PLAINTEXT_PORT,SSL:\/\/:$SSL_PORT,SASL_SSL:\/\/:$SASL_SSL_PORT,SASL_PLAINTEXT:\/\/:$SASL_PLAINTEXT_PORT/g" $prop_file
  echo "sasl.enabled.mechanisms=PLAIN" >> $prop_file
fi

echo "sasl.enabled.mechanisms=PLAIN,SCRAM-SHA-256,SCRAM-SHA-512" >> $prop_file
echo "offsets.topic.replication.factor=1" >> $prop_file
echo "transaction.state.log.min.isr=1" >> $prop_file
echo "transaction.state.log.replication.factor=1" >> $prop_file

if [[ "$KAFKA_VERSION" = 0.9* ]]; then
  JAAS_CONF=""
elif [[ "$KAFKA_VERSION" = 0.10* ]]; then
  JAAS_CONF="-Djava.security.auth.login.config=/etc/kafka/jaas-plain.conf"
else
  JAAS_CONF="-Djava.security.auth.login.config=/etc/kafka/jaas-plain-scram.conf"
fi
#-Djavax.net.debug=all
KAFKA_HEAP_OPTS="-Xmx1G -Xms1G $JAAS_CONF" /opt/kafka/bin/kafka-server-start.sh $prop_file

