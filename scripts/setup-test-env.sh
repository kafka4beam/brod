#!/bin/bash -eu

VERSION=${KAFKA_VERSION:-1.1}
if [ -z $VERSION ]; then VERSION=$1; fi

case $VERSION in
  0.9*)
    VERSION="0.9";;
  0.10*)
    VERSION="0.10";;
  0.11*)
    VERSION="0.11";;
  1.*)
    VERSION="1.1";;
  *)
    VERSION="1.1";;
esac

echo "Using KAFKA_VERSION=$VERSION"
export KAFKA_VERSION=$VERSION

TD="$(cd "$(dirname "$0")" && pwd)"

sudo KAFKA_VERSION=${KAFKA_VERSION} docker-compose -f $TD/docker-compose.yml down || true
sudo KAFKA_VERSION=${KAFKA_VERSION} docker-compose -f $TD/docker-compose.yml up -d

n=0
while [ "$(sudo docker exec kafka-1 bash -c '/opt/kafka/bin/kafka-topics.sh --zookeeper localhost --describe')" != '' ]; do
  if [ $n -gt 4 ]; then
    echo "timeout waiting for kakfa_1"
    exit 1
  fi
  n=$(( n + 1 ))
  sleep 1
done

function create_topic {
  TOPIC_NAME="$1"
  PARTITIONS="${2:-1}"
  REPLICAS="${3:-1}"
  CMD="/opt/kafka/bin/kafka-topics.sh --zookeeper localhost --create --partitions $PARTITIONS --replication-factor $REPLICAS --topic $TOPIC_NAME --config min.insync.replicas=1"
  sudo docker exec kafka-1 bash -c "$CMD"
}

create_topic "brod-client-SUITE-topic"
create_topic "brod_consumer_SUITE"
create_topic "brod_producer_SUITE"            2
create_topic "brod_topic_subscriber_SUITE"    3 2
create_topic "brod-group-subscriber-1"        3 2
create_topic "brod-group-subscriber-2"        3 2
create_topic "brod-group-subscriber-3"        3 2
create_topic "brod-group-subscriber-4"
create_topic "brod-demo-topic-subscriber"     3 2
create_topic "brod-demo-group-subscriber-koc" 3 2
create_topic "brod-demo-group-subscriber-loc" 3 2
create_topic "brod_compression_SUITE"
create_topic "lz4-test"
create_topic "test-topic"

# this is to warm-up kafka group coordinator for deterministic in tests
sudo docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --new-consumer --group test-group --describe > /dev/null 2>&1

# for kafka 0.11 or later, add sasl-scram test credentials
if [[ "$KAFKA_VERSION" != 0.9* ]] && [[ "$KAFKA_VERSION" != 0.10* ]]; then
  sudo docker exec kafka-1 /opt/kafka/bin/kafka-configs.sh --zookeeper localhost:2181 --alter --add-config 'SCRAM-SHA-256=[iterations=8192,password=ecila],SCRAM-SHA-512=[password=ecila]' --entity-type users --entity-name alice
fi
