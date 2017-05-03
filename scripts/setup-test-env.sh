#!/bin/bash -e

THIS_DIR="$(cd "$(dirname "$0")" && pwd)"

cd $THIS_DIR/../docker

sudo docker-compose -f docker-compose-kafka-2.yml down || true
sudo docker-compose -f docker-compose-basic.yml build
sudo docker-compose -f docker-compose-kafka-2.yml up -d

n=0
while [ "$(sudo docker exec kafka_1 bash -c '/opt/kafka/bin/kafka-topics.sh --zookeeper zookeeper --list')" != '' ]; do
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
  CMD="/opt/kafka/bin/kafka-topics.sh --zookeeper zookeeper --create --partitions $PARTITIONS --replication-factor $REPLICAS --topic $TOPIC_NAME"
  sudo docker exec kafka_1 bash -c "$CMD"
}

create_topic "brod-client-SUITE-topic"
create_topic "brod_consumer_SUITE"
create_topic "brod_producer_SUITE"            2
create_topic "brod_topic_subscriber_SUITE"    3 2
create_topic "brod-group-subscriber-1"        3 2
create_topic "brod-group-subscriber-2"        3 2
create_topic "brod-group-subscriber-3"        3 2
create_topic "brod-demo-topic-subscriber"     3 2
create_topic "brod-demo-group-subscriber-koc" 3 2
create_topic "brod-demo-group-subscriber-loc" 3 2
create_topic "brod_compression_SUITE"
create_topic "test-topic"

