#!/bin/bash -e

THIS_DIR="$(dirname $(readlink -f $0))"

cd $THIS_DIR/../docker

sudo docker-compose -f docker-compose-basic.yml build
sudo docker-compose -f docker-compose-kafka-2.yml up -d

n=0
while [ "$(sudo docker exec kafka_1 bash -c '/opt/kafka/bin/kafka-topics.sh --zookeeper zookeeper --list')" != '' ]; do
  if [ $n -gt 4 ]; then
    echo timeout
    exit 1
  fi
  n=$(( n + 1 ))
  sleep 1
done

sudo docker exec kafka_1 bash -c "/opt/kafka/bin/kafka-topics.sh --zookeeper zookeeper --create --partitions 1 --replication-factor 1 --topic brod-cli-produce-test"
sudo docker exec kafka_1 bash -c "/opt/kafka/bin/kafka-topics.sh --zookeeper zookeeper --create --partitions 1 --replication-factor 1 --topic brod-client-SUITE-topic"
sudo docker exec kafka_1 bash -c "/opt/kafka/bin/kafka-topics.sh --zookeeper zookeeper --create --partitions 1 --replication-factor 1 --topic brod_producer_SUITE"
sudo docker exec kafka_1 bash -c "/opt/kafka/bin/kafka-topics.sh --zookeeper zookeeper --create --partitions 1 --replication-factor 1 --topic brod_consumer_SUITE"

