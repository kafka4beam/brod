#!/bin/bash -eu

if [ -n "${DEBUG:-}" ]; then
    set -x
fi

docker ps > /dev/null || {
    echo "You must be a member of docker group to run this script"
    exit 1
}

function docker_compose() {
    if command -v docker-compose ; then
        docker-compose $@
    else
        docker compose version &> /dev/null
        if [ $? -eq 0 ]; then
            docker compose $@
        else
            exit "couldn't find docker compose, needed for testing"
        fi
    fi
}

KAFKA_VERSION="${KAFKA_VERSION:${1:-4.0.0}}"

case $KAFKA_VERSION in
  0.9*)
    KAFKA_VERSION="0.9"
    ;;
  0.10*)
    KAFKA_VERSION="0.10"
    ;;
  0.11*)
    KAFKA_VERSION="0.11"
    ;;
  1.*)
    KAFKA_VERSION="1.1"
    ;;
  2.*)
    KAFKA_VERSION="2.8"
    ;;
  3.*)
    KAFKA_VERSION="3.9"
    ;;
  4.*)
    KAFKA_VERSION="4.0"
    ;;
  *)
    echo "Unsupported version $KAFKA_VERSION"
    exit 1
    ;;
esac

export KAFKA_IMAGE_VERSION="1.1-${KAFKA_VERSION}"
echo "env KAFKA_IMAGE_VERSION=$KAFKA_IMAGE_VERSION"

KAFKA_MAJOR=$(echo "$KAFKA_VERSION" | cut -d. -f1)
if [ "$KAFKA_MAJOR" -lt 3 ]; then
    NEED_ZOOKEEPER=true
else
    NEED_ZOOKEEPER=false
fi

function bootstrap_opts() {
  local port="${1:-:9092}"
  if [[ "$NEED_ZOOKEEPER" = true ]]; then
    echo "--zookeeper localhost:2181"
  else
    echo "--bootstrap-server localhost${port}"
  fi
}
TD="$(cd "$(dirname "$0")" && pwd)"

docker_compose -f $TD/docker-compose.yml down || true
docker_compose -f $TD/docker-compose-kraft.yml down || true

if [[ "$NEED_ZOOKEEPER" = true ]]; then
  docker_compose -f $TD/docker-compose.yml up -d
else
  docker_compose -f $TD/docker-compose-kraft.yml up -d
fi

# give kafka some time
sleep 5

MAX_WAIT_SEC=10

function wait_for_kafka() {
  local which_kafka="$1"
  local n=0
  local port=':9092'
  local topic_list listener
  if [ "$which_kafka" = 'kafka-2' ]; then
    port=':9192'
  fi
  while true; do
    listener="$(netstat -tnlp 2>&1 | grep $port || true)"
    if [ "$listener" != '' ]; then
      cmd="opt/kafka/bin/kafka-topics.sh $(bootstrap_opts $port) --list"
      topic_list="$(docker exec $which_kafka $cmd 2>&1)"
      if [ "${topic_list-}" = '' ]; then
          break
      fi
    fi
    if [ $n -gt $MAX_WAIT_SEC ]; then
      echo "timeout waiting for kafka-1"
      echo "last print: ${topic_list:-}"
      exit 1
    fi
    n=$(( n + 1 ))
    sleep 1
  done
}

wait_for_kafka kafka-1
wait_for_kafka kafka-2

function create_topic() {
  TOPIC_NAME="$1"
  PARTITIONS="${2:-1}"
  REPLICAS="${3:-1}"
  CMD="/opt/kafka/bin/kafka-topics.sh $(bootstrap_opts) --create --partitions $PARTITIONS --replication-factor $REPLICAS --topic $TOPIC_NAME --config min.insync.replicas=1"
  docker exec kafka-1 bash -c "$CMD"
}

create_topic "dummy" || true
create_topic "brod_SUITE"
create_topic "brod-client-SUITE-topic"
create_topic "brod_consumer_SUITE"
create_topic "brod_producer_SUITE"            2
create_topic "brod-group-coordinator"         3 2
create_topic "brod-group-coordinator-1"       3 2
create_topic "brod-demo-topic-subscriber"     3 2
create_topic "brod-demo-group-subscriber-koc" 3 2
create_topic "brod-demo-group-subscriber-loc" 3 2
create_topic "brod_txn_SUITE_1" 3 2
create_topic "brod_txn_SUITE_2" 3 2
create_topic "brod_txn_subscriber_input" 3 2
create_topic "brod_txn_subscriber_output_1" 3 2
create_topic "brod_txn_subscriber_output_2" 3 2
create_topic "brod_compression_SUITE"
create_topic "lz4-test"
create_topic "test-topic"

if [ "$KAFKA_MAJOR" -ge 2 ]; then
  MAYBE_NEW_CONSUMER=""
else
  MAYBE_NEW_CONSUMER="--new-consumer"
fi
# this is to warm-up kafka group coordinator for tests
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 $MAYBE_NEW_CONSUMER --group test-group --describe > /dev/null 2>&1 || true

# for kafka 0.11 or later, add sasl-scram test credentials
if [[ "$KAFKA_VERSION" != 0.9* ]] && [[ "$KAFKA_VERSION" != 0.10* ]]; then
  docker exec kafka-1 /opt/kafka/bin/kafka-configs.sh \
    $(bootstrap_opts) \
    --alter \
    --add-config 'SCRAM-SHA-256=[iterations=8192,password=ecila]' \
    --entity-type users \
    --entity-name alice

  docker exec kafka-1 /opt/kafka/bin/kafka-configs.sh \
    $(bootstrap_opts) \
    --alter \
    --add-config 'SCRAM-SHA-512=[password=ecila]' \
    --entity-type users \
    --entity-name alice
fi
