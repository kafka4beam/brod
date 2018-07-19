FROM java:openjdk-8-jre

RUN apt-get update && \
    apt-get install -y zookeeper wget supervisor dnsutils && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get clean

ENV SCALA_VERSION 2.11

ARG KAFKA_VERSION
ENV KAFKA_VERSION ${KAFKA_VERSION}

RUN case $KAFKA_VERSION in \
      0.9*) \
        DOWNLOAD_URL_PREFIX="https://archive.apache.org/dist/kafka";; \
      *) \
        DOWNLOAD_URL_PREFIX="https://apache.org/dist/kafka";; \
    esac && \
    KAFKA_DOWNLOAD_URL="$DOWNLOAD_URL_PREFIX/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" && \
    wget -q "${KAFKA_DOWNLOAD_URL}" -O /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz && \
    tar xfz /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz -C /opt && \
    rm /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz && \
    ln -s /opt/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION" /opt/kafka && \
    mkdir -p /etc/kafka

COPY docker-entrypoint.sh /docker-entrypoint.sh
COPY server.properties /etc/kafka/server.properties
COPY server.jks /etc/kafka/server.jks
COPY truststore.jks /etc/kafka/truststore.jks
COPY jaas-plain.conf /etc/kafka/jaas-plain.conf
COPY jaas-plain-scram.conf /etc/kafka/jaas-plain-scram.conf

ENTRYPOINT ["/docker-entrypoint.sh"]

## run kafka by default, 'run zookeeper' to start zookeeper instead
CMD ["run", "kafka"]

