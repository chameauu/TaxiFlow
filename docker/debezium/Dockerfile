# Debezium's stock image ships Apicurio, not the Confluent Avro converter.
# Add the converter and its Confluent dependencies to an isolated plugin directory
# so the Kafka Connect worker can use Schema Registry without replacing the
# Debezium connector plugins.
FROM confluentinc/cp-schema-registry:7.5.0 AS confluent

FROM debezium/connect:2.5.1.Final

USER root

COPY --from=confluent /usr/share/java/schema-registry /tmp/confluent

RUN set -eux; \
    mkdir -p /kafka/connect/confluent-avro; \
    find /tmp/confluent -maxdepth 1 -type f -name '*.jar' \
        ! -name 'kafka-clients-*' \
        ! -name 'kafka_2.13-*' \
        ! -name 'kafka-server-common-*' \
        ! -name 'kafka-metadata-*' \
        ! -name 'kafka-group-coordinator-*' \
        ! -name 'kafka-storage-*' \
        ! -name 'kafka-raft-*' \
        ! -name 'kafka-tools-api-*' \
        ! -name 'zookeeper*' \
        ! -name 'netty*' \
        ! -name 'scala-*' \
        ! -name 'jackson-module-scala*' \
        ! -name 'jersey-*' \
        ! -name 'kafka-schema-registry-7.5.0.jar' \
        ! -name 'kafka-json-schema-provider-*' \
        ! -name 'kafka-protobuf-*' \
        ! -name 'dek-*' \
        ! -name 'guice*' \
        ! -name 'governator*' \
        ! -name 'javapoet*' \
        ! -name 'kotlin*' \
        ! -name 'wire-*' \
        ! -name 'protobuf-*' \
        ! -name 'proto-google-*' \
        ! -name 'tink-*' \
        ! -name 'jose4j-*' \
        ! -name 'classgraph-*' \
        ! -name 'jopt-simple-*' \
        ! -name 'minimal-json-*' \
        ! -name 're2j-*' \
        ! -name 'metrics-core-2.2.0*' \
        ! -name 'slf4j-*' \
        ! -name 'reload4j-*' \
        -exec cp {} /kafka/connect/confluent-avro/ \; ; \
    base=https://packages.confluent.io/maven/io/confluent; \
    curl -fsSL -o /kafka/connect/confluent-avro/kafka-connect-avro-converter-7.5.0.jar \
        "$base/kafka-connect-avro-converter/7.5.0/kafka-connect-avro-converter-7.5.0.jar"; \
    curl -fsSL -o /kafka/connect/confluent-avro/kafka-connect-avro-data-7.5.0.jar \
        "$base/kafka-connect-avro-data/7.5.0/kafka-connect-avro-data-7.5.0.jar"; \
    curl -fsSL -o /kafka/connect/confluent-avro/kafka-schema-converter-7.5.0.jar \
        "$base/kafka-schema-converter/7.5.0/kafka-schema-converter-7.5.0.jar"; \
    curl -fsSL -o /kafka/connect/confluent-avro/kafka-schema-serializer-7.5.0.jar \
        "$base/kafka-schema-serializer/7.5.0/kafka-schema-serializer-7.5.0.jar"; \
    curl -fsSL -o /kafka/connect/confluent-avro/kafka-avro-serializer-7.5.0.jar \
        "$base/kafka-avro-serializer/7.5.0/kafka-avro-serializer-7.5.0.jar"; \
    echo "5df1917cfc5a67a4c02d1cb76d715c892a8de1429069c897fb8fa3237c8c05f5  /kafka/connect/confluent-avro/kafka-connect-avro-converter-7.5.0.jar" | sha256sum -c -; \
    echo "929c0893c919e72e5c8e241280432d921db0f82b94c4294d674717f85310aeaf  /kafka/connect/confluent-avro/kafka-connect-avro-data-7.5.0.jar" | sha256sum -c -; \
    echo "8399ecbf1196da4375623eceb6590654c2f84a1cc786768fa02273e19de5be79  /kafka/connect/confluent-avro/kafka-schema-converter-7.5.0.jar" | sha256sum -c -; \
    echo "2bbca3b6bb8ec65a61f326ce5207621c1fc7bbdd1e945a6a7b8a9385aa81d463  /kafka/connect/confluent-avro/kafka-schema-serializer-7.5.0.jar" | sha256sum -c -; \
    echo "76139b6e0cbb8cda0b9d95f9b4e4bb8c7a540a1e8ed48642acadfc1c3302b13a  /kafka/connect/confluent-avro/kafka-avro-serializer-7.5.0.jar" | sha256sum -c -; \
    chown -R kafka:kafka /kafka/connect/confluent-avro; \
    rm -rf /tmp/confluent

USER kafka

LABEL org.opencontainers.image.title="TaxiFlow Debezium" \
      org.opencontainers.image.description="Debezium Connect with Confluent Avro converter"
