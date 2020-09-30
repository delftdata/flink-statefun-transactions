FROM martijn-thesis-flink-statefun:latest

RUN mkdir -p /opt/statefun/modules/test

COPY module.yaml /opt/statefun/modules/test/
COPY kafka_egress/target/kafka_egress*jar /opt/statefun/modules/test/
