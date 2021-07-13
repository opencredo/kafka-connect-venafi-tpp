# we start from a Kafka image as the connector is in Kafka distribution
FROM wurstmeister/kafka:2.12-2.3.0

# we copy the venafy connector in the container
COPY target/kafka-connect-venafi-20.4-fat.jar ./venafi-connector/kafka-connect-venafi.jar

# we replace the default connect-standalone.properties so we can properly resolve to our local Kafka docker development
COPY integration/connect-standalone.properties /opt/kafka/config/

COPY integration/venafi-source-connector.properties /opt/kafka/config/

# we replace the start command creating a connector instead of starting a kafka broker.
COPY integration/start-kafka.sh /usr/bin/

# permissions
RUN chmod a+x /usr/bin/start-kafka.sh