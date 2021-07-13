# kafka-connect-venafi
[![Build Status](https://travis-ci.com/opencredo/kafka-connect-venafi.svg?branch=master)](https://travis-ci.com/opencredo/kafka-connect-venafi)

kafka-connect-venafi is a [Kafka connector](http://kafka.apache.org/documentation.html#connect) for Venafi Trust Protection Platform security events.

| Connector Version | Source Technology Version | Confluent Platorm Version |   
| --- | --- | --- |  
| 0.9.X | Venafi Trust Protection Platform 18.X | Compatible Confluent Platform Version - ≥5.0.X | 
| 20.4  | Venafi Trust Protection Platform 20.4 | Compatible Confluent Platform Version - ≥5.0.X | 
---

Description
---
This connector connects via HTTP to your instance of the Venafi and pulls your Log events into Kafka, allowing you to do any filtering/transforming/processing you'd like to do within a comfortable Kafka environment.  
 **N.B.** Currently the connector starts from the beginning of time (i.e. processes all past events first), a future release will allow the option of starting from now (i.e. skipping all previous events).

###### EventLog object produced by this source Connector as defined by the Venafi WebSDK
* **ClientTimestamp**: The time that the client generated the event.
* **Component**: A string that identifies a component DN. For events that occur in a subsystem other than Config, such as a Secret Store, the Distinguished Name (DN) component may be blank.
* **ComponentId**: The component ID that originated the event.
* **ComponentSubsystem**: The component subsytem that originated the event.
* **Grouping**: An integer for the Component ID or Group Id that correlates to a set of events.
* **Id**: A hexadecimal value that corresponds to an Event ID.
* **Name**: The event name.
* **ServerTimestamp**: The time the Trust Protection Platform server received the event.
* **Severity**: A string value for one of these event severities: _Emergency, Alert, Critical, Error, Warning, Notice, Info, or Debug_
* **SourceIP**: The IP address of the host that originated the event.
* **Text1**: A string variable that contains all or part of an event message.
* **Text2**: An additional string, if present, that appears as part of an event message.
* **Value1**: An integer variable, if present, that appears as part of an event message.
* **Value2**: An additional integer variable, if present, that appears as part of an event message.

---



# Development
To manually install the connector:
1. Build the JAR with `mvn package`
2. Find the JAR in your target folder called venafi-log-connector-<version you're building>-fat.jar
3. Create a connect property file 
```
name=venafi
connector.class=com.opencredo.connect.venafi.tpp.log.TppLogSourceConnector
tasks.max=1
venafi.base.url=<your_base_url>
venafi.username=<your_api_username>
venafi.password=<your_api_password>
```
This is filled with the minimum values required, any default values are provided by the [config definition class](./src/main/java/com/opencredo/connect/venafi/tpp/log/TppLogSourceConfig.java). 
This can also be looked at for more information on configuration, or look at the [wiki on the config definitions.](https://github.com/opencredo/kafka-connect-venafi-tpp/wiki/Config-Definitions-explained.)
Make sure to replace the items with the <> brackets with what's needed to connect to your Venafi VEDSDK instance.   
There is a quickstart properties file included with the code [here](./config/source-quickstart.properties) which has some dummy values for your base Url, username & password but can be used as an example.  
4. Create a directory and place the JAR file in it, e.g. `<path-to-confluent>/share/java/kafka-connect-venafi-tpp`.  
5. Then call: `<path-to-confluent>/bin/confluent load venafi -d venafi.properties`  

For more information please look at the [Confluent instructions on manually installing connectors](https://docs.confluent.io/current/connect/managing/install.html#connect-install-connectors).

---
If you need to unload/reload the connector, use: `<path-to-confluent>/bin/confluent unload venafi`  
If you intend to change the JAR please stop, change the JAR, then start the cluster.  

# Useful commands while developing
```
(v5.2.3 and lower)
sudo bin/confluent start  
sudo bin/confluent status
sudo bin/confluent load venafi -d ~/venafi.properties 
sudo bin/confluent status venafi

sudo bin/kafka-topics --list --zookeeper localhost:2181
sudo bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic connect-offsets --from-beginning
sudo bin/kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic VENAFI-LOGS
sudo bin/confluent log connect
``` 

# Installing on Vanilla Kafka
1. Prepare the venafi-log-connector-<version you're building>-fat.jar using `mvn package`
2. locate your connect-quickstart.properties in your kafka installation, add the following line:
   `plugin.path=/path/to/target/kafka-connect-venafi-0.9.6-SNAPSHOT-fat.jar`
   note: while developing, it is also possible to point the `plugin.path` to your build directory
3. Start the Kafka connect process:
   `% $KAFKA_HOME/bin/connect-standalone.sh $KAFKA_HOME/config/connect-standalone.properties config/source-quickstart.properties`
   note: the `source-quickstart.properties` should be edited to contain your connection details.
  
  
  
You'll want to start the following in order:
1. Zookeeper:
kafka-connect-venafi/kafka-install/kafka_2.13-2.4.0/bin % ./zookeeper-server-start.sh ../config/zookeeper.properties
2. Kafka server:
kafka-connect-venafi/kafka-install/kafka_2.13-2.4.0/bin % ./kafka-server-start.sh ../config/server.properties
3. Kafka connect:
kafka-connect-venafi % kafka-install/kafka_2.13-2.4.0/bin/connect-standalone.sh kafka-install/kafka_2.13-2.4.0/config/connect-standalone.properties config/source-quickstart.properties
   
---
# Integration Test

If you want to run the integration test as a maven stage
1. Prepare the venafi-log-connector<version you're building>-fat.jar using `mvn package`
2. Run integration test by using `mvn verify`.This will spin up docker containers as defined in the integration/docker-compose.yaml file; Venafi's Connector configuration is also in the same folder, it connects to a mocked service.

You can also use the same docker-compose configuration to connect the Venafi Connector to the real service:
1. Run `mvn package` to generate the jar.
2. Update connection details (base.url, username, password) on `integration/venafi-source-connector.properties` 
3. Run `docker-compose -f integration/docker-compose.yml up [-d]`
4. Run `docker exec -it <your kafka container name> /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server <your kafka container name>:9092 --topic VENAFI-LOGS --from-beginning` to verify that the EventLogs are written to kafka

---
# Config Definitions explained.

``venafi.base.url``
  URL to API

  * Type: string
  * Valid Values: non-empty string and no ISO control characters
  * Importance: high

``venafi.password``
  The password to use with the API.

  * Type: string
  * Importance: high

``venafi.username``
  The username to use with the API.

  * Type: string
  * Importance: high

``venafi.topic``
  Topic to publish Venafi log data to.

  * Type: string
  * Default: VENAFI-LOGS
  * Valid Values: non-empty string and no ISO control characters
  * Importance: high

``venafi.batch.size``
  Window of data to pull from log API.

  * Type: int
  * Default: 100
  * Valid Values: [2,...,10000]
  * Importance: low

``venafi.poll.interval``
  Poll interval in milliseconds.

  * Type: int
  * Default: 1000
  * Importance: low




