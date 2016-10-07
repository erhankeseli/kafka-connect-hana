# Kafka Connector for SAP HANA

kafka-connect-hana is a [Kafka Connector](http://kafka.apache.org/documentation.html#connect) for copying data to and from SAP HANA.

## Installation from source

To install the connector from source, use the following command.

```
mvn clean install -DskipTests
```

## QuickStart

For getting started with this connector, the following steps need to be completed.

- Create the config file for sink named `kafka-connect-hana-sink.properties`.

```
name=test-sink
connector.class=com.sap.kafka.connect.sink.HANASinkConnector
tasks.max=1

topics=test_topic
connection.url=jdbc:sap://<url>/
connection.user=<username>
connection.password=<password>
auto.create=true
schema.registry.url=<schema registry url>

kafka_streams_testing19.table.name="SYSTEM"."DUMMY_TABLE"
```

- Start the kafka-connect-hana sink connector using the following command.

```
./bin/connect-standalone ./etc/schema-registry/connect-avro-standalone.properties ./etc/kafka/kafka-connect-hana-sink.properties
```

- Create the config file for source named `kafka-connect-hana-source.properties`.

```
name=kafka-connect-source
connector.class=com.sap.kafka.connect.source.HANASourceConnector
tasks.max=1

topics=kafka_streams_testing24,kafka_streams_testing25
connection.url=jdbc:sap://<url>/
connection.user=<username>
connection.password=<password>

kafka_streams_testing24.table.name="SYSTEM"."com.sap.test::hello18"
```

- - Start the kafka-connect-hana source connector using the following command.

```
./bin/connect-standalone ./etc/schema-registry/connect-avro-standalone.properties ./etc/kafka/kafka-connect-hana-source.properties
```

Configuration
=============

The `kafka connector for SAP Hana` provides a wide set of configuration options both for source & sink.
The full list of configuration options is documented [here](https://github.wdf.sap.corp/i033085/kafka-connect-hana/blob/master/Configuration.md). 

Examples
========

The `unit tests` provide examples on every possible mode in which the connector can be configured.

Features
========

The `kafka connector for SAP Hana` provides several configuration options out of which couple of them require special mention.

- Sink

`{topic}.table.type` - This is a Hana specific configuration setting which allows creation of Row & Column tables if `auto.create` is set to true.

`{topic}.table.partition.mode` - This is a HANA Sink specific configuration setting which determines the table partitioning in HANA. Default value is 'none'. And supported values are 'none', 'hash', 'round_robin'.

- Source

`{topic}.incrementing.column.name` - In order to fetch from a SAP Hana table, an incremental ( or auto-incremental ) column needs to be provided. The type 
of the column can be `Int, Float, Decimal, Timestamp`. This considers SAP Hana Timeseries tables also.



