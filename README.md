QuickStart Guide
======================
Table of contents
=================
  * [Architecture](#architecture)
    * [Modules](#modules) 
    * [Client APIs](#client-apis)
    * [Parallelism](#parallelism)
    * [Partition Mapping](#partition-mapping)
    * [Data Delivery](#data-delivery)
    * [Flush Control](#flush-control)
  * [Tests](#tests)
    * [Unit Tests](#unit-tests)
    * [System Tests](#system-tests)
  * [Streaming Data from Kafka into Xenon](#streaming-data-from-kafka-into-xenon)
    * [Core Types](#core-types)
    * [Logical Types](#logical-types)
  * [Packaging](#packaging)
  * [Deployment example](#deployment)
    * [JSON without Schema setup](#json-without-schema-setup)
      * [Standalone mode](#standalone-mode)
      * [Remote mode](#remote-mode)

The following guide provides instructions to integrate *Xenon* with *Kafka*.
KafkaXenonSinkConnector is a Kafka Sink Connector for loading data from Kafka to Xenon with support for mulitple data formats  like Json(schema'd and schemaless) and Avro.
The connector is aimed at making *Xenon* *Kafka* accessible, implying that data may be streamed from Kafka to *Xenon* via *Kafka Connect*.


Architecture
==============
Kafka Connect provides APIs for both source and sink connector; currently, we have only implemented sink connector for Xenon and we will only discuss sink connector in the following.


### Modules

XenonSinkConnector is composed of the following modules:
* ``XenonSinkConnectorConfig`` (Helper that validates config file)
* ``XenonWrapper`` (Major implementation)
* ``XenonSinkConnector(API)``	 (Read config file)
* ``XenonSinkTask(API)``	(Call XenonWrapper APIs)


### Client APIs

* XenonSinkConnector (extends SinkConnector)
  * ``config()``	(Build config for the topic)
* XenonSinkTask (extends SinkTask)
  * ``start(config)``		(Start task + connect to Xenon)
  * ``put(Collection<SinkRecords>)`` 	(Save to Xenon)
  * ``stop()``	(Disconnect from Xenon + stop task)


### Parallelism

Parallelism is naturally handled by SinkTask: Each topic may associate with multiple tasks; each task is a dedicated thread that is attached with a XenonWrapper object, which corresponds to a thread communicating with Xenon server.


### Partition Mapping

We use a simple hash for partition mapping: p => p % fanout, where p is the partition number associated with a SinkRecord and fanout is the Xenon fanout, normally equal to the number of cpu cores used by Xenon.


### Data Delivery

Xenon connector sends sink records exactly once. Once data is sent to Xenon, Xenon will send back the number of records that have been saved to Xenon. If the returned count does not match the number of records sent, the system should throw an exception.


### Flush Control

Using SinkTask.put(), we flush for each collection of SinkRecords received by the connector. Max number of records in each collection can be set to tune the performance of the connector.


Tests
============

### Unit Tests

Integrated into XenonSinkConnector, there are three test classes:
* ``XenonSinkConnectorTest`` (Verify config loading)
* ``XenonWrapperTest`` (Verify Xenon connection and Xenon file operations)
* ``XenonSinkTaskTest`` (Validate the functionality of the connector) 

A typical functional test includes the following steps:
  * Start connector and build config using a map
  * Create synthetic records
  * Start a new task using config
  * Call Task.put to pass records to Xenon
  * Stop the task
  * Verify data saved on Xenon

### System Tests

For tests, integrating with a running Kafka system, see [Deployment example](#deployment).

-----

Streaming Data from Kafka into Xenon
---------------------------------------

### Core Types
Currently the connector is able to process Kafka Connect SinkRecords with
support for the following schema types [Schema.Type](https://kafka.apache.org/0100/javadoc/org/apache/kafka/connect/data/Schema.Type.html):
``INT8``, ``INT16``, ``INT32``, ``INT64``, ``FLOAT32``, ``FLOAT64``, ``BOOLEAN``, ``STRING``, ``BYTES``, ``ARRAY``, ``MAP`` and ``STRUCT``.


### Logical Types
Besides the core types it is possible to use logical types by checking schema name in order to have field type support
for ``Decimal``, ``Date``, ``Time (millis/micros)`` and ``Timestamp (millis/micros)``.
However, Logical Types(requiring schema name) may only be supported for either **AVRO** or **JSON + Schema** data.

### Supported Data Formats
The sink connector implementation is configurable in order to support:
* **AVRO** (makes use of Confluent's Kafka Schema Registry)
* **JSON with Schema** (offers JSON record structure with explicit schema information)
* **JSON without Schema** (offers JSON record structure without any attached schema)

Since these settings can be independently configured, it's possible to have different setups respectively.

The ``XenonSinkConnector`` is configured using a properties file that
accepts the following parameters:

* ``name``: name assigned to the sink connector.
* ``host``: xenon host.
* ``port``: xenon port.
* ``buffer.capacity``:size of the ByteBuffer(Bytes) pushed to xenon.
* ``connector.class``: class of implementation of the SinkConnector.
* ``topics``: comma separated list of topics to be used as source of data.
* ``tasks.max``: maximum number of tasks to be created.
* ``dataset.name``: name of the dataset to be opened in xenon.
* ``dataset.schema``: schema associated with the dataset.
* ``schema.version``: schema version of the records being sent(mainly for records with schema).

-----

Packaging
----------------------------
* Install XenonClient jar and pom in local maven repository:
```bash
mvn install:install-file -Dfile=$KAFKA_CONNECTOR_HOME/src/main/resources/XenonClient-1.0.0.jar -DpomFile=$KAFKA_CONNECTOR_HOME/src/main/resources/pom.xml 
```
* The *kafka-xenon* connector can be run in standalone/remote mode.
* mvn clean package or mvn clean package -DskipTests (to skip the test directory)

* JAR file produced by this project:  
``kafka-connect-xenon-1.0.0.jar`` - default JAR (to be used)

* Copy ``kafka-connect-xenon-1.0.0.jar`` and ``XenonClient-1.0.0.jar`` to target directory kafka-connect-xenon.


Deployment
----------------------------
* [Download,install and start Xenon](https://levyx.github.io/spark-xenon/)
* [Download and install Confluent](http://www.confluent.io/)
* Below is a simple deployment example. For more details regarding deployment configuration please contact 
  [Levyx, Inc.](http://www.levyx.com/contact-us)
* Create a configuration file (``connect-xenon.properties``) for the sink connector(example below):

```bash
name=xenon-sink-connector
host=localhost
port=41000
buffer.capacity=8388608
connector.class=XenonSinkConnector
topics=testOne
tasks.max=10
dataset.name=test
dataset.schema={Date:I64, Type:CHAR, SymbolID:I64, SequenceID:DOUBLE, BuySell:CHAR, Volume:I32, Symbol:CHAR, Durationms:I64, Attribute:CHAR}
schema.version=1
```
* Copy kafka-connect-xenon-1.0.0.jar and connect-xenon.properties from the project build location to `$CONFLUENT_HOME/share/java/kafka-connect-xenon`
```bash
mkdir $CONFLUENT_HOME/share/java/kafka-connect-xenon
cp target/kafka-connect-xenon-1.0.0.jar  $CONFLUENT_HOME/share/java/kafka-connect-xenon/
cp connect-xenon.properties $CONFLUENT_HOME/share/java/kafka-connect-xenon/
cp $KAFKA_CONNECTOR_HOME/src/main/resources/XenonClient-1.0.0.jar $CONFLUENT_HOME/share/java/kafka-connect-xenon/
```

#### JSON without Schema setup


* Start Zookeeper and Kafka
```bash
$CONFLUENT_HOME/bin/zookeeper-server-start $CONFLUENT_HOME/etc/kafka/zookeeper.properties &
$CONFLUENT_HOME/bin/kafka-server-start $CONFLUENT_HOME/etc/kafka/server.properties &

```
* Start Xenon server(as root or user based on how you decide to run the setup) after mounting device on file system.
```bash
mkfs.xfs /dev/vdb
mkdir /mnt/nvme
mkdir /mnt/nvme/tmp
mount /dev/vdb /mnt/nvme/tmp
$XENON_HOME/src/xenon --format --nowarn /dev/vdb
$XENON_HOME/src/xenon --server --address localhost:41000 xe://localhost:41000//dev/vdb
```
* Sample input data (trial.txt) for JSON without Schema setup.

```json
{"Date":20150925, "Type":"F", "SymbolID":34200000000000, "SequenceID":1.1, "BuySell":"B", "Volume":100, "Symbol":"HAPE", "Durationms":49990, "Attribute":"ABCD"}
```

* Create topic testOne(replication factor 1 and partitions 10)
```bash
$CONFLUENT_HOME/bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic testOne
```

* Verify the correctness of the records delivered by running kafka-console-consumer in another terminal:
```bash
$CONFLUENT_HOME/bin/kafka-console-consumer \
--bootstrap-server=localhost:9092 --topic testOne
````

* Configure Standalone or distributed worker properties file.

```properties
key.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false

value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false
```

* Starting connectors on sink(Standalone/Remote mode).
#### Standalone mode
```bash
export CLASSPATH=$CONFLUENT_HOME/share/java/kafka-connect-xenon/*
$CONFLUENT_HOME/bin/connect-standalone $CONFLUENT_HOME/etc/kafka/connect-standalone.properties $CONFLUENT_HOME/share/java/kafka-connect-xenon/connect-xenon.properties 
```

#### Remote mode
```bash
export CLASSPATH=$CONFLUENT_HOME/share/java/kafka-connect-xenon/*
$CONFLUENT_HOME/bin/connect-distributed etc/kafka/connect-distributed.properties
```

* Start kafka-console-producer to write content of file to it.
```bash
$CONFLUENT_HOME/bin/kafka-console-producer \
--broker-list localhost:9092 --topic testOne \
< /mnt/nvme/tmp/trial.txt &
```
-----
