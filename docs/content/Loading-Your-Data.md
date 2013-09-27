---
layout: doc_page
---
Once you have a realtime node working, it is time to load your own data to see how Druid performs.

Druid can ingest data in three ways: via Kafka and a realtime node, via the indexing service, and via the Hadoop batch loader. Data is ingested in realtime using a [Firehose](Firehose.html).

## Create Config Directories ##
Each type of node needs its own config file and directory, so create them as subdirectories under the druid directory.

```bash
mkdir config
mkdir config/realtime
mkdir config/master
mkdir config/compute
mkdir config/broker
```

## Loading Data with Kafka ##

[KafkaFirehoseFactory](https://github.com/metamx/druid/blob/druid-0.5.x/realtime/src/main/java/com/metamx/druid/realtime/firehose/KafkaFirehoseFactory.java) is how druid communicates with Kafka. Using this [Firehose](Firehose.html) with the right configuration, we can import data into Druid in realtime without writing any code. To load data to a realtime node via Kafka, we'll first need to initialize Zookeeper and Kafka, and then configure and initialize a [Realtime](Realtime.html) node.

### Booting Kafka ###

Instructions for booting a Zookeeper and then Kafka cluster are available [here](http://kafka.apache.org/07/quickstart.html).

1. Download Apache Kafka 0.7.2 from [http://kafka.apache.org/downloads.html](http://kafka.apache.org/downloads.html)

  ```bash
  wget http://apache.spinellicreations.com/incubator/kafka/kafka-0.7.2-incubating/kafka-0.7.2-incubating-src.tgz
  tar -xvzf kafka-0.7.2-incubating-src.tgz
  cd kafka-0.7.2-incubating-src
  ```

2. Build Kafka

  ```bash
  ./sbt update
  ./sbt package
  ```

3. Boot Kafka

  ```bash
  cat config/zookeeper.properties
  bin/zookeeper-server-start.sh config/zookeeper.properties
  # in a new console
  bin/kafka-server-start.sh config/server.properties
  ```

4. Launch the console producer (so you can type in JSON kafka messages in a bit)

  ```bash
  bin/kafka-console-producer.sh --zookeeper localhost:2181 --topic druidtest
  ```

### Launching a Realtime Node

1. Create a valid configuration file similar to this called config/realtime/runtime.properties:

  ```properties
  druid.host=0.0.0.0:8080
  druid.port=8080

  com.metamx.emitter.logging=true

  druid.processing.formatString=processing_%s
  druid.processing.numThreads=1
  druid.processing.buffer.sizeBytes=10000000

  #emitting, opaque marker
  druid.service=example

  druid.request.logging.dir=/tmp/example/log
  druid.realtime.specFile=realtime.spec
  com.metamx.emitter.logging=true
  com.metamx.emitter.logging.level=debug

  # below are dummy values when operating a realtime only node
  druid.processing.numThreads=3

  com.metamx.aws.accessKey=dummy_access_key
  com.metamx.aws.secretKey=dummy_secret_key
  druid.pusher.s3.bucket=dummy_s3_bucket

  druid.zk.service.host=localhost
  druid.server.maxSize=300000000000
  druid.zk.paths.base=/druid
  druid.database.segmentTable=prod_segments
  druid.database.user=user
  druid.database.password=diurd
  druid.database.connectURI=
  druid.host=127.0.0.1:8080
  ```

2. Create a valid realtime configuration file similar to this called realtime.spec:

  ```json
  [{
    "schema" : { "dataSource":"druidtest",
                 "aggregators":[ {"type":"count", "name":"impressions"},
                                    {"type":"doubleSum","name":"wp","fieldName":"wp"}],
                 "indexGranularity":"minute",
             "shardSpec" : { "type": "none" } },
    "config" : { "maxRowsInMemory" : 500000,
                 "intermediatePersistPeriod" : "PT10m" },
    "firehose" : { "type" : "kafka-0.7.2",
                   "consumerProps" : { "zk.connect" : "localhost:2181",
                                       "zk.connectiontimeout.ms" : "15000",
                                       "zk.sessiontimeout.ms" : "15000",
                                       "zk.synctime.ms" : "5000",
                                       "groupid" : "topic-pixel-local",
                                       "fetch.size" : "1048586",
                                       "autooffset.reset" : "largest",
                                       "autocommit.enable" : "false" },
                   "feed" : "druidtest",
                   "parser" : { "timestampSpec" : { "column" : "utcdt", "format" : "iso" },
                                "data" : { "format" : "json" },
                                "dimensionExclusions" : ["wp"] } },
    "plumber" : { "type" : "realtime",
                  "windowPeriod" : "PT10m",
                  "segmentGranularity":"hour",
                  "basePersistDirectory" : "/tmp/realtime/basePersist",
                  "rejectionPolicy": {"type": "messageTime"} }

  }]
  ```

3. Launch the realtime node

  ```bash
  java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 \
  -Ddruid.realtime.specFile=config/realtime/realtime.spec \
  -classpath lib/*:config/realtime com.metamx.druid.realtime.RealtimeMain
  ```

4. Paste data into the Kafka console producer

  ```json
  {"utcdt": "2010-01-01T01:01:01", "wp": 1000, "gender": "male", "age": 100}
  {"utcdt": "2010-01-01T01:01:02", "wp": 2000, "gender": "female", "age": 50}
  {"utcdt": "2010-01-01T01:01:03", "wp": 3000, "gender": "male", "age": 20}
  {"utcdt": "2010-01-01T01:01:04", "wp": 4000, "gender": "female", "age": 30}
  {"utcdt": "2010-01-01T01:01:05", "wp": 5000, "gender": "male", "age": 40}
  ```

5. Watch the events as they are ingested by Druid's realtime node

  ```bash
  ...
  2013-06-17 21:41:55,569 INFO [Global--0] com.metamx.emitter.core.LoggingEmitter - Event [{"feed":"metrics","timestamp":"2013-06-17T21:41:55.569Z","service":"example","host":"127.0.0.1","metric":"events/processed","value":5,"user2":"druidtest"}]
  ...
  ```

6. In a new console, edit a file called query.body:

  ```json
  {
      "queryType": "groupBy",
      "dataSource": "druidtest",
      "granularity": "all",
      "dimensions": [],
      "aggregations": [
          { "type": "count", "name": "rows" },
          {"type": "longSum", "name": "imps", "fieldName": "impressions"},
          {"type": "doubleSum", "name": "wp", "fieldName": "wp"}
      ],
      "intervals": ["2010-01-01T00:00/2020-01-01T00"]
  }
  ```

7. Submit the query via curl

  ```bash
  curl -X POST "http://localhost:8080/druid/v2/?pretty" \
  -H 'content-type: application/json' -d @query.body
  ```

8. View Result!

  ```json
  [ {
    "timestamp" : "2010-01-01T01:01:00.000Z",
    "result" : {
      "imps" : 20,
      "wp" : 60000.0,
      "rows" : 5
    }
  } ]
  ```

Now you're ready for [Querying Your Data](Querying-Your-Data.html)!

## Loading Data with the HadoopDruidIndexer ##

Historical data can be loaded via a Hadoop job. 

The setup for a single node, 'standalone' Hadoop cluster is available at [http://hadoop.apache.org/docs/stable/single_node_setup.html](http://hadoop.apache.org/docs/stable/single_node_setup.html).

### Setup MySQL ###
1. If you don't already have it, download MySQL Community Server here: [http://dev.mysql.com/downloads/mysql/](http://dev.mysql.com/downloads/mysql/)
2. Install MySQL
3. Create a druid user and database

```bash
mysql -u root
```

```sql
GRANT ALL ON druid.* TO 'druid'@'localhost' IDENTIFIED BY 'diurd';
CREATE database druid;
```

The [Master](Master.html) node will create the tables it needs based on its configuration.

### Make sure you have ZooKeeper Running ###

Make sure that you have a zookeeper instance running.  If you followed the instructions for Kafka, it is probably running.  If you are unsure if you have zookeeper running, try running

```bash
ps auxww | grep zoo | grep -v grep
```

If you get any result back, then zookeeper is most likely running.  If you haven't setup Kafka or do not have zookeeper running, then you can download it and start it up with

```bash
curl http://www.motorlogy.com/apache/zookeeper/zookeeper-3.4.5/zookeeper-3.4.5.tar.gz -o zookeeper-3.4.5.tar.gz
tar xzf zookeeper-3.4.5.tar.gz
cd zookeeper-3.4.5
cp conf/zoo_sample.cfg conf/zoo.cfg
./bin/zkServer.sh start
cd ..
```

### Launch a Master Node ###

If you've already setup a realtime node, be aware that although you can run multiple node types on one physical computer, you must assign them unique ports. Having used 8080 for the [Realtime](Realtime.html) node, we use 8081 for the [Master](Master.html).

1. Setup a configuration file called config/master/runtime.properties similar to:

  ```properties
  druid.host=0.0.0.0:8081
  druid.port=8081

  com.metamx.emitter.logging=true

  druid.processing.formatString=processing_%s
  druid.processing.numThreads=1
  druid.processing.buffer.sizeBytes=10000000

  # emitting, opaque marker
  druid.service=example

  druid.master.startDelay=PT60s
  druid.request.logging.dir=/tmp/example/log
  druid.realtime.specFile=realtime.spec
  com.metamx.emitter.logging=true
  com.metamx.emitter.logging.level=debug

  # below are dummy values when operating a realtime only node
  druid.processing.numThreads=3

  com.metamx.aws.accessKey=dummy_access_key
  com.metamx.aws.secretKey=dummy_secret_key
  druid.pusher.s3.bucket=dummy_s3_bucket

  druid.zk.service.host=localhost
  druid.server.maxSize=300000000000
  druid.zk.paths.base=/druid
  druid.database.segmentTable=prod_segments
  druid.database.user=druid
  druid.database.password=diurd
  druid.database.connectURI=jdbc:mysql://localhost:3306/druid
  druid.zk.paths.discoveryPath=/druid/discoveryPath
  druid.database.ruleTable=rules
  druid.database.configTable=config

  # Path on local FS for storage of segments; dir will be created if needed
  druid.paths.indexCache=/tmp/druid/indexCache
  # Path on local FS for storage of segment metadata; dir will be created if needed
  druid.paths.segmentInfoCache=/tmp/druid/segmentInfoCache
  ```

2. Launch the [Master](Master.html) node

  ```bash
  java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 \
  -classpath lib/*:config/master \
  com.metamx.druid.http.MasterMain
  ```

### Launch a Compute/Historical Node ###

1. Create a configuration file in config/compute/runtime.properties similar to:

  ```properties
  druid.host=0.0.0.0:8082
  druid.port=8082

  com.metamx.emitter.logging=true

  druid.processing.formatString=processing_%s
  druid.processing.numThreads=1
  druid.processing.buffer.sizeBytes=10000000

  # emitting, opaque marker
  druid.service=example

  druid.request.logging.dir=/tmp/example/log
  druid.realtime.specFile=realtime.spec
  com.metamx.emitter.logging=true
  com.metamx.emitter.logging.level=debug

  # below are dummy values when operating a realtime only node
  druid.processing.numThreads=3

  com.metamx.aws.accessKey=dummy_access_key
  com.metamx.aws.secretKey=dummy_secret_key
  druid.pusher.s3.bucket=dummy_s3_bucket

  druid.zk.service.host=localhost
  druid.server.maxSize=300000000000
  druid.zk.paths.base=/druid
  druid.database.segmentTable=prod_segments
  druid.database.user=druid
  druid.database.password=diurd
  druid.database.connectURI=jdbc:mysql://localhost:3306/druid
  druid.zk.paths.discoveryPath=/druid/discoveryPath
  druid.database.ruleTable=rules
  druid.database.configTable=config

# Path on local FS for storage of segments; dir will be created if needed
  druid.paths.indexCache=/tmp/druid/indexCache
# Path on local FS for storage of segment metadata; dir will be created if needed
  druid.paths.segmentInfoCache=/tmp/druid/segmentInfoCache
# Setup local storage mode
  druid.pusher.local.storageDirectory=/tmp/druid/localStorage
  druid.pusher.local=true
  ```

2. Launch the compute node:

  ```bash
  java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 \
  -classpath lib/*:config/compute \
  com.metamx.druid.http.ComputeMain
  ```

### Create a File of Records ###

We can use the same records we have been, in a file called records.json:

```json
{"utcdt": "2010-01-01T01:01:01", "wp": 1000, "gender": "male", "age": 100}
{"utcdt": "2010-01-01T01:01:02", "wp": 2000, "gender": "female", "age": 50}
{"utcdt": "2010-01-01T01:01:03", "wp": 3000, "gender": "male", "age": 20}
{"utcdt": "2010-01-01T01:01:04", "wp": 4000, "gender": "female", "age": 30}
{"utcdt": "2010-01-01T01:01:05", "wp": 5000, "gender": "male", "age": 40}
```

### Run the Hadoop Job ###

Now its time to run the Hadoop [Batch-ingestion](Batch-ingestion.html) job, HadoopDruidIndexer, which will fill a historical [Compute](Compute.html) node with data. First we'll need to configure the job.

1. Create a config called batchConfig.json similar to:

  ```json
  {
    "dataSource": "druidtest",
    "timestampColumn": "utcdt",
    "timestampFormat": "iso",
    "dataSpec": {
      "format": "json",
      "dimensions": ["gender", "age"]
    },
    "granularitySpec": {
      "type":"uniform",
      "intervals":["2010-01-01T01/PT1H"],
      "gran":"hour"
    },
    "pathSpec": { "type": "static",
                  "paths": "/Users/rjurney/Software/druid/records.json" },
    "rollupSpec": { "aggs":[ {"type":"count", "name":"impressions"},
                             {"type":"doubleSum","name":"wp","fieldName":"wp"}
                             ],
                    "rollupGranularity": "minute"},
    "workingPath": "/tmp/working_path",
    "segmentOutputPath": "/tmp/segments",
    "leaveIntermediate": "false",
    "partitionsSpec": {
      "targetPartitionSize": 5000000
    },
    "updaterJobSpec": {
      "type":"db",
      "connectURI":"jdbc:mysql://localhost:3306/druid",
      "user":"druid",
      "password":"diurd",
      "segmentTable":"prod_segments"
    }
  }
  ```

2. Now run the job, with the config pointing at batchConfig.json:

  ```bash
  java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 \
       -Ddruid.realtime.specFile=realtime.spec -classpath lib/* \
       com.metamx.druid.indexer.HadoopDruidIndexerMain batchConfig.json
  ```

You can now move on to [Querying Your Data](Querying-Your-Data.html)!
