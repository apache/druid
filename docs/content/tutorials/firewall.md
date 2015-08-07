---
layout: doc_page
---

What to Do When You Have a Firewall
-----------------------------------
When you are behind a firewall, the Maven Druid dependencies will not be accessible, as well as the IRC wikipedia channels that feed realtime data into Druid. To workaround those two challenges, you will need to:

1. Make the Maven Druid dependencies available offline
2. Make the Wikipedia example GeoLite DB dependency available offline

## Making Maven Druid Dependencies Available Offline
1. Extract Druid to a machine that has internet access; e.g. `/Users/foo/druid-<version>`
2. Create a repository directory to download the dependencies to; e.g. `/Users/foo/druid-<version>\repo`
3. Create property `druid.extensions.localRepository=`*`path to repo directory`* in the *`Druid Directory`*`\config\_common/common.runtime.properties` file; e.g. `druid.extensions.localRepository=/Users/foo/druid-<version>/repo`
4. From within Druid directory, run the `pull-deps` command to download all Druid dependencies to the repository specified in the `common.runtime.properties` file:

    ```
    java -classpath "config\_common;lib\*" io.druid.cli.Main tools pull-deps
    ```

5. Once all dependencies have been downloaded successfully, replicate the `repo` directory to the machine behind the firewall; e.g. `/opt/druid-<version>/repo`
6. Create property `druid.extensions.localRepository=`*`path to repo directory`* in the *`Druid Directory`*`/config/_common/common.runtime.properties` file; e.g. `druid.extensions.localRepository=/opt/druid-<version>/repo`

## Making the Wikipedia Example GeoLite DB Dependency Available Offline
1. Download GeoLite2 City DB from http://dev.maxmind.com/geoip/geoip2/geolite2/
2. Copy and extract the DB to *`java.io.tmpdir`*`/io.druid.segment.realtime.firehose.WikipediaIrcDecoder.GeoLite2-City.mmdb`; e.g. `/tmp/io.druid.segment.realtime.firehose.WikipediaIrcDecoder.GeoLite2-City.mmdb`

    **Note**: depending on the machine's reboot policy, if the `java.io.tmpdir` resolves to the `/tmp` directory, you may have to create this file again in the `tmp` directory after a machine reboot

## Loading the Data into Druid directly from Kafka
As an alternative to reading the data from the IRC channels, which is a challenge to try to do it from behind a firewall, we will use Kafka to stream the data to Druid. To do so, we will need to:

1. Configure the Wikipedia example to read streaming data from Kafka
2. Set up and configure Kafka

#### Wikipedia Example Configuration
1. In your favorite editor, open the file `druid-<version>/examples/wikipedia/wikipedia_realtime.spec`
2. Backup the file, if necessary, then replace the file content with the following:

    ```json
    [
       {
           "dataSchema": {
               "dataSource": "wikipedia",
               "parser": {
                   "type": "string",
                   "parseSpec": {
                       "format": "json",
                       "timestampSpec": {
                           "column": "timestamp",
                           "format": "auto"
                       },
                       "dimensionsSpec": {
                           "dimensions": [
                               "page",
                               "language",
                               "user",
                               "unpatrolled",
                               "newPage",
                               "robot",
                               "anonymous",
                               "namespace",
                               "continent",
                               "country",
                               "region",
                               "city"
                           ],
                           "dimensionExclusions": [],
                           "spatialDimensions": []
                       }
                   }
               },
               "metricsSpec": [
                   {
                       "type": "count",
                       "name": "count"
                   },
                   {
                       "type": "doubleSum",
                       "name": "added",
                       "fieldName": "added"
                   },
                   {
                       "type": "doubleSum",
                       "name": "deleted",
                       "fieldName": "deleted"
                   },
                   {
                       "type": "doubleSum",
                       "name": "delta",
                       "fieldName": "delta"
                   }
               ],
               "granularitySpec": {
                   "type": "uniform",
                   "segmentGranularity": "DAY",
                   "queryGranularity": "NONE"
               }
           },
           "ioConfig": {
               "type": "realtime",
               "firehose": {
                   "type": "kafka-0.8",
                   "consumerProps": {
                       "zookeeper.connect": "localhost:2181",
                       "zookeeper.connection.timeout.ms": "15000",
                       "zookeeper.session.timeout.ms": "15000",
                       "zookeeper.sync.time.ms": "5000",
                       "group.id": "druid-example",
                       "fetch.message.max.bytes": "1048586",
                       "auto.offset.reset": "largest",
                       "auto.commit.enable": "false"
                   },
                   "feed": "wikipedia"
               },
               "plumber": {
                   "type": "realtime"
               }
           },
           "tuningConfig": {
               "type": "realtime",
               "maxRowsInMemory": 500000,
               "intermediatePersistPeriod": "PT10m",
               "windowPeriod": "PT10m",
               "basePersistDirectory": "/tmp/realtime/basePersist",
               "rejectionPolicy": {
                   "type": "messageTime"
               }
           }
       }
    ]
    ```

3. Refer to the [Running Example Scripts](#running-example-scripts) section to start the example Druid Realtime node by issuing the following from within your Druid directory:

    ```bash
    ./run_example_server.sh
    ```

#### Kafka Setup and Configuration
1. Download Kafka

    For this tutorial we will [download Kafka 0.8.2.1]
    (https://www.apache.org/dyn/closer.cgi?path=/kafka/0.8.2.1/kafka_2.10-0.8.2.1.tgz)

    ```bash
    tar -xzf kafka_2.10-0.8.2.1.tgz
    cd kafka_2.10-0.8.2.1
    ```

2. Start Kafka

    **First, launch ZooKeeper** (refer to the [Set up Zookeeper](#set-up-zookeeper) section for details), then start the Kafka server (in a separate console):

    ```bash
    ./bin/kafka-server-start.sh config/server.properties
    ```

3. Create a topic named `wikipedia`

    ```bash
    ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic wikipedia
    ```

4. Launch a console producer for the topic `wikipedia`

    ```bash
    ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic wikipedia
    ```

5. Copy and paste the following data into the terminal where we launched the Kafka console producer in the previous step:

    ```json
    {"timestamp": "2013-08-31T01:02:33Z", "page": "Gypsy Danger", "language" : "en", "user" : "nuclear", "unpatrolled" : "true", "newPage" : "true", "robot": "false", "anonymous": "false", "namespace":"article", "continent":"North America", "country":"United States", "region":"Bay Area", "city":"San Francisco", "added": 57, "deleted": 200, "delta": -143}
    {"timestamp": "2013-08-31T03:32:45Z", "page": "Striker Eureka", "language" : "en", "user" : "speed", "unpatrolled" : "false", "newPage" : "true", "robot": "true", "anonymous": "false", "namespace":"wikipedia", "continent":"Australia", "country":"Australia", "region":"Cantebury", "city":"Syndey", "added": 459, "deleted": 129, "delta": 330}
    {"timestamp": "2013-08-31T07:11:21Z", "page": "Cherno Alpha", "language" : "ru", "user" : "masterYi", "unpatrolled" : "false", "newPage" : "true", "robot": "true", "anonymous": "false", "namespace":"article", "continent":"Asia", "country":"Russia", "region":"Oblast", "city":"Moscow", "added": 123, "deleted": 12, "delta": 111}
    {"timestamp": "2013-08-31T11:58:39Z", "page": "Crimson Typhoon", "language" : "zh", "user" : "triplets", "unpatrolled" : "true", "newPage" : "false", "robot": "true", "anonymous": "false", "namespace":"wikipedia", "continent":"Asia", "country":"China", "region":"Shanxi", "city":"Taiyuan", "added": 905, "deleted": 5, "delta": 900}
    {"timestamp": "2013-08-31T12:41:27Z", "page": "Coyote Tango", "language" : "ja", "user" : "stringer", "unpatrolled" : "true", "newPage" : "false", "robot": "true", "anonymous": "false", "namespace":"wikipedia", "continent":"Asia", "country":"Japan", "region":"Kanto", "city":"Tokyo", "added": 1, "deleted": 10, "delta": -9}
    ```

#### Finally
Now, that data has been fed into Druid, refer to the [Running Example Scripts](#running-example-scripts) section to query the real-time node by issuing the following from within the Druid directory:
```bash
./run_example_client.sh
```

The [Querying Druid](../querying/querying.md) section also has further querying examples.
