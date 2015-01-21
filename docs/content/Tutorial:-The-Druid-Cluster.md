---
layout: doc_page
---

# Tutorial: The Druid Cluster
Welcome back! In our first [tutorial](Tutorial%3A-A-First-Look-at-Druid.html), we introduced you to the most basic Druid setup: a single realtime node. We streamed in some data and queried it. Realtime nodes collect very recent data and periodically hand that data off to the rest of the Druid cluster. Some questions about the architecture must naturally come to mind. What does the rest of Druid cluster look like? How does Druid load available static data?

This tutorial will hopefully answer these questions!

In this tutorial, we will set up other types of Druid nodes and external dependencies for a fully functional Druid cluster. The architecture of Druid is very much like the [Megazord](http://www.youtube.com/watch?v=7mQuHh1X4H4) from the popular 90s show Mighty Morphin' Power Rangers. Each Druid node has a specific purpose and the nodes come together to form a fully functional system.

## Downloading Druid

If you followed the first tutorial, you should already have Druid downloaded. If not, let's go back and do that first.

You can download the latest version of druid [here](http://static.druid.io/artifacts/releases/druid-services-0.6.171-bin.tar.gz)

and untar the contents within by issuing:

```bash
tar -zxvf druid-services-*-bin.tar.gz
cd druid-services-*
```

You can also [Build From Source](Build-from-source.html).

## External Dependencies

Druid requires 3 external dependencies. A "deep" storage that acts as a backup data repository, a relational database such as MySQL to hold configuration and metadata information, and [Apache Zookeeper](http://zookeeper.apache.org/) for coordination among different pieces of the cluster.

For deep storage, we have made a public S3 bucket (static.druid.io) available where data for this particular tutorial can be downloaded. More on the data later.

#### Setting up MySQL

1. If you don't already have it, download MySQL Community Server here: [http://dev.mysql.com/downloads/mysql/](http://dev.mysql.com/downloads/mysql/).
2. Install MySQL.
3. Create a druid user and database.

```bash
mysql -u root
```

```sql
GRANT ALL ON druid.* TO 'druid'@'localhost' IDENTIFIED BY 'diurd';
CREATE database druid;
```

#### Setting up Zookeeper

```bash
Download zookeeper from [http://www.apache.org/dyn/closer.cgi/zookeeper/](http://www.apache.org/dyn/closer.cgi/zookeeper/)
Install zookeeper.

e.g.
curl http://www.gtlib.gatech.edu/pub/apache/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz -o zookeeper-3.4.6.tar.gz
tar xzf zookeeper-3.4.6.tar.gz
cd zookeeper-3.4.6
cp conf/zoo_sample.cfg conf/zoo.cfg
./bin/zkServer.sh start
cd ..
```

## The Data

Similar to the first tutorial, the data we will be loading is based on edits that have occurred on Wikipedia. Every time someone edits a page in Wikipedia, metadata is generated about the editor and edited page. Druid collects each individual event and packages them together in a container known as a [segment](Segments.html). Segments contain data over some span of time. We've prebuilt a segment for this tutorial and will cover making your own segments in other [pages](Tutorial%3A-Loading-Your-Data-Part-1.html).The segment we are going to work with has the following format:

Dimensions (things to filter on):

```json
"page"
"language"
"user"
"unpatrolled"
"newPage"
"robot"
"anonymous"
"namespace"
"continent"
"country"
"region"
"city"
```

Metrics (things to aggregate over):

```json
"count"
"added"
"delta"
"deleted"
```

## The Cluster

Let's start up a few nodes and download our data. First, let's make sure we have configs in the config directory for our various nodes. Issue the following from the Druid home directory:

```
ls config
```

If you are interested in learning more about Druid configuration files, check out this [link](Configuration.html). Many aspects of Druid are customizable. For the purposes of this tutorial, we are going to use default values for most things.

#### Start a Coordinator Node

Coordinator nodes are in charge of load assignment and distribution. Coordinator nodes monitor the status of the cluster and command historical nodes to assign and drop segments.
For more information about coordinator nodes, see [here](Coordinator.html).

The coordinator config file should already exist at:

```
config/coordinator
```

In the directory, there should be a `runtime.properties` file with the following contents:

```
druid.host=localhost
druid.service=coordinator
druid.port=8082

druid.zk.service.host=localhost

druid.db.connector.connectURI=jdbc\:mysql\://localhost\:3306/druid
druid.db.connector.user=druid
druid.db.connector.password=diurd

druid.coordinator.startDelay=PT70s
```

To start the coordinator node:

```bash
java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath lib/*:config/coordinator io.druid.cli.Main server coordinator
```

#### Start a Historical Node

Historical nodes are the workhorses of a cluster and are in charge of loading historical segments and making them available for queries. Our Wikipedia segment will be downloaded by a historical node.
For more information about Historical nodes, see [here](Historical.html).

The historical config file should exist at:

```
config/historical
```

In the directory we just created, we should have the file `runtime.properties` with the following contents:

```
druid.host=localhost
druid.service=historical
druid.port=8081

druid.zk.service.host=localhost

druid.extensions.coordinates=["io.druid.extensions:druid-s3-extensions:0.6.171"]

# Dummy read only AWS account (used to download example data)
druid.s3.secretKey=QyyfVZ7llSiRg6Qcrql1eEUG7buFpAK6T6engr1b
druid.s3.accessKey=AKIAIMKECRUYKDQGR6YQ

druid.server.maxSize=10000000000

# Change these to make Druid faster
druid.processing.buffer.sizeBytes=100000000
druid.processing.numThreads=1

druid.segmentCache.locations=[{"path": "/tmp/druid/indexCache", "maxSize"\: 10000000000}]
```

To start the historical node:

```bash
java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath lib/*:config/historical io.druid.cli.Main server historical
```

#### Start a Broker Node

Broker nodes are responsible for figuring out which historical and/or realtime nodes correspond to which queries. They also merge partial results from these nodes in a scatter/gather fashion.
For more information about Broker nodes, see [here](Broker.html).

The broker config file should exist at:

```
config/broker
```

In the directory, there should be a `runtime.properties` file with the following contents:

```
druid.host=localhost
druid.service=broker
druid.port=8080

druid.zk.service.host=localhost
```

To start the broker node:

```bash
java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath lib/*:config/broker io.druid.cli.Main server broker
```

## Loading the Data

The MySQL dependency we introduced earlier on contains a 'segments' table that contains entries for segments that should be loaded into our cluster. The Druid coordinator compares this table with segments that already exist in the cluster to determine what should be loaded and dropped. To load our wikipedia segment, we need to create an entry in our MySQL segment table.

Usually, when new segments are created, these MySQL entries are created directly so you never have to do this by hand. For this tutorial, we can do this manually by going back into MySQL and issuing:

``` sql
use druid;
INSERT INTO druid_segments (id, dataSource, created_date, start, end, partitioned, version, used, payload) VALUES ('wikipedia_2013-08-01T00:00:00.000Z_2013-08-02T00:00:00.000Z_2013-08-08T21:22:48.989Z', 'wikipedia', '2013-08-08T21:26:23.799Z', '2013-08-01T00:00:00.000Z', '2013-08-02T00:00:00.000Z', '0', '2013-08-08T21:22:48.989Z', '1', '{\"dataSource\":\"wikipedia\",\"interval\":\"2013-08-01T00:00:00.000Z/2013-08-02T00:00:00.000Z\",\"version\":\"2013-08-08T21:22:48.989Z\",\"loadSpec\":{\"type\":\"s3_zip\",\"bucket\":\"static.druid.io\",\"key\":\"data/segments/wikipedia/20130801T000000.000Z_20130802T000000.000Z/2013-08-08T21_22_48.989Z/0/index.zip\"},\"dimensions\":\"dma_code,continent_code,geo,area_code,robot,country_name,network,city,namespace,anonymous,unpatrolled,page,postal_code,language,newpage,user,region_lookup\",\"metrics\":\"count,delta,variation,added,deleted\",\"shardSpec\":{\"type\":\"none\"},\"binaryVersion\":9,\"size\":24664730,\"identifier\":\"wikipedia_2013-08-01T00:00:00.000Z_2013-08-02T00:00:00.000Z_2013-08-08T21:22:48.989Z\"}');
```

If you look in your coordinator node logs, you should, after a maximum of a minute or so, see logs of the following form:

```
2013-08-08 22:48:41,967 INFO [main-EventThread] com.metamx.druid.coordinator.LoadQueuePeon - Server[/druid/loadQueue/127.0.0.1:8081] done processing [/druid/loadQueue/127.0.0.1:8081/wikipedia_2013-08-01T00:00:00.000Z_2013-08-02T00:00:00.000Z_2013-08-08T21:22:48.989Z]
2013-08-08 22:48:41,969 INFO [ServerInventoryView-0] com.metamx.druid.client.SingleServerInventoryView - Server[127.0.0.1:8081] added segment[wikipedia_2013-08-01T00:00:00.000Z_2013-08-02T00:00:00.000Z_2013-08-08T21:22:48.989Z]
```

When the segment completes downloading and ready for queries, you should see the following message on your historical node logs:

```
2013-08-08 22:48:41,959 INFO [ZkCoordinator-0] com.metamx.druid.coordination.BatchDataSegmentAnnouncer - Announcing segment[wikipedia_2013-08-01T00:00:00.000Z_2013-08-02T00:00:00.000Z_2013-08-08T21:22:48.989Z] at path[/druid/segments/127.0.0.1:8081/2013-08-08T22:48:41.959Z]
```

At this point, we can query the segment. For more information on querying, see this [link](Querying.html).

### Bonus Round: Start a Realtime Node

To start the realtime node that was used in our first tutorial, you simply have to issue:

```
java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -Ddruid.realtime.specFile=examples/wikipedia/wikipedia_realtime.spec -classpath lib/*:config/realtime io.druid.cli.Main server realtime
```

The configurations are located in `config/realtime/runtime.properties` and should contain the following:

```
druid.host=localhost
druid.service=realtime
druid.port=8083

druid.zk.service.host=localhost

druid.extensions.coordinates=["io.druid.extensions:druid-examples:0.6.171","io.druid.extensions:druid-kafka-seven:0.6.171"]

# Change this config to db to hand off to the rest of the Druid cluster
druid.publish.type=noop

# These configs are only required for real hand off
# druid.db.connector.connectURI=jdbc\:mysql\://localhost\:3306/druid
# druid.db.connector.user=druid
# druid.db.connector.password=diurd

druid.processing.buffer.sizeBytes=100000000
druid.processing.numThreads=1

druid.monitoring.monitors=["io.druid.segment.realtime.RealtimeMetricsMonitor"]
```

Next Steps
----------
If you are interested in how data flows through the different Druid components, check out the [Druid data flow architecture](Design.html). Now that you have an understanding of what the Druid cluster looks like, why not load some of your own data?
Check out the next [tutorial](Tutorial%3A-Loading-Your-Data-Part-1.html) section for more info!
