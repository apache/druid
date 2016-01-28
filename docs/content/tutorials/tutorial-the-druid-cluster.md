---
layout: doc_page
---

# Tutorial: The Druid Cluster
Welcome back! In our first [tutorial](../tutorials/tutorial-a-first-look-at-druid.html), we introduced you to the most basic Druid setup: a single realtime node. We streamed in some data and queried it. Realtime nodes collect very recent data and periodically hand that data off to the rest of the Druid cluster. Some questions about the architecture must naturally come to mind. What does the rest of Druid cluster look like?

This tutorial will hopefully answer these questions!

In this tutorial, we will set up other types of Druid nodes and external dependencies for a fully functional Druid cluster. The architecture of Druid is very much like the [Megazord](http://www.youtube.com/watch?v=7mQuHh1X4H4) from the popular 90s show Mighty Morphin' Power Rangers. Each Druid node has a specific purpose and the nodes come together to form a fully functional system.

## Downloading Druid
If you followed the first tutorial, you should already have Druid downloaded. If not, let's go back and do that first.

You can download the latest version of druid [here](http://druid.io/downloads.html). You can also [Build From Source](../development/build.html) and grab the tarball from distribution/target/druid-<version>-bin.tar.gz.

Either way, once you have the tarball, untar the contents within by issuing:

```bash
tar -zxvf druid-<version>-bin.tar.gz
cd druid-<version>
```

## External Dependencies
Druid requires 3 external dependencies.
* A "deep storage" that acts as a data repository. This is generally distributed storage like HDFS or S3. For prototyping or experimentation on a single machine, Druid can use the local filesystem.
* A "metadata storage" to hold configuration and metadata information. This is generally a small, shared database like MySQL or PostgreSQL. For prototyping or experimentation on a single machine, Druid can use a local instance of [Apache Derby](http://db.apache.org/derby/).
* [Apache Zookeeper](http://zookeeper.apache.org/) for coordination among different pieces of the cluster.

For deep storage, we will use the local disk in this tutorial, but for production, HDFS and S3 are popular options. For the metadata storage, Derby is used in this tutorial, but for production MySQL or PostgreSQL etc should be used.

#### Set up Zookeeper

* Download zookeeper from [http://www.apache.org/dyn/closer.cgi/zookeeper/](http://www.apache.org/dyn/closer.cgi/zookeeper/)
* Install zookeeper.

```bash
curl http://www.gtlib.gatech.edu/pub/apache/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz -o zookeeper-3.4.6.tar.gz
tar xzf zookeeper-3.4.6.tar.gz
cd zookeeper-3.4.6
cp conf/zoo_sample.cfg conf/zoo.cfg
./bin/zkServer.sh start
cd ..
```

## The Data

Similar to the first tutorial, the data we will be loading is based on edits that have occurred on Wikipedia. Every time someone edits a page in Wikipedia, metadata is generated about the editor and edited page. Druid collects each individual event and packages them together in a container known as a [segment](../design/segments.html). Segments contain data over some span of time. We've prebuilt a segment for this tutorial and will cover making your own segments in other [pages](../tutorials/tutorial-loading-streaming-data.html).The segment we are going to work with has the following format:

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

Before we get started, let's make sure we have configs in the config directory for our various nodes. Issue the following from the Druid home directory:

```
ls config
```

If you are interested in learning more about Druid configuration files, check out this [link](../configuration/index.html). Many aspects of Druid are customizable. For the purposes of this tutorial, we are going to use default values for most things.

#### Common Configuration

There are a couple of cluster wide configuration options we have to define. The common/cluster configuration files should exist under:

```
config/_common
```

In the directory, there should be a `common.runtime.properties` file with the following contents:

```
# Extensions
druid.extensions.loadList=["druid-examples","druid-kafka-eight"]

# Zookeeper
druid.zk.service.host=localhost

# Deep storage (local filesystem for examples - don't use this in production)
druid.storage.type=local
druid.storage.storage.storageDirectory=/tmp/druid/localStorage

# Query Cache (we use a simple 10mb heap-based local cache on the broker)
druid.cache.type=local
druid.cache.sizeInBytes=10000000

# Indexing service discovery
druid.selectors.indexing.serviceName=overlord

# Monitoring (disabled for examples)
# druid.monitoring.monitors=["com.metamx.metrics.JvmMonitor"]

# Metrics logging (disabled for examples)
druid.emitter=noop
```

In this file we define our external dependencies and cluster wide configs.

#### Start a Coordinator Node

Coordinator nodes are in charge of load assignment and distribution. Coordinator nodes monitor the status of the cluster and command historical nodes to assign and drop segments.
For more information about coordinator nodes, see [here](../design/coordinator.html).

The coordinator config file should already exist at:

```
config/coordinator
```

In the directory, there should be a `runtime.properties` file with the following contents:

```
druid.host=localhost
druid.port=8081
druid.service=druid/coordinator

# The coordinator begins assignment operations after the start delay.
# We override the default here to start things up faster for examples.
druid.coordinator.startDelay=PT70s
```

To start the coordinator node:

```bash
java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath config/_common:config/coordinator:lib/* io.druid.cli.Main server coordinator
```

Note: we will be running a single historical node in these examples, so you may see some warnings about not being able to replicate segments. These can be safely ignored, but in production, you should always replicate segments across multiple historical nodes.

#### Start a Historical Node

Historical nodes are the workhorses of a cluster and are in charge of loading historical segments and making them available for queries. Realtime nodes hand off segments to historical nodes.
For more information about Historical nodes, see [here](../design/historical.html).

The historical config file should exist at:

```
config/historical
```

In the directory we just created, we should have the file `runtime.properties` with the following contents:

```
druid.host=localhost
druid.port=8083
druid.service=druid/historical

# We can only 1 scan segment in parallel with these configs.
# Our intermediate buffer is also very small so longer topNs will be slow.
druid.processing.buffer.sizeBytes=100000000
druid.processing.numThreads=1

druid.segmentCache.locations=[{"path": "/tmp/druid/indexCache", "maxSize"\: 10000000000}]
druid.server.maxSize=10000000000
```

To start the historical node:

```bash
java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath config/_common:config/historical:lib/* io.druid.cli.Main server historical
```

#### Start a Broker Node

Broker nodes are responsible for figuring out which historical and/or realtime nodes correspond to which queries. They also merge partial results from these nodes in a scatter/gather fashion.
For more information about Broker nodes, see [here](../design/broker.html).

The broker config file should exist at:

```
config/broker
```

In the directory, there should be a `runtime.properties` file with the following contents:

```
druid.host=localhost
druid.port=8082
druid.service=druid/broker

druid.broker.cache.useCache=true
druid.broker.cache.populateCache=true

# Bump these up only for faster nested groupBy
druid.processing.buffer.sizeBytes=100000000
druid.processing.numThreads=1
```

To start the broker node:

```bash
java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath config/_common:config/broker:lib/* io.druid.cli.Main server broker
```

#### Start a Realtime Node

Our goal is to ingest some data and hand-off that data to the rest of our Druid cluster. To accomplish this goal, we need to make some small configuration changes.

In your favorite editor, open up:

```
examples/wikipedia/wikipedia_realtime.spec
```

We need to change some configuration in order to force hand-off faster.

Let's change:

```
"segmentGranularity": "HOUR",
```

to

```
"segmentGranularity": "FIVE_MINUTE",
```

and

```
"intermediatePersistPeriod": "PT10m",
"windowPeriod": "PT10m",
```

to

```
"intermediatePersistPeriod": "PT3m",
"windowPeriod": "PT1m",
```

Now we should be handing off segments every 6 minutes or so.

To start the realtime node that was used in our first tutorial, you simply have to issue:

```
java -Xmx512m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -Ddruid.realtime.specFile=examples/wikipedia/wikipedia_realtime.spec -classpath config/_common:config/realtime:lib/* io.druid.cli.Main server realtime
```

The configurations are located in `config/realtime/runtime.properties` and should contain the following:

```
druid.host=localhost
druid.port=8084
druid.service=druid/realtime

# We can only 1 scan segment in parallel with these configs.
# Our intermediate buffer is also very small so longer topNs will be slow.
druid.processing.buffer.sizeBytes=100000000
druid.processing.numThreads=2

# Enable Real monitoring
# druid.monitoring.monitors=["com.metamx.metrics.JvmMonitor","io.druid.segment.realtime.RealtimeMetricsMonitor"]
```

Once the real-time node starts up, it should begin ingesting data and handing that data off to the rest of the Druid cluster. You can use a web UI located at coordinator_ip:port to view the status of data being loaded. Once data is handed off from the real-time nodes to historical nodes, the historical nodes should begin serving segments.

#### Query

At any point during ingestion, we can query for data. For example:

```
curl -X POST 'http://localhost:8082/druid/v2/?pretty' -H 'content-type: application/json' -d@examples/wikipedia/query.body
```

This query will span across both realtime and historical nodes. If you're curious, you can query the historical node directly by sending the same query to the historical node's port:

```
curl -X POST 'http://localhost:8083/druid/v2/?pretty' -H 'content-type: application/json' -d@examples/wikipedia/query.body
```

This query may produce no results if the realtime node hasn't run long enough to hand off the segment (we configured it above to be 5 minutes). Query the realtime node directly by sending the same query to the realtime node's port:

```
curl -X POST 'http://localhost:8084/druid/v2/?pretty' -H 'content-type: application/json' -d@examples/wikipedia/query.body
```

The realtime query results will reflect the data that was recently indexed from wikipedia, and not handed off to the historical node yet. Once the historical node acknowledges it has loaded the segment, the realtime node will drop the segment.

Querying the historical and realtime node directly is useful for understanding how the segment handling is working, but if you just want to run a query for all the data (realtime and historical), then send the query to the broker at port 8082 (which is what we did in the first example). The broker will send the query to the historical and realtime nodes and merge the results.

For more information on querying, see this [link](../querying/querying.html).

Next Steps
----------
If you are interested in how data flows through the different Druid components, check out the [Druid data flow architecture](../design/design.html). Now that you have an understanding of what the Druid cluster looks like, why not load some of your own data?
Check out the next [tutorial](../tutorials/tutorial-loading-streaming-data.html) section for more info!
