---
layout: doc_page
---
Simple Cluster Configuration
===============================

This simple Druid cluster configuration can be used for initially experimenting with Druid on your local machine. For a more realistic production Druid cluster, see [Production Cluster Configuration](../configuration/production-cluster.html).

### Common Configuration (common.runtime.properties)

```
# Extensions
-Ddruid.extensions.coordinates=["io.druid.extensions:druid-kafka-eight"]

# Zookeeper (defaults to localhost)

# Metadata Storage (defaults to derby with no username and password)
```

### Overlord Node (Indexing Service)

Run:

```
io.druid.cli.Main server overlord
```

Configuration:

```
-server
-Xmx256m
-Duser.timezone=UTC
-Dfile.encoding=UTF-8
-Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager

-Ddruid.indexer.queue.startDelay=PT0M
-Ddruid.indexer.runner.javaOpts=-server -Xmx1g
-Ddruid.indexer.fork.property.druid.processing.numThreads=1
-Ddruid.indexer.fork.property.druid.computation.buffer.size=100000000
```

This runs the indexing service in local mode, and can support real-time ingestion tasks (with one processing thread for queries).

### Coordinator Node

Run:

```
io.druid.cli.Main server coordinator
```

Configuration:

```
-server
-Xmx256m
-Duser.timezone=UTC
-Dfile.encoding=UTF-8
-Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager

druid.coordinator.startDelay=PT70s
```

This simple coordinator assumes local deep storage.

### Historical Node

Run:

```
io.druid.cli.Main server historical
```

Configuration:

```
-server
-Xmx256m
-Duser.timezone=UTC
-Dfile.encoding=UTF-8
-Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager

druid.server.maxSize=10000000000

druid.processing.buffer.sizeBytes=100000000
druid.processing.numThreads=1

druid.segmentCache.locations=[{"path": "/tmp/druid/indexCache", "maxSize"\: 10000000000}]
```

This historical node will be able to load 100 MB of data and be able to process 1 segment at a time. Deep storage is assumed to be local storage here.

### Broker Node

Run:

```
io.druid.cli.Main server broker
```

Configuration:

```
-server
-Xmx256m
-Duser.timezone=UTC
-Dfile.encoding=UTF-8
-Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager

druid.processing.buffer.sizeBytes=100000000
druid.processing.numThreads=1
```

This simple broker will run groupBys in a single thread.
