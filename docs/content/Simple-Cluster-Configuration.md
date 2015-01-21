---
layout: doc_page
---
Simple Cluster Configuration
===============================

This simple Druid cluster configuration can be used for initially experimenting with Druid on your local machine. For a more realistic production Druid cluster, see [Production Cluster Configuration](Production-Cluster-Configuration.html).

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

-Ddruid.host=localhost
-Ddruid.port=8080
-Ddruid.service=overlord

-Ddruid.zk.service.host=localhost

-Ddruid.extensions.coordinates=["io.druid.extensions:druid-kafka-seven:0.6.171"]

-Ddruid.db.connector.connectURI=jdbc:mysql://localhost:3306/druid
-Ddruid.db.connector.user=druid
-Ddruid.db.connector.password=diurd

-Ddruid.selectors.indexing.serviceName=overlord
-Ddruid.indexer.queue.startDelay=PT0M
-Ddruid.indexer.runner.javaOpts="-server -Xmx1g"
-Ddruid.indexer.runner.startPort=8088
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

druid.host=localhost
druid.service=coordinator
druid.port=8082

druid.zk.service.host=localhost

druid.db.connector.connectURI=jdbc\:mysql\://localhost\:3306/druid
druid.db.connector.user=druid
druid.db.connector.password=diurd

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

druid.host=localhost
druid.service=historical
druid.port=8083

druid.zk.service.host=localhost

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

druid.host=localhost
druid.service=broker
druid.port=8084

druid.zk.service.host=localhost

druid.processing.buffer.sizeBytes=100000000
druid.processing.numThreads=1
```

This simple broker will run groupBys in a single thread.
