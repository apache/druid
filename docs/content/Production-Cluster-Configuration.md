---
layout: doc_page
---
Production Cluster Configuration
================================

__This configuration is an example of what a production cluster could look like. Many other hardware combinations are possible! Cheaper hardware is absolutely possible.__

This production Druid cluster assumes that metadata storage and Zookeeper are already set up. The deep storage that is used for examples is S3 and memcached is used for a distributed cache.

The nodes that respond to queries (Historical, Broker, and Middle manager nodes) will use as many cores as are available, depending on usage, so it is best to keep these on dedicated machines. The upper limit of effectively utilized cores is not well characterized yet and would depend on types of queries, query load, and the schema. Historical daemons should have a heap a size of at least 1GB per core for normal usage, but could be squeezed into a smaller heap for testing. Since in-memory caching is essential for good performance, even more RAM is better. Broker nodes will use RAM for caching, so they do more than just route queries. SSDs are highly recommended for Historical nodes not all data is loaded in available memory.

The nodes that are responsible for coordination (Coordinator and Overlord nodes) require much less processing.

The effective utilization of cores by Zookeeper, metadata storage, and Coordinator nodes is likely to be between 1 and 2 for each process/daemon, so these could potentially share a machine with lots of cores. These daemons work with heap a size between 500MB and 1GB.

We'll use r3.8xlarge nodes for query facing nodes and m1.xlarge nodes for coordination nodes. The following examples work relatively well in production, however, a more optimized tuning for the nodes we selected and more optimal hardware for a Druid cluster are both definitely possible.

For general purposes of high availability, there should be at least 2 of every node type.

To setup a local Druid cluster, see [Simple Cluster Configuration](Simple-Cluster-Configuration.html).

### Common Configuration (common.runtime.properties)

```
# Extensions
druid.extensions.coordinates=["io.druid.extensions:druid-s3-extensions", "io.druid.extensions:druid-histogram", "io.druid.extensions:mysql-metadata-storage"]

# Zookeeper
druid.zk.service.host=#{ZK_IPs}
druid.zk.paths.base=/druid/prod

druid.discovery.curator.path=/prod/discovery

# Request logging, monitoring, and metrics
druid.request.logging.type=emitter
druid.request.logging.feed=druid_requests

druid.monitoring.monitors=["com.metamx.metrics.SysMonitor","com.metamx.metrics.JvmMonitor"]

druid.emitter=http
druid.emitter.http.recipientBaseUrl=#{EMITTER_URL}

# Metadata storage
druid.metadata.storage.type=mysql
druid.metadata.storage.connector.connectURI=jdbc:mysql://#{MYSQL_URL}:3306/druid?characterEncoding=UTF-8
druid.metadata.storage.connector.user=#{MYSQL_USER}
druid.metadata.storage.connector.password=#{MYSQL_PW}

# Deep storage
druid.storage.type=s3
druid.s3.accessKey=#{S3_ACCESS_KEY}
druid.s3.secretKey=#{S3_SECRET_KEY}

# Caching
druid.cache.type=memcached
druid.cache.hosts=#{MEMCACHED_IPS}
druid.cache.expiration=2147483647
druid.cache.memcachedPrefix=d1
druid.cache.maxOperationQueueSize=1073741824
druid.cache.readBufferSize=10485760

# Indexing Service Service Discovery
druid.selectors.indexing.serviceName=druid:prod:overlord

```

### Overlord Node

Run:

```
io.druid.cli.Main server overlord
```

Hardware:

```
m1.xlarge (Cores: 4, Memory: 15.0 GB)
```

JVM Configuration:

```
-server
-Xmx4g
-Xms4g
-XX:NewSize=256m
-XX:MaxNewSize=256m
-XX:+UseConcMarkSweepGC
-XX:+PrintGCDetails
-XX:+PrintGCTimeStamps
-Duser.timezone=UTC
-Dfile.encoding=UTF-8
-Djava.io.tmpdir=/mnt/tmp
```

Runtime.properties:

```
druid.host=#{IP_ADDR}:8080
druid.port=8080
druid.service=druid/prod/overlord

# Only required if you are autoscaling middle managers
druid.indexer.autoscale.doAutoscale=true
druid.indexer.autoscale.strategy=ec2
druid.indexer.autoscale.workerIdleTimeout=PT90m
druid.indexer.autoscale.terminatePeriod=PT5M
druid.indexer.autoscale.workerVersion=#{WORKER_VERSION}

# Upload all task logs to deep storage
druid.indexer.logs.type=s3
druid.indexer.logs.s3Bucket=druid
druid.indexer.logs.s3Prefix=prod/logs/v1

# Run in remote mode
druid.indexer.runner.type=remote
druid.indexer.runner.minWorkerVersion=#{WORKER_VERSION}

# Store all task state in the metadata storage
druid.indexer.storage.type=metadata
```

### MiddleManager Node

Run:

```
io.druid.cli.Main server middleManager
```

Hardware:

```
r3.8xlarge (Cores: 32, Memory: 244 GB, SSD)
```

JVM Configuration:

```
-server
-Xmx64m
-Xms64m
-XX:+UseConcMarkSweepGC
-XX:+PrintGCDetails
-XX:+PrintGCTimeStamps
-Duser.timezone=UTC
-Dfile.encoding=UTF-8
-Djava.io.tmpdir=/mnt/tmp
```

Runtime.properties:

```
druid.host=#{IP_ADDR}:8080
druid.port=8080
druid.service=druid/prod/middlemanager

# Store task logs in deep storage
druid.indexer.logs.type=s3
druid.indexer.logs.s3Bucket=#{LOGS_BUCKET}
druid.indexer.logs.s3Prefix=prod/logs/v1

# Resources for peons
druid.indexer.runner.javaOpts=-server -Xmx3g -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps
druid.indexer.task.baseTaskDir=/mnt/persistent/task/

# Peon properties
druid.indexer.fork.property.druid.monitoring.monitors=["com.metamx.metrics.JvmMonitor"]
druid.indexer.fork.property.druid.processing.buffer.sizeBytes=536870912
druid.indexer.fork.property.druid.processing.numThreads=2
druid.indexer.fork.property.druid.segmentCache.locations=[{"path": "/mnt/persistent/zk_druid", "maxSize": 0}]
druid.indexer.fork.property.druid.server.http.numThreads=50
druid.indexer.fork.property.druid.storage.archiveBaseKey=prod
druid.indexer.fork.property.druid.storage.archiveBucket=aws-prod-druid-archive
druid.indexer.fork.property.druid.storage.baseKey=prod/v1
druid.indexer.fork.property.druid.storage.bucket=druid
druid.indexer.fork.property.druid.storage.type=s3

druid.worker.capacity=9
druid.worker.ip=#{IP_ADDR}
druid.worker.version=#{WORKER_VERSION}
```

### Coordinator Node

Run:

```
io.druid.cli.Main server coordinator
```

Hardware:

```
m1.xlarge (Cores: 4, Memory: 15.0 GB)
```

JVM Configuration:

```
-server
-Xmx10g
-Xms10g
-XX:NewSize=512m
-XX:MaxNewSize=512m
-XX:+UseG1GC
-XX:+PrintGCDetails
-XX:+PrintGCTimeStamps
-Duser.timezone=UTC
-Dfile.encoding=UTF-8
-Djava.io.tmpdir=/mnt/tmp
```

Runtime.properties:

```
druid.host=#{IP_ADDR}:8080
druid.port=8080
druid.service=druid/prod/coordinator
```

### Historical Node

Run:

```
io.druid.cli.Main server historical
```

Hardware:

```
r3.8xlarge (Cores: 32, Memory: 244 GB, SSD)
```

JVM Configuration:

```
-server
-Xmx12g
-Xms12g
-XX:NewSize=6g
-XX:MaxNewSize=6g
-XX:MaxDirectMemorySize=32g
-XX:+UseConcMarkSweepGC
-XX:+PrintGCDetails
-XX:+PrintGCTimeStamps
-Duser.timezone=UTC
-Dfile.encoding=UTF-8
-Djava.io.tmpdir=/mnt/tmp
```

Runtime.properties:

```
druid.host=#{IP_ADDR}:8080
druid.port=8080
druid.service=druid/prod/historical

druid.historical.cache.useCache=true
druid.historical.cache.populateCache=true

druid.processing.buffer.sizeBytes=1073741824
druid.processing.numThreads=31

druid.server.http.numThreads=50
druid.server.maxSize=300000000000

druid.segmentCache.locations=[{"path": "/mnt/persistent/zk_druid", "maxSize": 300000000000}]

druid.monitoring.monitors=["io.druid.server.metrics.ServerMonitor", "com.metamx.metrics.SysMonitor","com.metamx.metrics.JvmMonitor"]
```

### Broker Node

Run:

```
io.druid.cli.Main server broker
```

Hardware:

```
r3.8xlarge (Cores: 32, Memory: 244 GB, SSD - this hardware is a bit overkill for the broker but we choose it for simplicity)
```

JVM Configuration:

```
-server
-Xmx25g
-Xms25g
-XX:NewSize=6g
-XX:MaxNewSize=6g
-XX:MaxDirectMemorySize=64g
-XX:+UseConcMarkSweepGC
-XX:+PrintGCDetails
-XX:+PrintGCTimeStamps
-Duser.timezone=UTC
-Dfile.encoding=UTF-8
-Djava.io.tmpdir=/mnt/tmp

-Dcom.sun.management.jmxremote.port=17071
-Dcom.sun.management.jmxremote.authenticate=false
-Dcom.sun.management.jmxremote.ssl=false
```

Runtime.properties:

```
druid.host=#{IP_ADDR}:8080
druid.port=8080
druid.service=druid/prod/broker

druid.broker.http.numConnections=20
druid.broker.http.readTimeout=PT5M

druid.processing.buffer.sizeBytes=2147483648
druid.processing.numThreads=31

druid.server.http.numThreads=50
```
