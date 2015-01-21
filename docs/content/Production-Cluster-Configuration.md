---
layout: doc_page
---
Production Cluster Configuration
================================

__This configuration is an example of what a production cluster could look like. Many other hardware combinations are possible! Cheaper hardware is absolutely possible.__

This production Druid cluster assumes that MySQL and Zookeeper are already set up. The deep storage that is used for examples is S3 and memcached is used for a distributed cache.

The nodes that respond to queries (Historical, Broker, and Middle manager nodes) will use as many cores as are available, depending on usage, so it is best to keep these on dedicated machines. The upper limit of effectively utilized cores is not well characterized yet and would depend on types of queries, query load, and the schema. Historical daemons should have a heap a size of at least 1GB per core for normal usage, but could be squeezed into a smaller heap for testing. Since in-memory caching is essential for good performance, even more RAM is better. Broker nodes will use RAM for caching, so they do more than just route queries. SSDs are highly recommended for Historical nodes not all data is loaded in available memory.

The nodes that are responsible for coordination (Coordinator and Overlord nodes) require much less processing.

The effective utilization of cores by Zookeeper, MySQL, and Coordinator nodes is likely to be between 1 and 2 for each process/daemon, so these could potentially share a machine with lots of cores. These daemons work with heap a size between 500MB and 1GB.

We'll use r3.8xlarge nodes for query facing nodes and m1.xlarge nodes for coordination nodes. The following examples work relatively well in production, however, a more optimized tuning for the nodes we selected and more optimal hardware for a Druid cluster are both definitely possible.

For general purposes of high availability, there should be at least 2 of every node type.

To setup a local Druid cluster, see [Simple Cluster Configuration](Simple-Cluster-Configuration.html).

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

druid.extensions.coordinates=["io.druid.extensions:druid-s3-extensions:0.6.171"]

druid.zk.service.host=#{ZK_IPs}
druid.zk.paths.base=/druid/prod

druid.discovery.curator.path=/prod/discovery

druid.s3.accessKey=#{ACCESS_KEY}
druid.s3.secretKey=#{SECRET_KEY}

druid.db.connector.connectURI=jdbc:mysql://#{MYSQL_URL}:3306/druid
druid.db.connector.user=#{MYSQL_USER}
druid.db.connector.password=#{MYSQL_PW}
druid.db.connector.useValidationQuery=true
druid.db.tables.base=prod

# Only required if you are autoscaling middle managers
druid.indexer.autoscale.doAutoscale=true
druid.indexer.autoscale.strategy=ec2
druid.indexer.autoscale.workerIdleTimeout=PT90m
druid.indexer.autoscale.terminatePeriod=PT5M
druid.indexer.autoscale.workerVersion=#{WORKER_VERSION}

# Upload all task logs to deep storage
druid.indexer.logs.type=s3
druid.indexer.logs.s3Bucket=#{LOGS_BUCKET}
druid.indexer.logs.s3Prefix=prod/logs/v1

# Run in remote mode
druid.indexer.runner.type=remote
druid.indexer.runner.compressZnodes=true
druid.indexer.runner.minWorkerVersion=#{WORKER_VERSION}

# Store all task state in MySQL
druid.indexer.storage.type=db

druid.monitoring.monitors=["com.metamx.metrics.SysMonitor","com.metamx.metrics.JvmMonitor"]

# Emit metrics over http
druid.emitter=http
druid.emitter.http.recipientBaseUrl=#{EMITTER_URL}

# If you choose to compress ZK announcements, you must do so for every node type
druid.announcer.type=batch
druid.curator.compress=true
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

druid.extensions.coordinates=["io.druid.extensions:druid-s3-extensions:0.6.171","io.druid.extensions:druid-kafka-seven:0.6.171"]

druid.zk.service.host=#{ZK_IPs}
druid.zk.paths.base=/druid/prod

druid.discovery.curator.path=/prod/discovery

druid.s3.accessKey=#{ACCESS_KEY}
druid.s3.secretKey=#{SECRET_KEY}

# Store task logs in deep storage
druid.indexer.logs.type=s3
druid.indexer.logs.s3Bucket=#{LOGS_BUCKET}
druid.indexer.logs.s3Prefix=prod/logs/v1

# Dedicate more resources to peons
druid.indexer.runner.javaOpts=-server -Xmx3g -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps
druid.indexer.task.baseTaskDir=/mnt/persistent/task/
druid.indexer.task.chathandler.type=announce

druid.indexer.fork.property.druid.indexer.hadoopWorkingPath=/tmp/druid-indexing
druid.indexer.fork.property.druid.computation.buffer.size=536870912
druid.indexer.fork.property.druid.processing.numThreads=3
druid.indexer.fork.property.druid.request.logging.type=file
druid.indexer.fork.property.druid.request.logging.dir=request_logs/
druid.indexer.fork.property.druid.segmentCache.locations=[{"path": "/mnt/persistent/zk_druid", "maxSize": 0}]
druid.indexer.fork.property.druid.server.http.numThreads=50
druid.indexer.fork.property.druid.storage.type=s3
druid.indexer.fork.property.druid.storage.baseKey=prod/v1
druid.indexer.fork.property.druid.storage.bucket=#{LOGS_BUCKET}

druid.worker.capacity=10
druid.worker.ip=#{IP_ADDR}
druid.worker.version=#{WORKER_VERSION}

druid.selectors.indexing.serviceName=druid:prod:overlord

druid.monitoring.monitors=["com.metamx.metrics.SysMonitor","com.metamx.metrics.JvmMonitor"]

# Emit metrics over http
druid.emitter=http
druid.emitter.http.recipientBaseUrl=#{EMITTER_URL}

# If you choose to compress ZK announcements, you must do so for every node type
druid.announcer.type=batch
druid.curator.compress=true
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

druid.zk.service.host=#{ZK_IPs}
druid.zk.paths.base=/druid/prod

druid.discovery.curator.path=/prod/discovery

druid.db.connector.connectURI=jdbc:mysql://#{MYSQL_URL}:3306/druid
druid.db.connector.user=#{MYSQL_USER}
druid.db.connector.password=#{MYSQL_PW}
druid.db.connector.useValidationQuery=true
druid.db.tables.base=prod

druid.selectors.indexing.serviceName=druid:prod:overlord

druid.monitoring.monitors=["com.metamx.metrics.SysMonitor", "com.metamx.metrics.JvmMonitor"]

# Emit metrics over http
druid.emitter=http
druid.emitter.http.recipientBaseUrl=#{EMITTER_URL}

# If you choose to compress ZK announcements, you must do so for every node type
druid.announcer.type=batch
druid.curator.compress=true
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

druid.extensions.coordinates=["io.druid.extensions:druid-s3-extensions:0.6.171"]

druid.zk.service.host=#{ZK_IPs}
druid.zk.paths.base=/druid/prod

druid.s3.accessKey=#{ACCESS_KEY}
druid.s3.secretKey=#{SECRET_KEY}

druid.server.maxSize=300000000000
druid.server.http.numThreads=50

druid.processing.buffer.sizeBytes=1073741824
druid.processing.numThreads=31

druid.segmentCache.locations=[{"path": "/mnt/persistent/zk_druid", "maxSize": 300000000000}]

druid.request.logging.type=file
druid.request.logging.dir=request_logs/

druid.monitoring.monitors=["io.druid.server.metrics.ServerMonitor", "com.metamx.metrics.SysMonitor","com.metamx.metrics.JvmMonitor"]

# Emit metrics over http
druid.emitter=http
druid.emitter.http.recipientBaseUrl=#{EMITTER_URL}

# If you choose to compress ZK announcements, you must do so for every node type
druid.announcer.type=batch
druid.curator.compress=true
```

### Broker Node

Run:

```
io.druid.cli.Main server broker
```

Hardware:

```
r3.8xlarge (Cores: 32, Memory: 244 GB, SSD)
```

JVM Configuration:

```
-server
-Xmx50g
-Xms50g
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

druid.zk.service.host=#{ZK_IPs}
druid.zk.paths.base=/druid/prod

druid.discovery.curator.path=/prod/discovery

druid.broker.cache.type=memcached
druid.broker.cache.hosts=#{MC_HOST1}:11211,#{MC_HOST2}:11211,#{MC_HOST3}:11211
druid.broker.cache.expiration=2147483647
druid.broker.cache.memcachedPrefix=d1
druid.broker.http.numConnections=20
druid.broker.http.readTimeout=PT5M

druid.processing.buffer.sizeBytes=2147483648
druid.processing.numThreads=31

druid.server.http.numThreads=50

druid.request.logging.type=emitter
druid.request.logging.feed=druid_requests

druid.monitoring.monitors=["com.metamx.metrics.SysMonitor","com.metamx.metrics.JvmMonitor"]

# Emit metrics over http
druid.emitter=http
druid.emitter.http.recipientBaseUrl=#{EMITTER_URL}

# If you choose to compress ZK announcements, you must do so for every node type
druid.announcer.type=batch
druid.curator.compress=true
```
