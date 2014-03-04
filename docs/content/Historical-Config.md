---
layout: doc_page
---
Historical Node Configuration
=============================
For general Historical Node information, see [here](Historical.html).

Quick Start
-----------
Run:

```
io.druid.cli.Main server historical
```

With the following JVM configuration:

```
-server
-Xmx256m
-Duser.timezone=UTC
-Dfile.encoding=UTF-8

druid.host=localhost
druid.service=historical
druid.port=8081

druid.zk.service.host=localhost

druid.server.maxSize=10000000000

# Change these to make Druid faster
druid.processing.buffer.sizeBytes=100000000
druid.processing.numThreads=1

druid.segmentCache.locations=[{"path": "/tmp/druid/indexCache", "maxSize"\: 10000000000}]
```

Note: This will spin up a Historical node with the local filesystem as deep storage.

Production Configs
------------------
These production configs are using S3 as a deep store.

JVM settings:

```
-server
-Xmx#{HEAP_MAX}g
-Xms#{HEAP_MIN}g
-XX:NewSize=#{NEW_SIZE}g
-XX:MaxNewSize=#{MAX_NEW_SIZE}g
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
druid.service=druid/prod/historical/_default

druid.extensions.coordinates=["io.druid.extensions:druid-s3-extensions:#{DRUID_VERSION}"]

druid.zk.service.host=#{ZK_IPs}
druid.zk.paths.base=/druid/prod

druid.s3.accessKey=#{ACCESS_KEY}
druid.s3.secretKey=#{SECRET_KEY}

druid.server.type=historical
druid.server.maxSize=#{SERVER_MAXSIZE}
druid.server.http.numThreads=50

druid.processing.buffer.sizeBytes=#{BUFFER_SIZE}}
druid.processing.numThreads=#{NUM_THREADS}}

druid.segmentCache.locations=[{"path": "/mnt/persistent/zk_druid", "maxSize": #{SERVER_MAXSIZE}}]

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

The historical module uses several of the default modules in [Configuration](Configuration.html) and has no uniques configs of its own.
