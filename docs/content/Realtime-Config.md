---
layout: doc_page
---
Realtime Node Configuration
===========================
For general Real-time Node information, see [here](Realtime.html).

For Real-time Ingestion, see [Realtime Ingestion](Realtime-ingestion.html).

Quick Start
-----------
Run:

```
io.druid.cli.Main server realtime
```

With the following JVM configuration:

```
-server
-Xmx256m
-Duser.timezone=UTC
-Dfile.encoding=UTF-8

druid.host=localhost
druid.service=realtime
druid.port=8083

druid.extensions.coordinates=["io.druid.extensions:druid-kafka-seven:0.6.171"]


druid.zk.service.host=localhost

# The realtime config file.
druid.realtime.specFile=/path/to/specFile

# Choices: db (hand off segments), noop (do not hand off segments).
druid.publish.type=db

druid.db.connector.connectURI=jdbc\:mysql\://localhost\:3306/druid
druid.db.connector.user=druid
druid.db.connector.password=diurd

druid.processing.buffer.sizeBytes=100000000
```

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
druid.service=druid/prod/realtime

druid.extensions.coordinates=["io.druid.extensions:druid-s3-extensions:0.6.171","io.druid.extensions:druid-kafka-seven:0.6.171"]

druid.zk.service.host=#{ZK_IPs}
druid.zk.paths.base=/druid/prod

druid.s3.accessKey=#{ACCESS_KEY}
druid.s3.secretKey=#{SECRET_KEY}

druid.db.connector.connectURI=jdbc:mysql://#{MYSQL_URL}:3306/druid
druid.db.connector.user=#{MYSQL_USER}
druid.db.connector.password=#{MYSQL_PW}
druid.db.connector.useValidationQuery=true
druid.db.tables.base=prod

druid.publish.type=db

druid.processing.numThreads=3

druid.request.logging.type=file
druid.request.logging.dir=request_logs/

druid.realtime.specFile=conf/schemas.json

druid.segmentCache.locations=[{"path": "/mnt/persistent/zk_druid", "maxSize": 0}]

druid.storage.type=s3
druid.storage.bucket=#{S3_STORAGE_BUCKET}
druid.storage.baseKey=prod-realtime/v1

druid.monitoring.monitors=["com.metamx.metrics.SysMonitor", "io.druid.segment.realtime.RealtimeMetricsMonitor"]

# Emit metrics over http
druid.emitter=http
druid.emitter.http.recipientBaseUrl=#{EMITTER_URL}

# If you choose to compress ZK announcements, you must do so for every node type
druid.announcer.type=batch
druid.curator.compress=true
```

The realtime module also uses several of the default modules in [Configuration](Configuration.html). For more information on the realtime spec file (or configuration file), see [realtime ingestion](Realtime-ingestion.html) page.
