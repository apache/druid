---
layout: doc_page
---
Realtime Nodes 
========

Realtime nodes provide a realtime index. Data indexed via these nodes is immediately available for querying. Realtime nodes will periodically build segments representing the data they’ve collected over some span of time and transfer these segments off to [Historical](Historical.html) nodes. They use ZooKeeper to monitor the transfer and MySQL to store metadata about the transfered segment. Once transfered, segments are forgotten by the Realtime nodes.


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

druid.extensions.coordinates=["io.druid.extensions:druid-kafka-seven:0.6.48"]


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

The realtime module also uses several of the default modules in [Configuration](Configuration.html). For more information on the realtime spec file (or configuration file), see [realtime ingestion](Realtime-ingestion.html) page.


Requirements
------------

Realtime nodes currently require a Kafka cluster to sit in front of them and collect results. There’s [more configuration](Tutorial\:-Loading-Your-Data-Part-2.md#set-up-kafka) required for these as well.
