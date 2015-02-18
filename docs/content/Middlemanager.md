---
layout: doc_page
---

Middle Manager Node
------------------

The middle manager node is a worker node that executes submitted tasks. Middle Managers forward tasks to peons that run in separate JVMs.
The reason we have separate JVMs for tasks is for log isolation. Each [Peon](Peons.html) is capable of running only one task at a time, however, a middle manager may have multiple peons.

Quick Start
------------------

#### Running

```
io.druid.cli.Main server middleManager
```

With the following JVM configuration:

```
-Duser.timezone=UTC
-Dfile.encoding=UTF-8

-Ddruid.host=localhost
-Ddruid.port=8091
-Ddruid.service=middleManager

-Ddruid.zk.service.host=localhost

-Ddruid.metadata.storage.connector.connectURI=jdbc:mysql://localhost:3306/druid
-Ddruid.metadata.storage.connector.user=druid
-Ddruid.metadata.storage.connector.password=diurd
-Ddruid.selectors.indexing.serviceName=overlord
-Ddruid.indexer.runner.startPort=8092
-Ddruid.indexer.fork.property.druid.computation.buffer.size=268435456
```

#### JVM Configuration

Middle managers pass their configurations down to their child peons. The middle manager module requires the following configs:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.worker.ip`|The IP of the worker.|localhost|
|`druid.worker.version`|Version identifier for the middle manager.|0|
|`druid.worker.capacity`|Maximum number of tasks the middle manager can accept.|Number of available processors - 1|
|`druid.indexer.runner.compressZnodes`|Indicates whether or not the middle managers should compress Znodes.|false|
|`druid.indexer.runner.maxZnodeBytes`|The maximum size Znode in bytes that can be created in Zookeeper.|524288|
|`druid.indexer.runner.javaCommand`|Command required to execute java.|java|
|`druid.indexer.runner.javaOpts`|-X Java options to run the peon in its own JVM.|""|
|`druid.indexer.runner.classpath`|Java classpath for the peon.|System.getProperty("java.class.path")|
|`druid.indexer.runner.startPort`|The port that peons begin running on.|8100|
|`druid.indexer.runner.allowedPrefixes`|Whitelist of prefixes for configs that can be passed down to child peons.|"com.metamx", "druid", "io.druid", "user.timezone","file.encoding"|

