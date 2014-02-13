---
layout: doc_page
---
For general Indexing Service information, see [here](Indexing-Service.html).

Quick Start
-----------

```
io.druid.cli.Main server overlord
```

With the following JVM configuration:

```
-server
-Xmx256m
-Duser.timezone=UTC
-Dfile.encoding=UTF-8

-Ddruid.host=localhost
-Ddruid.port=8080
-Ddruid.service=overlord

-Ddruid.zk.service.host=localhost

-Ddruid.db.connector.connectURI=jdbc:mysql://localhost:3306/druid
-Ddruid.db.connector.user=druid
-Ddruid.db.connector.password=diurd

-Ddruid.selectors.indexing.serviceName=overlord
-Ddruid.indexer.queue.startDelay=PT0M
-Ddruid.indexer.runner.javaOpts="-server -Xmx1g"
-Ddruid.indexer.runner.startPort=8081
-Ddruid.indexer.fork.property.druid.computation.buffer.size=268435456
```

Production Configs
------------------
These production configs are using S3 as a deep store and running the indexing service in distributed mode.

JVM settings for both overlord and middle manager:

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

Runtime.properties for overlord:

```
druid.host=#{IP_ADDR}:8080
druid.port=8080
druid.service=druid/prod/indexer

druid.extensions.coordinates=["io.druid.extensions:druid-s3-extensions:0.6.59"]

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

druid.indexer.autoscale.doAutoscale=true
druid.indexer.autoscale.strategy=ec2
druid.indexer.autoscale.workerIdleTimeout=PT90m
druid.indexer.autoscale.terminatePeriod=PT5M
druid.indexer.autoscale.workerVersion=#{WORKER_VERSION}

druid.indexer.firehoseId.prefix=druid:prod:chat
druid.indexer.logs.type=s3
druid.indexer.logs.s3Bucket=#{INDEXER_LOGS_BUCKET}
druid.indexer.logs.s3Prefix=prod/logs/v1
druid.indexer.runner.type=remote
druid.indexer.runner.compressZnodes=true
druid.indexer.runner.minWorkerVersion=#{WORKER_VERSION}
druid.indexer.storage.type=db

druid.monitoring.monitors=["com.metamx.metrics.SysMonitor","com.metamx.metrics.JvmMonitor"]

# Emit metrics over http
druid.emitter=http
druid.emitter.http.recipientBaseUrl=#{EMITTER_URL}

# If you choose to compress ZK announcements, you must do so for every node type
druid.announcer.type=batch
druid.curator.compress=true
```

Runtime.properties for middle manager:

```
druid.host=#{IP_ADDR}:8080
druid.port=8080
druid.service=druid/prod/worker

druid.extensions.coordinates=["io.druid.extensions:druid-s3-extensions:0.6.59","io.druid.extensions:druid-kafka-seven:0.6.59"]

druid.zk.service.host=#{ZK_IPs}
druid.zk.paths.base=/druid/prod

druid.discovery.curator.path=/prod/discovery

druid.s3.accessKey=#{ACCESS_KEY}
druid.s3.secretKey=#{SECRET_KEY}

druid.indexer.logs.type=s3
druid.indexer.logs.s3Bucket=#{INDEXER_LOGS_BUCKET}
druid.indexer.logs.s3Prefix=prod/logs/v1
druid.indexer.runner.javaOpts=-server -Xmx#{HEAP_MAX}g -Xms#{HEAP_MIN}g -XX:NewSize=#{NEW_SIZE}m -XX:MaxNewSize=#{MAX_NEW_SIZE}6m -XX:+PrintGCDetails -XX:+PrintGCTimeStamps
druid.indexer.runner.startPort=8081
druid.indexer.runner.taskDir=/mnt/persistent/task/
druid.indexer.task.taskDir=/mnt/persistent/task/
druid.indexer.task.chathandler.type=announce

druid.indexer.firehoseId.prefix=druid:prod:chat

druid.indexer.fork.property.druid.indexer.hadoopWorkingPath=/tmp/druid-indexing
druid.indexer.fork.property.druid.computation.buffer.size=#{BUFFER_SIZE}
druid.indexer.fork.property.druid.processing.numThreads=#{NUM_WORKER_THREADS}
druid.indexer.fork.property.druid.request.logging.type=file
druid.indexer.fork.property.druid.request.logging.dir=request_logs/
druid.indexer.fork.property.druid.segmentCache.locations=[{"path": "/mnt/persistent/zk_druid", "maxSize": 0}]
druid.indexer.fork.property.druid.storage.type=s3
druid.indexer.fork.property.druid.storage.baseKey=prod/v1
druid.indexer.fork.property.druid.storage.bucket=#{INDEXER_LOGS_BUCKET}
druid.server.http.numThreads=20

druid.worker.capacity=#{NUM_WORKER_THREADS}
druid.worker.ip=#{IP_ADDR}
druid.worker.version=#{WORKER_VERSION}

druid.selectors.indexing.serviceName=druid:prod:indexer

druid.monitoring.monitors=["com.metamx.metrics.SysMonitor","com.metamx.metrics.JvmMonitor"]

# Emit metrics over http
druid.emitter=http
druid.emitter.http.recipientBaseUrl=#{EMITTER_URL}

# If you choose to compress ZK announcements, you must do so for every node type
druid.announcer.type=batch
druid.curator.compress=true
```

#### Runtime Configuration

In addition to the configuration of some of the default modules in [Configuration](Configuration.html), the overlord has the following basic configs:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.runner.type`|Choices "local" or "remote". Indicates whether tasks should be run locally or in a distributed environment.|local|
|`druid.indexer.storage.type`|Choices are "local" or "db". Indicates whether incoming tasks should be stored locally (in heap) or in a database. Storing incoming tasks in a database allows for tasks to be resumed if the overlord should fail.|local|
|`druid.indexer.storage.recentlyFinishedThreshold`|A duration of time to store task results.|PT24H|
|`druid.indexer.queue.maxSize`|Maximum number of active tasks at one time.|Integer.MAX_VALUE|
|`druid.indexer.queue.startDelay`|Sleep this long before starting overlord queue management. This can be useful to give a cluster time to re-orient itself after e.g. a widespread network issue.|PT1M|
|`druid.indexer.queue.restartDelay`|Sleep this long when overlord queue management throws an exception before trying again.|PT30S|
|`druid.indexer.queue.storageSyncRate`|Sync overlord state this often with an underlying task persistence mechanism.|PT1M|

The following configs only apply if the overlord is running in remote mode:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.runner.taskAssignmentTimeout`|How long to wait after a task as been assigned to a middle manager before throwing an error.|PT5M|
|`druid.indexer.runner.minWorkerVersion`|The minimum middle manager version to send tasks to. |none|
|`druid.indexer.runner.compressZnodes`|Indicates whether or not the overlord should expect middle managers to compress Znodes.|false|
|`druid.indexer.runner.maxZnodeBytes`|The maximum size Znode in bytes that can be created in Zookeeper.|524288|

There are additional configs for autoscaling (if it is enabled):

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.autoscale.strategy`|Choices are "noop" or "ec2". Sets the strategy to run when autoscaling is required.|noop|
|`druid.indexer.autoscale.doAutoscale`|If set to "true" autoscaling will be enabled.|false|
|`druid.indexer.autoscale.provisionPeriod`|How often to check whether or not new middle managers should be added.|PT1M|
|`druid.indexer.autoscale.terminatePeriod`|How often to check when middle managers should be removed.|PT1H|
|`druid.indexer.autoscale.originTime`|The starting reference timestamp that the terminate period increments upon.|2012-01-01T00:55:00.000Z|
|`druid.indexer.autoscale.workerIdleTimeout`|How long can a worker be idle (not a run task) before it can be considered for termination.|PT10M|
|`druid.indexer.autoscale.maxScalingDuration`|How long the overlord will wait around for a middle manager to show up before giving up.|PT15M|
|`druid.indexer.autoscale.numEventsToTrack`|The number of autoscaling related events (node creation and termination) to track.|10|
|`druid.indexer.autoscale.pendingTaskTimeout`|How long a task can be in "pending" state before the overlord tries to scale up.|PT30S|
|`druid.indexer.autoscale.workerVersion`|If set, will only create nodes of set version during autoscaling. Overrides dynamic configuration. |null|
|`druid.indexer.autoscale.workerPort`|The port that middle managers will run on.|8080|

#### Dynamic Configuration

Overlord dynamic configuration is mainly for autoscaling. The overlord reads a worker setup spec as a JSON object from the Druid [MySQL](MySQL.html) config table. This object contains information about the version of middle managers to create, the maximum and minimum number of middle managers in the cluster at one time, and additional information required to automatically create middle managers.

The JSON object can be submitted to the overlord via a POST request at:

```
http://<COORDINATOR_IP>:<port>/druid/indexer/v1/worker/setup
```

A sample worker setup spec is shown below:

```json
{
  "minVersion":"some_version",
  "minNumWorkers":"0",
  "maxNumWorkers":"10",
  "nodeData": {
    "type":"ec2",
    "amiId":"ami-someId",
    "instanceType":"m1.xlarge",
    "minInstances":"1",
    "maxInstances":"1",
    "securityGroupIds":["securityGroupIds"],
    "keyName":"keyName"
  },
  "userData":{
    "classType":"galaxy",
    "env":"druid",
    "version":"druid_version",
    "type":"sample_cluster/worker"
  }
}
```

Issuing a GET request at the same URL will return the current worker setup spec that is currently in place. The worker setup spec list above is just a sample and it is possible to extend the code base for other deployment environments. A description of the worker setup spec is shown below.

|Property|Description|Default|
|--------|-----------|-------|
|`minVersion`|The coordinator only assigns tasks to workers with a version greater than the minVersion. If this is not specified, the minVersion will be the same as the coordinator version.|none|
|`minNumWorkers`|The minimum number of workers that can be in the cluster at any given time.|0|
|`maxNumWorkers`|The maximum number of workers that can be in the cluster at any given time.|0|
|`nodeData`|A JSON object that contains metadata about new nodes to create.|none|
|`userData`|A JSON object that contains metadata about how the node should register itself on startup. This data is sent with node creation requests.|none|
