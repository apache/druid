---
layout: doc_page
---
The indexing service is a highly-available, distributed service that runs indexing related tasks. Indexing service [tasks](Tasks.html) create (and sometimes destroy) Druid [segments](Segments.html). The indexing service has a master/slave like architecture.

The indexing service is composed of three main components: a peon component that can run a single task, a middle manager component that manages peons, and an overlord component that manages task distribution to middle managers.
Overlords and middle managers may run on the same node or across multiple nodes while middle managers and peons always run on the same node.

Quick Start
----------------------------------------
Run:

```
io.druid.cli.Main server overlord
```

With the following JVM configuration:

```
-server
-Xmx2g
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
-Ddruid.indexer.runner.javaOpts="-server -Xmx1g"
-Ddruid.indexer.runner.startPort=8081
-Ddruid.indexer.fork.property.druid.computation.buffer.size=268435456
```

You can now submit simple indexing tasks to the indexing service.

<!--
Preamble
--------

The truth is, the indexing service is an experience that is difficult to characterize with words. When they asked me to write this preamble, I was taken aback. I wasn’t quite sure what exactly to write or how to describe this… entity. I accepted the job, as much for the challenge and inner growth as the money, and took to the mountains for reflection. Six months later, I knew I had it, I was done and had achieved the next euphoric victory in the continuous struggle that plagues my life. But, enough about me. This is about the indexing service.

The indexing service is philosophical transcendence, an infallible truth that will shape your soul, mold your character, and define your reality. The indexing service is creating world peace, playing with puppies, unwrapping presents on Christmas morning, cradling a loved one, and beating Goro in Mortal Kombat for the first time. The indexing service is sustainable economic growth, global propensity, and a world of transparent financial transactions. The indexing service is a true belieber. The indexing service is panicking because you forgot you signed up for a course and the big exam is in a few minutes, only to wake up and realize it was all a dream. What is the indexing service? More like what isn’t the indexing service. The indexing service is here and it is ready, but are you?
-->

Indexing Service Overview
-------------------------

![Indexing Service](../img/indexing_service.png "Indexing Service")

Overlord Node
-------------

The overlord node is responsible for accepting tasks, coordinating task distribution, creating locks around tasks, and returning statuses to callers.

#### Usage

Tasks are submitted to the overlord node in the form of JSON objects. Tasks can be submitted via POST requests to:

```
http://<OVERLORD_IP>:<port>/druid/indexer/v1/task
```

Tasks can cancelled via POST requests to:

```
http://<OVERLORD_IP>:<port>/druid/indexer/v1/task/{taskId}/shutdown
```

Issuing the cancel request will kill –9 the task.

Task statuses can be retrieved via GET requests to:

```
http://<OVERLORD_IP>:<port>/druid/indexer/v1/task/{taskId}/status
```

Task segments can be retrieved via GET requests to:

```
http://<OVERLORD_IP>:<port>/druid/indexer/v1/task/{taskId}/segments
```

#### Console

The overlord console can be used to view pending tasks, running tasks, available workers, and recent worker creation and termination. The console can be accessed at:

```
http://<OVERLORD_IP>:8080/console.html
```

#### Autoscaling

The Autoscaling mechanisms currently in place are tightly coupled with our deployment infrastructure but the framework should be in place for other implementations. We are highly open to new implementations or extensions of the existing mechanisms. In our own deployments, middle manager nodes are Amazon AWS EC2 nodes and they are provisioned to register themselves in a [galaxy](https://github.com/ning/galaxy) environment.

If autoscaling is enabled, new middle managers may be added when a task has been in pending state for too long. Middle managers may be terminated if they have not run any tasks for a period of time.

#### JVM Configuration

In addition to the configuration of some of the default modules in [Configuration](Configuration.html), the overlord module requires the following basic configs to run in remote mode:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.runner.type`|Choices "local" or "remote". Indicates whether tasks should be run locally or in a distributed environment.|local|
|`druid.indexer.storage.type`|Choices are "local" or "db". Indicates whether incoming tasks should be stored locally (in heap) or in a database. Storing incoming tasks in a database allows for tasks to be bootstrapped if the overlord should fail.|local|

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
|`druid.indexer.autoscale.doAutoscale`|If set to "true" autoscaling will be enabled.|false|
|`druid.indexer.autoscale.provisionPeriod`|How often to check whether or not new middle managers should be added.|PT1M|
|`druid.indexer.autoscale.terminatePeriod`|How often to check when middle managers should be removed.|PT1H|
|`druid.indexer.autoscale.originTime`|The starting reference timestamp that the terminate period increments upon.|2012-01-01T00:55:00.000Z|
|`druid.indexer.autoscale.strategy`|Choices are "noop" or "ec2". Sets the strategy to run when autoscaling is required.|noop|
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

#### Running

```
io.druid.cli.Main server overlord
```

Note: When running the overlord in local mode, all middle manager and peon configurations must be provided as well.

MiddleManager Node
------------------

The middle manager node is a worker node that executes submitted tasks. Middle Managers forward tasks to peons that run in separate JVMs. Each peon is capable of running only one task at a time, however, a middle manager may have multiple peons.

#### JVM Configuration

Middle managers pass their configurations down to their child peons. The middle manager module requires the following configs:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.worker.ip`|The IP of the worker.|localhost|
|`druid.worker.version`|Version identifier for the middle manager.|0|
|`druid.worker.capacity`|Maximum number of tasks the middle manager can accept.|Number of available processors - 1|
|`druid.indexer.runner.compressZnodes`|Indicates whether or not the middle managers should compress Znodes.|false|
|`druid.indexer.runner.maxZnodeBytes`|The maximum size Znode in bytes that can be created in Zookeeper.|524288|
|`druid.indexer.runner.taskDir`|Temporary intermediate directory used during task execution.|/tmp/persistent|
|`druid.indexer.runner.javaCommand`|Command required to execute java.|java|
|`druid.indexer.runner.javaOpts`|-X Java options to run the peon in its own JVM.|""|
|`druid.indexer.runner.classpath`|Java classpath for the peon.|System.getProperty("java.class.path")|
|`druid.indexer.runner.startPort`|The port that peons begin running on.|8080|
|`druid.indexer.runner.allowedPrefixes`|Whitelist of prefixes for configs that can be passed down to child peons.|"com.metamx", "druid", "io.druid", "user.timezone","file.encoding"|

#### Running

```
io.druid.cli.Main server middleManager
```

Peons
-----
Peons run a single task in a single JVM. Peons are a part of middle managers and should rarely (if ever) be run on their own.

#### JVM Configuration
Although peons inherit the configurations of their parent middle managers, explicit child peon configs can be set by prefixing them with:

```
druid.indexer.fork.property
```

Additional peon configs include:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.peon.mode`|Choices are "local" and "remote". Setting this to local means you intend to run the peon as a standalone node (Not recommended).|remote|
|`druid.indexer.task.baseDir`|Base temporary working directory.|/tmp|
|`druid.indexer.task.baseTaskDir`|Base temporary working directory for tasks.|/tmp/persistent/tasks|
|`druid.indexer.task.hadoopWorkingPath`|Temporary working directory for Hadoop tasks.|/tmp/druid-indexing|
|`druid.indexer.task.defaultRowFlushBoundary`|Highest row count before persisting to disk. Used for indexing generating tasks.|50000|
|`druid.indexer.task.chathandler.type`|Choices are "noop" and "announce". Certain tasks will use service discovery to announce an HTTP endpoint that events can be posted to.|noop|

If the peon is running in remote mode, there must be an overlord up and running. Running peons in remote mode require the following configurations:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.peon.taskActionClient.retry.minWait`|The minimum retry time to communicate with overlord.|PT1M|
|`druid.peon.taskActionClient.retry.maxWait`|The maximum retry time to communicate with overlord.|PT10M|
|`druid.peon.taskActionClient.retry.maxRetryCount`|The maximum number of retries to communicate with overlord.|10|

#### Running

The peon should very rarely ever be run independent of the middle manager.

```
io.druid.cli.Main internal peon <task_file> <status_file>
```

The task file contains the task JSON object.
The status file indicates where the task status will be output.

Tasks
-----

See [Tasks](Tasks.html).
