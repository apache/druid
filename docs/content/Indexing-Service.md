---
layout: doc_page
---
Disclaimer: We are still in the process of finalizing the indexing service and these configs are prone to change at any time. We will announce when we feel the indexing service and the configurations described are stable.

The indexing service is a distributed task/job queue. It accepts requests in the form of [Tasks](Tasks.html) and executes those tasks across a set of worker nodes. Worker capacity can be automatically adjusted based on the number of tasks pending in the system. The indexing service is highly available, has built in retry logic, and can backup per task logs in deep storage.

The indexing service is composed of two main components, a coordinator node that manages task distribution and worker capacity, and worker nodes that execute tasks in separate JVMs.

Preamble
--------

The truth is, the indexing service is an experience that is difficult to characterize with words. When they asked me to write this preamble, I was taken aback. I wasn’t quite sure what exactly to write or how to describe this… entity. I accepted the job, as much for the challenge and inner growth as the money, and took to the mountains for reflection. Six months later, I knew I had it, I was done and had achieved the next euphoric victory in the continuous struggle that plagues my life. But, enough about me. This is about the indexing service.

The indexing service is philosophical transcendence, an infallible truth that will shape your soul, mold your character, and define your reality. The indexing service is creating world peace, playing with puppies, unwrapping presents on Christmas morning, cradling a loved one, and beating Goro in Mortal Kombat for the first time. The indexing service is sustainable economic growth, global propensity, and a world of transparent financial transactions. The indexing service is a true belieber. The indexing service is panicking because you forgot you signed up for a course and the big exam is in a few minutes, only to wake up and realize it was all a dream. What is the indexing service? More like what isn’t the indexing service. The indexing service is here and it is ready, but are you?

Indexer Coordinator Node
------------------------

The indexer coordinator node exposes HTTP endpoints where tasks can be submitted by posting a JSON blob to specific endpoints. It can be started by launching IndexerCoordinatorMain.java. The indexer coordinator node can operate in local mode or remote mode. In local mode, the coordinator and worker run on the same host and port. In remote mode, worker processes run on separate hosts and ports.

Tasks can be submitted via POST requests to:

```
http://<COORDINATOR_IP>:<port>/druid/indexer/v1/task
```

Tasks can cancelled via POST requests to:

```
http://<COORDINATOR_IP>:<port>/druid/indexer/v1/task/{taskId}/shutdown
```

Issuing the cancel request once sends a graceful shutdown request. Graceful shutdowns may not stop a task right away, but instead issue a safe stop command at a point deemed least impactful to the system. Issuing the cancel request twice in succession will kill –9 the task.

Task statuses can be retrieved via GET requests to:

```
http://<COORDINATOR_IP>:<port>/druid/indexer/v1/task/{taskId}/status
```

Task segments can be retrieved via GET requests to:

```
http://<COORDINATOR_IP>:<port>/druid/indexer/v1/task/{taskId}/segments
```

When a task is submitted, the coordinator creates a lock over the data source and interval of the task. The coordinator also stores the task in a MySQL database table. The database table is read at startup time to bootstrap any tasks that may have been submitted to the coordinator but may not yet have been executed.

The coordinator also exposes a simple UI to show what tasks are currently running on what nodes at

```
http://<COORDINATOR_IP>:<port>/static/console.html
```

#### Task Execution

The coordinator retrieves worker setup metadata from the Druid [MySQL](MySQL.html) config table. This metadata contains information about the version of workers to create, the maximum and minimum number of workers in the cluster at one time, and additional information required to automatically create workers.

Tasks are assigned to workers by creating entries under specific /tasks paths associated with a worker, similar to how the Druid master node assigns segments to compute nodes. See [Worker Configuration](Indexing-Service#configuration-1). Once a worker picks up a task, it deletes the task entry and announces a task status under a /status path associated with the worker. Tasks are submitted to a worker until the worker hits capacity. If all workers in a cluster are at capacity, the indexer coordinator node automatically creates new worker resources.

#### Autoscaling

The Autoscaling mechanisms currently in place are tightly coupled with our deployment infrastructure but the framework should be in place for other implementations. We are highly open to new implementations or extensions of the existing mechanisms. In our own deployments, worker nodes are Amazon AWS EC2 nodes and they are provisioned to register themselves in a [galaxy](https://github.com/ning/galaxy) environment.

The Coordinator node controls the number of workers in the cluster according to a worker setup spec that is submitted via a POST request to the indexer at:

```
http://<COORDINATOR_IP>:<port>/druid/indexer/v1/worker/setup
```

A sample worker setup spec is shown below:

```
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

Issuing a GET request at the same URL will return the current worker setup spec that is currently in place. The worker setup spec list above is just a sample and it is possible to write worker setup specs for other deployment environments. A description of the worker setup spec is shown below.

|Property|Description|Default|
|--------|-----------|-------|
|`minVersion`|The coordinator only assigns tasks to workers with a version greater than the minVersion. If this is not specified, the minVersion will be the same as the coordinator version.|none|
|`minNumWorkers`|The minimum number of workers that can be in the cluster at any given time.|0|
|`maxNumWorkers`|The maximum number of workers that can be in the cluster at any given time.|0|
|`nodeData`|A JSON object that contains metadata about new nodes to create.|none|
|`userData`|A JSON object that contains metadata about how the node should register itself on startup. This data is sent with node creation requests.|none|

For more information about configuring Auto-scaling, see [Auto-Scaling Configuration](https://github.com/metamx/druid/wiki/Indexing-Service#auto-scaling-configuration).

#### Running

Indexer Coordinator nodes can be run using the `com.metamx.druid.indexing.coordinator.http.IndexerCoordinatorMain` class.

#### Configuration

Indexer Coordinator nodes require [basic service configuration](https://github.com/metamx/druid/wiki/Configuration#basic-service-configuration). In addition, there are several extra configurations that are required.

```
-Ddruid.zk.paths.indexer.announcementsPath=/druid/indexer/announcements
-Ddruid.zk.paths.indexer.leaderLatchPath=/druid/indexer/leaderLatchPath
-Ddruid.zk.paths.indexer.statusPath=/druid/indexer/status
-Ddruid.zk.paths.indexer.tasksPath=/druid/demo/indexer/tasks

-Ddruid.indexer.runner=remote
-Ddruid.indexer.taskDir=/mnt/persistent/task/
-Ddruid.indexer.configTable=sample_config
-Ddruid.indexer.workerSetupConfigName=worker_setup
-Ddruid.indexer.strategy=ec2
-Ddruid.indexer.hadoopWorkingPath=/tmp/druid-indexing
-Ddruid.indexer.logs.s3bucket=some_bucket
-Ddruid.indexer.logs.s3prefix=some_prefix
```

The indexing service requires some additional Zookeeper configs.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.zk.paths.indexer.announcementsPath`|The base path where workers announce themselves.|none|
|`druid.zk.paths.indexer.leaderLatchPath`|The base that coordinator nodes use to determine a leader.|none|
|`druid.zk.paths.indexer.statusPath`|The base path where workers announce task statuses.|none|
|`druid.zk.paths.indexer.tasksPath`|The base path where the coordinator assigns new tasks.|none|

There’s several additional configs that are required to run tasks.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.runner`|Indicates whether tasks should be run locally or in a distributed environment. "local" or "remote".|local|
|`druid.indexer.taskDir`|Intermediate temporary directory that tasks may use.|none|
|`druid.indexer.configTable`|The MySQL config table where misc configs live.|none|
|`druid.indexer.strategy`|The autoscaling strategy to use.|noop|
|`druid.indexer.hadoopWorkingPath`|Intermediate temporary hadoop working directory that certain index tasks may use.|none|
|`druid.indexer.logs.s3bucket`|S3 bucket to store logs.|none|
|`druid.indexer.logs.s3prefix`|S3 key prefix to store logs.|none|

#### Console

The indexer console can be used to view pending tasks, running tasks, available workers, and recent worker creation and termination. The console can be accessed at:

```
http://<COORDINATOR_IP>:8080/static/console.html
```

Worker Node
-----------

The worker node executes submitted tasks. Workers run tasks in separate JVMs.

#### Running

Worker nodes can be run using the `com.metamx.druid.indexing.worker.http.WorkerMain` class. Worker nodes can automatically be created by the Indexer Coordinator as part of autoscaling.

#### Configuration

Worker nodes require [basic service configuration](https://github.com/metamx/druid/wiki/Configuration#basic-service-configuration). In addition, there are several extra configurations that are required.

```
-Ddruid.worker.version=0
-Ddruid.worker.capacity=3

-Ddruid.indexer.threads=3
-Ddruid.indexer.taskDir=/mnt/persistent/task/
-Ddruid.indexer.hadoopWorkingPath=/tmp/druid-indexing

-Ddruid.worker.masterService=druid:sample_cluster:indexer

-Ddruid.indexer.fork.hostpattern=<IP>:%d
-Ddruid.indexer.fork.startport=8080
-Ddruid.indexer.fork.main=com.metamx.druid.indexing.worker.executor.ExecutorMain
-Ddruid.indexer.fork.opts="-server -Xmx1g -Xms1g -XX:NewSize=256m -XX:MaxNewSize=256m"
-Ddruid.indexer.fork.property.druid.service=druid/sample_cluster/executor

# These configs are the same configs you would set for basic service configuration, just with a different prefix
-Ddruid.indexer.fork.property.druid.monitoring.monitorSystem=false
-Ddruid.indexer.fork.property.druid.computation.buffer.size=268435456
-Ddruid.indexer.fork.property.druid.indexer.taskDir=/mnt/persistent/task/
-Ddruid.indexer.fork.property.druid.processing.formatString=processing-%s
-Ddruid.indexer.fork.property.druid.processing.numThreads=1
-Ddruid.indexer.fork.property.druid.server.maxSize=0
-Ddruid.indexer.fork.property.druid.request.logging.dir=request_logs/
```

Many of the configurations for workers are similar to those for basic service configuration":https://github.com/metamx/druid/wiki/Configuration\#basic-service-configuration, but with a different config prefix. Below we describe the unique worker configs.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.worker.version`|Version identifier for the worker.|0|
|`druid.worker.capacity`|Maximum number of tasks the worker can accept.|1|
|`druid.indexer.threads`|Number of processing threads per worker.|1|
|`druid.worker.masterService`|Name of the indexer coordinator used for service discovery.|none|
|`druid.indexer.fork.hostpattern`|The format of the host name.|none|
|`druid.indexer.fork.startport`|Port in which child JVM starts from.|none|
|`druid.indexer.fork.opts`|JVM options for child JVMs.|none|

