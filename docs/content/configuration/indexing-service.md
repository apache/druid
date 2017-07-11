---
layout: doc_page
---
For general Indexing Service information, see [here](../design/indexing-service.html).

## Runtime Configuration

The indexing service uses several of the global configs in [Configuration](../configuration/index.html) and has the following set of configurations as well:

### Must be set on Overlord and Middle Manager

#### Overlord Node Configs

|Property|Description|Default|
|--------|-----------|-------|
|`druid.host`|The host for the current node. This is used to advertise the current processes location as reachable from another node and should generally be specified such that `http://${druid.host}/` could actually talk to this process|InetAddress.getLocalHost().getCanonicalHostName()|
|`druid.plaintextPort`|This is the port to actually listen on; unless port mapping is used, this will be the same port as is on `druid.host`|8090|
|`druid.tlsPort`|TLS port for HTTPS connector, if [druid.server.http.tls](../operations/tls-support.html) is set then this config will be used. If `druid.host` contains port then that port will be ignored. This should be a non-negative Integer.|8290|
|`druid.service`|The name of the service. This is used as a dimension when emitting metrics and alerts to differentiate between the various services|druid/overlord|

#### MiddleManager Node Configs

|Property|Description|Default|
|--------|-----------|-------|
|`druid.host`|The host for the current node. This is used to advertise the current processes location as reachable from another node and should generally be specified such that `http://${druid.host}/` could actually talk to this process|InetAddress.getLocalHost().getCanonicalHostName()|
|`druid.plaintextPort`|This is the port to actually listen on; unless port mapping is used, this will be the same port as is on `druid.host`|8091|
|`druid.tlsPort`|TLS port for HTTPS connector, if [druid.server.http.tls](../operations/tls-support.html) is set then this config will be used. If `druid.host` contains port then that port will be ignored. This should be a non-negative Integer.|8291|
|`druid.service`|The name of the service. This is used as a dimension when emitting metrics and alerts to differentiate between the various services|druid/middlemanager|

#### Task Logging

If you are running the indexing service in remote mode, the task logs must be stored in S3, Azure Blob Store, Google Cloud Storage or HDFS.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.logs.type`|Choices:noop, s3, azure, google, hdfs, file. Where to store task logs|file|

You can also configure the Overlord to automatically retain the task logs only for last x milliseconds by configuring following additional properties.
Caution: Automatic log file deletion typically works based on log file modification timestamp on the backing store, so large clock skews between druid nodes and backing store nodes might result in un-intended behavior.  

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.logs.kill.enabled`|Boolean value for whether to enable deletion of old task logs. |false|
|`druid.indexer.logs.kill.durationToRetain`| Required if kill is enabled. In milliseconds, task logs to be retained created in last x milliseconds. |None|
|`druid.indexer.logs.kill.initialDelay`| Optional. Number of milliseconds after overlord start when first auto kill is run. |random value less than 300000 (5 mins)|
|`druid.indexer.logs.kill.delay`|Optional. Number of milliseconds of delay between successive executions of auto kill run. |21600000 (6 hours)|

##### File Task Logs

Store task logs in the local filesystem.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.logs.directory`|Local filesystem path.|log|

##### S3 Task Logs

Store task logs in S3.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.logs.s3Bucket`|S3 bucket name.|none|
|`druid.indexer.logs.s3Prefix`|S3 key prefix.|none|

#### Azure Blob Store Task Logs
Store task logs in Azure Blob Store.

Note: this uses the same storage account as the deep storage module for azure.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.logs.container`|The Azure Blob Store container to write logs to|none|
|`druid.indexer.logs.prefix`|The path to prepend to logs|none|

#### Google Cloud Storage Task Logs
Store task logs in Google Cloud Storage.

Note: this uses the same storage settings as the deep storage module for google.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.logs.bucket`|The Google Cloud Storage bucket to write logs to|none|
|`druid.indexer.logs.prefix`|The path to prepend to logs|none|

##### HDFS Task Logs

Store task logs in HDFS.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.logs.directory`|The directory to store logs.|none|

### Overlord Configs

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.runner.type`|Choices "local" or "remote". Indicates whether tasks should be run locally or in a distributed environment.|local|
|`druid.indexer.storage.type`|Choices are "local" or "metadata". Indicates whether incoming tasks should be stored locally (in heap) or in metadata storage. Storing incoming tasks in metadata storage allows for tasks to be resumed if the overlord should fail.|local|
|`druid.indexer.storage.recentlyFinishedThreshold`|A duration of time to store task results.|PT24H|
|`druid.indexer.queue.maxSize`|Maximum number of active tasks at one time.|Integer.MAX_VALUE|
|`druid.indexer.queue.startDelay`|Sleep this long before starting overlord queue management. This can be useful to give a cluster time to re-orient itself after e.g. a widespread network issue.|PT1M|
|`druid.indexer.queue.restartDelay`|Sleep this long when overlord queue management throws an exception before trying again.|PT30S|
|`druid.indexer.queue.storageSyncRate`|Sync overlord state this often with an underlying task persistence mechanism.|PT1M|

The following configs only apply if the overlord is running in remote mode:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.runner.taskAssignmentTimeout`|How long to wait after a task as been assigned to a middle manager before throwing an error.|PT5M|
|`druid.indexer.runner.minWorkerVersion`|The minimum middle manager version to send tasks to. |"0"|
|`druid.indexer.runner.compressZnodes`|Indicates whether or not the overlord should expect middle managers to compress Znodes.|true|
|`druid.indexer.runner.maxZnodeBytes`|The maximum size Znode in bytes that can be created in Zookeeper.|524288|
|`druid.indexer.runner.taskCleanupTimeout`|How long to wait before failing a task after a middle manager is disconnected from Zookeeper.|PT15M|
|`druid.indexer.runner.taskShutdownLinkTimeout`|How long to wait on a shutdown request to a middle manager before timing out|PT1M|
|`druid.indexer.runner.pendingTasksRunnerNumThreads`|Number of threads to allocate pending-tasks to workers, must be at least 1.|1|
|`druid.indexer.runner.maxRetriesBeforeBlacklist`|Number of consecutive times the middle manager can fail tasks,  before the worker is blacklisted, must be at least 1|5|
|`druid.indexer.runner.workerBlackListBackoffTime`|How long to wait before a task is whitelisted again. This value should be greater that the value set for taskBlackListCleanupPeriod.|PT15M|
|`druid.indexer.runner.workerBlackListCleanupPeriod`|A duration after which the cleanup thread will startup to clean blacklisted workers.|PT5M|
|`druid.indexer.runner.maxPercentageBlacklistWorkers`|The maximum percentage of workers to blacklist, this must be between 0 and 100.|20|

There are additional configs for autoscaling (if it is enabled):

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.autoscale.strategy`|Choices are "noop" or "ec2". Sets the strategy to run when autoscaling is required.|noop|
|`druid.indexer.autoscale.doAutoscale`|If set to "true" autoscaling will be enabled.|false|
|`druid.indexer.autoscale.provisionPeriod`|How often to check whether or not new middle managers should be added.|PT1M|
|`druid.indexer.autoscale.terminatePeriod`|How often to check when middle managers should be removed.|PT5M|
|`druid.indexer.autoscale.originTime`|The starting reference timestamp that the terminate period increments upon.|2012-01-01T00:55:00.000Z|
|`druid.indexer.autoscale.workerIdleTimeout`|How long can a worker be idle (not a run task) before it can be considered for termination.|PT90M|
|`druid.indexer.autoscale.maxScalingDuration`|How long the overlord will wait around for a middle manager to show up before giving up.|PT15M|
|`druid.indexer.autoscale.numEventsToTrack`|The number of autoscaling related events (node creation and termination) to track.|10|
|`druid.indexer.autoscale.pendingTaskTimeout`|How long a task can be in "pending" state before the overlord tries to scale up.|PT30S|
|`druid.indexer.autoscale.workerVersion`|If set, will only create nodes of set version during autoscaling. Overrides dynamic configuration. |null|
|`druid.indexer.autoscale.workerPort`|The port that middle managers will run on.|8080|

#### Dynamic Configuration

The overlord can dynamically change worker behavior.

The JSON object can be submitted to the overlord via a POST request at:

```
http://<OVERLORD_IP>:<port>/druid/indexer/v1/worker
```

Optional Header Parameters for auditing the config change can also be specified.

|Header Param Name| Description | Default |
|----------|-------------|---------|
|`X-Druid-Author`| author making the config change|""|
|`X-Druid-Comment`| comment describing the change being done|""|

A sample worker config spec is shown below:

```json
{
  "selectStrategy": {
    "type": "fillCapacityWithAffinity",
    "affinityConfig": {
      "affinity": {
        "datasource1": ["host1:port", "host2:port"],
        "datasource2": ["host3:port"]
      }
    }
  },
  "autoScaler": {
    "type": "ec2",
    "minNumWorkers": 2,
    "maxNumWorkers": 12,
    "envConfig": {
      "availabilityZone": "us-east-1a",
      "nodeData": {
        "amiId": "${AMI}",
        "instanceType": "c3.8xlarge",
        "minInstances": 1,
        "maxInstances": 1,
        "securityGroupIds": ["${IDs}"],
        "keyName": ${KEY_NAME}
      },
      "userData": {
        "impl": "string",
        "data": "${SCRIPT_COMMAND}",
        "versionReplacementString": ":VERSION:",
        "version": null
      }
    }
  }
}
```

Issuing a GET request at the same URL will return the current worker config spec that is currently in place. The worker config spec list above is just a sample for EC2 and it is possible to extend the code base for other deployment environments. A description of the worker config spec is shown below.

|Property|Description|Default|
|--------|-----------|-------|
|`selectStrategy`|How to assign tasks to middle managers. Choices are `fillCapacity`, `fillCapacityWithAffinity`, `equalDistribution`, `equalDistributionWithAffinity` and `javascript`.|fillCapacity|
|`autoScaler`|Only used if autoscaling is enabled. See below.|null|

To view the audit history of worker config issue a GET request to the URL -

```
http://<OVERLORD_IP>:<port>/druid/indexer/v1/worker/history?interval=<interval>
```

default value of interval can be specified by setting `druid.audit.manager.auditHistoryMillis` (1 week if not configured) in overlord runtime.properties.

To view last <n> entries of the audit history of worker config issue a GET request to the URL -

```
http://<OVERLORD_IP>:<port>/druid/indexer/v1/worker/history?count=<n>
```

#### Worker Select Strategy

##### Fill Capacity

Workers are assigned tasks until capacity.

|Property|Description|Default|
|--------|-----------|-------|
|`type`|`fillCapacity`.|required; must be `fillCapacity`|

Note that, if `druid.indexer.runner.pendingTasksRunnerNumThreads` is set to n (> 1) then it means to fill n workers upto capacity simultaneously and then moving on.

##### Fill Capacity With Affinity

An affinity config can be provided.

|Property|Description|Default|
|--------|-----------|-------|
|`type`|`fillCapacityWithAffinity`.|required; must be `fillCapacityWithAffinity`|
|`affinity`|JSON object mapping a datasource String name to a list of indexing service middle manager host:port String values. Druid doesn't perform DNS resolution, so the 'host' value must match what is configured on the middle manager and what the middle manager announces itself as (examine the Overlord logs to see what your middle manager announces itself as).|{}|

Tasks will try to be assigned to preferred workers. Fill capacity strategy is used if no preference for a datasource specified.

Note that, if `druid.indexer.runner.pendingTasksRunnerNumThreads` is set to n (> 1) then it means to fill n preferred workers upto capacity simultaneously and then moving on.

##### Equal Distribution

The workers with the least amount of tasks is assigned the task.

|Property|Description|Default|
|--------|-----------|-------|
|`type`|`equalDistribution`.|required; must be `equalDistribution`|

##### Equal Distribution With Affinity

An affinity config can be provided.

|Property|Description|Default|
|--------|-----------|-------|
|`type`|`equalDistributionWithAffinity`.|required; must be `equalDistributionWithAffinity`|
|`affinity`|Exactly same with `fillCapacityWithAffinity` 's affinity.|{}|

Tasks will try to be assigned to preferred workers. Equal Distribution strategy is used if no preference for a datasource specified.


##### Javascript

Allows defining arbitrary logic for selecting workers to run task using a JavaScript function.
The function is passed remoteTaskRunnerConfig, map of workerId to available workers and task to be executed and returns the workerId on which the task should be run or null if the task cannot be run.
It can be used for rapid development of missing features where the worker selection logic is to be changed or tuned often.
If the selection logic is quite complex and cannot be easily tested in javascript environment,
its better to write a druid extension module with extending current worker selection strategies written in java.


|Property|Description|Default|
|--------|-----------|-------|
|`type`|`javascript`.|required; must be `javascript`|
|`function`|String representing javascript function||

Example: a function that sends batch_index_task to workers 10.0.0.1 and 10.0.0.2 and all other tasks to other available workers.

```
{
"type":"javascript",
"function":"function (config, zkWorkers, task) {\nvar batch_workers = new java.util.ArrayList();\nbatch_workers.add(\"10.0.0.1\");\nbatch_workers.add(\"10.0.0.2\");\nworkers = zkWorkers.keySet().toArray();\nvar sortedWorkers = new Array()\n;for(var i = 0; i < workers.length; i++){\n sortedWorkers[i] = workers[i];\n}\nArray.prototype.sort.call(sortedWorkers,function(a, b){return zkWorkers.get(b).getCurrCapacityUsed() - zkWorkers.get(a).getCurrCapacityUsed();});\nvar minWorkerVer = config.getMinWorkerVersion();\nfor (var i = 0; i < sortedWorkers.length; i++) {\n var worker = sortedWorkers[i];\n  var zkWorker = zkWorkers.get(worker);\n  if(zkWorker.canRunTask(task) && zkWorker.isValidVersion(minWorkerVer)){\n    if(task.getType() == 'index_hadoop' && batch_workers.contains(worker)){\n      return worker;\n    } else {\n      if(task.getType() != 'index_hadoop' && !batch_workers.contains(worker)){\n        return worker;\n      }\n    }\n  }\n}\nreturn null;\n}"
}
```

<div class="note info">
JavaScript-based functionality is disabled by default. Please refer to the Druid <a href="../development/javascript.html">JavaScript programming guide</a> for guidelines about using Druid's JavaScript functionality, including instructions on how to enable it.
</div>

#### Autoscaler

Amazon's EC2 is currently the only supported autoscaler.

|Property|Description|Default|
|--------|-----------|-------|
|`minNumWorkers`|The minimum number of workers that can be in the cluster at any given time.|0|
|`maxNumWorkers`|The maximum number of workers that can be in the cluster at any given time.|0|
|`availabilityZone`|What availability zone to run in.|none|
|`nodeData`|A JSON object that describes how to launch new nodes.|none; required|
|`userData`|A JSON object that describes how to configure new nodes. If you have set druid.indexer.autoscale.workerVersion, this must have a versionReplacementString. Otherwise, a versionReplacementString is not necessary.|none; optional|

### MiddleManager Configs

Middle managers pass their configurations down to their child peons. The middle manager requires the following configs:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.runner.allowedPrefixes`|Whitelist of prefixes for configs that can be passed down to child peons.|"com.metamx", "druid", "io.druid", "user.timezone","file.encoding"|
|`druid.indexer.runner.compressZnodes`|Indicates whether or not the middle managers should compress Znodes.|true|
|`druid.indexer.runner.classpath`|Java classpath for the peon.|System.getProperty("java.class.path")|
|`druid.indexer.runner.javaCommand`|Command required to execute java.|java|
|`druid.indexer.runner.javaOpts`|*DEPRECATED* A string of -X Java options to pass to the peon's JVM. Quotable parameters or parameters with spaces are encouraged to use javaOptsArray|""|
|`druid.indexer.runner.javaOptsArray`|A json array of strings to be passed in as options to the peon's jvm. This is additive to javaOpts and is recommended for properly handling arguments which contain quotes or spaces like `["-XX:OnOutOfMemoryError=kill -9 %p"]`|`[]`|
|`druid.indexer.runner.maxZnodeBytes`|The maximum size Znode in bytes that can be created in Zookeeper.|524288|
|`druid.indexer.runner.startPort`|Starting port used for peon processes, should be greater than 1023.|8100|
|`druid.indexer.runner.tlsStartPort`|Starting TLS port for peon processes, should be greater than 1023.|8300|
|`druid.indexer.runner.separateIngestionEndpoint`|*Deprecated.* Use separate server and consequently separate jetty thread pool for ingesting events. Not supported with TLS.|false|
|`druid.worker.ip`|The IP of the worker.|localhost|
|`druid.worker.version`|Version identifier for the middle manager.|0|
|`druid.worker.capacity`|Maximum number of tasks the middle manager can accept.|Number of available processors - 1|


#### Peon Configs
Although peons inherit the configurations of their parent middle managers, explicit child peon configs in middle manager can be set by prefixing them with:

```
druid.indexer.fork.property
```
Additional peon configs include:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.peon.mode`|Choices are "local" and "remote". Setting this to local means you intend to run the peon as a standalone node (Not recommended).|remote|
|`druid.indexer.task.baseDir`|Base temporary working directory.|`System.getProperty("java.io.tmpdir")`|
|`druid.indexer.task.baseTaskDir`|Base temporary working directory for tasks.|`${druid.indexer.task.baseDir}/persistent/tasks`|
|`druid.indexer.task.defaultHadoopCoordinates`|Hadoop version to use with HadoopIndexTasks that do not request a particular version.|org.apache.hadoop:hadoop-client:2.3.0|
|`druid.indexer.task.defaultRowFlushBoundary`|Highest row count before persisting to disk. Used for indexing generating tasks.|75000|
|`druid.indexer.task.directoryLockTimeout`|Wait this long for zombie peons to exit before giving up on their replacements.|PT10M|
|`druid.indexer.task.gracefulShutdownTimeout`|Wait this long on middleManager restart for restorable tasks to gracefully exit.|PT5M|
|`druid.indexer.task.hadoopWorkingPath`|Temporary working directory for Hadoop tasks.|`/tmp/druid-indexing`|
|`druid.indexer.task.restoreTasksOnRestart`|If true, middleManagers will attempt to stop tasks gracefully on shutdown and restore them on restart.|false|
|`druid.indexer.server.maxChatRequests`|Maximum number of concurrent requests served by a task's chat handler. Set to 0 to disable limiting.|0|

If the deprecated `druid.indexer.runner.separateIngestionEndpoint` property is set to true then following configurations
are available for the ingestion server at peon:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.server.chathandler.http.numThreads`|*Deprecated.* Number of threads for HTTP requests.|Math.max(10, (Number of available processors * 17) / 16 + 2) + 30|
|`druid.indexer.server.chathandler.http.maxIdleTime`|*Deprecated.* The Jetty max idle time for a connection.|PT5m|

If the peon is running in remote mode, there must be an overlord up and running. Peons in remote mode can set the following configurations:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.peon.taskActionClient.retry.minWait`|The minimum retry time to communicate with overlord.|PT5S|
|`druid.peon.taskActionClient.retry.maxWait`|The maximum retry time to communicate with overlord.|PT1M|
|`druid.peon.taskActionClient.retry.maxRetryCount`|The maximum number of retries to communicate with overlord.|60|
