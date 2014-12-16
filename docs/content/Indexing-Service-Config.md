---
layout: doc_page
---
For general Indexing Service information, see [here](Indexing-Service.html).

## Runtime Configuration

The indexing service uses several of the global configs in [Configuration](Configuration.html) and has the following set of configurations as well:

### Must be set on Overlord and Middle Manager

#### Node Configs

|Property|Description|Default|
|--------|-----------|-------|
|`druid.host`|The host for the current node. This is used to advertise the current processes location as reachable from another node and should generally be specified such that `http://${druid.host}/` could actually talk to this process|none|
|`druid.port`|This is the port to actually listen on; unless port mapping is used, this will be the same port as is on `druid.host`|none|
|`druid.service`|The name of the service. This is used as a dimension when emitting metrics and alerts to differentiate between the various services|none|

#### Task Logging

If you are running the indexing service in remote mode, the task logs must S3 or HDFS.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.indexer.logs.type`|Choices:noop, s3, hdfs, file. Where to store task logs|file|

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
http://<COORDINATOR_IP>:<port>/druid/indexer/v1/worker
```

A sample worker config spec is shown below:

```json
{
  "selectStrategy": {
    "type": "fillCapacityWithAffinity",
    "affinityConfig": {
      "affinity": {
        "datasource1": ["ip1:port", "ip2:port"],
        "datasource2": ["ip3:port"]
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
|`selectStrategy`|How to assign tasks to middlemanagers. Choices are `fillCapacity` and `fillCapacityWithAffinity`.|fillCapacity|
|`autoScaler`|Only used if autoscaling is enabled. See below.|null|

#### Worker Select Strategy

##### Fill Capacity

Workers are assigned tasks until capacity.

|Property|Description|Default|
|--------|-----------|-------|
|`type`|`fillCapacity`.|fillCapacity|

##### Fill Capacity With Affinity

An affinity config can be provided.

|Property|Description|Default|
|--------|-----------|-------|
|`type`|`fillCapacityWithAffinity`.|fillCapacityWithAffinity|
|`affinity`|A map to String to list of String host names.|{}|

Tasks will try to be assigned to preferred workers. Fill capacity strategy is used if no preference for a datasource specified.

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
|`druid.worker.ip`|The IP of the worker.|localhost|
|`druid.worker.version`|Version identifier for the middle manager.|0|
|`druid.worker.capacity`|Maximum number of tasks the middle manager can accept.|Number of available processors - 1|
|`druid.indexer.runner.compressZnodes`|Indicates whether or not the middle managers should compress Znodes.|false|
|`druid.indexer.runner.maxZnodeBytes`|The maximum size Znode in bytes that can be created in Zookeeper.|524288|
|`druid.indexer.runner.javaCommand`|Command required to execute java.|java|
|`druid.indexer.runner.javaOpts`|-X Java options to run the peon in its own JVM.|""|
|`druid.indexer.runner.classpath`|Java classpath for the peon.|System.getProperty("java.class.path")|
|`druid.indexer.runner.startPort`|The port that peons begin running on.|8081|
|`druid.indexer.runner.allowedPrefixes`|Whitelist of prefixes for configs that can be passed down to child peons.|"com.metamx", "druid", "io.druid", "user.timezone","file.encoding"|
