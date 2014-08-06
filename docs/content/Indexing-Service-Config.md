---
layout: doc_page
---
For general Indexing Service information, see [here](Indexing-Service.html).

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
|`druid.indexer.runner.minWorkerVersion`|The minimum middle manager version to send tasks to. |"0"|
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
    "impl":"string",
    "data":"version=:VERSION:",
    "versionReplacementString":":VERSION:"
  }
}
```

Issuing a GET request at the same URL will return the current worker setup spec that is currently in place. The worker setup spec list above is just a sample and it is possible to extend the code base for other deployment environments. A description of the worker setup spec is shown below.

|Property|Description|Default|
|--------|-----------|-------|
|`minNumWorkers`|The minimum number of workers that can be in the cluster at any given time.|0|
|`maxNumWorkers`|The maximum number of workers that can be in the cluster at any given time.|0|
|`nodeData`|A JSON object that describes how to launch new nodes. Currently, only EC2 is supported.|none; required|
|`userData`|A JSON object that describes how to configure new nodes. Currently, only EC2 is supported. If you have set druid.indexer.autoscale.workerVersion, this must have a versionReplacementString. Otherwise, a versionReplacementString is not necessary.|none; optional|
