---
layout: doc_page
---

Peons
-----
Peons run a single task in a single JVM. MiddleManager is responsible for creating Peons for running tasks.
Peons should rarely (if ever for testing purposes) be run on their own.

#### JVM Configuration
Although peons inherit the configurations of their parent middle managers, explicit child peon configs in middlemanager can be set by prefixing them with:

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
|`druid.indexer.task.defaultHadoopCoordinates`|Hadoop version to use with HadoopIndexTasks that do not request a particular version.|org.apache.hadoop:hadoop-client:2.3.0|
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