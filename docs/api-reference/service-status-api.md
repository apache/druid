---
id: service-status-api
title: Service status API
sidebar_label: Service status
---

<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

<style>
get { color: blue; font-weight: bold }
post { color: green; font-weight: bold }
</style>

This document describes the API endpoints to retrieve service (process) status, cluster information for Apache Druid.

In this document, `{domain}` is a placeholder for the server address of deployment.

## Common

All processes support the following endpoints.

### Get process information

#### URL
<get>`GET`</get> `/status`

Returns the Druid version, loaded extensions, memory used, total memory, and other useful information about the process.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved service information*  
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "{domain}/status"
```
<!--Python-->
```python
import requests

url = "{domain}/status"

response = requests.get(url)

print(response.text)
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response
<details>
  <summary>Click to show sample response</summary>
  
  ```json
  {
    "version": "26.0.0",
    "modules": [
        {
            "name": "org.apache.druid.common.aws.AWSModule",
            "artifact": "druid-aws-common",
            "version": "26.0.0"
        },
        {
            "name": "org.apache.druid.common.gcp.GcpModule",
            "artifact": "druid-gcp-common",
            "version": "26.0.0"
        },
        {
            "name": "org.apache.druid.storage.hdfs.HdfsStorageDruidModule",
            "artifact": "druid-hdfs-storage",
            "version": "26.0.0"
        },
        {
            "name": "org.apache.druid.indexing.kafka.KafkaIndexTaskModule",
            "artifact": "druid-kafka-indexing-service",
            "version": "26.0.0"
        },
        {
            "name": "org.apache.druid.query.aggregation.datasketches.theta.SketchModule",
            "artifact": "druid-datasketches",
            "version": "26.0.0"
        },
        {
            "name": "org.apache.druid.query.aggregation.datasketches.theta.oldapi.OldApiSketchModule",
            "artifact": "druid-datasketches",
            "version": "26.0.0"
        },
        {
            "name": "org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchModule",
            "artifact": "druid-datasketches",
            "version": "26.0.0"
        },
        {
            "name": "org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchModule",
            "artifact": "druid-datasketches",
            "version": "26.0.0"
        },
        {
            "name": "org.apache.druid.query.aggregation.datasketches.hll.HllSketchModule",
            "artifact": "druid-datasketches",
            "version": "26.0.0"
        },
        {
            "name": "org.apache.druid.query.aggregation.datasketches.kll.KllSketchModule",
            "artifact": "druid-datasketches",
            "version": "26.0.0"
        },
        {
            "name": "org.apache.druid.msq.guice.MSQExternalDataSourceModule",
            "artifact": "druid-multi-stage-query",
            "version": "26.0.0"
        },
        {
            "name": "org.apache.druid.msq.guice.MSQIndexingModule",
            "artifact": "druid-multi-stage-query",
            "version": "26.0.0"
        },
        {
            "name": "org.apache.druid.msq.guice.MSQDurableStorageModule",
            "artifact": "druid-multi-stage-query",
            "version": "26.0.0"
        },
        {
            "name": "org.apache.druid.msq.guice.MSQServiceClientModule",
            "artifact": "druid-multi-stage-query",
            "version": "26.0.0"
        },
        {
            "name": "org.apache.druid.msq.guice.MSQSqlModule",
            "artifact": "druid-multi-stage-query",
            "version": "26.0.0"
        },
        {
            "name": "org.apache.druid.msq.guice.SqlTaskModule",
            "artifact": "druid-multi-stage-query",
            "version": "26.0.0"
        }
    ],
    "memory": {
        "maxMemory": 268435456,
        "totalMemory": 268435456,
        "freeMemory": 139060688,
        "usedMemory": 129374768,
        "directMemory": 134217728
    }
  }
  ```
</details>

### Get process health

#### URL
<get>`GET`</get> `/status/health`

Always returns a boolean `true` value with a 200 OK response, useful for automated health checks.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved service health*  
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "{domain}/status/health"
```
<!--Python-->
```python
import requests

url = "{domain}/status/health"

response = requests.get(url)

print(response.text)
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response
<details>
  <summary>Click to show sample response</summary>
  ```json
  true
  ```
</details>


### Get configuration properties

#### URL
<get>`GET`</get> `/status/properties`

Returns the current configuration properties of the process.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved process configuration properties*  
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "{domain}/status/properties"
```
<!--Python-->
```python
import requests

url = "{domain}/status/properties"

response = requests.get(url)

print(response.text)
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response
<details>
  <summary>Click to show sample response</summary>

  ```json
  {
    "gopherProxySet": "false",
    "awt.toolkit": "sun.lwawt.macosx.LWCToolkit",
    "druid.monitoring.monitors": "[\"org.apache.druid.java.util.metrics.JvmMonitor\"]",
    "java.specification.version": "11",
    "sun.cpu.isalist": "",
    "druid.plaintextPort": "8888",
    "sun.jnu.encoding": "UTF-8",
    "druid.indexing.doubleStorage": "double",
    "druid.metadata.storage.connector.port": "1527",
    "java.class.path": "genericJavaClassPath",
    "log4j.shutdownHookEnabled": "true",
    "java.vm.vendor": "Homebrew",
    "sun.arch.data.model": "64",
    "druid.extensions.loadList": "[\"druid-hdfs-storage\", \"druid-kafka-indexing-service\", \"druid-datasketches\", \"druid-multi-stage-query\"]",
    "java.vendor.url": "https://github.com/Homebrew/homebrew-core/issues",
    "druid.router.coordinatorServiceName": "druid/coordinator",
    "user.timezone": "UTC",
    "druid.global.http.eagerInitialization": "false",
    "os.name": "Mac OS X",
    "java.vm.specification.version": "11",
    "sun.java.launcher": "SUN_STANDARD",
    "user.country": "US",
    "sun.boot.library.path": "/opt/homebrew/Cellar/openjdk@11/11.0.19/libexec/openjdk.jdk/Contents/Home/lib",
    "sun.java.command": "org.apache.druid.cli.Main server router",
    "http.nonProxyHosts": "local|*.local|169.254/16|*.169.254/16",
    "jdk.debug": "release",
    "druid.metadata.storage.connector.host": "localhost",
    "sun.cpu.endian": "little",
    "druid.zk.paths.base": "/druid",
    "user.home": "/Users/genericUser",
    "user.language": "en",
    "java.specification.vendor": "Oracle Corporation",
    "java.version.date": "2023-04-18",
    "java.home": "/opt/homebrew/Cellar/openjdk@11/11.0.19/libexec/openjdk.jdk/Contents/Home",
    "druid.service": "druid/router",
    "druid.selectors.coordinator.serviceName": "druid/coordinator",
    "druid.metadata.storage.connector.connectURI": "jdbc:derby://localhost:1527/var/druid/metadata.db;create=true",
    "file.separator": "/",
    "druid.selectors.indexing.serviceName": "druid/overlord",
    "java.vm.compressedOopsMode": "Zero based",
    "druid.metadata.storage.type": "derby",
    "line.separator": "\n",
    "druid.log.path": "/Users/genericUser/downloads/apache-druid-26.0.0/bin/../log",
    "java.vm.specification.vendor": "Oracle Corporation",
    "java.specification.name": "Java Platform API Specification",
    "druid.indexer.logs.directory": "var/druid/indexing-logs",
    "java.awt.graphicsenv": "sun.awt.CGraphicsEnvironment",
    "druid.router.defaultBrokerServiceName": "druid/broker",
    "druid.storage.storageDirectory": "var/druid/segments",
    "sun.management.compiler": "HotSpot 64-Bit Tiered Compilers",
    "ftp.nonProxyHosts": "local|*.local|169.254/16|*.169.254/16",
    "java.runtime.version": "11.0.19+0",
    "user.name": "user",
    "druid.indexer.logs.type": "file",
    "druid.host": "localhost",
    "log4j2.is.webapp": "false",
    "path.separator": ":",
    "os.version": "12.6.5",
    "druid.lookup.enableLookupSyncOnStartup": "false",
    "java.runtime.name": "OpenJDK Runtime Environment",
    "druid.zk.service.host": "localhost",
    "file.encoding": "UTF-8",
    "druid.sql.planner.useGroupingSetForExactDistinct": "true",
    "druid.router.managementProxy.enabled": "true",
    "java.vm.name": "OpenJDK 64-Bit Server VM",
    "java.vendor.version": "Homebrew",
    "druid.startup.logging.logProperties": "true",
    "java.vendor.url.bug": "https://github.com/Homebrew/homebrew-core/issues",
    "log4j.shutdownCallbackRegistry": "org.apache.druid.common.config.Log4jShutdown",
    "java.io.tmpdir": "var/tmp",
    "druid.sql.enable": "true",
    "druid.emitter.logging.logLevel": "info",
    "java.version": "11.0.19",
    "user.dir": "/Users/genericUser/Downloads/apache-druid-26.0.0",
    "os.arch": "aarch64",
    "java.vm.specification.name": "Java Virtual Machine Specification",
    "druid.node.type": "router",
    "java.awt.printerjob": "sun.lwawt.macosx.CPrinterJob",
    "sun.os.patch.level": "unknown",
    "java.util.logging.manager": "org.apache.logging.log4j.jul.LogManager",
    "java.library.path": "/Users/genericUser/Library/Java/Extensions:/Library/Java/Extensions:/Network/Library/Java/Extensions:/System/Library/Java/Extensions:/usr/lib/java:.",
    "java.vendor": "Homebrew",
    "java.vm.info": "mixed mode",
    "java.vm.version": "11.0.19+0",
    "druid.emitter": "noop",
    "sun.io.unicode.encoding": "UnicodeBig",
    "druid.storage.type": "local",
    "druid.expressions.useStrictBooleans": "true",
    "java.class.version": "55.0",
    "socksNonProxyHosts": "local|*.local|169.254/16|*.169.254/16",
    "druid.server.hiddenProperties": "[\"druid.s3.accessKey\",\"druid.s3.secretKey\",\"druid.metadata.storage.connector.password\", \"password\", \"key\", \"token\", \"pwd\"]"
}
```
</details>


### Get node discovery status and cluster integration confirmation

#### URL
<get>`GET`</get> `/status/selfDiscovered/status`

Returns a JSON map of the form `{"selfDiscovered": true/false}`, indicating whether the node has received a confirmation
from the central node discovery mechanism (currently ZooKeeper) of the Druid cluster that the node has been added to the
cluster. 

It is recommended to not consider a Druid node "healthy" or "ready" in automated deployment/container
management systems until it returns `{"selfDiscovered": true}` from this endpoint. This is because a node may be
isolated from the rest of the cluster due to network issues and it doesn't make sense to consider nodes "healthy" in
this case. Also, when nodes such as Brokers use ZooKeeper segment discovery for building their view of the Druid cluster
(as opposed to HTTP segment discovery), they may be unusable until the ZooKeeper client is fully initialized and starts
to receive data from the ZooKeeper cluster. `{"selfDiscovered": true}` is a proxy event indicating that the ZooKeeper
client on the node has started to receive data from the ZooKeeper cluster and it's expected that all segments and other
nodes will be discovered by this node timely from this point.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Node was successfully added to the cluster*  
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "{domain}/status/selfDiscovered/status"
```
<!--Python-->
```python
import requests

url = "{domain}/status/selfDiscovered/status"

response = requests.get(url)

print(response.text)
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response
<details>
  <summary>Click to show sample response</summary>
  ```json
  {
    "selfDiscovered": true
  }
  ```
</details>


### Get node self-discovery status

#### URL
<get>`GET`</get> `/status/selfDiscovered`

Similar to `/status/selfDiscovered/status`, but returns status codes. This endpoint might be useful because some
monitoring checks such as AWS load balancer health checks are not able to look at the response body.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved node status*  

<!--503 SERVICE UNAVAILABLE-->
<br/>
*Unsuccessful node self-discovery*  
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "{domain}/status/selfDiscovered"
```
<!--Python-->
```python
import requests

url = "{domain}/status/selfDiscovered"

response = requests.get(url)

print(response.text)
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response
A successful response to this endpoint results in an empty response body.



## Coordinator

### Get leader coordinator

#### URL

<get>`GET`</get> `/druid/coordinator/v1/leader`

Returns the current leader Coordinator of the cluster.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved leader coordinator*  
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->
<!--cURL-->
```shell
curl "{domain}/druid/coordinator/v1/leader"
```
<!--Python-->
```python
import requests

url = "{domain}/druid/coordinator/v1/leader"

response = requests.get(url)

print(response.text)
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response
<details>
  <summary>Click to show sample response</summary>
  ```json
  http://localhost:8081
  ```
</details>

### Get leader status

#### URL
<get>`GET`</get> `/druid/coordinator/v1/isLeader`

Returns a JSON object with `leader` parameter, either true or false, indicating if this server is the current leader
Coordinator of the cluster. In addition, returns HTTP 200 if the server is the current leader and HTTP 404 if not.
This is suitable for use as a load balancer status check if you only want the active leader to be considered in-service
at the load balancer.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Current server is the leader*  


<!--404 NOT FOUND->
<br/>
*Current server is not the leader*  

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->
<!--cURL-->
```shell
curl "{domain}/druid/coordinator/v1/isLeader"
```
<!--Python-->
```python
import requests

url = "{domain}/druid/coordinator/v1/isLeader"

response = requests.get(url)

print(response.text)
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response
<details>
  <summary>Click to show sample response</summary>
  ```json
  {
    "leader":true
  }
  ```
</details>


<a name="coordinator-segment-loading"></a>

## Overlord

### Get leader overlord

#### URL

<get>`GET`</get> `/druid/indexer/v1/leader`

Returns the current leader Overlord of the cluster. If you have multiple Overlords, just one is leading at any given time. The others are on standby.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Current server is the leader*  
 
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->
<!--cURL-->
```shell
curl "{domain}/druid/indexer/v1/leader"
```
<!--Python-->
```python
import requests

url = "{domain}/druid/indexer/v1/leader"

response = requests.get(url)

print(response.text)
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response
<details>
  <summary>Click to show sample response</summary>

  ```json
  http://localhost:8081
  ```

</details>


### Get leader status

#### URL
<get>`GET`</get> `/druid/indexer/v1/isLeader`

This returns a JSON object with field `leader`, either true or false. In addition, this call returns HTTP 200 if the
server is the current leader and HTTP 404 if not. This is suitable for use as a load balancer status check if you
only want the active leader to be considered in-service at the load balancer.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Current server is the leader*  


<!--404 NOT FOUND->
<br/>
*Current server is not the leader*  

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->
<!--cURL-->
```shell
curl "{domain}/druid/indexer/v1/isLeader"
```
<!--Python-->
```python
import requests

url = "{domain}/druid/indexer/v1/isLeader"

response = requests.get(url)

print(response.text)
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response
<details>
  <summary>Click to show sample response</summary>

  ```json
  {
    "leader":true
  }
  ```
</details>


## MiddleManager

### Get state status

#### URL

<get>`GET`</get> `/druid/worker/v1/enabled`

Check whether a MiddleManager is in an enabled or disabled state. Returns JSON object keyed by the combined `druid.host`
and `druid.port` with the boolean state as the value.

```json
{"localhost:8091":true}
```
#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved MiddleManager state*  


<!--404 NOT FOUND->
<br/>
*MiddleManager state could not be found at the specified node or request was sent to an incorrect node*  

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->
<!--cURL-->
```shell
curl "{domain}/druid/worker/v1/enabled"
```
<!--Python-->
```python
import requests

url = "{domain}/druid/worker/v1/enabled"

response = requests.get(url)

print(response.text)
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response
<details>
  <summary>Click to show sample response</summary>
  
  ```json
  {
    "localhost:8091": true
  }
  ```
</details>

### Get active tasks 

#### URL 
<get>`GET`</get> `/druid/worker/v1/tasks`

Retrieve a list of active tasks being run on MiddleManager. Returns JSON list of taskid strings. Normal usage should
prefer to use the `/druid/indexer/v1/tasks` [Tasks API](./tasks-api.md) or one of it's task state specific variants instead.

```json
["index_wikiticker_2019-02-11T02:20:15.316Z"]
```

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved active tasks*  


<!--404 NOT FOUND->
<br/>
*Active tasks not be found at the specified node or request was sent to an incorrect node*  

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->
<!--cURL-->
```shell
curl "{domain}/druid/worker/v1/tasks"
```
<!--Python-->
```python
import requests

url = "{domain}/druid/worker/v1/tasks"

response = requests.get(url)

print(response.text)
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response
<details>
  <summary>Click to show sample response</summary>
  
  ```json
  [
    "index_parallel_wikipedia_mgchefio_2023-06-13T22:18:05.360Z"
  ]
  ```
</details>

### Get task log

#### URL
<get>`GET`</get> `/druid/worker/v1/task/{taskid}/log`

Retrieve task log output stream by task id. Normal usage should prefer to use the `/druid/indexer/v1/task/{taskId}/log`
[Tasks API](./tasks-api.md) instead.

#### Parameters
* `taskid`: String
    * The id value of the task. Required. 

#### Responses
<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved task log*  


<!--404 NOT FOUND->
<br/>
*Task log not found*  

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->
<!--cURL-->
```shell
curl "{domain}/druid/worker/v1/task/{taskid}/log"
```
<!--Python-->
```python
import requests

url = "{domain}/druid/worker/v1/task/{taskid}/log"

response = requests.get(url)

print(response.text)
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response
<details>
  <summary>Click to show sample response</summary>

</details>

### Shutdown running task

#### URL
<post>`POST`</post> `/druid/worker/v1/task/{taskid}/shutdown`

Shutdown a running task by `taskid`. Normal usage should prefer to use the `/druid/indexer/v1/task/{taskId}/shutdown`
[Tasks API](./tasks-api.md) instead. Returns JSON:

#### Parameters
* `taskid`: String
    * The id value of the task. Required. 

#### Responses
<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully shut down a task*  

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->
<!--cURL-->
```shell
curl "{domain}/druid/worker/v1/task/{taskid}/shutdown"
```
<!--Python-->
```python
import requests

url = "{domain}/druid/worker/v1/task/{taskid}/shutdown"

response = requests.get(url)

print(response.text)
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response
<details>
  <summary>Click to show sample response</summary>
  
  ```json
  {
    "task":"index_kafka_wikiticker_f7011f8ffba384b_fpeclode"
  }
  ```

</details>

### Disable MiddleManager

#### URL
<post>`POST`</post> `/druid/worker/v1/disable`

Disable a MiddleManager, causing it to stop accepting new tasks but complete all existing tasks. Returns JSON  object
keyed by the combined `druid.host` and `druid.port`.

#### Responses
<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully disabled MiddleManager*  

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->
<!--cURL-->
```shell
curl "{domain}/druid/worker/v1/disable"
```
<!--Python-->
```python
import requests

url = "{domain}/druid/worker/v1/disable"

response = requests.get(url)

print(response.text)
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response
<details>
  <summary>Click to show sample response</summary>
  
  ```json
  {
    "localhost:8091":"disabled"
  }
  ```

</details>

### Enable MiddleManager

#### URL

<post>`POST`</post> `/druid/worker/v1/enable`

Enable a MiddleManager, allowing it to accept new tasks again if it was previously disabled. Returns JSON  object
keyed by the combined `druid.host` and `druid.port`.

#### Responses
<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully enabled MiddleManager*  

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->
<!--cURL-->
```shell
curl "{domain}/druid/worker/v1/enable"
```
<!--Python-->
```python
import requests

url = "{domain}/druid/worker/v1/enable"

response = requests.get(url)

print(response.text)
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response
<details>
  <summary>Click to show sample response</summary>
  
  ```json
  {
    "localhost:8091":"enabled"
  }
  ```

</details>

## Historical  

### Get segment load status

#### URL

<get>`GET`</get> `/druid/historical/v1/loadstatus`

Returns JSON of the form `{"cacheInitialized":<value>}`, where value is either `true` or `false` indicating if all
segments in the local cache have been loaded. This can be used to know when a Historical process is ready
to be queried after a restart.

#### Responses
<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved status*  
<!--404 NOT FOUND-->
<br/>
*Resource not found or request was sent to an incorrect node*  

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->
<!--cURL-->
```shell
curl "{domain}/druid/historical/v1/loadstatus"
```
<!--Python-->
```python
import requests

url = "{domain}/druid/historical/v1/loadstatus"

response = requests.get(url)

print(response.text)
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response
<details>
  <summary>Click to show sample response</summary>
  
  ```json
  {
    "cacheInitialized": true
  }
  ```
</details>

### Get segment readiness status

#### URL
<get>`GET`</get> `/druid/historical/v1/readiness`

Similar to `/druid/historical/v1/loadstatus`, but instead of returning JSON with a flag, it returns status codes.

#### Responses
<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Segments in local cache successfully loaded*  
<!--503 SERVICE UNAVAILABLE-->
<br/>
*Segments in local cache have not been loaded*  

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->
<!--cURL-->
```shell
curl "{domain}/druid/historical/v1/readiness"
```
<!--Python-->
```python
import requests

url = "{domain}/druid/historical/v1/readiness"

response = requests.get(url)

print(response.text)
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response
A successful response to this endpoint results in an empty response body.


## Load Status

### Get Broker query load status

#### URL

<get>`GET`</get> `/druid/broker/v1/loadstatus`

Returns a flag indicating if the Broker knows about all segments in the cluster. This can be used to know when a Broker process is ready to be queried after a restart.

#### Responses
<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Segments successfully loaded*  

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->
<!--cURL-->
```shell
curl "{domain}/druid/broker/v1/loadstatus"
```
<!--Python-->
```python
import requests

url = "{domain}/druid/broker/v1/loadstatus"

response = requests.get(url)

print(response.text)
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response
<details>
  <summary>Click to show sample response</summary>
  ```json
  {
    "inventoryInitialized": true
  }
  ```
</details>

### Get Broker query readiness status

#### URL
<get>`GET`</get> `/druid/broker/v1/readiness`

Similar to `/druid/broker/v1/loadstatus`, but instead of returning a JSON, it returns status codes.

#### Responses
<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Segments successfully loaded*  

<!--503 SERVICE UNAVAILABLE-->
<br/>
*Segments have not been loaded*  

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->
<!--cURL-->
```shell
curl "{domain}/druid/broker/v1/readiness"
```
<!--Python-->
```python
import requests

url = "{domain}/druid/broker/v1/readiness"

response = requests.get(url)

print(response.text)
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response
A successful response to this endpoint results in an empty response body.