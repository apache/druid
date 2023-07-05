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


This document describes the API endpoints to retrieve service (process) status, cluster information for Apache Druid.

In this document, `{domain}` is a placeholder for the server address of deployment. For example, on the quickstart configuration, replace `{domain}` with `http://localhost:8888`.

## Common

All processes support the following endpoints.

### Get process information

#### URL
<code class="getAPI">GET</code> `/status`

Retrieves the Druid version, loaded extensions, memory used, total memory, and other useful information about the process.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved process information*  
<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "{domain}/status"
```
<!--HTTP-->
```http
GET /status HTTP/1.1
Host: {domain}
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

<code class="getAPI">GET</code> `/status/health`

Retrieves the health of the Druid service. If online, it will always return a JSON object with the boolean `true` value, indicating that the service can receive API calls. This endpoint is suitable for automated health checks.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved process health*  
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->
```shell
curl "{domain}/status/health"
```
<!--HTTP-->
```http
GET /status/health HTTP/1.1
Host: {domain}
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
<code class="getAPI">GET</code> `/status/properties`

Retrieves the current configuration properties of the process.

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
<!--HTTP-->
```http
GET /status/properties HTTP/1.1
Host: {domain}
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
<code class="getAPI">GET</code> `/status/selfDiscovered/status`

Retrieves a JSON map of the form `{"selfDiscovered": true/false}`, indicating whether the node has received a confirmation from the central node discovery mechanism (currently ZooKeeper) of the Druid cluster that the node has been added to the
cluster. 

It is recommended to only consider a Druid node "healthy" or "ready" in automated deployment/container management systems when it returns `{"selfDiscovered": true}` from this endpoint. Nodes experiencing network issues may become isolated and should not be considered "healthy." Nodes utilizing Zookeeper segment discovery may be unusable until the Zookeeper client is fully initialized and receives data from the Zookeeper cluster. The presence of `{"selfDiscovered": true}` indicates that the node's Zookeeper client has started receiving data, enabling timely discovery of segments and other nodes.

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
<!--HTTP-->
```http
GET /status/selfDiscovered/status HTTP/1.1
Host: {domain}
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
<code class="getAPI">GET</code> `/status/selfDiscovered`

Retrieves a status code to indicate if a node discovered itself within the Druid cluster. This is similar to the `status/selfDiscovered/status` endpoint but relies on HTTP status codes alone. This is useful for certain monitoring checks such as AWS load balancer health checks that are unable to examine the response body.

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
<!--HTTP-->
```http
GET /status/selfDiscovered HTTP/1.1
Host: {domain}
```

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response
A successful response to this endpoint results in an empty response body.

## Coordinator

### Get leader address

#### URL

<code class="getAPI">GET</code> `/druid/coordinator/v1/leader`

Retrieves the address of the current leader Coordinator of the cluster.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved leader Coordinator address*  
<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->
<!--cURL-->
```shell
curl "{domain}/druid/coordinator/v1/leader"
```
<!--HTTP-->
```http
GET /druid/coordinator/v1/leader HTTP/1.1
Host: {domain}
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
<code class="getAPI">GET</code> `/druid/coordinator/v1/isLeader`

Retrieves a JSON object with a `leader` key. The value can be `true` or `false`, indicating if this server is the current leader Coordinator of the cluster. This is suitable for use as a load balancer status check if you only want the active leader to be considered in-service at the load balancer.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Current server is the leader*  

<!--404 NOT FOUND->
<br/>
*Current server is not the leader*  

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->
<!--cURL-->
```shell
curl "{domain}/druid/coordinator/v1/isLeader"
```
<!--HTTP-->
```http
GET /druid/coordinator/v1/isLeader HTTP/1.1
Host: {domain}
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response
<details>
  <summary>Click to show sample response</summary>
  ```json
  {
    "leader": true
  }
  ```
</details>


<a name="coordinator-segment-loading"></a>

## Overlord

### Get leader address

#### URL

<code class="getAPI">GET</code> `/druid/indexer/v1/leader`

Retrieves the address of the current leader Overlord of the cluster. In a cluster of multiple Overlords, only one Overlord assumes the leading role, while the remaining Overlords remain on standby.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved leader Overlord address*  
<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->
<!--cURL-->
```shell
curl "{domain}/druid/indexer/v1/leader"
```
<!--HTTP-->
```http
GET /druid/indexer/v1/leader HTTP/1.1
Host: {domain}
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
<code class="getAPI">GET</code> `/druid/indexer/v1/isLeader`

Retrieves a JSON object with a `leader` property. The value can be `true` or `false`, indicating if this server is the current leader Overlord of the cluster. This is suitable for use as a load balancer status check if you only want the active leader to be considered in-service at the load balancer.

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Current server is the leader*  
<!--404 NOT FOUND->
<br/>
*Current server is not the leader*  

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->
<!--cURL-->
```shell
curl "{domain}/druid/indexer/v1/isLeader"
```
<!--HTTP-->
```http
GET /druid/indexer/v1/isLeader HTTP/1.1
Host: {domain}
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response
<details>
  <summary>Click to show sample response</summary>

  ```json
  {
    "leader": true
  }
  ```
</details>


## MiddleManager

### Get state status

#### URL

<code class="getAPI">GET</code> `/druid/worker/v1/enabled`

Retrieves the enabled state of the MiddleManager. Returns JSON object keyed by the combined `druid.host` and `druid.port` with a boolean `true` or `false` state as the value.

To use this endpoint, `{domain}` should be the address of the MiddleManager service. For example, on the quickstart configuration, `{domain}` should be replaced with `http://localhost:8091`. 

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved MiddleManager state*  

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->
<!--cURL-->
```shell
curl "{domain}/druid/worker/v1/enabled"
```
<!--HTTP-->
```http
GET /druid/worker/v1/enabled HTTP/1.1
Host: {domain}
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
<code class="getAPI">GET</code> `/druid/worker/v1/tasks`

Retrieves a list of active tasks being run on MiddleManager. Returns JSON list of task ID strings. Normal usage should prefer to use the `/druid/indexer/v1/tasks` [Tasks API](./tasks-api.md) or one of it's task state specific variants instead.

To use this endpoint, `{domain}` should be the address of the MiddleManager service. For example, on the quickstart configuration, `{domain}` should be replaced with `http://localhost:8091`. 

#### Responses

<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved active tasks*  
<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->
<!--cURL-->
```shell
curl "{domain}/druid/worker/v1/tasks"
```
<!--HTTP-->
```http
GET /druid/worker/v1/tasks HTTP/1.1
Host: {domain}
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
<code class="getAPI">GET</code> `/druid/worker/v1/task/{taskid}/log`

Retrieves task log output stream by task ID. It is strongly advised that, for normal usage, you should use the `/druid/indexer/v1/task/{taskId}/log`
[Tasks API](./tasks-api.md) instead.

### Shut down running task

#### URL
<code class="postAPI">POST</code> `/druid/worker/v1/task/{taskid}/shutdown`

Shuts down a running task by ID. For normal usage, you should prefer to use the `/druid/indexer/v1/task/{taskId}/shutdown`
[Tasks API](./tasks-api.md) instead.

#### Responses
<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully shut down a task*  

<!--END_DOCUSAURUS_CODE_TABS-->

---

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->
<!--cURL-->
```shell
curl "{domain}/druid/worker/v1/task/index_kafka_wikiticker_f7011f8ffba384b_fpeclode/shutdown"
```
<!--HTTP-->
```http
POST /druid/worker/v1/task/index_kafka_wikiticker_f7011f8ffba384b_fpeclode/shutdown HTTP/1.1
Host: {domain}
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
<code class="postAPI">POST</code> `/druid/worker/v1/disable`

Disables a MiddleManager, causing it to stop accepting new tasks but complete all existing tasks. Returns a JSON  object
keyed by the combined `druid.host` and `druid.port`.

To use this endpoint, `{domain}` should be the address of the MiddleManager service. For example, on the quickstart configuration, `{domain}` should be replaced with `http://localhost:8091`. 

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
<!--HTTP-->
```http
POST /druid/worker/v1/disable HTTP/1.1
Host: {domain}
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

<code class="postAPI">POST</code> `/druid/worker/v1/enable`

Enables a MiddleManager, allowing it to accept new tasks again if it was previously disabled. Returns a JSON  object
keyed by the combined `druid.host` and `druid.port`.

To use this endpoint, `{domain}` should be the address of the MiddleManager service. For example, on the quickstart configuration, `{domain}` should be replaced with `http://localhost:8091`. 

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
<!--HTTP-->
```http
POST /druid/worker/v1/enable HTTP/1.1
Host: {domain}
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

<code class="getAPI">GET</code> `/druid/historical/v1/loadstatus`

Retrieves a JSON object of the form `{"cacheInitialized":<value>}`, where value is either `true` or `false` indicating if all segments in the local cache have been loaded. This can be used to know when a Historical process is ready to be queried after a restart.

To use this endpoint, `{domain}` should be the address of the Historical service. For example, on the quickstart configuration, `{domain}` should be replaced with `http://localhost:8093`.

#### Responses
<!--DOCUSAURUS_CODE_TABS-->

<!--200 SUCCESS-->
<br/>
*Successfully retrieved status*  

<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample request

<!--DOCUSAURUS_CODE_TABS-->
<!--cURL-->
```shell
curl "{domain}/druid/historical/v1/loadstatus"
```
<!--HTTP-->
```http
GET /druid/historical/v1/loadstatus HTTP/1.1
Host: {domain}
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

### Get segment readiness

#### URL
<code class="getAPI">GET</code> `/druid/historical/v1/readiness`

Retrieves a status code to indicate if all segments in the local cache have been loaded. Similar to `/druid/historical/v1/loadstatus`, but instead of returning JSON with a flag, it returns status codes.

To use this endpoint, `{domain}` should be the address of the Historical service. For example, on the quickstart configuration, `{domain}` should be replaced with `http://localhost:8093`.

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
<!--HTTP-->
```http
GET /druid/historical/v1/readiness HTTP/1.1
Host: {domain}
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response
A successful response to this endpoint results in an empty response body.

## Load Status

### Get Broker query load status

#### URL

<code class="getAPI">GET</code> `/druid/broker/v1/loadstatus`

Retrieves a flag indicating if the Broker knows about all segments in the cluster. This can be used to know when a Broker process is ready to be queried after a restart.

To use this endpoint, `{domain}` should be the address of the Historical service. For example, on the quickstart configuration, `{domain}` should be replaced with `http://localhost:8092`.

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
<!--HTTP-->
```http
GET /druid/broker/v1/loadstatus HTTP/1.1
Host: {domain}
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

### Get Broker query readiness

#### URL

<code class="getAPI">GET</code> `/druid/broker/v1/readiness`

Retrieves a status code indicating if the Broker knows about all segments in the cluster and is ready to be queried after a restart. Similar to `/druid/broker/v1/loadstatus`, but instead of returning a JSON, it returns status codes.

To use this endpoint, `{domain}` should be the address of the Historical service. For example, on the quickstart configuration, `{domain}` should be replaced with `http://localhost:8092`.

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
<!--HTTP-->
```http
GET /druid/broker/v1/readiness HTTP/1.1
Host: {domain}
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Sample response
A successful response to this endpoint results in an empty response body.


