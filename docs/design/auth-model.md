---
id: auth-model
title: "Authorization model"
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

## Authorization model

There are two versions of auth model in Druid, auth v1 model (legacy) and newer auth v2 model. Auth v1 is default and switch
to v2 can be made by setting the flag `druid.auth.authVersion=v2` .

Here are the details below -

## Auth V1 Model (Legacy)
There are two action types in Druid: READ and WRITE

There are three resource types in Druid: DATASOURCE, CONFIG, and STATE.

### DATASOURCE
Resource names for this type are datasource names. Specifying a datasource permission allows the administrator to grant users access to specific datasources.

### CONFIG
There are two possible resource names for the "CONFIG" resource type, "CONFIG" and "security". Granting a user access to CONFIG resources allows them to access the following endpoints.

"CONFIG" resource name covers the following endpoints:

|Endpoint|Process Type|
|--------|---------|
|`/druid/coordinator/v1/config`|coordinator|
|`/druid/indexer/v1/worker`|overlord|
|`/druid/indexer/v1/worker/history`|overlord|
|`/druid/worker/v1/disable`|middleManager|
|`/druid/worker/v1/enable`|middleManager|

"security" resource name covers the following endpoint:

|Endpoint|Process Type|
|--------|---------|
|`/druid-ext/basic-security/authentication`|coordinator|
|`/druid-ext/basic-security/authorization`|coordinator|

### STATE
There is only one possible resource name for the "STATE" config resource type, "STATE". Granting a user access to STATE resources allows them to access the following endpoints.

"STATE" resource name covers the following endpoints:

|Endpoint|Process Type|
|--------|---------|
|`/druid/coordinator/v1`|coordinator|
|`/druid/coordinator/v1/rules`|coordinator|
|`/druid/coordinator/v1/rules/history`|coordinator|
|`/druid/coordinator/v1/servers`|coordinator|
|`/druid/coordinator/v1/tiers`|coordinator|
|`/druid/broker/v1`|broker|
|`/druid/v2/candidates`|broker|
|`/druid/indexer/v1/leader`|overlord|
|`/druid/indexer/v1/isLeader`|overlord|
|`/druid/indexer/v1/action`|overlord|
|`/druid/indexer/v1/workers`|overlord|
|`/druid/indexer/v1/scaling`|overlord|
|`/druid/worker/v1/enabled`|middleManager|
|`/druid/worker/v1/tasks`|middleManager|
|`/druid/worker/v1/task/{taskid}/shutdown`|middleManager|
|`/druid/worker/v1/task/{taskid}/log`|middleManager|
|`/druid/historical/v1`|historical|
|`/druid-internal/v1/segments/`|historical|
|`/druid-internal/v1/segments/`|peon|
|`/druid-internal/v1/segments/`|realtime|
|`/status`|all process types|

### HTTP methods

For information on what HTTP methods are supported on a particular request endpoint, please refer to the [API documentation](../operations/api-reference.md).

GET requires READ permission, while POST and DELETE require WRITE permission.

### SQL Permissions

Queries on Druid datasources require DATASOURCE READ permissions for the specified datasource.

Queries on the [INFORMATION_SCHEMA tables](../querying/sql.html#information-schema) will
return information about datasources that the caller has DATASOURCE READ access to. Other
datasources will be omitted.

Queries on the [system schema tables](../querying/sql.html#system-schema) require the following permissions:
- `segments`: Segments will be filtered based on DATASOURCE READ permissions.
- `servers`: The user requires STATE READ permissions.
- `server_segments`: The user requires STATE READ permissions and segments will be filtered based on DATASOURCE READ permissions.
- `tasks`: Tasks will be filtered based on DATASOURCE READ permissions.

## Auth V2 Model 

This model can be enabled by setting the flag `druid.auth.authVersion=v2`. The idea behind this model is to support user
personas like admin, viewer etc. in an easy manner.
 
There are two action types in Druid: READ and WRITE. Depending on HTTP method used for endpoints action is decided, for
GET and HEAD method actoin is `READ`, for all other methods action is `WRITE`.

There are 4 resource types in Druid: DATASOURCE, INTERNAL, LOOKUP and SERVER.

1. `DATASOURCE` resource type is concerned with all the actions that can be taken for querying, indexing, setting retention,
compaction rules for a dataset. Thus, if a user has read/write action on datasource they can control the lifecycle of that datasouce.

1. `SERVER` resource type covers all the cluster administration endpoints.

1. `LOOKUP` resource type covers lookups similar to `DATASOURCE` resource type.

1. `INTERNAL` resource type covers all the resources/endpoints that druid internally uses to communicate among nodes.

Examples - A server admin role can have read/write on `DATASOURCE`, `SERVER` and `LOOKUP` resource types. A viewer can have
just read on all or specific `DATASOURCE`. Many times users may want to create roles with custom permissions, all these 
is supported using `Resource Name`, each `Resource type` can have mulitple of them. Authorizer uses `Action`, `Resource Type` 
and `Resource Name` to authorize users action. For example, while querying a datasource named `ds` a `READ` action on 
resource type `DATASOURCE` with resource name `ds` is required. Below are the details -
  
### DATASOURCE

Resource names for this type are datasource names. Specifying a datasource permission allows the administrator to grant users access to specific datasources.
When users get `READ` permission on a datasource, they will be able to query that datasource, will be able to see retention/compaction rules 
for them, get schema details and vice versa with `WRITE` permissions.

Below are the endpoints protected/filtered using datasource permissions. These permissions are enforced in SQL queries as well, see `SQL Permissions` section below. 

|Endpoint|Process Type|
|--------|---------|
|`GET/POST /druid/coordinator/v1/datasources/...`|coordinator|
|`GET /druid/coordinator/v1/rules`|coordinator|
|`GET/POST /druid/coordinator/v1/rules/{dataSourceName}`|coordinator|
|`GET /druid/coordinator/v1/rules/{dataSourceName}/history`|coordinator|
|`GET /druid/coordinator/v1/tiers/{tierName}`|coordinator|
|`GET/POST /druid/v2/datasources/...`|broker|
|`GET/POST /druid/coordinator/v1/metadata/...`|coordinator|
|`GET/POST /druid/coordinator/v1/config/compaction`|coordinator|
|`GET/DELETE /druid/coordinator/v1/config/compaction/{dataSource}`|coordinator|
|`POST /druid/indexer/v1/task`|overlord|
|`GET /druid/indexer/v1/task/{taskid}`|overlord|
|`GET /druid/indexer/v1/task/{taskid}/status`|overlord|
|`GET /druid/indexer/v1/task/{taskid}/segments`|overlord|
|`POST /druid/indexer/v1/task/{taskid}/shutdown`|overlord|
|`POST /druid/indexer/v1/datasources/{dataSource}/shutdownAllTasks`|overlord|
|`GET /druid/indexer/v1/waitingTasks`|overlord|
|`GET /druid/indexer/v1/pendingTasks`|overlord|
|`GET /druid/indexer/v1/runningTasks`|overlord|
|`GET /druid/indexer/v1/completeTasks`|overlord|
|`GET /druid/indexer/v1/tasks`|overlord|
|`DELETE /druid/indexer/v1/pendingSegments/{dataSource}`|overlord|
|`GET /druid/indexer/v1/task/{taskid}/log`|overlord|
|`GET /druid/indexer/v1/task/{taskid}/reports`|overlord|

Note - When reindexing from an existing datasource, user needs read on source datasource permission in addition to
write on destination datasource.  

### SERVER

There are 3 possible resource name for the "SERVER" config resource type, `SERVER`, `STATUS` and `USER`.

`SERVER` resoruce name covers following the end points, these are relevant for managing the cluster and can be used by
cluster managers. This resource name is used in SQL queries asking for server information see `SQL Permissions` section below.

|Endpoint|Process Type|
|--------|---------|
|`GET /druid/indexer/v1/workers`|overlord|
|`POST /druid/indexer/v1/worker/{host}/enable`|overlord|
|`POST /druid/indexer/v1/worker/{host}/disable`|overlord|
|`GET /druid/coordinator/v1/loadstatus`|coordinator|
|`GET /druid/coordinator/v1/loadqueue`|coordinator|
|`GET /druid/coordinator/v1/rules/history`|coordinator|
|`GET /druid/coordinator/v1/servers`|coordinator|
|`GET /druid/coordinator/v1/servers/{serverName}`|coordinator|
|`GET /druid/coordinator/v1/servers/{serverName}/segments`|coordinator|
|`GET /druid/coordinator/v1/servers/{serverName}/segments/{segmentId}`|coordinator|
|`GET /druid/indexer/v1/worker`|overlord|
|`POST /druid/indexer/v1/worker`|overlord|
|`GET /druid/indexer/v1/worker/history`|overlord|
|`GET /status/properties`|all|
|`POST /druid/coordinator/v1/config/compaction/taskslots`|coordinator|
|`GET /druid/coordinator/v1/config`|coordinator|
|`POST /druid/coordinator/v1/config`|coordinator|
|`GET /druid/coordinator/v1/config/history`|coordinator|
|`GET /druid/coordinator/v1/lookups/nodeStatus`|coordinator|
|`GET /druid/coordinator/v1/lookups/nodeStatus/{tier}`|coordinator|
|`GET /druid/coordinator/v1/lookups/config/{tier}/{hostAndPort}`|coordinator|
|`GET/POST /druid-ext/basic-security/authentication`|coordinator|
|`GET/POST /druid-ext/basic-security/authorization`|coordinator|

`STATUS` resource name covers following the end points and are relevant to check status of various processes and can be
used by cluster managers and systems checking status of different druid processes.

|Endpoint|Process Type|
|--------|---------|
|`GET /druid/indexer/v1/leader`|overlord|
|`GET /druid/coordinator/v1/leader`|coordinator|
|`GET /druid/coordinator/v1/isLeader`|coordinator|
|`GET /druid/historical/v1/loadstatus`|broker,historical,peon|
|`GET /druid/broker/v1/loadstatus`|broker|
|`GET /status`|all|

`USER` resource name covers following the end points and are relevant to general users using the cluster needing access 
to some `SERVER` resources during routine work.

|Endpoint|Process Type|
|--------|---------|
|`POST /druid/indexer/v1/sampler`|overlord|
|`GET /druid/coordinator/v1/lookups/config`|coordinator|
|`GET /druid/coordinator/v1/tiers`|coordinator|
|`GET /druid/coordinator/v1/tiers/{tierName}`|coordinator|

### INTERNAL

There is only one possible resource names for the `INTERNAL` resource type - `INTERNAL`. These permissions mostly makes sense
for druid nodes talking to each other and cluster admins. 

|Endpoint|Process Type|
|--------|---------|
|`GET /druid-internal/v1/httpRemoteTaskRunner/knownTasks `|overlord|
|`GET /druid-internal/v1/httpRemoteTaskRunner/pendingTasksQueue`|overlord|
|`GET /druid-internal/v1/httpRemoteTaskRunner/workerSyncerDebugInfo`|overlord|
|`GET /druid-internal/v1/httpRemoteTaskRunner/blacklistedWorkers`|overlord|
|`GET /druid-internal/v1/httpRemoteTaskRunner/lazyWorker`|overlord|
|`GET /druid-internal/v1/httpRemoteTaskRunner/workersWithUnacknowledgedTasks`|overlord|
|`GET /druid-internal/v1/httpRemoteTaskRunner/workersEilgibleToRunTasks`|overlord|
|`POST /druid/indexer/v1/taskStatus`|overlord|
|`POST /druid/indexer/v1/action`|overlord|
|`GET /druid/indexer/v1/scaling`|overlord|
|`GET /druid/worker/v1/shuffle/task/{supervisorTaskId}/{subTaskId}/partition`|middleManager|
|`DELETE /druid/worker/v1/shuffle/task/{supervisorTaskId}`|middleManager|
|`GET /druid-internal/v1/worker/`|middleManager|
|`POST /druid-internal/v1/worker/assignTask`|middleManager|
|`GET /druid/worker/v1/enabled`|middleManager|
|`GET /druid/worker/v1/tasks`|middleManager|
|`POST /druid/worker/v1/task/{taskid}/shutdown`|middleManager|
|`GET /druid/worker/v1/task/{taskid}/log`|middleManager|
|`POST /druid/worker/v1/disable`|middleManager|
|`POST /druid/worker/v1/enable`|middleManager|
|`GET /druid-internal/v1/httpServerInventoryView`|broker,coordinator|
|`POST /druid/v2/candidates`|broker|
|`GET /druid/coordinator/v1/cluster`|coordinator|
|`GET /druid/coordinator/v1/cluster/{nodeRole}`|coordinator|
|`GET /druid/router/v1/brokers`|router|
|`GET /druid-internal/v1/segments/`|broker,historical,peon|
|`GET /druid-internal/v1/segments/changeRequests`|broker,historical,peon|
|`GET /status/selfDiscovered/status`|all|
|`GET /status/selfDiscovered`|all|
|`POST /druid/listen/v1/lookups`|broker,historical,peon|
|`POST /druid/listen/v1/lookups/updates`|broker,historical,peon|
|`GET /druid/listen/v1/lookups`|broker,historical,peon|
|`GET /druid/listen/v1/lookups/{id}`|broker,historical,peon|
|`POST /druid/listen/v1/lookups/{id}`|broker,historical,peon|
|`DELETE /druid/listen/v1/lookups/{id}`|broker,historical,peon|

### LOOKUP

`LOOKUP` is similar to `DATASOURCE` resource type having lookup name as resource name and specifying a lookup permission 
allows the administrator to grant users access to specific lookup name. Endpoints that return list of lookpus will return
filtered list by the granted permissions. It covers the following endpoints.

|Endpoint|Process Type|
|--------|---------|
|`POST /druid/coordinator/v1/lookups/config`|coordinator|
|`GET /druid/coordinator/v1/lookups/config/all`|coordinator|
|`DELETE /druid/coordinator/v1/lookups/config/{tier}`|coordinator|
|`DELETE /druid/coordinator/v1/lookups/config/{tier}/{lookup}`|coordinator|
|`POST /druid/coordinator/v1/lookups/config/{tier}/{lookup}`|coordinator|
|`GET /druid/coordinator/v1/lookups/config/{tier}/{lookup}`|coordinator|
|`GET /druid/coordinator/v1/lookups/config/{tier}`|coordinator|
|`GET /druid/coordinator/v1/lookups/status`|coordinator|
|`GET /druid/coordinator/v1/lookups/status/{tier}`|coordinator|
|`GET /druid/coordinator/v1/lookups/status/{tier}/{lookup}`|coordinator|

### EXAMPLE ROLES-PERMISSION MAPPING

These are some example policies that be used. Policies are of form `ACTION:RESOURCE_TYPE:RESOURCE_NAME`
1. Cluster admin (super user/druid system) can have following policies - `READ:DATASOURCE:*`, `WRITE:DATASOURCE:*`, 
`READ:INTERNAL:*`, `WRITE:INTERNAL:*`, `READ:SERVER:*`, `WRITE:SERVER:*`, `READ:LOOKUP:*` and `WRITE:LOOKUP:*`. 
1. Cluster manager can have following policies - `READ:DATASOURCE:*`, `WRITE:DATASOURCE:*`, `READ:SERVER:*`, `WRITE:SERVER:*`,
 `READ:LOOKUP:*` and `WRITE:LOOKUP:*`.
1. A user just wanting to read/write to specific datasource and lookup can have following policies - `READ:DATASOURCE:<ds_name>`, 
`WRITE:DATASOURCE:<ds_name>`, `READ:LOOKUP:<lookupId>`, `WRITE:LOOKUP:<lookupId>`, `READ:SERVER:USER`, `WRITE:SERVER:USER` AND `READ:SERVER:STATUS`.
1. A user just wanting to read from specific datasource and lookup can have following policies - `READ:DATASOURCE:<ds_name>`, 
`READ:LOOKUP:<lookupId>`, and `READ:SERVER:USER`.
1. An external monitoring system can just have `READ:SERVER:STATUS` permission.

### HTTP methods

For information on what HTTP methods are supported on a particular request endpoint, please refer to the [API documentation](../operations/api-reference.md).

GET and HEAD requires READ permission, while POST and DELETE require WRITE permission.

### SQL Permissions

Queries on Druid datasources require DATASOURCE READ permissions for the specified datasource.

Queries on the [INFORMATION_SCHEMA tables](../querying/sql.html#information-schema) will
return information about datasources that the caller has DATASOURCE READ access to. Other
datasources will be omitted.

Queries on the [system schema tables](../querying/sql.html#system-schema) require the following permissions:
- `segments`: Segments will be filtered based on DATASOURCE READ permissions.
- `servers`: The user requires `READ` on `SERVER` resource type with `SERVER` resource name.
- `server_segments`: The user requires `READ` on `SERVER` resource type with `SERVER` resource name permissions and segments 
will be filtered based on DATASOURCE READ permissions.
- `tasks`: Tasks will be filtered based on DATASOURCE READ permissions.

### Backwards compatibility and rolling upgrade to auth v2 model from v1
Unless the flag `druid.auth.authVersion` is set to `v2`, older model will be used. For rolling upgrade to v2 model 
there are few prerequisites -
1. Define the new permissions and policies in the auth system you are using.
1. Make sure the auth extension you are using supports the newer model, you need to make sure the `Authorizer` implementation
has implemented `authorizeV2` method implemented.
1. Please make sure both newer and older set of permissions/roles or policies exist in the auth system till all the nodes are upgraded.

Once all the above things are in place follow the rolling upgrade [guide](../operations/rolling-updates.html) and set `druid.auth.authVersion=v2` 
on each node before upgrading.