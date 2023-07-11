---
id: security-user-auth
title: "User authentication and authorization"
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


This document describes the Druid security model that extensions use to enable user authentication and authorization services to Druid. 

## Authentication and authorization model

At the center of the Druid user authentication and authorization model are _resources_ and _actions_. A resource is something that authenticated users are trying to access or modify. An action is something that users are trying to do. 

### Resource types

Druid uses the following resource types:

* DATASOURCE &ndash; Each Druid table (i.e., `tables` in the `druid` schema in SQL) is a resource.
* CONFIG &ndash; Configuration resources exposed by the cluster components. 
* EXTERNAL &ndash; External data read through the [EXTERN function](../multi-stage-query/concepts.md#extern) in SQL.
* STATE &ndash; Cluster-wide state resources.
* SYSTEM_TABLE &ndash; when the Broker property `druid.sql.planner.authorizeSystemTablesDirectly` is true, then Druid uses this resource type to authorize the system tables in the `sys` schema in SQL.

For specific resources associated with the resource types, see [Defining permissions](#defining-permissions) and the corresponding endpoint descriptions in [API reference](./api-reference.md).

### Actions

Users perform one of the following actions on resources:

* READ &ndash; Used for read-only operations.
* WRITE &ndash; Used for operations that are not read-only.

WRITE permission on a resource does not include READ permission. If a user requires both READ and WRITE permissions on a resource, you must grant them both explicitly. For instance, a user with only `DATASOURCE READ` permission
might have access to an API or a system schema record that a user with `DATASOURCE WRITE` permission would not have access to.

### User types

In practice, most deployments will only need to define two classes of users: 

* Administrators, who have WRITE action permissions on all resource types. These users will add datasources and administer the system.  
* Data users, who only need READ access to DATASOURCE. These users should access Query APIs only through an API gateway. Other APIs and permissions include functionality that should be limited to server admins. 

It is important to note that WRITE access to DATASOURCE grants a user broad access. For instance, such users will have access to the Druid file system, S3 buckets, and credentials, among other things. As such, the ability to add and manage datasources should be allocated selectively to administrators.   

## Default user accounts

### Authenticator
If `druid.auth.authenticator.<authenticator-name>.initialAdminPassword` is set, a default admin user named "admin" will be created, with the specified initial password. If this configuration is omitted, the "admin" user will not be created.

If `druid.auth.authenticator.<authenticator-name>.initialInternalClientPassword` is set, a default internal system user named "druid_system" will be created, with the specified initial password. If this configuration is omitted, the "druid_system" user will not be created.


### Authorizer

Each Authorizer will always have a default "admin" and "druid_system" user with full privileges.

## Defining permissions

You define permissions that you then grant to user groups.
Permissions are defined by resource type, action, and resource name.
This section describes the resource names available for each resource type.

### `DATASOURCE`
Resource names for this type are datasource names. Specifying a datasource permission allows the administrator to grant users access to specific datasources.

### `CONFIG`
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

### `EXTERNAL`

The EXTERNAL resource type only accepts the resource name "EXTERNAL".
Granting a user access to EXTERNAL resources allows them to run queries that include
the [EXTERN function](../multi-stage-query/concepts.md#extern) in SQL
to read external data.

### `STATE`
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

### `SYSTEM_TABLE`
Resource names for this type are system schema table names in the `sys` schema in SQL, for example `sys.segments` and `sys.server_segments`. Druid only enforces authorization for `SYSTEM_TABLE` resources when the Broker property `druid.sql.planner.authorizeSystemTablesDirectly` is true.
### HTTP methods

For information on what HTTP methods are supported on a particular request endpoint, refer to [API reference](./api-reference.md).

`GET` requests require READ permissions, while `POST` and `DELETE` requests require WRITE permissions.

### SQL permissions

Queries on Druid datasources require DATASOURCE READ permissions for the specified datasource.

Queries to access external data through the [EXTERN function](../multi-stage-query/concepts.md#extern) require EXTERNAL READ permissions.

Queries on [INFORMATION_SCHEMA tables](../querying/sql-metadata-tables.md#information-schema) return information about datasources that the caller has DATASOURCE READ access to. Other
datasources are omitted.

Queries on the [system schema tables](../querying/sql-metadata-tables.md#system-schema) require the following permissions:
- `segments`: Druid filters segments according to DATASOURCE READ permissions.
- `servers`: The user requires STATE READ permissions.
- `server_segments`: The user requires STATE READ permissions. Druid filters segments according to DATASOURCE READ permissions.
- `tasks`: Druid filters tasks according to DATASOURCE READ permissions.
- `supervisors`: Druid filters supervisors according to DATASOURCE READ permissions.

When the Broker property `druid.sql.planner.authorizeSystemTablesDirectly` is true, users also require  `SYSTEM_TABLE` authorization on a system schema table to query it.

## Configuration propagation

To prevent excessive load on the Coordinator, the Authenticator and Authorizer user/role Druid metadata store state is cached on each Druid process.

Each process will periodically poll the Coordinator for the latest Druid metadata store state, controlled by the `druid.auth.basic.common.pollingPeriod` and `druid.auth.basic.common.maxRandomDelay` properties.

When a configuration update occurs, the Coordinator can optionally notify each process with the updated Druid metadata store state. This behavior is controlled by the `enableCacheNotifications` and `cacheNotificationTimeout` properties on Authenticators and Authorizers.

Note that because of the caching, changes made to the user/role Druid metadata store may not be immediately reflected at each Druid process.
