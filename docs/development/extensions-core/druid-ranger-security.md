---
id: druid-ranger-security
title: "Apache Ranger Security"
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

This Apache Druid extension adds an Authorizer which implements access control for Druid, backed by [Apache Ranger](https://ranger.apache.org/). Please see [Authentication and Authorization](../../operations/auth.md) for more information on the basic facilities this extension provides.

Make sure to [include](../../configuration/extensions.md#loading-extensions) `druid-ranger-security` in the extensions load list.


## Configuration

Support for Apache Ranger authorization consists of three elements:
* configuring the extension in Apache Druid
* configuring the connection to Apache Ranger
* providing the service definition for Druid to Apache Ranger

### Enabling the extension
Ensure that you have a valid authenticator chain and escalator set in your `common.runtime.properties`. For every authenticator your wish to use the authorizer for, set `druid.auth.authenticator.<authenticatorName>.authorizerName` to the name you will give the authorizer, e.g. `ranger`.

Then add the following and amend to your needs (in case you need to use multiple authorizers):

```
druid.auth.authorizers=["ranger"]
druid.auth.authorizer.ranger.type=ranger
```

The following is an example that showcases using `druid-basic-security` for authentication and `druid-ranger-security` for authorization.

```
druid.auth.authenticatorChain=["basic"]
druid.auth.authenticator.basic.type=basic
druid.auth.authenticator.basic.initialAdminPassword=password1
druid.auth.authenticator.basic.initialInternalClientPassword=password2
druid.auth.authenticator.basic.credentialsValidator.type=metadata
druid.auth.authenticator.basic.skipOnFailure=false
druid.auth.authenticator.basic.enableCacheNotifications=true
druid.auth.authenticator.basic.authorizerName=ranger

druid.auth.authorizers=["ranger"]
druid.auth.authorizer.ranger.type=ranger

# Escalator
druid.escalator.type=basic
druid.escalator.internalClientUsername=druid_system
druid.escalator.internalClientPassword=password2
druid.escalator.authorizerName=ranger
```

:::info
 Contrary to the documentation of `druid-basic-auth` Ranger does not automatically provision a highly privileged system user, you will need to do this yourself. This system user in the case of `druid-basic-auth` is named `druid_system` and for the escalator it is configurable, as shown above. Make sure to take note of these user names and configure `READ` access to `state:STATE` and to `config:security` in your ranger policies, otherwise system services will not work properly.
:::

#### Properties to configure the extension in Apache Druid
|Property|Description|Default|required|
|--------|-----------|-------|--------|
|`druid.auth.ranger.keytab`|Defines the keytab to be used while authenticating against Apache Ranger to obtain policies and provide auditing|null|No|
|`druid.auth.ranger.principal`|Defines the principal to be used while authenticating against Apache Ranger to obtain policies and provide auditing|null|No|
|`druid.auth.ranger.use_ugi`|Determines if groups that the authenticated user belongs to should be obtained from Hadoop's `UserGroupInformation`|null|No|

### Configuring the connection to Apache Ranger

The Apache Ranger authorization extension will read several configuration files. Discussing the contents of those files is beyond the scope of this document. Depending on your needs you will need to create them. The minimum you will need to have is a `ranger-druid-security.xml` file that you will need to put in the classpath (e.g. `_common`). For auditing, the configuration is in `ranger-druid-audit.xml`.

### Adding the service definition for Apache Druid to Apache Ranger

At the time of writing of this document Apache Ranger (2.0) does not include an out of the box service and service definition for Druid. You can add the service definition to Apache Ranger by entering the following command:

`curl -u <user>:<password> -d "@ranger-servicedef-druid.json" -X POST -H "Accept: application/json" -H "Content-Type: application/json" http://localhost:6080/service/public/v2/api/servicedef/`

You should get back `json` describing the service definition you just added. You can now go to the web interface of Apache Ranger which should now include a widget for "Druid". Click the plus sign and create the new service. Ensure your service name is equal to what you configured in `ranger-druid-security.xml`.

#### Configuring Apache Ranger policies

When installing a new Druid service in Apache Ranger for the first time, Ranger will provision the policies to allow the administrative user `read/write` access to all properties and data sources. You might want to limit this. Do not forget to add the correct policies for the `druid_system` user and the `internalClientUserName` of the escalator.

:::info
 Loading new data sources requires `write` access to the `datasource` prior to the loading itself. So if you want to create a datasource `wikipedia` you are required to have an `allow` policy inside Apache Ranger before trying to load the spec.
:::

## Usage

### HTTP methods

For information on what HTTP methods are supported for a particular request endpoint, please refer to the [API documentation](../../api-reference/api-reference.md).

GET requires READ permission, while POST and DELETE require WRITE permission.

### SQL Permissions

Queries on Druid datasources require DATASOURCE READ permissions for the specified datasource.

Queries on the [INFORMATION_SCHEMA tables](../../querying/sql-metadata-tables.md#information-schema) will return information about datasources that the caller has DATASOURCE READ access to. Other datasources will be omitted.

Queries on the [system schema tables](../../querying/sql-metadata-tables.md#system-schema) require the following permissions:
- `segments`: Segments will be filtered based on DATASOURCE READ permissions.
- `servers`: The user requires STATE READ permissions.
- `server_segments`: The user requires STATE READ permissions and segments will be filtered based on DATASOURCE READ permissions.
- `tasks`: Tasks will be filtered based on DATASOURCE READ permissions.


### Debugging

If you face difficulty grasping why access is denied to certain elements, and the `audit` section in Apache Ranger does not give you any detail, you can enable debug logging for `org.apache.druid.security.ranger`. To do so add the following in your `log4j2.xml`:

```xml
<!-- Set level="debug" to see access requests to Apache Ranger -->
<Logger name="org.apache.druid.security" level="debug" additivity="false">
  <Appender-ref ref="Console"/>
</Logger>
```
