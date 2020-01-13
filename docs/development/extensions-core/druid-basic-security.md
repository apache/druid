---
id: druid-basic-security
title: "Basic Security"
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


This Apache Druid extension adds:

- an Authenticator which supports [HTTP Basic authentication](https://en.wikipedia.org/wiki/Basic_access_authentication) using the Druid metadata store or LDAP as its credentials store
- an Authorizer which implements basic role-based access control for Druid metadata store or LDAP users and groups

Make sure to [include](../../development/extensions.md#loading-extensions) `druid-basic-security` as an extension.

Please see [Authentication and Authorization](../../design/auth.md) for more information on the extension interfaces being implemented.

## Configuration

The examples in the section will use "MyBasicMetadataAuthenticator", "MyBasicLDAPAuthenticator", "MyBasicMetadataAuthorizer", and "MyBasicLDAPAuthorizer" as names for the Authenticators and Authorizer.

These properties are not tied to specific Authenticator or Authorizer instances.

These configuration properties should be added to the common runtime properties file.

### Properties
|Property|Description|Default|required|
|--------|-----------|-------|--------|
|`druid.auth.basic.common.pollingPeriod`|Defines in milliseconds how often processes should poll the Coordinator for the current Druid metadata store authenticator/authorizer state.|60000|No|
|`druid.auth.basic.common.maxRandomDelay`|Defines in milliseconds the amount of random delay to add to the pollingPeriod, to spread polling requests across time.|6000|No|
|`druid.auth.basic.common.maxSyncRetries`|Determines how many times a service will retry if the authentication/authorization Druid metadata store state sync with the Coordinator fails.|10|No|
|`druid.auth.basic.common.cacheDirectory`|If defined, snapshots of the basic Authenticator and Authorizer Druid metadata store caches will be stored on disk in this directory. If this property is defined, when a service is starting, it will attempt to initialize its caches from these on-disk snapshots, if the service is unable to initialize its state by communicating with the Coordinator.|null|No|


### Creating an Authenticator that uses the Druid metadata store to lookup and validate credentials
```
druid.auth.authenticatorChain=["MyBasicMetadataAuthenticator"]

druid.auth.authenticator.MyBasicMetadataAuthenticator.type=basic
druid.auth.authenticator.MyBasicMetadataAuthenticator.initialAdminPassword=password1
druid.auth.authenticator.MyBasicMetadataAuthenticator.initialInternalClientPassword=password2
druid.auth.authenticator.MyBasicMetadataAuthenticator.credentialsValidator.type=metadata
druid.auth.authenticator.MyBasicMetadataAuthenticator.skipOnFailure=false
druid.auth.authenticator.MyBasicMetadataAuthenticator.authorizerName=MyBasicMetadataAuthorizer
```

To use the Basic authenticator, add an authenticator with type `basic` to the authenticatorChain.
The authenticator needs to also define a credentialsValidator with type 'metadata' or 'ldap'.
If credentialsValidator is not specified, type 'metadata' will be used as default.

Configuration of the named authenticator is assigned through properties with the form:

```
druid.auth.authenticator.<authenticatorName>.<authenticatorProperty>
```

The authenticator configuration examples in the rest of this document will use "MyBasicMetadataAuthenticator" or "MyBasicLDAPAuthenticator" as the name of the authenticators being configured.


#### Properties for Druid metadata store user authentication
|Property|Description|Default|required|
|--------|-----------|-------|--------|
|`druid.auth.authenticator.MyBasicMetadataAuthenticator.initialAdminPassword`|Initial [Password Provider](../../operations/password-provider.md) for the automatically created default admin user. If no password is specified, the default admin user will not be created. If the default admin user already exists, setting this property will not affect its password.|null|No|
|`druid.auth.authenticator.MyBasicMetadataAuthenticator.initialInternalClientPassword`|Initial [Password Provider](../../operations/password-provider.md) for the default internal system user, used for internal process communication. If no password is specified, the default internal system user will not be created. If the default internal system user already exists, setting this property will not affect its password.|null|No|
|`druid.auth.authenticator.MyBasicMetadataAuthenticator.enableCacheNotifications`|If true, the Coordinator will notify Druid processes whenever a configuration change to this Authenticator occurs, allowing them to immediately update their state without waiting for polling.|true|No|
|`druid.auth.authenticator.MyBasicMetadataAuthenticator.cacheNotificationTimeout`|The timeout in milliseconds for the cache notifications.|5000|No|
|`druid.auth.authenticator.MyBasicMetadataAuthenticator.credentialIterations`|Number of iterations to use for password hashing.|10000|No|
|`druid.auth.authenticator.MyBasicMetadataAuthenticator.credentialsValidator.type`|The type of credentials store (metadata) to validate requests credentials.|metadata|No|
|`druid.auth.authenticator.MyBasicMetadataAuthenticator.skipOnFailure`|If true and the request credential doesn't exists or isn't fully configured in the credentials store, the request will proceed to next Authenticator in the chain.|false|No|
|`druid.auth.authenticator.MyBasicMetadataAuthenticator.authorizerName`|Authorizer that requests should be directed to|N/A|Yes|

#### Properties for LDAP user authentication
|Property|Description|Default|required|
|--------|-----------|-------|--------|
|`druid.auth.authenticator.MyBasicLDAPAuthenticator.initialAdminPassword`|Initial [Password Provider](../../operations/password-provider.md) for the automatically created default admin user. If no password is specified, the default admin user will not be created. If the default admin user already exists, setting this property will not affect its password.|null|No|
|`druid.auth.authenticator.MyBasicLDAPAuthenticator.initialInternalClientPassword`|Initial [Password Provider](../../operations/password-provider.md) for the default internal system user, used for internal process communication. If no password is specified, the default internal system user will not be created. If the default internal system user already exists, setting this property will not affect its password.|null|No|
|`druid.auth.authenticator.MyBasicLDAPAuthenticator.enableCacheNotifications`|If true, the Coordinator will notify Druid processes whenever a configuration change to this Authenticator occurs, allowing them to immediately update their state without waiting for polling.|true|No|
|`druid.auth.authenticator.MyBasicLDAPAuthenticator.cacheNotificationTimeout`|The timeout in milliseconds for the cache notifications.|5000|No|
|`druid.auth.authenticator.MyBasicLDAPAuthenticator.credentialIterations`|Number of iterations to use for password hashing.|10000|No|
|`druid.auth.authenticator.MyBasicLDAPAuthenticator.credentialsValidator.type`|The type of credentials store (ldap) to validate requests credentials.|metadata|No|
|`druid.auth.authenticator.MyBasicLDAPAuthenticator.credentialsValidator.url`|URL of the LDAP server.|null|Yes|
|`druid.auth.authenticator.MyBasicLDAPAuthenticator.credentialsValidator.bindUser`|LDAP bind user username.|null|Yes|
|`druid.auth.authenticator.MyBasicLDAPAuthenticator.credentialsValidator.bindPassword`|[Password Provider](../../operations/password-provider.md) LDAP bind user password.|null|Yes|
|`druid.auth.authenticator.MyBasicLDAPAuthenticator.credentialsValidator.baseDn`|The point from where the LDAP server will search for users.|null|Yes|
|`druid.auth.authenticator.MyBasicLDAPAuthenticator.credentialsValidator.userSearch`|The filter/expression to use for the search. For example, (&(sAMAccountName=%s)(objectClass=user))|null|Yes|
|`druid.auth.authenticator.MyBasicLDAPAuthenticator.credentialsValidator.userAttribute`|The attribute id identifying the attribute that will be returned as part of the search. For example, sAMAccountName. |null|Yes|
|`druid.auth.authenticator.MyBasicLDAPAuthenticator.credentialsValidator.credentialVerifyDuration`|The duration in seconds for how long valid credentials are verifiable within the cache when not requested.|600|No|
|`druid.auth.authenticator.MyBasicLDAPAuthenticator.credentialsValidator.credentialMaxDuration`|The max duration in seconds for valid credentials that can reside in cache regardless of how often they are requested.|3600|No|
|`druid.auth.authenticator.MyBasicLDAPAuthenticator.credentialsValidator.credentialCacheSize`|The valid credentials cache size. The cache uses a LRU policy.|100|No|
|`druid.auth.authenticator.MyBasicLDAPAuthenticator.skipOnFailure`|If true and the request credential doesn't exists or isn't fully configured in the credentials store, the request will proceed to next Authenticator in the chain.|false|No|
|`druid.auth.authenticator.MyBasicLDAPAuthenticator.authorizerName`|Authorizer that requests should be directed to.|N/A|Yes|

### Creating an Escalator

```
# Escalator
druid.escalator.type=basic
druid.escalator.internalClientUsername=druid_system
druid.escalator.internalClientPassword=password2
druid.escalator.authorizerName=MyBasicMetadataAuthorizer
```

#### Properties
|Property|Description|Default|required|
|--------|-----------|-------|--------|
|`druid.escalator.internalClientUsername`|The escalator will use this username for requests made as the internal system user.|n/a|Yes|
|`druid.escalator.internalClientPassword`|The escalator will use this [Password Provider](../../operations/password-provider.md) for requests made as the internal system user.|n/a|Yes|
|`druid.escalator.authorizerName`|Authorizer that requests should be directed to.|n/a|Yes|


### Creating an Authorizer
```
druid.auth.authorizers=["MyBasicMetadataAuthorizer"]

druid.auth.authorizer.MyBasicMetadataAuthorizer.type=basic
```

To use the Basic authorizer, add an authorizer with type `basic` to the authorizers list.

Configuration of the named authorizer is assigned through properties with the form:

```
druid.auth.authorizer.<authorizerName>.<authorizerProperty>
```

The authorizer configuration examples in the rest of this document will use "MyBasicMetadataAuthorizer" or "MyBasicLDAPAuthorizer" as the name of the authenticators being configured.

#### Properties for Druid metadata store user authorization
|Property|Description|Default|required|
|--------|-----------|-------|--------|
|`druid.auth.authorizer.MyBasicMetadataAuthorizer.enableCacheNotifications`|If true, the Coordinator will notify Druid processes whenever a configuration change to this Authorizer occurs, allowing them to immediately update their state without waiting for polling.|true|No|
|`druid.auth.authorizer.MyBasicMetadataAuthorizer.cacheNotificationTimeout`|The timeout in milliseconds for the cache notifications.|5000|No|
|`druid.auth.authorizer.MyBasicMetadataAuthorizer.initialAdminUser`|The initial admin user with role defined in initialAdminRole property if specified, otherwise the default admin role will be assigned.|admin|No|
|`druid.auth.authorizer.MyBasicMetadataAuthorizer.initialAdminRole`|The initial admin role to create if it doesn't already exists.|admin|No|
|`druid.auth.authorizer.MyBasicMetadataAuthorizer.roleProvider.type`|The type of role provider to authorize requests credentials.|metadata|No

#### Properties for LDAP user authorization
|Property|Description|Default|required|
|--------|-----------|-------|--------|
|`druid.auth.authorizer.MyBasicLDAPAuthorizer.enableCacheNotifications`|If true, the Coordinator will notify Druid processes whenever a configuration change to this Authorizer occurs, allowing them to immediately update their state without waiting for polling.|true|No|
|`druid.auth.authorizer.MyBasicLDAPAuthorizer.cacheNotificationTimeout`|The timeout in milliseconds for the cache notifications.|5000|No|
|`druid.auth.authorizer.MyBasicLDAPAuthorizer.initialAdminUser`|The initial admin user with role defined in initialAdminRole property if specified, otherwise the default admin role will be assigned.|admin|No|
|`druid.auth.authorizer.MyBasicLDAPAuthorizer.initialAdminRole`|The initial admin role to create if it doesn't already exists.|admin|No|
|`druid.auth.authorizer.MyBasicLDAPAuthorizer.initialAdminGroupMapping`|The initial admin group mapping with role defined in initialAdminRole property if specified, otherwise the default admin role will be assigned. The name of this initial admin group mapping will be set to adminGroupMapping|null|No|
|`druid.auth.authorizer.MyBasicLDAPAuthorizer.roleProvider.type`|The type of role provider (ldap) to authorize requests credentials.|metadata|No
|`druid.auth.authorizer.MyBasicLDAPAuthorizer.roleProvider.groupFilters`|Array of LDAP group filters used to filter out the allowed set of groups returned from LDAP search. Filters can be begin with *, or end with ,* to provide configurational flexibility to limit or filter allowed set of groups available to LDAP Authorizer.|null|No|

## Usage

### Coordinator Security API
To use these APIs, a user needs read/write permissions for the CONFIG resource type with name "security".

#### Authentication API

Root path: `/druid-ext/basic-security/authentication`

Each API endpoint includes {authenticatorName}, specifying which Authenticator instance is being configured.

##### User/Credential Management
`GET(/druid-ext/basic-security/authentication/db/{authenticatorName}/users)`
Return a list of all user names.

`GET(/druid-ext/basic-security/authentication/db/{authenticatorName}/users/{userName})`
Return the name and credentials information of the user with name {userName}

`POST(/druid-ext/basic-security/authentication/db/{authenticatorName}/users/{userName})`
Create a new user with name {userName}

`DELETE(/druid-ext/basic-security/authentication/db/{authenticatorName}/users/{userName})`
Delete the user with name {userName}

`POST(/druid-ext/basic-security/authentication/db/{authenticatorName}/users/{userName}/credentials)`
Assign a password used for HTTP basic authentication for {userName}
Content: JSON password request object

Example request body:

```
{
  "password": "helloworld"
}
```

##### Cache Load Status
`GET(/druid-ext/basic-security/authentication/loadStatus)`
Return the current load status of the local caches of the authentication Druid metadata store.

#### Authorization API

Root path: `/druid-ext/basic-security/authorization`

Each API endpoint includes {authorizerName}, specifying which Authorizer instance is being configured.

##### User Creation/Deletion
`GET(/druid-ext/basic-security/authorization/db/{authorizerName}/users)`
Return a list of all user names.

`GET(/druid-ext/basic-security/authorization/db/{authorizerName}/users/{userName})`
Return the name and role information of the user with name {userName}

Example output:

```json
{
  "name": "druid2",
  "roles": [
    "druidRole"
  ]
}
```

This API supports the following flags:

- `?full`: The response will also include the full information for each role currently assigned to the user.

Example output:

```json
{
  "name": "druid2",
  "roles": [
    {
      "name": "druidRole",
      "permissions": [
        {
          "resourceAction": {
            "resource": {
              "name": "A",
              "type": "DATASOURCE"
            },
            "action": "READ"
          },
          "resourceNamePattern": "A"
        },
        {
          "resourceAction": {
            "resource": {
              "name": "C",
              "type": "CONFIG"
            },
            "action": "WRITE"
          },
          "resourceNamePattern": "C"
        }
      ]
    }
  ]
}
```

The output format of this API when `?full` is specified is deprecated and in later versions will be switched to the output format used when both `?full` and `?simplifyPermissions` flag is set.

The `resourceNamePattern` is a compiled version of the resource name regex. It is redundant and complicates the use of this API for clients such as frontends that edit the authorization configuration, as the permission format in this output does not match the format used for adding permissions to a role.

- `?full?simplifyPermissions`: When both `?full` and `?simplifyPermissions` are set, the permissions in the output will contain only a list of `resourceAction` objects, without the extraneous `resourceNamePattern` field.

```json
{
  "name": "druid2",
  "roles": [
    {
      "name": "druidRole",
      "users": null,
      "permissions": [
        {
          "resource": {
            "name": "A",
            "type": "DATASOURCE"
          },
          "action": "READ"
        },
        {
          "resource": {
            "name": "C",
            "type": "CONFIG"
          },
          "action": "WRITE"
        }
      ]
    }
  ]
}
```

`POST(/druid-ext/basic-security/authorization/db/{authorizerName}/users/{userName})`
Create a new user with name {userName}

`DELETE(/druid-ext/basic-security/authorization/db/{authorizerName}/users/{userName})`
Delete the user with name {userName}

##### Group mapping Creation/Deletion
`GET(/druid-ext/basic-security/authorization/db/{authorizerName}/groupMappings)`
Return a list of all group mappings.

`GET(/druid-ext/basic-security/authorization/db/{authorizerName}/groupMappings/{groupMappingName})`
Return the group mapping and role information of the group mapping with name {groupMappingName}

`POST(/druid-ext/basic-security/authorization/db/{authorizerName}/groupMappings/{groupMappingName})`
Create a new group mapping with name {groupMappingName}
Content: JSON group mapping object
Example request body:

```
{
    "name": "user",
    "groupPattern": "CN=aaa,OU=aaa,OU=Groupings,DC=corp,DC=company,DC=com",
    "roles": [
        "user"
    ]
}
```

`DELETE(/druid-ext/basic-security/authorization/db/{authorizerName}/groupMappings/{groupMappingName})`
Delete the group mapping with name {groupMappingName}

#### Role Creation/Deletion
`GET(/druid-ext/basic-security/authorization/db/{authorizerName}/roles)`
Return a list of all role names.

`GET(/druid-ext/basic-security/authorization/db/{authorizerName}/roles/{roleName})`
Return name and permissions for the role named {roleName}.

Example output:

```json
{
  "name": "druidRole2",
  "permissions": [
    {
      "resourceAction": {
        "resource": {
          "name": "E",
          "type": "DATASOURCE"
        },
        "action": "WRITE"
      },
      "resourceNamePattern": "E"
    }
  ]
}
```

The default output format of this API is deprecated and in later versions will be switched to the output format used when the `?simplifyPermissions` flag is set. The `resourceNamePattern` is a compiled version of the resource name regex. It is redundant and complicates the use of this API for clients such as frontends that edit the authorization configuration, as the permission format in this output does not match the format used for adding permissions to a role.

This API supports the following flags:

- `?full`: The output will contain an extra `users` list, containing the users that currently have this role.

```json
{"users":["druid"]}
```

- `?simplifyPermissions`: The permissions in the output will contain only a list of `resourceAction` objects, without the extraneous `resourceNamePattern` field. The `users` field will be null when `?full` is not specified.

Example output:

```json
{
  "name": "druidRole2",
  "users": null,
  "permissions": [
    {
      "resource": {
        "name": "E",
        "type": "DATASOURCE"
      },
      "action": "WRITE"
    }
  ]
}
```


`POST(/druid-ext/basic-security/authorization/db/{authorizerName}/roles/{roleName})`
Create a new role with name {roleName}.
Content: username string

`DELETE(/druid-ext/basic-security/authorization/db/{authorizerName}/roles/{roleName})`
Delete the role with name {roleName}.


#### Role Assignment
`POST(/druid-ext/basic-security/authorization/db/{authorizerName}/users/{userName}/roles/{roleName})`
Assign role {roleName} to user {userName}.

`DELETE(/druid-ext/basic-security/authorization/db/{authorizerName}/users/{userName}/roles/{roleName})`
Unassign role {roleName} from user {userName}

`POST(/druid-ext/basic-security/authorization/db/{authorizerName}/groupMappings/{groupMappingName}/roles/{roleName})`
Assign role {roleName} to group mapping {groupMappingName}.

`DELETE(/druid-ext/basic-security/authorization/db/{authorizerName}/groupMappings/{groupMappingName}/roles/{roleName})`
Unassign role {roleName} from group mapping {groupMappingName}


#### Permissions
`POST(/druid-ext/basic-security/authorization/db/{authorizerName}/roles/{roleName}/permissions)`
Set the permissions of {roleName}. This replaces the previous set of permissions on the role.

Content: List of JSON Resource-Action objects, e.g.:

```
[
{
  "resource": {
    "name": "wiki.*",
    "type": "DATASOURCE"
  },
  "action": "READ"
},
{
  "resource": {
    "name": "wikiticker",
    "type": "DATASOURCE"
  },
  "action": "WRITE"
}
]
```

The "name" field for resources in the permission definitions are regexes used to match resource names during authorization checks.

Please see [Defining permissions](#defining-permissions) for more details.

##### Cache Load Status
`GET(/druid-ext/basic-security/authorization/loadStatus)`
Return the current load status of the local caches of the authorization Druid metadata store.

## Default user accounts

### Authenticator
If `druid.auth.authenticator.<authenticator-name>.initialAdminPassword` is set, a default admin user named "admin" will be created, with the specified initial password. If this configuration is omitted, the "admin" user will not be created.

If `druid.auth.authenticator.<authenticator-name>.initialInternalClientPassword` is set, a default internal system user named "druid_system" will be created, with the specified initial password. If this configuration is omitted, the "druid_system" user will not be created.


### Authorizer

Each Authorizer will always have a default "admin" and "druid_system" user with full privileges.

## Defining permissions

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

For information on what HTTP methods are supported on a particular request endpoint, please refer to the [API documentation](../../operations/api-reference.md).

GET requires READ permission, while POST and DELETE require WRITE permission.

### SQL Permissions

Queries on Druid datasources require DATASOURCE READ permissions for the specified datasource.

Queries on the [INFORMATION_SCHEMA tables](../../querying/sql.html#information-schema) will
return information about datasources that the caller has DATASOURCE READ access to. Other
datasources will be omitted.

Queries on the [system schema tables](../../querying/sql.html#system-schema) require the following permissions:
- `segments`: Segments will be filtered based on DATASOURCE READ permissions.
- `servers`: The user requires STATE READ permissions.
- `server_segments`: The user requires STATE READ permissions and segments will be filtered based on DATASOURCE READ permissions.
- `tasks`: Tasks will be filtered based on DATASOURCE READ permissions.

## Configuration Propagation

To prevent excessive load on the Coordinator, the Authenticator and Authorizer user/role Druid metadata store state is cached on each Druid process.

Each process will periodically poll the Coordinator for the latest Druid metadata store state, controlled by the `druid.auth.basic.common.pollingPeriod` and `druid.auth.basic.common.maxRandomDelay` properties.

When a configuration update occurs, the Coordinator can optionally notify each process with the updated Druid metadata store state. This behavior is controlled by the `enableCacheNotifications` and `cacheNotificationTimeout` properties on Authenticators and Authorizers.

Note that because of the caching, changes made to the user/role Druid metadata store may not be immediately reflected at each Druid process.
