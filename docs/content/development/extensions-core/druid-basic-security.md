---
layout: doc_page
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

# Druid Basic Security

This extension adds:
- an Authenticator which supports [HTTP Basic authentication](https://en.wikipedia.org/wiki/Basic_access_authentication) using database or LDAP as its credentials store
- an Authorizer which implements basic role-based access control for database and LDAP users and groups

Make sure to [include](../../operations/including-extensions.html) `druid-basic-security` as an extension.

Please see [Authentication and Authorization](../../design/auth.html) for more information on the extension interfaces being implemented.

## Configuration

The examples in the section will use "MyBasicDBAuthenticator", "MyBasicLDAPAuthenticator" and "MyBasicAuthorizer" as names for the Authenticators and Authorizer.

These properties are not tied to specific Authenticator or Authorizer instances.

These configuration properties should be added to the common runtime properties file.

### Properties
|Property|Description|Default|required|
|--------|-----------|-------|--------|
|`druid.auth.basic.common.pollingPeriod`|Defines in milliseconds how often nodes should poll the Coordinator for the current escalator/authenticator/authorizer database state.|60000|No|
|`druid.auth.basic.common.maxRandomDelay`|Defines in milliseconds the amount of random delay to add to the pollingPeriod, to spread polling requests across time.|6000|No|
|`druid.auth.basic.common.maxSyncRetries`|Determines how many times a service will retry if the authentication/authorization database state sync with the Coordinator fails.|10|No|
|`druid.auth.basic.common.cacheDirectory`|If defined, snapshots of the basic Authenticator and Authorizer database caches will be stored on disk in this directory. If this property is defined, when a service is starting, it will attempt to initialize its caches from these on-disk snapshots, if the service is unable to initialize its state by communicating with the Coordinator.|null|No|


### Creating an Authenticator that uses database to lookup and validate credentials
```
druid.auth.authenticatorChain=["MyBasicDBAuthenticator"]

druid.auth.authenticator.MyBasicDBAuthenticator.type=basic
druid.auth.authenticator.MyBasicDBAuthenticator.initialAdminPassword=password1
druid.auth.authenticator.MyBasicDBAuthenticator.initialInternalClientPassword=password2
druid.auth.authenticator.MyBasicDBAuthenticator.credentialsValidator.type=db
druid.auth.authenticator.MyBasicDBAuthenticator.skipOnFailure=false
druid.auth.authenticator.MyBasicDBAuthenticator.authorizerName=MyBasicAuthorizer
```

To use the Basic authenticator, add an authenticator with type `basic` to the authenticatorChain.
The authenticator needs to also define a credentialsValidator with type 'db' or 'ldap'.
If credentialsValidator is not specified, type 'db' will be used as default.

Configuration of the named authenticator is assigned through properties with the form:

```
druid.auth.authenticator.<authenticatorName>.<authenticatorProperty>
```

The configuration examples in the rest of this document will use "MyBasicDBAuthenticator" or "MyBasicLDAPAuthenticator" as the name of the authenticator being configured.


#### Properties for database user authentication
|Property|Description|Default|required|
|--------|-----------|-------|--------|
|`druid.auth.authenticator.MyBasicDBAuthenticator.initialAdminPassword`|Initial [Password Provider](../../operations/password-provider.html) for the automatically created default admin user. If no password is specified, the default admin user will not be created. If the default admin user already exists, setting this property will not affect its password.|null|No|
|`druid.auth.authenticator.MyBasicDBAuthenticator.initialInternalClientPassword`|Initial [Password Provider](../../operations/password-provider.html) for the default internal system user, used for internal node communication. If no password is specified, the default internal system user will not be created. If the default internal system user already exists, setting this property will not affect its password.|null|No|
|`druid.auth.authenticator.MyBasicDBAuthenticator.enableCacheNotifications`|If true, the Coordinator will notify Druid nodes whenever a configuration change to this Authenticator occurs, allowing them to immediately update their state without waiting for polling.|true|No|
|`druid.auth.authenticator.MyBasicDBAuthenticator.cacheNotificationTimeout`|The timeout in milliseconds for the cache notifications.|5000|No|
|`druid.auth.authenticator.MyBasicDBAuthenticator.credentialIterations`|Number of iterations to use for password hashing.|10000|No|
|`druid.auth.authenticator.MyBasicDBAuthenticator.credentialsValidator.type`|The type of credentials store (db) to validate requests credentials.|db|No|
|`druid.auth.authenticator.MyBasicDBAuthenticator.skipOnFailure`|If true and the request credential doesn't exists or isn't fully configured in the credentials store, the request will proceed to next Authenticator in the chain.|false|No|
|`druid.auth.authenticator.MyBasicDBAuthenticator.authorizerName`|Authorizer that requests should be directed to|N/A|Yes|

#### Properties for LDAP user authentication
|Property|Description|Default|required|
|--------|-----------|-------|--------|
|`druid.auth.authenticator.MyBasicLDAPAuthenticator.initialAdminPassword`|Initial [Password Provider](../../operations/password-provider.html) for the automatically created default admin user. If no password is specified, the default admin user will not be created. If the default admin user already exists, setting this property will not affect its password.|null|No|
|`druid.auth.authenticator.MyBasicLDAPAuthenticator.initialInternalClientPassword`|Initial [Password Provider](../../operations/password-provider.html) for the default internal system user, used for internal node communication. If no password is specified, the default internal system user will not be created. If the default internal system user already exists, setting this property will not affect its password.|null|No|
|`druid.auth.authenticator.MyBasicLDAPAuthenticator.enableCacheNotifications`|If true, the Coordinator will notify Druid nodes whenever a configuration change to this Authenticator occurs, allowing them to immediately update their state without waiting for polling.|true|No|
|`druid.auth.authenticator.MyBasicLDAPAuthenticator.cacheNotificationTimeout`|The timeout in milliseconds for the cache notifications.|5000|No|
|`druid.auth.authenticator.MyBasicLDAPAuthenticator.credentialIterations`|Number of iterations to use for password hashing.|10000|No|
|`druid.auth.authenticator.MyBasicLDAPAuthenticator.credentialsValidator.type`|The type of credentials store (ldap) to validate requests credentials.|db|No|
|`druid.auth.authenticator.MyBasicLDAPAuthenticator.credentialsValidator.url`|URL of the LDAP server.|null|Yes|
|`druid.auth.authenticator.MyBasicLDAPAuthenticator.credentialsValidator.bindUser`|LDAP bind user username.|null|Yes|
|`druid.auth.authenticator.MyBasicLDAPAuthenticator.credentialsValidator.bindPassword`|[Password Provider](../../operations/password-provider.html) LDAP bind user password.|null|Yes|
|`druid.auth.authenticator.MyBasicLDAPAuthenticator.credentialsValidator.baseDn`|The point from where the LDAP server will search for users.|null|Yes|
|`druid.auth.authenticator.MyBasicLDAPAuthenticator.credentialsValidator.userSearch`|The filter/expression to use for the search. For example, (&(sAMAccountName=%s)(objectClass=user))|null|Yes|
|`druid.auth.authenticator.MyBasicLDAPAuthenticator.credentialsValidator.userAttribute`|The attribute id identifying the attribute that will be returned as part of the search. For example, sAMAccountName. |null|Yes|
|`druid.auth.authenticator.MyBasicLDAPAuthenticator.credentialsValidator.groupFilters`|Array of filters used to filter out the allowed set of groups returned from search. Filters can be begin with *, or end with ,* to provide configurational flexibility.|null|Yes|
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
druid.escalator.authorizerName=MyBasicAuthorizer
```

#### Properties
|Property|Description|Default|required|
|--------|-----------|-------|--------|
|`druid.escalator.internalClientUsername`|The escalator will use this username for requests made as the internal systerm user.|n/a|Yes|
|`druid.escalator.internalClientPassword`|The escalator will use this [Password Provider](../../operations/password-provider.html) for requests made as the internal system user.|n/a|Yes|
|`druid.escalator.authorizerName`|Authorizer that requests should be directed to.|n/a|Yes|
|`druid.escalator.enableCacheNotifications`|If true, the Coordinator will notify Druid nodes whenever a configuration change to this Escalator occurs, allowing them to immediately update their state without waiting for polling.|true|No|
|`druid.escalator.cacheNotificationTimeout`|The timeout in milliseconds for the cache notifications.|5000|No|
|`druid.escalator.internalClientCredentialPoll`|Defines in seconds how often Escalator will poll for its current internal client credentials|10|No|


### Creating an Authorizer
```
druid.auth.authorizers=["MyBasicAuthorizer"]

druid.auth.authorizer.MyBasicAuthorizer.type=basic
```

To use the Basic authorizer, add an authorizer with type `basic` to the authorizers list.

Configuration of the named authorizer is assigned through properties with the form:

```
druid.auth.authorizer.<authorizerName>.<authorizerProperty>
```

#### Properties
|Property|Description|Default|required|
|--------|-----------|-------|--------|
|`druid.auth.authorizer.MyBasicAuthorizer.enableCacheNotifications`|If true, the Coordinator will notify Druid nodes whenever a configuration change to this Authorizer occurs, allowing them to immediately update their state without waiting for polling.|true|No|
|`druid.auth.authorizer.MyBasicAuthorizer.cacheNotificationTimeout`|The timeout in milliseconds for the cache notifications.|5000|No|
|`druid.auth.authorizer.MyBasicAuthorizer.initialAdminUser`|The initial admin user with role defined in initialAdminRole property if specified, otherwise the default admin role will be assigned.|admin|No|
|`druid.auth.authorizer.MyBasicAuthorizer.initialAdminRole`|The initial admin role to create if it doesn't already exists.|admin|No|
|`druid.auth.authorizer.MyBasicAuthorizer.initialAdminGroupMapping`|The initial admin group mapping with role defined in initialAdminRole property if specified, otherwise the default admin role will be assigned.|admin|No|

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

`GET(/druid-ext/basic-security/authentication/db/{authenticatorName}/config)`
Return the authenticator current set of updatable configuration.

`POST(/druid-ext/basic-security/authentication/db/{authenticatorName}/config)`
Update the authenticator current set of updatable configuration.
Content: JSON config request object
Example request body:
```
{
    "url": "ldaps://host:port",
    "bindUser": "DHC\\username",
    "bindPassword": "password",
    "baseDn": "DC=corp,DC=company,DC=com",
    "userSearch": "(&(sAMAccountName=%s)(objectClass=user))",
    "userAttribute": "sAMAccountName",
    "groupFilters": [
        "*,OU=Groupings,DC=corp,DC=company,DC=com"
    ]
}
```

##### Cache Load Status
`GET(/druid-ext/basic-security/authentication/loadStatus)`
Return the current load status of the local caches of the authentication database.

#### Authorization API

Root path: `/druid-ext/basic-security/authorization`

Each API endpoint includes {authorizerName}, specifying which Authorizer instance is being configured.

##### User Creation/Deletion
`GET(/druid-ext/basic-security/authorization/db/{authorizerName}/users)`
Return a list of all user names.

`GET(/druid-ext/basic-security/authorization/db/{authorizerName}/users/{userName})`
Return the name and role information of the user with name {userName}

`POST(/druid-ext/basic-security/authorization/db/{authorizerName}/users/{userName})`
Create a new user with name {userName}

`DELETE(/druid-ext/basic-security/authorization/db/{authorizerName}/users/{userName})`
Delete the user with name {userName}

##### Group mapping Creation/Deletion
`GET(/druid-ext/basic-security/authorization/db/{authorizerName}/groupmappings)`
Return a list of all group mappings.

`GET(/druid-ext/basic-security/authorization/db/{authorizerName}/groupmappings/{groupMappingName})`
Return the group mapping and role information of the group mapping with name {groupMappingName}

`POST(/druid-ext/basic-security/authorization/db/{authorizerName}/groupmappings/{groupMappingName})`
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

`DELETE(/druid-ext/basic-security/authorization/db/{authorizerName}/groupmappings/{groupMappingName})`
Delete the group mapping with name {groupMappingName}

#### Role Creation/Deletion
`GET(/druid-ext/basic-security/authorization/db/{authorizerName}/roles)`
Return a list of all role names.

`GET(/druid-ext/basic-security/authorization/db/{authorizerName}/roles/{roleName})`
Return name and permissions for the role named {roleName}

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

`POST(/druid-ext/basic-security/authorization/db/{authorizerName}/groupmappings/{groupMappingName}/roles/{roleName})`
Assign role {roleName} to group mapping {groupMappingName}.

`DELETE(/druid-ext/basic-security/authorization/db/{authorizerName}/groupmappings/{groupMappingName}/roles/{roleName})`
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
Return the current load status of the local caches of the authorization database.

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

|Endpoint|Node Type|
|--------|---------|
|`/druid/coordinator/v1/config`|coordinator|
|`/druid/indexer/v1/worker`|overlord|
|`/druid/indexer/v1/worker/history`|overlord|
|`/druid/worker/v1/disable`|middleManager|
|`/druid/worker/v1/enable`|middleManager|

"security" resource name covers the following endpoint:

|Endpoint|Node Type|
|--------|---------|
|`/druid-ext/basic-security/authentication`|coordinator|
|`/druid-ext/basic-security/authorization`|coordinator|

### STATE
There is only one possible resource name for the "STATE" config resource type, "STATE". Granting a user access to STATE resources allows them to access the following endpoints.

"STATE" resource name covers the following endpoints:

|Endpoint|Node Type|
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

For information on what HTTP methods are supported on a particular request endpoint, please refer to the [API documentation](../../operations/api-reference.html).

GET requires READ permission, while POST and DELETE require WRITE permission.

## Configuration Propagation

To prevent excessive load on the Coordinator, the Authenticator and Authorizer user/role database state is cached on each Druid node.

Each node will periodically poll the Coordinator for the latest database state, controlled by the `druid.auth.basic.common.pollingPeriod` and `druid.auth.basic.common.maxRandomDelay` properties.

When a configuration update occurs, the Coordinator can optionally notify each node with the updated database state. This behavior is controlled by the `enableCacheNotifications` and `cacheNotificationTimeout` properties on Authenticators and Authorizers.

Note that because of the caching, changes made to the user/role database may not be immediately reflected at each Druid node.
