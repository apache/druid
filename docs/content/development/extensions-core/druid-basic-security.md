---
layout: doc_page
---

# Druid Basic Security

This extension adds:
- an Authenticator which supports [HTTP Basic authentication](https://en.wikipedia.org/wiki/Basic_access_authentication)
- an Authorizer which implements basic role-based access control

Make sure to [include](../../operations/including-extensions.html) `druid-basic-security` as an extension.


## Configuration


### Properties
|Property|Description|Default|required|
|--------|-----------|-------|--------|
|`druid.auth.basic.initialAdminPassword`|Password to assign when Druid automatically creates the default admin account. See [Default user accounts](#default-user-accounts) for more information.|"druid"|No|
|`druid.auth.basic.initialInternalClientPassword`|Password to assign when Druid automatically creates the default admin account. See [Default user accounts](#default-user-accounts) for more information.|"druid"|No|
|`druid.auth.basic.permissionCacheSize`|Resource names are used as regexes in permissions. Compiled regex Pattern objects are cached by the Basic authorizer. This property controls how many cached Pattern objects are stored.|5000|No|

### Creating an Authenticator
```
druid.auth.authenticatorChain=["MyBasicAuthenticator"]

druid.auth.authenticator.MyBasicAuthenticator.type=basic
```

To use the Basic authenticator, add an authenticator with type `basic` to the authenticatorChain. The example above uses the name "MyBasicAuthenticator" for the Authenticator.

Configuration of the named authenticator is assigned through properties with the form:

```
druid.auth.authenticator.<authenticatorName>.<authenticatorProperty>
```

The configuration examples in the rest of this document will use "MyBasicAuthenticator" as the name of the authenticator being configured.

Only one instance of a "basic" type authenticator should be created and used, multiple "basic" authenticator instances are not supported.

#### Properties
|Property|Description|Default|required|
|--------|-----------|-------|--------|
|`druid.auth.authenticator.MyBasicAuthenticator.internalClientUsername`| Username for the internal system user, used for internal node communication|N/A|Yes|
|`druid.auth.authenticator.MyBasicAuthenticator.internalClientPassword`| Password for the internal system user, used for internal node communication|N/A|Yes|
|`druid.auth.authenticator.MyBasicAuthenticator.authorizerName`|Authorizer that requests should be directed to|N/A|Yes|

### Creating an Authorizer
```
druid.auth.authorizers=["MyBasicAuthorizer"]

druid.auth.authorizer.MyBasicAuthorizer.type=basic
```

To use the Basic authorizer, add an authenticator with type `basic` to the authorizers list. The example above uses the name "MyBasicAuthorizer" for the Authorizer.

Configuration of the named authenticator is assigned through properties with the form:

```
druid.auth.authorizer.<authorizerName>.<authorizerProperty>
```

The Basic authorizer has no additional configuration properties at this time.

Only one instance of a "basic" type authorizer should be created and used, multiple "basic" authorizer instances are not supported.


## Usage


### Coordinator Security API
To use these APIs, a user needs read/write permissions for the CONFIG resource type with name "security".

Root path: `/druid/coordinator/v1/security`

#### User Management
`GET(/users)`
Return a list of all user names.

`GET(/users/{userName})`
Return the name, roles, permissions of the user named {userName}

`POST(/users/{userName})`
Create a new user with name {userName}

`DELETE(/users/{userName})`
Delete the user with name {userName}


#### User Credentials
`GET(/credentials/{userName})`
Return the salt/hash/iterations info used for HTTP basic authentication for {userName}

`POST(/credentials/{userName})`
Assign a password used for HTTP basic authentication for {userName}
Content: password string


#### Role Creation/Deletion
`GET(/roles)`
Return a list of all role names.

`GET(/roles/{roleName})`
Return name and permissions for the role named {roleName}

`POST(/roles/{roleName})`
Create a new role with name {roleName}.
Content: username string

`DELETE(/roles/{roleName})`
Delete the role with name {roleName}.


#### Role Assignment
`POST(/users/{userName}/roles/{roleName})`
Assign role {roleName} to user {userName}.

`DELETE(/users/{userName}/roles/{roleName})`
Unassign role {roleName} from user {userName}


#### Permissions
`POST(/roles/{roleName}/permissions)`
Create a new permissions and assign them to role named {roleName}.
Content: List of JSON Resource-Action objects, e.g.:
```
[
{ 
  resource": {
    "name": "wiki.*",
    "type": "DATASOURCE"
  },
  "action": "READ"
},
{ 
  resource": {
    "name": "wikiticker",
    "type": "DATASOURCE"
  },
  "action": "WRITE"
}
]
```

The "name" field for resources in the permission definitions are regexes used to match resource names during authorization checks. 

Please see [Defining permissions](#defining-permissions) for more details.

`DELETE(/permissions/{permId})`
Delete the permission with ID {permId}. Permission IDs are available from the output of individual user/role GET endpoints.

## Default user accounts

By default, an administrator account with full privileges is created with credentials `admin/druid`. The password assigned at account creation can be overridden by setting the `druid.auth.basic.initialAdminPassword` property.

A default internal system user account with full privileges, meant for internal communications between Druid services, is also created with credentials `druid_system/druid`. The password assigned at account creation can be overridden by setting the `druid.auth.basic.initialInternalClientPassword` property.

The values for `druid.authenticator.<authenticatorName>.internalClientUsername` and `druid.authenticator.<authenticatorName>.internalClientPassword` must match the credentials of the internal system user account.

Cluster administrators should change the default passwords for these accounts before exposing a cluster to users.

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
|`/druid/coordinator/v1/security`|coordinator|

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
|`/druid/worker/v1//task/{taskid}/log`|middleManager|
|`/druid/historical/v1`|historical|
|`/druid-internal/v1/segments/`|historical|
|`/druid-internal/v1/segments/`|peon|
|`/druid-internal/v1/segments/`|realtime|
|`/status`|all nodes|
