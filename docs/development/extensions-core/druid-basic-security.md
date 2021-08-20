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


The Basic Security extension for Apache Druid adds:

- an Authenticator which supports [HTTP Basic authentication](https://en.wikipedia.org/wiki/Basic_access_authentication) using the Druid metadata store or LDAP as its credentials store.
- an Authorizer which implements basic role-based access control for Druid metadata store or LDAP users and groups.

To load the extension, [include](../../development/extensions.md#loading-extensions) `druid-basic-security` in the `druid.extensions.loadList` in your `common.runtime.properties`. For example:
```
druid.extensions.loadList=["postgresql-metadata-storage", "druid-hdfs-storage", "druid-basic-security"]
```

See [Authentication and Authorization](../../design/auth.md) for more information on the implemented extension interfaces.

## Configuration

The examples in the section use the following names for the Authenticators and Authorizers:
- `MyBasicMetadataAuthenticator`
- `MyBasicLDAPAuthenticator`
- `MyBasicMetadataAuthorizer`
- `MyBasicLDAPAuthorizer`.

These properties are not tied to specific Authenticator or Authorizer instances.

To set the value for the configuration properties, add them to the common runtime properties file.

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
The default credentials validator (`credentialsValidator`) is `metadata`. To use the LDAP validator, define a credentials validator with a type of 'ldap'.


Configuration of the named authenticator is assigned through properties with the form:

```
druid.auth.authenticator.<authenticatorName>.<authenticatorProperty>
```

The remaining examples of authenticator configuration use either `MyBasicMetadataAuthenticator` or `MyBasicLDAPAuthenticator` as the authenticator name.


#### Properties for Druid metadata store user authentication
|Property|Description|Default|required|
|--------|-----------|-------|--------|
|`druid.auth.authenticator.MyBasicMetadataAuthenticator.initialAdminPassword`|Initial [Password Provider](../../operations/password-provider.md) for the automatically created default admin user. If no password is specified, the default admin user will not be created. If the default admin user already exists, setting this property will not affect its password.|null|No|
|`druid.auth.authenticator.MyBasicMetadataAuthenticator.initialInternalClientPassword`|Initial [Password Provider](../../operations/password-provider.md) for the default internal system user, used for internal process communication. If no password is specified, the default internal system user will not be created. If the default internal system user already exists, setting this property will not affect its password.|null|No|
|`druid.auth.authenticator.MyBasicMetadataAuthenticator.enableCacheNotifications`|If true, the Coordinator will notify Druid processes whenever a configuration change to this Authenticator occurs, allowing them to immediately update their state without waiting for polling.|true|No|
|`druid.auth.authenticator.MyBasicMetadataAuthenticator.cacheNotificationTimeout`|The timeout in milliseconds for the cache notifications.|5000|No|
|`druid.auth.authenticator.MyBasicMetadataAuthenticator.credentialIterations`|Number of iterations to use for password hashing. See [Credential iterations and API performance](#credential-iterations-and-api-performance)|10000|No|
|`druid.auth.authenticator.MyBasicMetadataAuthenticator.credentialsValidator.type`|The type of credentials store (metadata) to validate requests credentials.|metadata|No|
|`druid.auth.authenticator.MyBasicMetadataAuthenticator.skipOnFailure`|If true and the request credential doesn't exists or isn't fully configured in the credentials store, the request will proceed to next Authenticator in the chain.|false|No|
|`druid.auth.authenticator.MyBasicMetadataAuthenticator.authorizerName`|Authorizer that requests should be directed to|N/A|Yes|

##### Credential iterations and API performance

As noted above, `credentialIterations` determines the number of iterations used to hash a password. A higher number increases security, but costs more in terms of CPU utilization. 

This cost affects API performance, including query times. The default setting of 10000 is intentionally high to prevent attackers from using brute force to guess passwords.

You can decrease the number of iterations to speed up API response times, but it may expose your system to dictionary attacks. Therefore, only reduce the number of iterations if your environment fits one of the following conditions:
- **All** passwords are long and random which make them as safe as a randomly-generated token.
- You have secured network access to Druid so that no attacker can execute a dictionary attack against it.

If Druid uses the default credentials validator (i.e., `credentialsValidator.type=metadata`), changing the `credentialIterations` value affects the number of hashing iterations only for users created after the change or for users who subsequently update their passwords via the `/druid-ext/basic-security/authentication/db/basic/users/{userName}/credentials` endpoint. If Druid uses the `ldap` validator, the change applies to any user at next log in (as well as to new users or users who update their passwords).

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

Please see [Defining permissions](../../operations/security-user-auth.md#defining-permissions) for more details.

##### Cache Load Status
`GET(/druid-ext/basic-security/authorization/loadStatus)`
Return the current load status of the local caches of the authorization Druid metadata store.
