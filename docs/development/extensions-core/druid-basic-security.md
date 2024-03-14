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
- an Escalator which determines the authentication scheme for internal Druid processes.
- an Authorizer which implements basic role-based access control for Druid metadata store or LDAP users and groups.

To load the extension, [include](../../configuration/extensions.md#loading-extensions) `druid-basic-security` in the `druid.extensions.loadList` in your `common.runtime.properties`. For example:
```
druid.extensions.loadList=["postgresql-metadata-storage", "druid-hdfs-storage", "druid-basic-security"]
```

To enable basic auth, configure the basic Authenticator, Escalator, and Authorizer in `common.runtime.properties`.
See [Security overview](../../operations/security-overview.md#enable-an-authenticator) for an example configuration for HTTP basic authentication.

Visit [Authentication and Authorization](../../operations/auth.md) for more information on the implemented extension interfaces and for an example configuration.

## Configuration

The examples in the section use the following names for the Authenticators and Authorizers:
- `MyBasicMetadataAuthenticator`
- `MyBasicLDAPAuthenticator`
- `MyBasicMetadataAuthorizer`
- `MyBasicLDAPAuthorizer`

These properties are not tied to specific Authenticator or Authorizer instances.

To set the value for the configuration properties, add them to the common runtime properties file.

### General properties

**`druid.auth.basic.common.pollingPeriod`**

Defines in milliseconds how often processes should poll the Coordinator for the current Druid metadata store authenticator/authorizer state.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: 60000

**`druid.auth.basic.common.maxRandomDelay`**

Defines in milliseconds the amount of random delay to add to the pollingPeriod, to spread polling requests across time.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: 6000

**`druid.auth.basic.common.maxSyncRetries`**

Determines how many times a service will retry if the authentication/authorization Druid metadata store state sync with the Coordinator fails.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: 10

**`druid.auth.basic.common.cacheDirectory`**

If defined, snapshots of the basic Authenticator and Authorizer Druid metadata store caches will be stored on disk in this directory. If this property is defined, when a service is starting, it will attempt to initialize its caches from these on-disk snapshots, if the service is unable to initialize its state by communicating with the Coordinator.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: null


### Authenticator

To use the Basic authenticator, add an authenticator with type `basic` to the authenticatorChain.
The default credentials validator (`credentialsValidator`) is `metadata`. To use the LDAP validator, define a credentials validator with a type of 'ldap'.


Use the following syntax to configure a named authenticator:

```
druid.auth.authenticator.<authenticatorName>.<authenticatorProperty>
```

Example configuration of an authenticator that uses the Druid metadata store to look up and validate credentials:
```
# Druid basic security
druid.auth.authenticatorChain=["MyBasicMetadataAuthenticator"]
druid.auth.authenticator.MyBasicMetadataAuthenticator.type=basic

# Default password for 'admin' user, should be changed for production.
druid.auth.authenticator.MyBasicMetadataAuthenticator.initialAdminPassword=password1

# Default password for internal 'druid_system' user, should be changed for production.
druid.auth.authenticator.MyBasicMetadataAuthenticator.initialInternalClientPassword=password2

# Uses the metadata store for storing users, you can use authentication API to create new users and grant permissions
druid.auth.authenticator.MyBasicMetadataAuthenticator.credentialsValidator.type=metadata

# If true and the request credential doesn't exists in this credentials store, the request will proceed to next Authenticator in the chain.
druid.auth.authenticator.MyBasicMetadataAuthenticator.skipOnFailure=false
druid.auth.authenticator.MyBasicMetadataAuthenticator.authorizerName=MyBasicMetadataAuthorizer
```
The remaining examples of authenticator configuration use either `MyBasicMetadataAuthenticator` or `MyBasicLDAPAuthenticator` as the authenticator name.


#### Properties for Druid metadata store user authentication

**`druid.auth.authenticator.MyBasicMetadataAuthenticator.initialAdminPassword`**

Initial [Password Provider](../../operations/password-provider.md) for the automatically created default admin user. If no password is specified, the default admin user will not be created. If the default admin user already exists, setting this property will not affect its password.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: null

**`druid.auth.authenticator.MyBasicMetadataAuthenticator.initialInternalClientPassword`**

Initial [Password Provider](../../operations/password-provider.md) for the default internal system user, used for internal process communication. If no password is specified, the default internal system user will not be created. If the default internal system user already exists, setting this property will not affect its password.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: null

**`druid.auth.authenticator.MyBasicMetadataAuthenticator.enableCacheNotifications`**

If true, the Coordinator will notify Druid processes whenever a configuration change to this Authenticator occurs, allowing them to immediately update their state without waiting for polling.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: True

**`druid.auth.authenticator.MyBasicMetadataAuthenticator.cacheNotificationTimeout`**

The timeout in milliseconds for the cache notifications.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: 5000

**`druid.auth.authenticator.MyBasicMetadataAuthenticator.credentialIterations`**

Number of iterations to use for password hashing. See [Credential iterations and API performance](#credential-iterations-and-api-performance)<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: 10000

**`druid.auth.authenticator.MyBasicMetadataAuthenticator.credentialsValidator.type`**

The type of credentials store (metadata) to validate requests credentials.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: metadata

**`druid.auth.authenticator.MyBasicMetadataAuthenticator.skipOnFailure`**

If true and the request credential doesn't exists or isn't fully configured in the credentials store, the request will proceed to next Authenticator in the chain.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: false

**`druid.auth.authenticator.MyBasicMetadataAuthenticator.authorizerName`**

Authorizer that requests should be directed to.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: Yes<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: N/A


##### Credential iterations and API performance

As noted above, the value of `credentialIterations` determines the number of iterations used to hash a password. A higher number of iterations increases security. The default value of 10,000 is intentionally high to prevent attackers from using brute force to guess passwords. We recommend that you don't lower this value. Druid caches the hash of up to 1000 passwords used in the last hour to ensure that having a large number of iterations does not meaningfully impact query performance. 

If Druid uses the default credentials validator (i.e., `credentialsValidator.type=metadata`), changing the `credentialIterations` value affects the number of hashing iterations only for users created after the change or for users who subsequently update their passwords via the `/druid-ext/basic-security/authentication/db/basic/users/{userName}/credentials` endpoint. If Druid uses the `ldap` validator, the change applies to any user at next log in (as well as to new users or users who update their passwords).

#### Properties for LDAP user authentication

**`druid.auth.authenticator.MyBasicLDAPAuthenticator.initialAdminPassword`**

Initial [Password Provider](../../operations/password-provider.md) for the automatically created default admin user. If no password is specified, the default admin user will not be created. If the default admin user already exists, setting this property will not affect its password.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: null

**`druid.auth.authenticator.MyBasicLDAPAuthenticator.initialInternalClientPassword`**

Initial [Password Provider](../../operations/password-provider.md) for the default internal system user, used for internal process communication. If no password is specified, the default internal system user will not be created. If the default internal system user already exists, setting this property will not affect its password.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: null

**`druid.auth.authenticator.MyBasicLDAPAuthenticator.enableCacheNotifications`**

If true, the Coordinator will notify Druid processes whenever a configuration change to this Authenticator occurs, allowing them to immediately update their state without waiting for polling.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: true

**`druid.auth.authenticator.MyBasicLDAPAuthenticator.cacheNotificationTimeout`**

The timeout in milliseconds for the cache notifications.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: 5000

**`druid.auth.authenticator.MyBasicLDAPAuthenticator.credentialIterations`**

Number of iterations to use for password hashing.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: 10000

**`druid.auth.authenticator.MyBasicLDAPAuthenticator.credentialsValidator.type`**

The type of credentials store (ldap) to validate requests credentials.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: metadata

**`druid.auth.authenticator.MyBasicLDAPAuthenticator.credentialsValidator.url`**

URL of the LDAP server.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: Yes<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: null

**`druid.auth.authenticator.MyBasicLDAPAuthenticator.credentialsValidator.bindUser`**

LDAP bind user username.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: Yes<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: null

**`druid.auth.authenticator.MyBasicLDAPAuthenticator.credentialsValidator.bindPassword`**

[Password Provider](../../operations/password-provider.md) LDAP bind user password.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: Yes<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: null

**`druid.auth.authenticator.MyBasicLDAPAuthenticator.credentialsValidator.baseDn`**

The point from where the LDAP server will search for users.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: Yes<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: null

**`druid.auth.authenticator.MyBasicLDAPAuthenticator.credentialsValidator.userSearch`**

The filter/expression to use for the search. For example, (&(sAMAccountName=%s)(objectClass=user))<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: Yes<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: null

**`druid.auth.authenticator.MyBasicLDAPAuthenticator.credentialsValidator.userAttribute`**

The attribute id identifying the attribute that will be returned as part of the search. For example, sAMAccountName.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: Yes<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: null

**`druid.auth.authenticator.MyBasicLDAPAuthenticator.credentialsValidator.credentialVerifyDuration`**

The duration in seconds for how long valid credentials are verifiable within the cache when not requested.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: 600

**`druid.auth.authenticator.MyBasicLDAPAuthenticator.credentialsValidator.credentialMaxDuration`**

The max duration in seconds for valid credentials that can reside in cache regardless of how often they are requested.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: 3600

**`druid.auth.authenticator.MyBasicLDAPAuthenticator.credentialsValidator.credentialCacheSize`**

The valid credentials cache size. The cache uses a LRU policy.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: 100

**`druid.auth.authenticator.MyBasicLDAPAuthenticator.skipOnFailure`**

If true and the request credential doesn't exists or isn't fully configured in the credentials store, the request will proceed to next Authenticator in the chain.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: false

**`druid.auth.authenticator.MyBasicLDAPAuthenticator.authorizerName`**

Authorizer that requests should be directed to.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: Yes<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: N/A

### Escalator

The Escalator determines the authentication scheme to use for internal Druid cluster communications, for example, when a Broker service communicates with a Historical service during query processing.

Example configuration:
```
# Escalator
druid.escalator.type=basic
druid.escalator.internalClientUsername=druid_system
druid.escalator.internalClientPassword=password2
druid.escalator.authorizerName=MyBasicMetadataAuthorizer
```

#### Properties

**`druid.escalator.internalClientUsername`**

The escalator will use this username for requests made as the internal system user.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: Yes<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: N/A

**`druid.escalator.internalClientPassword`**

The escalator will use this [Password Provider](../../operations/password-provider.md) for requests made as the internal system user.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: Yes<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: N/A

**`druid.escalator.authorizerName`**

Authorizer that requests should be directed to.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: Yes<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: N/A


### Authorizer

To use the Basic authorizer, add an authorizer with type `basic` to the authorizers list.

Use the following syntax to configure a named authorizer:

```
druid.auth.authorizer.<authorizerName>.<authorizerProperty>
```

Example configuration:
```
# Authorizer
druid.auth.authorizers=["MyBasicMetadataAuthorizer"]
druid.auth.authorizer.MyBasicMetadataAuthorizer.type=basic
```

The examples in the rest of this article use `MyBasicMetadataAuthorizer` or `MyBasicLDAPAuthorizer` as the authorizer name.

#### Properties for Druid metadata store user authorization

**`druid.auth.authorizer.MyBasicMetadataAuthorizer.enableCacheNotifications`**

If true, the Coordinator will notify Druid processes whenever a configuration change to this Authorizer occurs, allowing them to immediately update their state without waiting for polling.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: true

**`druid.auth.authorizer.MyBasicMetadataAuthorizer.cacheNotificationTimeout`**

The timeout in milliseconds for the cache notifications.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: 5000

**`druid.auth.authorizer.MyBasicMetadataAuthorizer.initialAdminUser`**

The initial admin user with role defined in initialAdminRole property if specified, otherwise the default admin role will be assigned.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: admin

**`druid.auth.authorizer.MyBasicMetadataAuthorizer.initialAdminRole`**

The initial admin role to create if it doesn't already exists.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: admin

**`druid.auth.authorizer.MyBasicMetadataAuthorizer.roleProvider.type`**

The type of role provider to authorize requests credentials.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: metadata

#### Properties for LDAP user authorization

**`druid.auth.authorizer.MyBasicLDAPAuthorizer.enableCacheNotifications`**

If true, the Coordinator will notify Druid processes whenever a configuration change to this Authorizer occurs, allowing them to immediately update their state without waiting for polling.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: true

**`druid.auth.authorizer.MyBasicLDAPAuthorizer.cacheNotificationTimeout`**

The timeout in milliseconds for the cache notifications.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: 5000

**`druid.auth.authorizer.MyBasicLDAPAuthorizer.initialAdminUser`**

The initial admin user with role defined in initialAdminRole property if specified, otherwise the default admin role will be assigned.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: admin

**`druid.auth.authorizer.MyBasicLDAPAuthorizer.initialAdminRole`**

The initial admin role to create if it doesn't already exists.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: admin

**`druid.auth.authorizer.MyBasicLDAPAuthorizer.initialAdminGroupMapping`**

The initial admin group mapping with role defined in initialAdminRole property if specified, otherwise the default admin role will be assigned. The name of this initial admin group mapping will be set to adminGroupMapping<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: null

**`druid.auth.authorizer.MyBasicLDAPAuthorizer.roleProvider.type`**

The type of role provider (ldap) to authorize requests credentials.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: metadata

**`druid.auth.authorizer.MyBasicLDAPAuthorizer.roleProvider.groupFilters`**

Array of LDAP group filters used to filter out the allowed set of groups returned from LDAP search. Filters can be begin with *, or end with ,* to provide configurational flexibility to limit or filter allowed set of groups available to LDAP Authorizer.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: null

#### Properties for LDAPS

Use the following properties to configure Druid authentication with LDAP over TLS (LDAPS). See [Configure LDAP authentication](../../operations/auth-ldap.md) for more information.

**`druid.auth.basic.ssl.protocol`**

SSL protocol to use. The TLS version is 1.2.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: Yes<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: tls

**`druid.auth.basic.ssl.trustStorePath`**

Path to the trust store file.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: Yes<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: N/A

**`druid.auth.basic.ssl.trustStorePassword`**

Password to access the trust store file.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: Yes<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: N/A

**`druid.auth.basic.ssl.trustStoreType`**

Format of the trust store file. For Java the format is jks.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: jks

**`druid.auth.basic.ssl.trustStoreAlgorithm`**

Algorithm used by the trust manager to validate certificate chains.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: N/A

**`druid.auth.basic.ssl.trustStorePassword`**

Password details that enable access to the truststore.<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Required**: No<br />
&nbsp; &nbsp; &nbsp; &nbsp; &nbsp;**Default**: N/A

Example LDAPS configuration:

```json
druid.auth.basic.ssl.protocol=tls
druid.auth.basic.ssl.trustStorePath=/usr/local/druid-path/certs/truststore.jks
druid.auth.basic.ssl.trustStorePassword=xxxxx
druid.auth.basic.ssl.trustStoreType=jks
druid.auth.basic.ssl.trustStoreAlgorithm=PKIX
```
You can configure `druid.auth.basic.ssl.trustStorePassword` to be a plain text password or you can set the password as an environment variable. See [Password providers](../../operations/password-provider.md) for more information.

## Usage

### Coordinator Security API
To use these APIs, a user needs read/write permissions for the CONFIG resource type with name "security".

#### Authentication API

Root path: `/druid-ext/basic-security/authentication`

Each API endpoint includes {authenticatorName}, specifying which Authenticator instance is being configured.

##### User/Credential Management
`GET(/druid-ext/basic-security/authentication/db/{authenticatorName}/users)`<br />
Return a list of all user names.

`GET(/druid-ext/basic-security/authentication/db/{authenticatorName}/users/{userName})`<br />
Return the name and credentials information of the user with name {userName}

`POST(/druid-ext/basic-security/authentication/db/{authenticatorName}/users/{userName})`<br />
Create a new user with name {userName}

`DELETE(/druid-ext/basic-security/authentication/db/{authenticatorName}/users/{userName})`<br />
Delete the user with name {userName}

`POST(/druid-ext/basic-security/authentication/db/{authenticatorName}/users/{userName}/credentials)`<br />
Assign a password used for HTTP basic authentication for {userName}
Content: JSON password request object

Example request body:

```
{
  "password": "helloworld"
}
```

##### Cache Load Status
`GET(/druid-ext/basic-security/authentication/loadStatus)`<br />
Return the current load status of the local caches of the authentication Druid metadata store.

#### Authorization API

Root path: `/druid-ext/basic-security/authorization`<br />

Each API endpoint includes {authorizerName}, specifying which Authorizer instance is being configured.

##### User Creation/Deletion
`GET(/druid-ext/basic-security/authorization/db/{authorizerName}/users)`<br />
Return a list of all user names.

`GET(/druid-ext/basic-security/authorization/db/{authorizerName}/users/{userName})`<br />
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

`POST(/druid-ext/basic-security/authorization/db/{authorizerName}/users/{userName})`<br />
Create a new user with name {userName}

`DELETE(/druid-ext/basic-security/authorization/db/{authorizerName}/users/{userName})`<br />
Delete the user with name {userName}

##### Group mapping Creation/Deletion
`GET(/druid-ext/basic-security/authorization/db/{authorizerName}/groupMappings)`<br />
Return a list of all group mappings.

`GET(/druid-ext/basic-security/authorization/db/{authorizerName}/groupMappings/{groupMappingName})`<br />
Return the group mapping and role information of the group mapping with name {groupMappingName}

`POST(/druid-ext/basic-security/authorization/db/{authorizerName}/groupMappings/{groupMappingName})`<br />
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

`DELETE(/druid-ext/basic-security/authorization/db/{authorizerName}/groupMappings/{groupMappingName})`<br />
Delete the group mapping with name {groupMappingName}

#### Role Creation/Deletion
`GET(/druid-ext/basic-security/authorization/db/{authorizerName}/roles)`<br />
Return a list of all role names.

`GET(/druid-ext/basic-security/authorization/db/{authorizerName}/roles/{roleName})`<br />
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


`POST(/druid-ext/basic-security/authorization/db/{authorizerName}/roles/{roleName})`<br />
Create a new role with name {roleName}.
Content: username string

`DELETE(/druid-ext/basic-security/authorization/db/{authorizerName}/roles/{roleName})`<br />
Delete the role with name {roleName}.


#### Role Assignment
`POST(/druid-ext/basic-security/authorization/db/{authorizerName}/users/{userName}/roles/{roleName})`<br />
Assign role {roleName} to user {userName}.

`DELETE(/druid-ext/basic-security/authorization/db/{authorizerName}/users/{userName}/roles/{roleName})`<br />
Unassign role {roleName} from user {userName}

`POST(/druid-ext/basic-security/authorization/db/{authorizerName}/groupMappings/{groupMappingName}/roles/{roleName})`<br />
Assign role {roleName} to group mapping {groupMappingName}.

`DELETE(/druid-ext/basic-security/authorization/db/{authorizerName}/groupMappings/{groupMappingName}/roles/{roleName})`<br />
Unassign role {roleName} from group mapping {groupMappingName}


#### Permissions
`POST(/druid-ext/basic-security/authorization/db/{authorizerName}/roles/{roleName}/permissions)`<br />
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
`GET(/druid-ext/basic-security/authorization/loadStatus)`<br />
Return the current load status of the local caches of the authorization Druid metadata store.
