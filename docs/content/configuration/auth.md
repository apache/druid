---
layout: doc_page
---

# Authentication and Authorization

|Property|Type|Description|Default|Required|
|--------|-----------|-------|
|`druid.auth.enabled`|boolean|Determines if authentication and authorization checks will be performed on requests.|false|no|
|`druid.auth.authenticationChainPath`|String|Path to a file containing a JSON list of Authenticator objects|null|yes|
|`druid.auth.internalAuthenticator`|String|Type of the Authenticator that should be used for internal Druid communications|null|yes|
|`druid.auth.authorizationManager`|String|Type of the AuthorizationManager to be used for authorization checks.|"default"|no|

## Enabling Authentication/Authorization

## Authentication Chain
Authentication decisions are handled by a chain of Authenticator instances. A request will be checked by Authenticators in the sequence defined by the `druid.auth.authenticationChainPath` file.

Authenticator implementions are provided by extensions.

For example, the following authentication chain definition enables the Kerberos and HTTP Basic authenticators, from the `druid-kerberos` and `druid-basic-security` core extensions, respectively:

```json
[
{
  "type": "kerberos"
},
{
  "type": "basic"
}
]
```

## Internal Authenticator
The `druid.auth.internalAuthenticator` property determines what authentication scheme should be used for internal Druid cluster communications (such as when a broker node communicates with historical nodes for query processing).

The Authenticator chosen for this property must also be present in the Authentication Chain.

## Authorization Manager
Authorization decisions are handled by an AuthorizationManager. The `druid.auth.authorizationManager` property determines what type of AuthorizationManager will be used. 

There are two built-in AuthorizationManagers, "default" and "noop". Other implementations are provided by extensions.

### Default Authorization Manager
The default AuthorizationManager with type name "default" rejects all requests.

### No-op Authorization Manager
The no-op AuthorizationManager with type name "noop" accepts all requests.
