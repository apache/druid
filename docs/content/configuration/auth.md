---
layout: doc_page
---

# Authentication and Authorization

|Property|Type|Description|Default|Required|
|--------|-----------|--------|--------|--------|
|`druid.auth.enabled`|boolean|Determines if authentication and authorization checks will be performed on requests.|false|no|
|`druid.auth.authenticationChain`|JSON List of Strings|List of Authenticator type names|null|yes, if auth enabled|
|`druid.auth.internalAuthenticator`|String|Type of the Authenticator that should be used for internal Druid communications|null|yes, if auth enabled|
|`druid.auth.authorizationManagers`|JSON List of Strings|List of AuthorizationManager type names |null|yes, if auth enabled|

## Enabling Authentication/Authorization

## Authentication Chain
Authentication decisions are handled by a chain of Authenticator instances. A request will be checked by Authenticators in the sequence defined by the `druid.auth.authenticationChain` file.

Authenticator implementions are provided by extensions.

For example, the following authentication chain definition enables the Kerberos and HTTP Basic authenticators, from the `druid-kerberos` and `druid-basic-security` core extensions, respectively:

```json
["kerberos", "basic"]
```

## Internal Authenticator
The `druid.auth.internalAuthenticator` property determines what authentication scheme should be used for internal Druid cluster communications (such as when a broker node communicates with historical nodes for query processing).

The Authenticator chosen for this property must also be present in `druid.auth.authenticationChain`.

## Authorization Managers
Authorization decisions are handled by an AuthorizationManager. The `druid.auth.authorizationManagers` property determines what AuthorizationManager implementations will be active.

There are two built-in AuthorizationManagers, "default" and "noop". Other implementations are provided by extensions.

For example, the following authorization managers definition enables the "basic" implementation from `druid-basic-security`:

```json
["basic"]
```

### Default Authorization Manager
The default AuthorizationManager with type name "default" rejects all requests.

### No-op Authorization Manager
The no-op AuthorizationManager with type name "noop" accepts all requests.


## Namespaces
Authenticator and AuthorizationManager implementations are linked through a namespace string. Authenticators tag an authenticated request with a namespace, which is used to route the authenticated request to the AuthorizationManager implementation that registered itself with a matching namespace.

This is to support cases where an AuthorizationManager implementation is only intended to authorize requests from a specific authenticator (an implementation may have assumptions about the user name format, for example).

The details of namespace configuration are left for implementors of Authenticator and AuthorizationManager to decide.