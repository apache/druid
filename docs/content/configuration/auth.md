---
layout: doc_page
---

# Authentication and Authorization

|Property|Type|Description|Default|Required|
|--------|-----------|--------|--------|--------|
|`druid.auth.authenticationChain`|JSON List of Strings|List of Authenticator type names|[]|no|
|`druid.auth.escalatedAuthenticator`|String|Type of the Authenticator that should be used for internal Druid communications|"allowAll"|no|
|`druid.auth.authorizers`|JSON List of Strings|List of Authorizer type names |[]|no|

## Enabling Authentication/Authorization

## Authentication Chain
Authentication decisions are handled by a chain of Authenticator instances. A request will be checked by Authenticators in the sequence defined by the `druid.auth.authenticationChain`.

Authenticator implementions are provided by extensions.

For example, the following authentication chain definition enables the Kerberos and HTTP Basic authenticators, from the `druid-kerberos` and `druid-basic-security` core extensions, respectively:

```
druid.auth.authenticationChain=["kerberos", "basic"]
```

A request will pass through all Authenticators in the chain, unless one of the Authenticators sends an HTTP error response. If no Authenticator in the chain successfully authenticated a request, an HTTP error response will be sent.

Druid includes a built-in Authenticator, used for the default unsecured configuration.

### AllowAll Authenticator

This built-in Authenticator authenticates all requests, and always directs them to an Authorizer named "allowAll". It is not intended to be used for anything other than the default unsecured configuration.

## Internal Authenticator
The `druid.auth.escalatedAuthenticator` property determines what authentication scheme should be used for internal Druid cluster communications (such as when a broker node communicates with historical nodes for query processing).

The Authenticator chosen for this property must also be present in `druid.auth.authenticationChain`.

## Authorizers
Authorization decisions are handled by an Authorizer. The `druid.auth.authorizers` property determines what Authorizer implementations will be active.

There are two built-in Authorizers, "default" and "noop". Other implementations are provided by extensions.

For example, the following authorizers definition enables the "basic" implementation from `druid-basic-security`:

```
druid.auth.authorizers=["basic"]
```


Only a single Authorizer will authorize any given request.

Druid includes two built in authorizers:

### DenyAll Authorizer
The Authorizer with type name "denyAll" rejects all requests.

### AllowAll Authorizer
The Authorizer with type name "allowAll" accepts all requests.

## Default Unsecured Configuration

When `druid.auth.authenticationChain` is left empty or unspecified, Druid will create an authentication chain with a single AllowAll Authenticator named "allowAll".

When `druid.auth.authorizers` is left empty or unspecified, Druid will create a single AllowAll Authorizer named "allowAll".

The default value of `druid.auth.escalatedAuthenticator` is "allowAll" to match the default unsecured Authenticator/Authorizer configurations.

## Authenticator to Authorizer Routing

When an Authenticator successfully authenticates a request, it must attach a AuthenticationResult to the request, containing an information about the identity of the requester, as well as the name of the Authorizer that should authorize the authenticated request.

An Authenticator implementation should provide some means through configuration to allow users to select what Authorizer(s) the Authenticator should route requests to.

## Internal System User

Internal requests between Druid nodes (non-user initiated communications) need to have authentication credentials attached. These requests should be run as an "internal system user".

We recommend that extension implementers follow the guidelines below regarding the "internal system user", for maximum compatibility between different Authenticator and Authorizer implementations.

### Authorizer Internal System User Handling

Authorizers implementations must recognize and authorize an identity for the "internal system user", with unrestricted permissions.

We recommend that this "internal system user" be represented by the identity string "__DRUID_INTERNAL_SYSTEM". This is a guideline only and not enforced; if an Authorizer needs to use a different identity string format, it is free to do so.

Allowing the user to redefine what identity string represents the internal system user is also recommended.

### Authenticator Internal System User Handling

Authenticators must implement three methods related to the internal system user:

```java
  public HttpClient createEscalatedClient(HttpClient baseClient);

  public org.eclipse.jetty.client.HttpClient createEscalatedJettyClient(org.eclipse.jetty.client.HttpClient baseClient);

  public AuthenticationResult createEscalatedAuthenticationResult();
```

`createEscalatedClient` returns an wrapped HttpClient that attaches the credentials of the "internal system user" to requests.

`createEscalatedJettyClient` is similar to `createEscalatedClient`, except that it operates on a Jetty HttpClient.

`createEscalatedAuthenticationResult` returns an AuthenticationResult containing the identity of the "internal system user".

As with Authenticators, we recommend that the "internal system user" be represented by default with the identity string "__DRUID_INTERNAL_SYSTEM". This is a guideline and not enforced.

We also recommend that Authenticator implementations allow the user to redefine the identity string used for the internal system users, if feasible.


