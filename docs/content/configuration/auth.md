---
layout: doc_page
---

# Authentication and Authorization

|Property|Type|Description|Default|Required|
|--------|-----------|--------|--------|--------|
|`druid.auth.authenticationChain`|JSON List of Strings|List of Authenticator type names|["allowAll"]|no|
|`druid.escalator.type`|String|Type of the Escalator that should be used for internal Druid communications. This Escalator must use an authentication scheme that is supported by an Authenticator in `druid.auth.authenticationChain`.|"noop"|no|
|`druid.auth.authorizers`|JSON List of Strings|List of Authorizer type names |["allowAll"]|no|
|`druid.auth.unsecuredPaths`| List of Strings|List of paths for which security checks will not be performed. All requests to these paths will be allowed.|[]|no|
|`druid.auth.allowUnauthenticatedHttpOptions`|Boolean|If true, skip authentication checks for HTTP OPTIONS requests. This is needed for certain use cases, such as supporting CORS pre-flight requests. Note that disabling authentication checks for OPTIONS requests will allow unauthenticated users to determine what Druid endpoints are valid (by checking if the OPTIONS request returns a 200 instead of 404), so enabling this option may reveal information about server configuration, including information about what extensions are loaded (if those extensions add endpoints).|false|no|

## Enabling Authentication/Authorization

## Authentication Chain
Authentication decisions are handled by a chain of Authenticator instances. A request will be checked by Authenticators in the sequence defined by the `druid.auth.authenticationChain`.

Authenticator implementions are provided by extensions.

For example, the following authentication chain definition enables the Kerberos and HTTP Basic authenticators, from the `druid-kerberos` and `druid-basic-security` core extensions, respectively:

```
druid.auth.authenticationChain=["kerberos", "basic"]
```

A request will pass through all Authenticators in the chain, until one of the Authenticators successfully authenticates the request or sends an HTTP error response. Authenticators later in the chain will be skipped after the first successful authentication or if the request is terminated with an error response.

If no Authenticator in the chain successfully authenticated a request or sent an HTTP error response, an HTTP error response will be sent at the end of the chain.

Druid includes a built-in Authenticator, used for the default unsecured configuration.

### AllowAll Authenticator

This built-in Authenticator authenticates all requests, and always directs them to an Authorizer named "allowAll". It is not intended to be used for anything other than the default unsecured configuration.

## Escalator
The `druid.escalator.type` property determines what authentication scheme should be used for internal Druid cluster communications (such as when a broker node communicates with historical nodes for query processing).

The Escalator chosen for this property must use an authentication scheme that is supported by an Authenticator in `druid.auth.authenticationChain`. Authenticator extension implementors must also provide a corresponding Escalator implementation if they intend to use a particular authentication scheme for internal Druid communications.

### Noop Escalator

This built-in default Escalator is intended for use only with the default AllowAll Authenticator and Authorizer.

## Authorizers
Authorization decisions are handled by an Authorizer. The `druid.auth.authorizers` property determines what Authorizer implementations will be active.

There are two built-in Authorizers, "default" and "noop". Other implementations are provided by extensions.

For example, the following authorizers definition enables the "basic" implementation from `druid-basic-security`:

```
druid.auth.authorizers=["basic"]
```


Only a single Authorizer will authorize any given request.

Druid includes one built in authorizer:

### AllowAll Authorizer
The Authorizer with type name "allowAll" accepts all requests.

## Default Unsecured Configuration

When `druid.auth.authenticationChain` is left empty or unspecified, Druid will create an authentication chain with a single AllowAll Authenticator named "allowAll".

When `druid.auth.authorizers` is left empty or unspecified, Druid will create a single AllowAll Authorizer named "allowAll".

The default value of `druid.escalator.type` is "noop" to match the default unsecured Authenticator/Authorizer configurations.

## Authenticator to Authorizer Routing

When an Authenticator successfully authenticates a request, it must attach a AuthenticationResult to the request, containing an information about the identity of the requester, as well as the name of the Authorizer that should authorize the authenticated request.

An Authenticator implementation should provide some means through configuration to allow users to select what Authorizer(s) the Authenticator should route requests to.

## Internal System User

Internal requests between Druid nodes (non-user initiated communications) need to have authentication credentials attached. 

These requests should be run as an "internal system user", an identity that represents the Druid cluster itself, with full access permissions.

The details of how the internal system user is defined is left to extension implementations.

### Authorizer Internal System User Handling

Authorizers implementations must recognize and authorize an identity for the "internal system user", with full access permissions.

### Authenticator and Escalator Internal System User Handling

An Authenticator implementation that is intended to support internal Druid communications must recognize credentials for the "internal system user", as provided by a corresponding Escalator implementation.

An Escalator must implement three methods related to the internal system user:

```java
  public HttpClient createEscalatedClient(HttpClient baseClient);

  public org.eclipse.jetty.client.HttpClient createEscalatedJettyClient(org.eclipse.jetty.client.HttpClient baseClient);

  public AuthenticationResult createEscalatedAuthenticationResult();
```

`createEscalatedClient` returns an wrapped HttpClient that attaches the credentials of the "internal system user" to requests.

`createEscalatedJettyClient` is similar to `createEscalatedClient`, except that it operates on a Jetty HttpClient.

`createEscalatedAuthenticationResult` returns an AuthenticationResult containing the identity of the "internal system user".

## Reserved Name Configuration Property

For extension implementers, please note that the following configuration properties are reserved for the names of Authenticators and Authorizers:

```
druid.auth.authenticator.<authenticator-name>.name=<authenticator-name>
druid.auth.authorizer.<authorizer-name>.name=<authorizer-name>

```

These properties provide the authenticator and authorizer names to the implementations as @JsonProperty parameters, potentially useful when multiple authenticators or authorizers of the same type are configured.
