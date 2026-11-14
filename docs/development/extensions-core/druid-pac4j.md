---
id: druid-pac4j
title: "Druid pac4j based Security extension"
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


Apache&circledR; Druid Extension to enable [OpenID Connect](https://openid.net/connect/) (OIDC) based authentication for Druid services, using [pac4j](https://github.com/pac4j/pac4j) as the underlying client library.
It works with any OIDC provider, such as [Okta](https://developer.okta.com/), Microsoft Entra ID, Google, or Keycloak.

The extension provides two authenticators:

- `pac4j` lets a group of users in an existing OIDC provider sign in to the [web console](../../operations/web-console.md) with their browser.
- `jwt` validates [ID tokens](https://openid.net/specs/openid-connect-core-1_0.html#CodeIDToken) sent in the `Authorization` header with the `Bearer ` prefix. It is intended for services that authenticate with the OIDC provider themselves and attach the ID token to every Druid request.

This extension does not support JDBC client authentication.

## How the pac4j authenticator works

Users reach the web console through the [Router](../../design/router.md), which serves the console and proxies its API requests to the other services.

1. When a browser request has no valid session, the `pac4j` authenticator redirects the browser to the OIDC provider's sign-in page.
2. After the user signs in, the provider redirects the browser back to Druid's callback endpoint, `/druid-ext/druid-pac4j/callback`.
   Druid exchanges the authorization code for tokens with the provider and stores the user's session in encrypted cookies.
3. The browser sends these cookies with every later request. The Router forwards them unchanged with the requests it proxies, so any service that runs the `pac4j` authenticator with the same `cookiePassphrase` can identify the user from them.

Because of this, run the `pac4j` authenticator on every service, not only on the Router:

- Services without it can't identify the user behind a proxied console request, so your authorizer can't tell users apart. Anyone who can reach those services directly also bypasses the sign-in.
- The services also send requests to each other. Those requests can't follow a browser redirect, so put an authenticator that handles them, such as `basic` from [druid-basic-security](druid-basic-security.md), before `pac4j` in the authenticator chain, and configure the matching [escalator](../../operations/auth.md#escalator).

The Druid identity of a user signed in through `pac4j` is the `sub` claim of their ID token. Use that value when you define users and permissions in your authorizer.

## Configuration

### Register Druid with your OIDC provider

Create an OAuth client application for Druid in your OIDC provider:

- Use the authorization code flow.
- Set the redirect URI to `https://<ROUTER_HOST>:<ROUTER_PORT>/druid-ext/druid-pac4j/callback`, using the address your users enter in their browser.
- Use a confidential client with a client secret that the provider accepts with `client_secret_basic` or `client_secret_post`. Alternatively, use a public client without a secret and set `druid.auth.pac4j.oidc.clientAuthenticationMethod` to `none`.

### Creating an authenticator

The authenticator chain is a single list. To use only the `pac4j` authenticator:

```
druid.auth.authenticatorChain=["pac4j"]
druid.auth.authenticator.pac4j.type=pac4j
druid.auth.authenticator.pac4j.authorizerName=<AUTHORIZER_NAME>
```

To use only the `jwt` authenticator:

```
druid.auth.authenticatorChain=["jwt"]
druid.auth.authenticator.jwt.type=jwt
druid.auth.authenticator.jwt.authorizerName=<AUTHORIZER_NAME>
```

To use both, list both in the same chain, for example `druid.auth.authenticatorChain=["jwt", "pac4j"]`.
Place `pac4j` last, because it redirects any request that no earlier authenticator accepted to the OIDC provider.

### Example

The following settings in `common.runtime.properties` enable web console sign-in on every service.
The `basic` authenticator and escalator handle the requests between services.

```
druid.extensions.loadList=["druid-basic-security", "druid-pac4j", ...]

druid.auth.authenticatorChain=["basic", "pac4j"]

druid.auth.authenticator.basic.type=basic
druid.auth.authenticator.basic.initialAdminPassword=<ADMIN_PASSWORD>
druid.auth.authenticator.basic.initialInternalClientPassword=<INTERNAL_PASSWORD>
druid.auth.authenticator.basic.authorizerName=allowAll

druid.escalator.type=basic
druid.escalator.internalClientUsername=druid_system
druid.escalator.internalClientPassword=<INTERNAL_PASSWORD>
druid.escalator.authorizerName=allowAll

druid.auth.authenticator.pac4j.type=pac4j
druid.auth.authenticator.pac4j.authorizerName=allowAll
druid.auth.pac4j.cookiePassphrase=<COOKIE_PASSPHRASE>
druid.auth.pac4j.oidc.clientID=<CLIENT_ID>
druid.auth.pac4j.oidc.clientSecret=<CLIENT_SECRET>
druid.auth.pac4j.oidc.discoveryURI=https://<OIDC_PROVIDER>/.well-known/openid-configuration
```

This example uses the built-in `allowAll` authorizer, which gives every user who can sign in to your OIDC provider full access to Druid.
In production, use an authorizer that grants permissions per user, such as the `basic` authorizer from [druid-basic-security](druid-basic-security.md).
Provide the secrets with a [password provider](../../operations/password-provider.md) rather than as plaintext.

### Properties
|Property|Description|Default|required|
|--------|---------------|-----------|-------|
|`druid.auth.authenticator.<NAME>.authorizerName`|Name of the authorizer that authorizes users authenticated by the `pac4j` or `jwt` authenticator named `<NAME>`.|none|Yes|
|`druid.auth.pac4j.cookiePassphrase`|Passphrase for encrypting the cookies that hold browser sessions. Must be the same on every service that runs the `pac4j` authenticator. It can be provided as plaintext string or the (recommended) [Password Provider](../../operations/password-provider.md).|none|Yes|
|`druid.auth.pac4j.readTimeout`|Socket connect and read timeout duration used when communicating with authentication server|PT5S|No|
|`druid.auth.pac4j.enableCustomSslContext`|Whether to use custom SSLContext setup via [simple-client-sslcontext](simple-client-sslcontext.md) extension which must be added to extensions list when this property is set to true.|false|No|
|`druid.auth.pac4j.oidc.clientID`|OAuth Client Application id.|none|Yes|
|`druid.auth.pac4j.oidc.clientSecret`|OAuth Client Application secret. It can be provided as plaintext string or The [Password Provider](../../operations/password-provider.md). Must not be set when `clientAuthenticationMethod` is `none`.|none|Yes, unless `clientAuthenticationMethod` is `none`|
|`druid.auth.pac4j.oidc.discoveryURI`|discovery URI for fetching OP metadata [see this](http://openid.net/specs/openid-connect-discovery-1_0.html).|none|Yes|
|`druid.auth.pac4j.oidc.oidcClaim`|[Claim](https://openid.net/specs/openid-connect-core-1_0.html#Claims) of the ID token that the `jwt` authenticator uses as the Druid identity. The `pac4j` authenticator ignores it and always uses `sub`.|name|No|
|`druid.auth.pac4j.oidc.scope`| scope is used by an application during authentication to authorize access to a user's details.|`openid profile email`|No|
|`druid.auth.pac4j.oidc.clientAuthenticationMethod`|How Druid authenticates itself to the OIDC provider when it exchanges the authorization code for tokens. It doesn't affect how browser sessions are stored. Supported values: `client_secret_basic`, `client_secret_post`, and `none` (for a public client that has no secret). Druid fails to start with any other value. If not specified, Druid uses the first of `client_secret_basic` and `client_secret_post` that the provider's discovery document lists.|Auto-detected from provider|No|

### Sessions

- A session ends when the access token from the OIDC provider expires, or after 15 minutes, whichever comes first. Activity doesn't extend it.
- When the session has ended, the browser has to go through the OIDC provider again. In the web console, reload the page. If the provider still has an active session for the user, this usually happens without a password prompt.
- There is no sign-out endpoint. A session stays valid until it ends as described above.
- The session cookies are always marked `Secure`, so browsers only send them over HTTPS. Serve the Router over HTTPS. Some browsers, such as Chrome, make an exception for `http://localhost`, which is useful for testing.

If a sign-in fails because of the configuration or the OIDC provider, for example because the discovery document can't be fetched, the user gets an HTTP 500 response and the service logs the cause at `ERROR` level.

:::info
Users must set a strong passphrase to ensure that an attacker is not able to guess it simply by brute force.
A compromised passphrase may allow an attacker to read and manipulate session cookies.
Anyone who knows the passphrase can create a valid session for any user on every service that runs the `pac4j` authenticator, so treat it like a signing key.
For more details, see [CVE-2024-45384](https://nvd.nist.gov/vuln/detail/CVE-2024-45384).
:::
