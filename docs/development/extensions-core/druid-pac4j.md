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


Apache Druid Extension to enable [OpenID Connect](https://openid.net/connect/) based Authentication for Druid Processes using [pac4j](https://github.com/pac4j/pac4j) as the underlying client library.
This can be used  with any authentication server that supports same e.g. [Okta](https://developer.okta.com/).
The pac4j authenticator should only be used at the router node to enable a group of users in existing authentication server to interact with Druid cluster, using the [web console](../../operations/web-console.md). 

This extension also provides a JWT authenticator that validates [ID Tokens](https://openid.net/specs/openid-connect-core-1_0.html#CodeIDToken) associated with a request. ID Tokens are attached to the request under the `Authorization` header with the bearer token prefix - `Bearer `. This authenticator is intended for services to talk to Druid by initially authenticating with an OIDC server to retrieve the ID Token which is then attached to every Druid request.

This extension does not support JDBC client authentication.

## Configuration

### Creating an Authenticator
```
#Create a pac4j web user authenticator
druid.auth.authenticatorChain=["pac4j"]
druid.auth.authenticator.pac4j.type=pac4j

#Create a JWT token authenticator
druid.auth.authenticatorChain=["jwt"]
druid.auth.authenticator.jwt.type=jwt
```

### Properties
|Property|Description|Default|required|
|--------|---------------|-----------|-------|
|`druid.auth.pac4j.cookiePassphrase`|passphrase for encrypting the cookies used to manage authentication session with browser. It can be provided as plaintext string or The [Password Provider](../../operations/password-provider.md).|none|Yes|
|`druid.auth.pac4j.readTimeout`|Socket connect and read timeout duration used when communicating with authentication server|PT5S|No|
|`druid.auth.pac4j.enableCustomSslContext`|Whether to use custom SSLContext setup via [simple-client-sslcontext](simple-client-sslcontext.md) extension which must be added to extensions list when this property is set to true.|false|No|
|`druid.auth.pac4j.oidc.clientID`|OAuth Client Application id.|none|Yes|
|`druid.auth.pac4j.oidc.clientSecret`|OAuth Client Application secret. It can be provided as plaintext string or The [Password Provider](../../operations/password-provider.md).|none|Yes|
|`druid.auth.pac4j.oidc.discoveryURI`|discovery URI for fetching OP metadata [see this](http://openid.net/specs/openid-connect-discovery-1_0.html).|none|Yes|
|`druid.auth.pac4j.oidc.oidcClaim`|[claim](https://openid.net/specs/openid-connect-core-1_0.html#Claims) that will be extracted from the ID Token after validation.|name|No|
|`druid.auth.pac4j.oidc.scope`| scope is used by an application during authentication to authorize access to a user's details                                                                                                       |`openid profile email`|No
