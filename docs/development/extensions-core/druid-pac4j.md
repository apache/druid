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
This extension should only be used at the router node to enable a group of users in existing authentication server to interact with Druid cluster, using the [Web Console](../../operations/druid-console.html). This extension does not support JDBC client authentication.

## Configuration

### Creating an Authenticator
```
druid.auth.authenticatorChain=["pac4j"]
druid.auth.authenticator.pac4j.type=pac4j
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
