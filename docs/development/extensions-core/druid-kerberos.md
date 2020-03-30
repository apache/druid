---
id: druid-kerberos
title: "Kerberos"
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


Apache Druid Extension to enable Authentication for Druid Processes using Kerberos.
This extension adds an Authenticator which is used to protect HTTP Endpoints using the simple and protected GSSAPI negotiation mechanism [SPNEGO](https://en.wikipedia.org/wiki/SPNEGO).
Make sure to [include](../../development/extensions.md#loading-extensions) `druid-kerberos` as an extension.


## Configuration

### Creating an Authenticator
```
druid.auth.authenticatorChain=["MyKerberosAuthenticator"]

druid.auth.authenticator.MyKerberosAuthenticator.type=kerberos
```

To use the Kerberos authenticator, add an authenticator with type `kerberos` to the authenticatorChain. The example above uses the name "MyKerberosAuthenticator" for the Authenticator.

Configuration of the named authenticator is assigned through properties with the form:

```
druid.auth.authenticator.<authenticatorName>.<authenticatorProperty>
```

The configuration examples in the rest of this document will use "kerberos" as the name of the authenticator being configured.

### Properties
|Property|Possible Values|Description|Default|required|
|--------|---------------|-----------|-------|--------|
|`druid.auth.authenticator.kerberos.serverPrincipal`|`HTTP/_HOST@EXAMPLE.COM`| SPNEGO service principal used by druid processes|empty|Yes|
|`druid.auth.authenticator.kerberos.serverKeytab`|`/etc/security/keytabs/spnego.service.keytab`|SPNego service keytab used by druid processes|empty|Yes|
|`druid.auth.authenticator.kerberos.authToLocal`|`RULE:[1:$1@$0](druid@EXAMPLE.COM)s/.*/druid DEFAULT`|It allows you to set a general rule for mapping principal names to local user names. It will be used if there is not an explicit mapping for the principal name that is being translated.|DEFAULT|No|
|`druid.auth.authenticator.kerberos.cookieSignatureSecret`|`secretString`| Secret used to sign authentication cookies. It is advisable to explicitly set it, if you have multiple druid nodes running on same machine with different ports as the Cookie Specification does not guarantee isolation by port.|<Random value>|No|
|`druid.auth.authenticator.kerberos.authorizerName`|Depends on available authorizers|Authorizer that requests should be directed to|Empty|Yes|

As a note, it is required that the SPNego principal in use by the druid processes must start with HTTP (This specified by [RFC-4559](https://tools.ietf.org/html/rfc4559)) and must be of the form "HTTP/_HOST@REALM".
The special string _HOST will be replaced automatically with the value of config `druid.host`

### `druid.auth.authenticator.kerberos.excludedPaths`

In older releases, the Kerberos authenticator had an `excludedPaths` property that allowed the user to specify a list of paths where authentication checks should be skipped. This property has been removed from the Kerberos authenticator because the path exclusion functionality is now handled across all authenticators/authorizers by setting `druid.auth.unsecuredPaths`, as described in the [main auth documentation](../../design/auth.md).

### Auth to Local Syntax
`druid.auth.authenticator.kerberos.authToLocal` allows you to set a general rules for mapping principal names to local user names.
The syntax for mapping rules is `RULE:\[n:string](regexp)s/pattern/replacement/g`. The integer n indicates how many components the target principal should have. If this matches, then a string will be formed from string, substituting the realm of the principal for $0 and the nth component of the principal for $n. e.g. if the principal was druid/admin then `\[2:$2$1suffix]` would result in the string `admindruidsuffix`.
If this string matches regexp, then the s//\[g] substitution command will be run over the string. The optional g will cause the substitution to be global over the string, instead of replacing only the first match in the string.
If required, multiple rules can be be joined by newline character and specified as a String.

### Increasing HTTP Header size for large SPNEGO negotiate header
In Active Directory environment, SPNEGO token in the Authorization header includes PAC (Privilege Access Certificate) information,
which includes all security groups for the user. In some cases when the user belongs to many security groups the header to grow beyond what druid can handle by default.
In such cases, max request header size that druid can handle can be increased by setting `druid.server.http.maxRequestHeaderSize` (default 8Kb) and `druid.router.http.maxRequestBufferSize` (default 8Kb).

## Configuring Kerberos Escalated Client

Druid internal processes communicate with each other using an escalated http Client. A Kerberos enabled escalated HTTP Client can be configured by following properties -


|Property|Example Values|Description|Default|required|
|--------|---------------|-----------|-------|--------|
|`druid.escalator.type`|`kerberos`| Type of Escalator client used for internal process communication.|n/a|Yes|
|`druid.escalator.internalClientPrincipal`|`druid@EXAMPLE.COM`| Principal user name, used for internal process communication|n/a|Yes|
|`druid.escalator.internalClientKeytab`|`/etc/security/keytabs/druid.keytab`|Path to keytab file used for internal process communication|n/a|Yes|
|`druid.escalator.authorizerName`|`MyBasicAuthorizer`|Authorizer that requests should be directed to.|n/a|Yes|

## Accessing Druid HTTP end points when kerberos security is enabled
1. To access druid HTTP endpoints via curl user will need to first login using `kinit` command as follows -

    ```
    kinit -k -t <path_to_keytab_file> user@REALM.COM
    ```

2. Once the login is successful verify that login is successful using `klist` command
3. Now you can access druid HTTP endpoints using curl command as follows -

    ```
    curl --negotiate -u:anyUser -b ~/cookies.txt -c ~/cookies.txt -X POST -H'Content-Type: application/json' <HTTP_END_POINT>
    ```

    e.g to send a query from file `query.json` to the Druid Broker use this command -

    ```
    curl --negotiate -u:anyUser -b ~/cookies.txt -c ~/cookies.txt -X POST -H'Content-Type: application/json'  http://broker-host:port/druid/v2/?pretty -d @query.json
    ```
    Note: Above command will authenticate the user first time using SPNego negotiate mechanism and store the authentication cookie in file. For subsequent requests the cookie will be used for authentication.

## Accessing Coordinator or Overlord console from web browser
To access Coordinator/Overlord console from browser you will need to configure your browser for SPNego authentication as follows -

1. Safari - No configurations required.
2. Firefox - Open firefox and follow these steps -
    1. Go to `about:config` and search for `network.negotiate-auth.trusted-uris`.
    2. Double-click and add the following values: `"http://druid-coordinator-hostname:ui-port"` and `"http://druid-overlord-hostname:port"`
3. Google Chrome - From the command line run following commands -
    1. `google-chrome --auth-server-whitelist="druid-coordinator-hostname" --auth-negotiate-delegate-whitelist="druid-coordinator-hostname"`
    2. `google-chrome --auth-server-whitelist="druid-overlord-hostname" --auth-negotiate-delegate-whitelist="druid-overlord-hostname"`
4. Internet Explorer -
    1. Configure trusted websites to include `"druid-coordinator-hostname"` and `"druid-overlord-hostname"`
    2. Allow negotiation for the UI website.

## Sending Queries programmatically
Many HTTP client libraries, such as Apache Commons [HttpComponents](https://hc.apache.org/), already have support for performing SPNEGO authentication. You can use any of the available HTTP client library to communicate with druid cluster.
