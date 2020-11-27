---
id: tls-support
title: "TLS support"
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

## General configuration

|Property|Description|Default|
|--------|-----------|-------|
|`druid.enablePlaintextPort`|Enable/Disable HTTP connector.|`true`|
|`druid.enableTlsPort`|Enable/Disable HTTPS connector.|`false`|

Although not recommended, the HTTP and HTTPS connectors can both be enabled at a time. The respective ports are configurable using `druid.plaintextPort`
and `druid.tlsPort` properties on each process. Please see `Configuration` section of individual processes to check the valid and default values for these ports.

## Jetty server configuration

Apache Druid uses Jetty as its embedded web server. 

To get familiar with TLS/SSL, along with related concepts like keys and certificates,
read [Configuring SSL/TLS](https://www.eclipse.org/jetty/documentation/current/configuring-ssl.html) in the Jetty documentation.
To get more in-depth knowledge of TLS/SSL support in Java in general, refer to the [Java Secure Socket Extension (JSSE) Reference Guide](http://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/JSSERefGuide.html).
The [Configuring the Jetty SslContextFactory](https://www.eclipse.org/jetty/documentation/current/configuring-ssl.html#configuring-sslcontextfactory)
section can help in understanding TLS/SSL configurations listed below. Finally, [Java Cryptography Architecture
Standard Algorithm Name Documentation for JDK 8](http://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html) lists all possible
values for the configs belong, among others provided by Java implementation.

|Property|Description|Default|Required|
|--------|-----------|-------|--------|
|`druid.server.https.keyStorePath`|The file path or URL of the TLS/SSL Key store.|none|yes|
|`druid.server.https.keyStoreType`|The type of the key store.|none|yes|
|`druid.server.https.certAlias`|Alias of TLS/SSL certificate for the connector.|none|yes|
|`druid.server.https.keyStorePassword`|The [Password Provider](../operations/password-provider.md) or String password for the Key Store.|none|yes|

The following table contains configuration options related to client certificate authentication.

|Property|Description|Default|Required|
|--------|-----------|-------|--------|
|`druid.server.https.requireClientCertificate`|If set to true, clients must identify themselves by providing a TLS certificate, without which connections will fail.|false|no|
|`druid.server.https.requestClientCertificate`|If set to true, clients may optionally identify themselves by providing a TLS certificate. Connections will not fail if TLS certificate is not provided. This property is ignored if `requireClientCertificate` is set to true. If `requireClientCertificate` and `requestClientCertificate` are false, the rest of the options in this table are ignored.|false|no|
|`druid.server.https.trustStoreType`|The type of the trust store containing certificates used to validate client certificates. Not needed if `requireClientCertificate` and `requestClientCertificate` are false.|`java.security.KeyStore.getDefaultType()`|no|
|`druid.server.https.trustStorePath`|The file path or URL of the trust store containing certificates used to validate client certificates. Not needed if `requireClientCertificate` and `requestClientCertificate` are false.|none|yes, only if `requireClientCertificate` is true|
|`druid.server.https.trustStoreAlgorithm`|Algorithm to be used by TrustManager to validate client certificate chains. Not needed if `requireClientCertificate` and `requestClientCertificate` are false.|`javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm()`|no|
|`druid.server.https.trustStorePassword`|The [password provider](../operations/password-provider.md) or String password for the Trust Store.  Not needed if `requireClientCertificate` and `requestClientCertificate` are false.|none|no|
|`druid.server.https.validateHostnames`|If set to true, check that the client's hostname matches the CN/subjectAltNames in the client certificate.  Not used if `requireClientCertificate` and `requestClientCertificate` are false.|true|no|
|`druid.server.https.crlPath`|Specifies a path to a file containing static [Certificate Revocation Lists](https://en.wikipedia.org/wiki/Certificate_revocation_list), used to check if a client certificate has been revoked. Not used if `requireClientCertificate` and `requestClientCertificate` are false.|null|no|

The following table contains non-mandatory advanced configuration options, use caution.

|Property|Description|Default|Required|
|--------|-----------|-------|--------|
|`druid.server.https.keyManagerFactoryAlgorithm`|Algorithm to use for creating KeyManager, more details [here](https://docs.oracle.com/javase/7/docs/technotes/guides/security/jsse/JSSERefGuide.html#KeyManager).|`javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm()`|no|
|`druid.server.https.keyManagerPassword`|The [Password Provider](../operations/password-provider.md) or String password for the Key Manager.|none|no|
|`druid.server.https.includeCipherSuites`|List of cipher suite names to include. You can either use the exact cipher suite name or a regular expression.|Jetty's default include cipher list|no|
|`druid.server.https.excludeCipherSuites`|List of cipher suite names to exclude. You can either use the exact cipher suite name or a regular expression.|Jetty's default exclude cipher list|no|
|`druid.server.https.includeProtocols`|List of exact protocols names to include.|Jetty's default include protocol list|no|
|`druid.server.https.excludeProtocols`|List of exact protocols names to exclude.|Jetty's default exclude protocol list|no|

## Internal communication over TLS

Whenever possible Druid processes will use HTTPS to talk to each other. To enable this communication Druid's HttpClient needs to
be configured with a proper [SSLContext](http://docs.oracle.com/javase/8/docs/api/javax/net/ssl/SSLContext.html) that is able
to validate the Server Certificates, otherwise communication will fail.

Since, there are various ways to configure SSLContext, by default, Druid looks for an instance of SSLContext Guice binding
while creating the HttpClient. This binding can be achieved writing a [Druid extension](../development/extensions.md)
which can provide an instance of SSLContext. Druid comes with a simple extension present [here](../development/extensions-core/simple-client-sslcontext.md)
which should be useful enough for most simple cases, see [this](../development/extensions.md#loading-extensions) for how to include extensions.
If this extension does not satisfy the requirements then please follow the extension [implementation](https://github.com/apache/druid/tree/master/extensions-core/simple-client-sslcontext)
to create your own extension.

When Druid Coordinator/Overlord have both HTTP and HTTPS enabled and Client sends request to non-leader process, then Client is always redirected to the HTTPS endpoint on leader process.
So, Clients should be first upgraded to be able to handle redirect to HTTPS. Then Druid Overlord/Coordinator should be upgraded and configured to run both HTTP and HTTPS ports. Then Client configuration should be changed to refer to Druid Coordinator/Overlord via the HTTPS endpoint and then HTTP port on Druid Coordinator/Overlord should be disabled.

## Custom certificate checks

Druid supports custom certificate check extensions. Please refer to the `org.apache.druid.server.security.TLSCertificateChecker` interface for details on the methods to be implemented.

To use a custom TLS certificate checker, specify the following property:

|Property|Description|Default|Required|
|--------|-----------|-------|--------|
|`druid.tls.certificateChecker`|Type name of custom TLS certificate checker, provided by extensions. Please refer to extension documentation for the type name that should be specified.|"default"|no|

The default checker delegates to the standard trust manager and performs no additional actions or checks.

If using a non-default certificate checker, please refer to the extension documentation for additional configuration properties needed.
