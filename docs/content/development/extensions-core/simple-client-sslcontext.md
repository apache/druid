---
layout: doc_page
title: "Simple SSLContext Provider Module"
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

# Simple SSLContext Provider Module

This Apache Druid (incubating) module contains a simple implementation of [SSLContext](http://docs.oracle.com/javase/8/docs/api/javax/net/ssl/SSLContext.html)
that will be injected to be used with HttpClient that Druid processes use internally to communicate with each other. To learn more about
Java's SSL support, please refer to [this](http://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/JSSERefGuide.html) guide.

# Configuration

|Property|Description|Default|Required|
|--------|-----------|-------|--------|
|`druid.client.https.protocol`|SSL protocol to use.|`TLSv1.2`|no|
|`druid.client.https.trustStoreType`|The type of the key store where trusted root certificates are stored.|`java.security.KeyStore.getDefaultType()`|no|
|`druid.client.https.trustStorePath`|The file path or URL of the TLS/SSL Key store where trusted root certificates are stored.|none|yes|
|`druid.client.https.trustStoreAlgorithm`|Algorithm to be used by TrustManager to validate certificate chains|`javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm()`|no|
|`druid.client.https.trustStorePassword`|The [Password Provider](../../operations/password-provider.html) or String password for the Trust Store.|none|yes|

The following table contains optional parameters for supporting client certificate authentication:

|Property|Description|Default|Required|
|--------|-----------|-------|--------|
|`druid.client.https.keyStorePath`|The file path or URL of the TLS/SSL Key store containing the client certificate that Druid will use when communicating with other Druid services. If this is null, the other properties in this table are ignored.|none|yes|
|`druid.client.https.keyStoreType`|The type of the key store.|none|yes|
|`druid.client.https.certAlias`|Alias of TLS client certificate in the keystore.|none|yes|
|`druid.client.https.keyStorePassword`|The [Password Provider](../../operations/password-provider.html) or String password for the Key Store.|none|no|
|`druid.client.https.keyManagerFactoryAlgorithm`|Algorithm to use for creating KeyManager, more details [here](https://docs.oracle.com/javase/7/docs/technotes/guides/security/jsse/JSSERefGuide.html#KeyManager).|`javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm()`|no|
|`druid.client.https.keyManagerPassword`|The [Password Provider](../../operations/password-provider.html) or String password for the Key Manager.|none|no|
|`druid.client.https.validateHostnames`|Validate the hostname of the server. This should not be disabled unless you are using [custom TLS certificate checks](../../operations/tls-support.html#custom-tls-certificate-checks) and know that standard hostname validation is not needed.|true|no|

This [document](http://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html) lists all the possible
values for the above mentioned configs among others provided by Java implementation.
