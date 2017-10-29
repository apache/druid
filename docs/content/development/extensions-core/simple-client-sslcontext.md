---
layout: doc_page
---

## Simple SSLContext Provider Module

This module contains a simple implementation of [SSLContext](http://docs.oracle.com/javase/8/docs/api/javax/net/ssl/SSLContext.html)
that will be injected to be used with HttpClient that Druid nodes use internally to communicate with each other. To learn more about
Java's SSL support, please refer to [this](http://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/JSSERefGuide.html) guide.

# Configuration

|Property|Description|Default|Required|
|--------|-----------|-------|--------|
|`druid.client.https.protocol`|SSL protocol to use.|`TLSv1.2`|no|
|`druid.client.https.trustStoreType`|The type of the key store where trusted root certificates are stored.|`java.security.KeyStore.getDefaultType()`|no|
|`druid.client.https.trustStorePath`|The file path or URL of the TLS/SSL Key store where trusted root certificates are stored.|none|yes|
|`druid.client.https.trustStoreAlgorithm`|Algorithm to be used by TrustManager to validate certificate chains|`javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm()`|no|
|`druid.client.https.trustStorePassword`|The [Password Provider](../../operations/password-provider.html) or String password for the Trust Store.|none|yes|

This [document](http://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html) lists all the possible
values for the above mentioned configs among others provided by Java implementation.