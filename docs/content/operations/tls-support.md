---
layout: doc_page
---

TLS Support
===============

# General Configuration

|Property|Description|Default|
|--------|-----------|-------|
|`druid.server.http.plaintext`|Enable/Disable HTTP connector.|`true`|
|`druid.server.http.tls`|Enable/Disable HTTPS connector.|`false`|

Although not recommended but both HTTP and HTTPS connectors can be enabled at a time and respective ports are configurable using `druid.plaintextPort`
and `druid.tlsPort` properties on each node. Please see `Configuration` section of individual nodes to check the valid and default values for these ports.

# Jetty Server TLS Configuration

Druid uses Jetty as an embedded web server. To get familiar with TLS/SSL in general and related concepts like Certificates etc.
reading this [Jetty documentation](http://www.eclipse.org/jetty/documentation/9.3.x/configuring-ssl.html) might be helpful.
To get more in depth knowledge of TLS/SSL support in Java in general, please refer to this [guide](http://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/JSSERefGuide.html).
The documentation [here](http://www.eclipse.org/jetty/documentation/9.3.x/configuring-ssl.html#configuring-sslcontextfactory)
can help in understanding TLS/SSL configurations listed below. This [document](http://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html) lists all the possible
values for the below mentioned configs among others provided by Java implementation.

|Property|Description|Default|Required|
|--------|-----------|-------|--------|
|`druid.server.https.keyStorePath`|The file path or URL of the TLS/SSL Key store.|none|yes|
|`druid.server.https.keyStoreType`|The type of the key store.|none|yes|
|`druid.server.https.certAlias`|Alias of TLS/SSL certificate for the connector.|none|yes|
|`druid.server.https.keyStorePassword`|The [Password Provider](../operations/password-provider.html) or String password for the Key Store.|none|yes|
|`druid.server.https.keyManagerPassword`|The [Password Provider](../operations/password-provider.html) or String password for the Key Manager.|none|no|

# Druid's internal communication over TLS

Whenever possible Druid nodes will use HTTPS to talk to each other. To enable this communication Druid's HttpClient needs to
be configured with a proper [SSLContext](http://docs.oracle.com/javase/8/docs/api/javax/net/ssl/SSLContext.html) that is able
to validate the Server Certificates, otherwise communication will fail.

Since, there are various ways to configure SSLContext, by default, Druid looks for an instance of SSLContext Guice binding
while creating the HttpClient. This binding can be achieved writing a [Druid extension](../development/extensions.html)
which can provide an instance of SSLContext. Druid comes with a simple extension present [here](../development/extensions-core/simple-client-sslcontext.html)
which should be useful enough for most simple cases, see [this](./including-extensions.html) for how to include extensions.
If this extension does not satisfy the requirement then please follow the extension [implementation] (https://github.com/druid-io/druid/tree/master/extensions-core/simple-client-sslcontext)
to create your own extension.