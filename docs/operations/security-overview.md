---
id: security-overview
title: "Security overview"
description: Overiew of Apache Druid security. Includes best practices, configuration instructions, a description of the security model and documentation on how to report security issues.
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


This document provides an overview of Apache Druid security features, configuration instructions, and some best practices to secure Druid.

By default, security features in Druid are disabled, which simplifies the initial deployment experience. However, security features must be configured in a production deployment. These features include TLS, authentication, and authorization.

## Best practices

The following recommendations apply to the Druid cluster setup:
* Run Druid as an unprivileged Unix user. Do not run Druid as the root user.
:::caution
Druid administrators have the same OS permissions as the Unix user account running Druid. See [Authentication and authorization model](security-user-auth.md#authentication-and-authorization-model). If the Druid process is running under the OS root user account, then Druid administrators can read or write all files that the root account has access to, including sensitive files such as `/etc/passwd`.
:::
* Enable authentication to the Druid cluster for production environments and other environments that can be accessed by untrusted networks.
* Enable authorization and do not expose the web console without authorization enabled. If authorization is not enabled, any user that has access to the web console has the same privileges as the operating system user that runs the web console process.
* Grant users the minimum permissions necessary to perform their functions. For instance, do not allow users who only need to query data to write to data sources or view state.
* Do not provide plain-text passwords for production systems in configuration specs. For example, sensitive properties should not be in the `consumerProperties` field of `KafkaSupervisorIngestionSpec`. See [Environment variable dynamic config provider](./dynamic-config-provider.md#environment-variable-dynamic-config-provider) for more information.
* Disable JavaScript, as noted in the [Security section](https://druid.apache.org/docs/latest/development/javascript.html#security) of the JavaScript guide.

The following recommendations apply to the network where Druid runs:
* Enable TLS to encrypt communication within the cluster.
* Use an API gateway to:
  - Restrict access from untrusted networks
  - Create an allow list of specific APIs that your users need to access
  - Implement account lockout and throttling features.
* When possible, use firewall and other network layer filtering to only expose Druid services and ports specifically required for your use case. For example, only expose Broker ports to downstream applications that execute queries. You can limit access to a specific IP address or IP range to further tighten and enhance security.

The following recommendation applies to Druid's authorization and authentication model:
* Only grant `WRITE` permissions to any `DATASOURCE` to trusted users. Druid's trust model assumes those users have the same privileges as the operating system user that runs the web console process. Additionally, users with `WRITE` permissions can make changes to datasources and they have access to both task and supervisor update (POST) APIs which may affect ingestion.
* Only grant `STATE READ`, `STATE WRITE`, `CONFIG WRITE`, and `DATASOURCE WRITE` permissions to highly-trusted users. These permissions allow users to access resources on behalf of the Druid server process regardless of the datasource.
* If your Druid client application allows less-trusted users to control the input source or firehose of an ingestion task, validate the URLs from the users. It is possible to point unchecked URLs to other locations and resources within your network or local file system.

## Enable TLS

Enabling TLS encrypts the traffic between external clients and the Druid cluster and traffic between services within the cluster.

### Generating keys
Before you enable TLS in Druid, generate the KeyStore and truststore. When one Druid process, e.g. Broker, contacts another Druid process , e.g. Historical, the first service is a client for the second service, considered the server.

The client uses a trustStore that contains certificates trusted by the client. For example, the Broker.

The server uses a KeyStore that contains private keys and certificate chain used to securely identify itself.

The following example demonstrates how to use Java keytool to generate the KeyStore for the server and then create a trustStore to trust the key for the client:

1. Generate the KeyStore with the Java `keytool` command:
```bash
keytool -keystore keystore.jks -alias druid -genkey -keyalg RSA
```
2. Export a public certificate:
```bash
keytool -export -alias druid -keystore keystore.jks -rfc -file public.cert
```
3. Create the trustStore:
```bash
keytool -import -file public.cert -alias druid -keystore truststore.jks
```

Druid uses Jetty as its embedded web server. See [Configuring SSL/TLS KeyStores
](https://www.eclipse.org/jetty/documentation/jetty-11/operations-guide/index.html#og-keystore) from the Jetty documentation.

:::caution
Do not use self-signed certificates for production environments. Instead, rely on your current public key infrastructure to generate and distribute trusted keys.
:::

### Update Druid TLS configurations
Edit `common.runtime.properties` for all Druid services on all nodes. Add or update the following TLS options. Restart the cluster when you are finished.

```properties
# Turn on TLS globally
druid.enableTlsPort=true

# Disable non-TLS communicatoins
druid.enablePlaintextPort=false

# For Druid processes acting as a client
# Load simple-client-sslcontext to enable client side TLS
# Add the following to extension load list
druid.extensions.loadList=[......., "simple-client-sslcontext"]

# Setup client side TLS
druid.client.https.protocol=TLSv1.2
druid.client.https.trustStoreType=jks
druid.client.https.trustStorePath=truststore.jks # replace with correct trustStore file
druid.client.https.trustStorePassword=secret123  # replace with your own password

# Setup server side TLS
druid.server.https.keyStoreType=jks
druid.server.https.keyStorePath=my-keystore.jks # replace with correct keyStore file
druid.server.https.keyStorePassword=secret123 # replace with your own password
druid.server.https.certAlias=druid
```
For more information, see [TLS support](tls-support.md) and [Simple SSLContext Provider Module](../development/extensions-core/simple-client-sslcontext.md).

## Authentication and authorization

You can configure authentication and authorization to control access to the Druid APIs. Then configure users, roles, and permissions, as described in the following sections. Make the configuration changes in the `common.runtime.properties` file on all Druid servers in the cluster.

Within Druid's operating context, authenticators control the way user identities are verified. Authorizers employ user roles to relate authenticated users to the datasources they are permitted to access. You can set the finest-grained permissions on a per-datasource basis.

The following graphic depicts the course of request through the authentication process:

![Druid security check flow](../assets/security-model-1.png "Druid security check flow")

## Enable an authenticator

To authenticate requests in Druid, you configure an Authenticator. Authenticator extensions exist for HTTP basic authentication, LDAP, and Kerberos.

The following takes you through sample configuration steps for enabling basic auth:

1. Add the `druid-basic-security` extension to `druid.extensions.loadList` in `common.runtime.properties`. For the quickstart installation, for example, the properties file is at `conf/druid/cluster/_common`:
   ```properties
   druid.extensions.loadList=["druid-basic-security", "druid-histogram", "druid-datasketches", "druid-kafka-indexing-service"]
   ```
2. Configure the basic Authenticator, Authorizer, and Escalator settings in the same common.runtime.properties file. The Escalator defines how Druid processes authenticate with one another.

   An example configuration:

   ```properties
   # Druid basic security
   druid.auth.authenticatorChain=["MyBasicMetadataAuthenticator"]
   druid.auth.authenticator.MyBasicMetadataAuthenticator.type=basic

   # Default password for 'admin' user, should be changed for production.
   druid.auth.authenticator.MyBasicMetadataAuthenticator.initialAdminPassword=password1

   # Default password for internal 'druid_system' user, should be changed for production.
   druid.auth.authenticator.MyBasicMetadataAuthenticator.initialInternalClientPassword=password2

   # Uses the metadata store for storing users.
   # You can use the authentication API to create new users and grant permissions
   druid.auth.authenticator.MyBasicMetadataAuthenticator.credentialsValidator.type=metadata

   # If true and if the request credential doesn't exist in this credentials store,
   # the request will proceed to next Authenticator in the chain.
   druid.auth.authenticator.MyBasicMetadataAuthenticator.skipOnFailure=false

   druid.auth.authenticator.MyBasicMetadataAuthenticator.authorizerName=MyBasicMetadataAuthorizer

   # Escalator
   druid.escalator.type=basic
   druid.escalator.internalClientUsername=druid_system
   druid.escalator.internalClientPassword=password2
   druid.escalator.authorizerName=MyBasicMetadataAuthorizer

   druid.auth.authorizers=["MyBasicMetadataAuthorizer"]

   druid.auth.authorizer.MyBasicMetadataAuthorizer.type=basic
   ```

3. Restart the cluster.

See the following topics for more information:

* [Authentication and Authorization](../operations/auth.md) for more information about the Authenticator,
Escalator, and Authorizer.
* [Basic Security](../development/extensions-core/druid-basic-security.md) for more information about
the extension used in the examples above.
* [Kerberos](../development/extensions-core/druid-kerberos.md) for Kerberos authentication.
* [User authentication and authorization](security-user-auth.md) for details about permissions.
* [SQL permissions](security-user-auth.md#sql-permissions) for permissions on SQL system tables.
* [The `druidapi` Python library](../tutorials/tutorial-jupyter-index.md),
  provided as part of the Druid tutorials, to set up users and roles for learning how security works.

## Enable authorizers

After enabling the basic auth extension, you can add users, roles, and permissions via the Druid Coordinator `user` endpoint. Note that you cannot assign permissions directly to individual users. They must be assigned through roles.

The following diagram depicts the authorization model, and the relationship between users, roles, permissions, and resources.

![Druid Security model](../assets/security-model-2.png "Druid security model")


The following steps walk through a sample setup procedure:

:::info
 The default Coordinator API port is 8081 for non-TLS connections and 8281 for secured connections.
:::

1. Create a user by issuing a POST request to `druid-ext/basic-security/authentication/db/MyBasicMetadataAuthenticator/users/<USERNAME>`.
   Replace `<USERNAME>` with the *new* username you are trying to create. For example:
   ```bash
   curl -u admin:password1 -XPOST https://my-coordinator-ip:8281/druid-ext/basic-security/authentication/db/MyBasicMetadataAuthenticator/users/myname
   ```
:::info
 If you have TLS enabled, be sure to adjust the curl command accordingly. For example, if your Druid servers use self-signed certificates,
you may choose to include the `insecure` curl option to forgo certificate checking for the curl command.
:::

2. Add a credential for the user by issuing a POST request to `druid-ext/basic-security/authentication/db/MyBasicMetadataAuthenticator/users/<USERNAME>/credentials`. For example:
   ```bash
   curl -u admin:password1 -H'Content-Type: application/json' -XPOST https://my-coordinator-ip:8281/druid-ext/basic-security/authentication/db/MyBasicMetadataAuthenticator/users/myname/credentials --data-raw '{"password": "my_password"}'
   ```
3. For each authenticator user you create, create a corresponding authorizer user by issuing a POST request to `druid-ext/basic-security/authorization/db/MyBasicMetadataAuthorizer/users/<USERNAME>`. For example:
   ```bash
   curl -u admin:password1 -XPOST https://my-coordinator-ip:8281/druid-ext/basic-security/authorization/db/MyBasicMetadataAuthorizer/users/myname
   ```
4. Create authorizer roles to control permissions by issuing a POST request to `druid-ext/basic-security/authorization/db/MyBasicMetadataAuthorizer/roles/<ROLENAME>`. For example:
   ```bash
   curl -u admin:password1 -XPOST https://my-coordinator-ip:8281/druid-ext/basic-security/authorization/db/MyBasicMetadataAuthorizer/roles/myrole
   ```
5. Assign roles to users by issuing a POST request to `druid-ext/basic-security/authorization/db/MyBasicMetadataAuthorizer/users/<USERNAME>/roles/<ROLENAME>`. For example:
   ```bash
   curl -u admin:password1 -XPOST https://my-coordinator-ip:8281/druid-ext/basic-security/authorization/db/MyBasicMetadataAuthorizer/users/myname/roles/myrole | jq
   ```

6. Finally, attach permissions to the roles to control how they can interact with Druid at `druid-ext/basic-security/authorization/db/MyBasicMetadataAuthorizer/roles/<ROLENAME>/permissions`. For example:
   ```bash
   curl -u admin:password1 -H'Content-Type: application/json' -XPOST --data-binary @perms.json https://my-coordinator-ip:8281/druid-ext/basic-security/authorization/db/MyBasicMetadataAuthorizer/roles/myrole/permissions
   ```
   The payload of `perms.json` should be in the following form:
   ```json
   [
      {
        "resource": {
          "type": "DATASOURCE",
          "name": "<PATTERN>"
        },
        "action": "READ"
      },
      {
        "resource": {
          "type": "STATE",
          "name": "STATE"
        },
        "action": "READ"
      }
   ]
   ```
:::info
 Note: Druid treats the resource name as a regular expression (regex). You can use a specific datasource name or regex to grant permissions for multiple datasources at a time.
:::


## Configuring an LDAP authenticator

As an alternative to using the basic metadata authenticator, you can use LDAP to authenticate users. See [Configure LDAP authentication](./auth-ldap.md) for information on configuring Druid for LDAP and LDAPS.

## Druid security trust model
Within Druid's trust model there users can have different authorization levels:
- Users with resource write permissions are allowed to do anything that the druid process can do.
- Authenticated read only users can execute queries against resources to which they have permissions.
- An authenticated user without any permissions is allowed to execute queries that don't require access to a resource.

Additionally, Druid operates according to the following principles:

From the innermost layer:
1. Druid processes have the same access to the local files granted to the specified system user running the process.
2. The Druid ingestion system can create new processes to execute tasks. Those tasks inherit the user of their parent process. This means that any user authorized to submit an ingestion task can use the ingestion task permissions to read or write any local files or external resources that the Druid process has access to.

:::info
 Note: Only grant the `DATASOURCE WRITE` to trusted users because they can act as the Druid process.
:::

Within the cluster:
1. Druid assumes it operates on an isolated, protected network where no reachable IP within the network is under adversary control. When you implement Druid, take care to setup firewalls and other security measures to secure both inbound and outbound connections.
Druid assumes network traffic within the cluster is encrypted, including API calls and data transfers. The default encryption implementation uses TLS.
2. Druid assumes auxiliary services such as the metadata store and ZooKeeper nodes are not under adversary control.

Cluster to deep storage:
1. Druid does not make assumptions about the security for deep storage. It follows the system's native security policies to authenticate and authorize with deep storage.
2. Druid does not encrypt files for deep storage. Instead, it relies on the storage system's native encryption capabilities to ensure compatibility with encryption schemes across all storage types.

Cluster to client:
1. Druid authenticates with the client based on the configured authenticator.
2. Druid only performs actions when an authorizer grants permission. The default configuration is `allowAll authorizer`.

## Reporting security issues

The Apache Druid team takes security very seriously. If you find a potential security issue in Druid, such as a way to bypass the security mechanisms described earlier, please report this problem to [security@apache.org](mailto:security@apache.org). This is a private mailing list. Please send one plain text email for each vulnerability you are reporting.

### Vulnerability handling

The following list summarizes the vulnerability handling process:

* The reporter reports the vulnerability privately to [security@apache.org](mailto:security@apache.org)
* The reporter receives a response that the Druid team has received the report and will investigate the issue.
* The Druid project security team works privately with the reporter to resolve the vulnerability.
* The Druid team delivers the fix by creating a new release of the package that the vulnerability affects.
* The Druid team publicly announces the vulnerability and describes how to apply the fix.

Committers should read a [more detailed description of the process](https://www.apache.org/security/committers.html). Reporters of security vulnerabilities may also find it useful.
