---
id: security-overview
title: "Security overview"
---


## Overview

By default, security features in Druid are disabled, that is, TLS is disabled and user authentication does not occur. To use these features, you need to configure security in Druid. 

This document aims to give you an overview of the security features in Druid, and a quick look on how to go about configuring them. 


## Best practices

* Do not expose the Druid Console on an untrusted network. Only trusted users should have access to it. Keep in mind that access to the console effectively confers access to the file system on the installation machine as well via file browsers built into the UI. 
* TBD... 


## Securing Druid


You can configure authentication and authorization to control access to the the Druid APIs. The first step is enabling TLS for the cluster nodes. Then configure users, roles, and permissions. 

Apache Druid uses Jetty as its embedded web server. The steps for securing a Druid cluster, therefore, aligns with securing any Jetty cluster.  

To secure communication between cluster components, 

#### Enable TLS

You can enable TLS to secure external client connections to Druid as well as connections between cluster nodes. 

Druid uses Jetty as its embedded web server. Therefore you refer to [Understanding Certificates and Keys](https://www.eclipse.org/jetty/documentation/current/configuring-ssl.html) for complete instructions. 

An overview of the steps are: 

1. Enable TLS by adding `druid.enableTlsPort=true` to `common.runtime.properties` on each node.
2. Follow the steps in to [Understanding Certificates and Keys](https://www.eclipse.org/jetty/documentation/current/configuring-ssl.html#understanding-certificates-and-keys) to generate or import a key and certificate. 
3. Configure the keystore and truststore settings in `common.runtime.properties`. The file should look something like this: 
  ```
  druid.enablePlaintextPort=false
  druid.enableTlsPort=true
  
  druid.server.https.keyStoreType=jks
  druid.server.https.keyStorePath=imply-keystore.jks
  druid.server.https.keyStorePassword=secret123 # replace with your own password
  druid.server.https.certAlias=druid 
  
  druid.client.https.protocol=TLSv1.2
  druid.client.https.trustStoreType=jks
  druid.client.https.trustStorePath=imply-truststore.jks
  druid.client.https.trustStorePassword=secret123  # replace with your own password

  ``` 
4. Add the `simple-client-sslcontext` extension to `druid.extensions.loadList` in common.runtime.properties. This enables TLS for Druid nodes acting as clients.
5. Restart the cluster.

For more information, see [TLS support](tls-support) and [Simple SSLContext Provider Module](../development/extensions-core/simple-client-sslcontext). 


### Configure Druid for basic-auth

Extensions extend the authentication and authorization features built into core Druid. There are extensions for HTTP basic authentication, LDAP, and Kerberos. The following takes you through sample configuration steps for enabling basic auth:  

1. Add the `basic-auth` extension to `druid.extensions.loadList` in common.runtime.properties. For the quickstart installation, for example, ,the properties file is at `conf/druid/cluster/_common`:
   ```
   druid.extensions.loadList=["druid-basic-security", "druid-histogram", "druid-datasketches", "druid-kafka-indexing-service", "imply-utility-belt"]
   ```
2. Configure the basic Authenticator, Authorizer, and Escalator settings in the same common.runtime.properties file. For example:
   ```
   # Druid basic security
   druid.auth.authenticatorChain=["MyBasicMetadataAuthenticator"]

   druid.auth.authenticator.MyBasicMetadataAuthenticator.type=basic
   druid.auth.authenticator.MyBasicMetadataAuthenticator.initialAdminPassword=password1
   druid.auth.authenticator.MyBasicMetadataAuthenticator.initialInternalClientPassword=password2
   druid.auth.authenticator.MyBasicMetadataAuthenticator.credentialsValidator.type=metadata
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


#### Create users, roles, and permissions

After enabling the basic auth extension, you can add users, roles, and permissions via the Druid Coordinator `user` endpoint.  

> The Coordinator API port is 8081 for non-TLS connections and 8281 for secured connections.

1. Create a user by issuing a POST request to `druid-ext/basic-security/authentication/db/MyBasicMetadataAuthenticator/users/<USERNAME>`, replacing USERNAME with the new username. For example: 
   ```
   curl -u admin:password -XPOST https://my-coordinator-ip:8281/druid-ext/basic-security/authentication/db/basic/users/myname
   ```
2. Add a credential for the user by issuing a POST to `druid-ext/basic-security/authentication/db/MyBasicMetadataAuthenticator/users/<USERNAME>/credentials`. For example:
    ```
    curl -u admin:password -H'Content-Type: application/json' -XPOST --data-binary @pass.json https://my-coordinator-ip:8281/druid-ext/basic-security/authentication/db/basic/users/myname/credentials

    ```
    The password is conveyed in the pass.json file, the payload of which should be in the form:
   	```
   	{
      "password": "password"
    }
    ```
2. For each authenticator user you create, create a corresponding authorizer user by issuing a POST request to `druid-ext/basic-security/authorization/db/MyBasicMetadataAuthorizer/users/<USERNAME>`. For example: 
	```
	curl -u admin:password -XPOST https://my-coordinator-ip:8281/druid-ext/basic-security/authorization/db/basic/users/myname
	```
3. Create authorizer roles to control permissions by issuing a POST request to `druid-ext/basic-security/authorization/db/MyBasicMetadataAuthorizer/roles/<ROLENAME>`. For example: 
	```
   curl -u admin:password -XPOST https://my-coordinator-ip:8281/druid-ext/basic-security/authorization/db/basic/roles/myrole
   ```
4. Assign roles to users by issuing a POST request to `druid-ext/basic-security/authorization/db/MyBasicMetadataAuthorizer/users/<USERNAME>/roles/<ROLENAME>`. For example: 
	```
	curl -u admin:password -XPOST https://my-coordinator-ip:8281/druid-ext/basic-security/authorization/db/basic/users/myname/roles/myrole | jq
	```
5. Finally, attach permissions to the roles to control how they can interact with Druid at `druid-ext/basic-security/authorization/db/MyBasicMetadataAuthorizer/roles/<ROLENAME>/permissions`. 
	For example: 
	```
	curl -u admin:password -H'Content-Type: application/json' -XPOST --data-binary @perms.json https://my-coordinator-ip:8281/druid-ext/basic-security/authorization/db/basic/roles/myrole/permissions
	```
	The payload of perms.json should be in the form:
   	```
    [
    {
      "resource": {
        "name": "<PATTERN>",
        "type": "DATASOURCE"
      },
      "action": "READ"
    },
    {
      "resource": {
      "name": "STATE",
      "type": "STATE"
    },
    "action": "READ"
    }
    ]
    ```


Congratulations, you now have permissioned roles with associated users in Druid!
