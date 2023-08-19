---
id: auth-ldap
title: "Configure LDAP authentication"
sidebar_label: "LDAP auth"
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

You can use [Lightweight Directory Access Protocol (LDAP)](https://en.wikipedia.org/wiki/Lightweight_Directory_Access_Protocol) to secure access to Apache Druid. This topic describes how to set up Druid authentication and authorization with LDAP and LDAP over TLS (LDAPS). The examples on this page show the configuration for an Active Directory LDAP system.

The first step is to enable LDAP authentication and authorization for Druid. You then map an LDAP group to Druid roles and assign permissions to those roles. After you've completed this configuration you can optionally choose to enable LDAPS to make LDAP traffic confidential and secure.

## Prerequisites

Before you start to configure LDAP for Druid, test your LDAP connection and perform a sample search.

### Check your LDAP connection

Test your LDAP connection to verify it works with user credentials. Later in the process you [configure Druid for LDAP authentication](#configure-druid-for-ldap-authentication) with this user as the `bindUser`.

The following example command tests the connection for the user `myuser@example.com`. Insert your LDAP server IP address. Modify the port number of your LDAP instance if it listens on a port other than `389`.

```bash
ldapwhoami -vv -H ldap://ip_address:389  -D "myuser@example.com" -W
```

Enter the password for the user when prompted and verify that the command succeeded. If it failed, check the following:

- Make sure you're using the correct port for your LDAP instance.
- Check if a network firewall is preventing connections to the LDAP port.
- Review your LDAP implementation details to see whether you need to specifically allow LDAP clients at the LDAP server. If so, add the Druid Coordinator server to the allow list.

### Test your LDAP search

Once your LDAP connection is working, search for a user. For example, the following command searches for the user `myuser` in an Active Directory system. The `sAMAccountName` attribute is specific to Active Directory and contains the authenticated user identity:

```bash
ldapsearch -x -W -H ldap://ip_address:389  -D "cn=admin,dc=example,dc=com" -b "dc=example,dc=com" "(sAMAccountName=myuser)" +
```

The `memberOf` attribute in the results shows the groups the user belongs to. For example, the following response shows that the user is a member of the `mygroup` group:

```bash
memberOf: cn=mygroup,ou=groups,dc=example,dc=com
```

You use this information to map the LDAP group to Druid roles in a later step. 

:::info
 Druid uses the `memberOf` attribute to determine a group's membership using LDAP. If your LDAP server implementation doesn't include this attribute, you must complete some additional steps when you [map LDAP groups to Druid roles](#map-ldap-groups-to-druid-roles).
:::

## Configure Druid for LDAP authentication

To configure Druid to use LDAP authentication, follow these steps. See [Configuration reference](../configuration/index.md) for the location of the configuration files. 

1. Create a user in your LDAP system that you'll use both for internal communication with Druid and as the LDAP initial admin user. See [Security overview](./security-overview.md) for more information.
In the example below, the LDAP user is `internal@example.com`.

2. Enable the `druid-basic-security` extension in the `common.runtime.properties` file.

3. In the `common.runtime.properties` file, add the following lines for LDAP properties and substitute the values for your own. See [Druid basic security](../development/extensions-core/druid-basic-security.md#properties-for-ldap-user-authentication) for details about these properties.
 
   ```
   druid.auth.authenticatorChain=["ldap"]
   druid.auth.authenticator.ldap.type=basic
   druid.auth.authenticator.ldap.enableCacheNotifications=true
   druid.auth.authenticator.ldap.credentialsValidator.type=ldap
   druid.auth.authenticator.ldap.credentialsValidator.url=ldap://ip_address:port
   druid.auth.authenticator.ldap.credentialsValidator.bindUser=administrator@example.com
   druid.auth.authenticator.ldap.credentialsValidator.bindPassword=adminpassword
   druid.auth.authenticator.ldap.credentialsValidator.baseDn=dc=example,dc=com
   druid.auth.authenticator.ldap.credentialsValidator.userSearch=(&(sAMAccountName=%s)(objectClass=user))
   druid.auth.authenticator.ldap.credentialsValidator.userAttribute=sAMAccountName
   druid.auth.authenticator.ldap.authorizerName=ldapauth
   druid.escalator.type=basic
   druid.escalator.internalClientUsername=internal@example.com
   druid.escalator.internalClientPassword=internaluserpassword
   druid.escalator.authorizerName=ldapauth
   druid.auth.authorizers=["ldapauth"]
   druid.auth.authorizer.ldapauth.type=basic
   druid.auth.authorizer.ldapauth.initialAdminUser=internal@example.com
   druid.auth.authorizer.ldapauth.initialAdminRole=admin
   druid.auth.authorizer.ldapauth.roleProvider.type=ldap
   ```
   Note the following:

   - `bindUser`: A user for connecting to LDAP. This should be the same user you used to [test your LDAP search](#test-your-ldap-search).
   - `userSearch`: Your LDAP search syntax.
   - `userAttribute`: The user search attribute.
   - `internal@example.com` is the LDAP user you created in step 1. In the example it serves as both the internal client user and the initial admin user.

:::info
 In the above example, the [Druid escalator](../development/extensions-core/druid-basic-security.md#escalator) and LDAP initial admin user are set to the same user - `internal@example.com`. If the escalator is set to a different user, you must follow steps 4 and 5 to create the group mapping and allocate initial roles before the rest of the cluster can function.
:::

4. Save your group mapping to a JSON file. An example file `groupmap.json` looks like this:
   
   ```
   {
      "name": "mygroupmap",
      "groupPattern": "CN=mygroup,CN=Users,DC=example,DC=com",
      "roles": [
         "readRole"
      ]
   }
   ```
   In the example, the LDAP group `mygroup` maps to Druid role `readRole` and the name of the mapping is `mygroupmap`.

5. Use the Druid API to create the group mapping and allocate initial roles according to your JSON file. The following example uses curl to create the mapping defined in `groupmap.json` for the LDAP group `mygroup`:
   
   ```
   curl -i -v  -H "Content-Type: application/json" -u internal -X POST -d @groupmap.json http://localhost:8081/druid-ext/basic-security/authorization/db/ldapauth/groupMappings/mygroupmap
   ```
6. Check that the group mapping was created successfully. The following example request lists all group mappings:

   ```
   curl -i -v  -H "Content-Type: application/json" -u internal -X GET  http://localhost:8081/druid-ext/basic-security/authorization/db/ldapauth/groupMappings
   ```

## Map LDAP groups to Druid roles

Once you've completed the initial setup and mapping, you can map more LDAP groups to Druid roles. Members of an LDAP group get access to the permissions of the corresponding Druid role.

### Create a Druid role

To create a Druid role, you can submit a POST request to the Coordinator process using the Druid REST API or you can use the Druid console.

The examples below use `localhost` as the Coordinator host and `8081` as the port. Amend these properties according to the details of your deployment. 

Example request to create a role named `readRole`:

```
curl -i -v  -H "Content-Type: application/json" -u internal -X POST  http://localhost:8081/druid-ext/basic-security/authorization/db/ldapauth/roles/readRole 
```

Check that Druid created the role successfully. The following example request lists all roles:

```
curl -i -v  -H "Content-Type: application/json" -u internal -X GET  http://localhost:8081/druid-ext/basic-security/authorization/db/ldapauth/roles
```

### Add permissions to the Druid role

Once you have a Druid role you can add permissions to it. The following example adds read-only access to a `wikipedia` data source.

Given the following JSON in a file named `perm.json`:

```
[
	{ "resource": { "name": "wikipedia", "type": "DATASOURCE" }, "action": "READ" },
    { "resource": { "name": ".*", "type": "STATE" }, "action": "READ" },
	{ "resource": {"name": ".*", "type": "CONFIG"}, "action": "READ"}
]
```

The following request associates the permissions in the JSON file with the `readRole` role:

```
curl -i -v  -H "Content-Type: application/json" -u internal -X POST -d@perm.json  http://localhost:8081/druid-ext/basic-security/authorization/db/ldapauth/roles/readRole/permissions
```

Druid users need the `STATE` and `CONFIG` permissions to view the data source in the Druid console. If you only want to assign querying permissions you can apply just the `READ` permission with the first line in the `perm.json` file.

You can also provide the data source name in the form of a regular expression. For example, to give access to all data sources starting with `wiki`, you would specify the data source name as `{ "name": "wiki.*" }` .

### Create the group mapping

You can now map an LDAP group to the Druid role. The following example request creates a mapping with name `mygroupmap`. It assumes that a group named `mygroup` exists in the directory.

```
{
    "name": "mygroupmap",
    "groupPattern": "CN=mygroup,CN=Users,DC=example,DC=com",
    "roles": [
        "readRole"
    ]
}
```

The following example request configures the mapping&mdash;the role mapping is in the file `groupmap.json`. See [Configure Druid for LDAP authentication](#configure-druid-for-ldap-authentication) for the contents of an example file.

```
curl -i -v  -H "Content-Type: application/json" -u internal -X POST -d @groupmap.json http://localhost:8081/druid-ext/basic-security/authorization/db/ldapauth/groupMappings/mygroupmap
```

To check whether the group mapping was created successfully, the following request lists all group mappings:

```
curl -i -v  -H "Content-Type: application/json" -u internal -X GET http://localhost:8081/druid-ext/basic-security/authorization/db/ldapauth/groupMappings
```

The following example request returns the details of the `mygroupmap` group:

```
curl -i -v  -H "Content-Type: application/json" -u internal -X GET http://localhost:8081/druid-ext/basic-security/authorization/db/ldapauth/groupMappings/mygroupmap
```

The following example request adds the role `queryRole` to the `mygroupmap` mapping:

```
curl -i -v  -H "Content-Type: application/json" -u internal -X POST http://localhost:8081/druid-ext/basic-security/authorization/db/ldapauth/groupMappings/mygroup/roles/queryrole
```

### Add an LDAP user to Druid and assign a role

You only need to complete this step if:
- Your LDAP user doesn't belong to any of your LDAP groups, or
- You want to configure a user with additional Druid roles that are not mapped to the LDAP groups that the user belongs to.

Example request to add the LDAP user `myuser` to Druid:

```
curl -i -v  -H "Content-Type: application/json" -u internal -X POST http://localhost:8081/druid-ext/basic-security/authorization/db/ldapauth/users/myuser 
```

Example request to assign the `myuser` user to the `queryRole` role:

```
curl -i -v  -H "Content-Type: application/json" -u internal -X POST http://localhost:8081/druid-ext/basic-security/authorization/db/ldapauth/users/myuser/roles/queryRole
```

## Enable LDAP over TLS (LDAPS)

Once you've configured LDAP authentication in Druid, you can optionally make LDAP traffic confidential and secure by using Transport Layer Security (TLS)&mdash;previously Secure Socket Layer(SSL)&mdash;technology. 

Configuring LDAPS establishes trust between Druid and the LDAP server.

## Prerequisites

Before you start to set up LDAPS in Druid, you must [configure Druid for LDAP authentication](#configure-druid-for-ldap-authentication). You also need:

- A certificate issued by a public certificate authority (CA) or a self-signed certificate by an internal CA.
- The root certificate for the CA that signed the certificate for the LDAP server. If you're using a common public CA, the certificate may already be in the Java truststore. Otherwise you need to import the certificate for the CA.

## Configure Druid for LDAPS

Complete the following steps to set up LDAPS for Druid. See [Configuration reference](../configuration/index.md) for the location of the configuration files. 

1. Import the CA certificate for your LDAP server or a self-signed certificate into the truststore location saved as `druid.client.https.trustStorePath` in your `common.runtime.properties` file.

   ```
   keytool -import -trustcacerts -keystore path/to/cacerts -storepass truststorepassword -alias aliasName -file path/to/certificate.cer
   ```

   Replace `path/to/cacerts` with the path to your truststore, `truststorepassword` with your truststore password, `aliasName` with an alias name for the keystore, and `path/to/certificate.cer` with the location and name of your certificate. For example:

   ```
   keytool -import -trustcacerts -keystore /Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home/jre/lib/security/cacerts -storepass mypassword -alias myAlias -file /etc/ssl/certs/my-certificate.cer
   ```

2. If the root certificate for the CA isn't already in the Java truststore, import it:

   ```
   keytool -importcert -keystore path/to/cacerts -storepass truststorepassword -alias aliasName -file path/to/certificate.cer
   ```

   Replace `path/to/cacerts` with the path to your truststore, `truststorepassword` with your truststore password, `aliasName` with an alias name for the keystore, and `path/to/certificate.cer` with the location and name of your certificate. For example:
	
   ```
   keytool -importcert -keystore /Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home/jre/lib/security/cacerts -storepass mypassword -alias myAlias -file /etc/ssl/certs/my-certificate.cer
   ```

3. In your `common.runtime.properties` file, add the following lines to the LDAP configuration section, substituting your own truststore path and password:

   ```
   druid.auth.basic.ssl.trustStorePath=/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home/jre/lib/security/cacerts
   druid.auth.basic.ssl.protocol=TLS
   druid.auth.basic.ssl.trustStorePassword=xxxxxx
   ```

   See [Druid basic security](../development/extensions-core/druid-basic-security.md#properties-for-ldaps) for details about these properties.

4. You can optionally configure additional LDAPS properties in the `common.runtime.properties` file. See [Druid basic security](../development/extensions-core/druid-basic-security.md#properties-for-ldaps) for more information.

5. Restart Druid.


## Troubleshooting tips

The following are some ideas to help you troubleshoot issues with LDAP and LDAPS.

### Check the coordinator logs

If your LDAP connection isn't working, check the coordinator logs. See [Logging](../configuration/logging.md) for details.

### Check the Druid escalator configuration

If the coordinator is working but the rest of the cluster isn't, check the escalator configuration. See the [Configuration reference](../configuration/index.md) for details. You can also check other service logs to see why the services are unable to fetch authorization details from the coordinator.

### Check your LDAP server response time

If a user can log in to the Druid console but the landing page shows a 401 error, check your LDAP server response time. In a large organization with a high number of LDAP users, LDAP may be slow to respond, and this can result in a connection timeout.
