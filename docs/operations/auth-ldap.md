---
id: auth-ldap
title: "LDAP auth"
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


This page describes how to set up Druid user authentication and authorization through LDAP. The first step is to enable LDAP authentication and authorization for Druid. You then map an LDAP group to roles and assign permissions to roles.

## Enable LDAP in Druid

Before starting, verify that the active directory is reachable from the Druid Master servers. Command line tools such as `ldapsearch` and `ldapwhoami`, which are included with OpenLDAP, are useful for this testing. 

### Check the connection

First test that the basic connection and user credential works. For example, given a user `uuser1@example.com`, try:

```bash
ldapwhoami -vv -H ldap://<ip_address>:389  -D"uuser1@example.com" -W
```

Enter the password associated with the user when prompted and verify that the command succeeded. If it didn't, try the following troubleshooting steps:  

* Verify that you've used the correct port for your LDAP instance. By default, the LDAP port is 389, but double-check with your LDAP admin if unable to connect. 
* Check whether a network firewall is not preventing connections to the LDAP port.
* Check whether LDAP clients need to be specifically whitelisted at the LDAP server to be able to reach it. If so, add the Druid Coordinator server to the AD whitelist. 


### Check the search criteria

After verifying basic connectivity, check your search criteria. For example, the command for searching for user `uuser1@example.com ` is as follows: 

```bash
ldapsearch -x -W -H ldap://<ldap_server>  -D"uuser1@example.com" -b "dc=example,dc=com" "(sAMAccountName=uuser1)"
```

Note the `memberOf` attribute in the results; it shows the groups that the user belongs to. You will use this value to map the LDAP group to the Druid roles later. This attribute may be implemented differently on different types of LDAP servers. For instance, some LDAP servers may support recursive groupings, and some may not. Some LDAP server implementations may not have any object classes that contain this attribute altogether. If your LDAP server does not use the `memberOf` attribute, then Druid will not be able to determine a user's group membership using LDAP. The sAMAccountName attribute used in this example contains the authenticated user identity. This is an attribute of an object class specific to Microsoft Active Directory. The object classes and attribute used in your LDAP server may be different.

## Configure Druid user authentication with LDAP/Active Directory 

1. Enable the `druid-basic-security` extension in the `common.runtime.properties` file. See [Security Overview](security-overview.md) for details.   
2. As a best practice, create a user in LDAP to be used for internal communication with Druid. 
3. In `common.runtime.properties`, update LDAP-related properties, as shown in the following listing: 
	```
	druid.auth.authenticatorChain=["ldap"]
	druid.auth.authenticator.ldap.type=basic
	druid.auth.authenticator.ldap.enableCacheNotifications=true
	druid.auth.authenticator.ldap.credentialsValidator.type=ldap
	druid.auth.authenticator.ldap.credentialsValidator.url=ldap://<AD host>:<AD port>
	druid.auth.authenticator.ldap.credentialsValidator.bindUser=<AD admin user, e.g.: Administrator@example.com>
	druid.auth.authenticator.ldap.credentialsValidator.bindPassword=<AD admin password>
	druid.auth.authenticator.ldap.credentialsValidator.baseDn=<base dn, e.g.: dc=example,dc=com>
	druid.auth.authenticator.ldap.credentialsValidator.userSearch=<The LDAP search, e.g.: (&(sAMAccountName=%s)(objectClass=user))>
	druid.auth.authenticator.ldap.credentialsValidator.userAttribute=sAMAccountName
	druid.auth.authenticator.ldap.authorizerName=ldapauth
	druid.escalator.type=basic
	druid.escalator.internalClientUsername=<AD internal user, e.g.: internal@example.com>
	druid.escalator.internalClientPassword=Welcome123
	druid.escalator.authorizerName=ldapauth
	druid.auth.authorizers=["ldapauth"]
	druid.auth.authorizer.ldapauth.type=basic
	druid.auth.authorizer.ldapauth.initialAdminUser=AD user who acts as the initial admin user, e.g.: internal@example.com>
	druid.auth.authorizer.ldapauth.initialAdminRole=admin
	druid.auth.authorizer.ldapauth.roleProvider.type=ldap
   ```

   Notice that the LDAP user created in the previous step, `internal@example.com`, serves as the internal client user and the initial admin user.

## Use LDAP groups to assign roles

You can map LDAP groups to a role in Druid. Members in the group get access to the permissions of the corresponding role. 


### Step 1: Create a role 

First create the role in Druid using the Druid REST API.

Creating a role involves submitting a POST request to the Coordinator process. 

The following REST APIs to create the role to read access for datasource, config, state.

> As mentioned, the REST API calls need to address the Coordinator node. The examples used below use localhost as the Coordinator host and 8081 as the port. Adjust these settings according to your deployment.

Call the following API to create role `readRole` . 

```
curl -i -v  -H "Content-Type: application/json" -u internal -X POST  http://localhost:8081/druid-ext/basic-security/authorization/db/ldapauth/roles/readRole 
```

Check that the role has been created successfully by entering the following:

```
curl -i -v  -H "Content-Type: application/json" -u internal -X GET  http://localhost:8081/druid-ext/basic-security/authorization/db/ldapauth/roles
```


### Step 2: Add permissions to a role 

You can now add one or more permission to the role. The following example adds read-only access to a `wikipedia` data source.  

Given the following JSON in a file named `perm.json`:

```
[{ "resource": { "name": "wikipedia", "type": "DATASOURCE" }, "action": "READ" }
,{ "resource": { "name": ".*", "type": "STATE" }, "action": "READ" },
{ "resource": {"name": ".*", "type": "CONFIG"}, "action": "READ"}]
```

The following command associates the permissions in the JSON file with the role 

```
curl -i -v  -H "Content-Type: application/json" -u internal -X POST -d@perm.json  http://localhost:8081/druid-ext/basic-security/authorization/db/ldapauth/roles/readRole/permissions
```

Note that the STATE and CONFIG permissions in `perm.json` are needed to see the data source in the web console. If only querying permissions are needed, the READ action is sufficient:

```
[{ "resource": { "name": "wikipedia", "type": "DATASOURCE" }, "action": "READ" }]
```

You can also provide the name in the form of regular expression. For example, to give access to all data sources starting with `wiki`, specify the name as  `{ "name": "wiki.*", .....`. 


### Step 3: Create group Mapping 

The following shows an example of a group to role mapping. It assumes that a group named `group1` exists in the directory. Also assuming the following role mapping in a file named `groupmap.json`:

```
{
    "name": "group1map",
    "groupPattern": "CN=group1,CN=Users,DC=example,DC=com",
    "roles": [
        "readRole"
    ]
}
```

You can configure the mapping as follows:

```
curl -i -v  -H "Content-Type: application/json" -u internal -X POST -d @groupmap.json http://localhost:8081/druid-ext/basic-security/authorization/db/ldapauth/groupMappings/group1map
```

To check whether the group mapping was created successfully, run the following command:

```
curl -i -v  -H "Content-Type: application/json" -u internal -X GET http://localhost:8081/druid-ext/basic-security/authorization/db/ldapauth/groupMappings
```

To check the details of a specific group mapping, use the following:

```
curl -i -v  -H "Content-Type: application/json" -u internal -X GET http://localhost:8081/druid-ext/basic-security/authorization/db/ldapauth/groupMappings/group1map
```

To add additional roles to the group mapping, use the following API:

```
curl -i -v  -H "Content-Type: application/json" -u internal -X POST http://localhost:8081/druid-ext/basic-security/authorization/db/ldapauth/groupMappings/group1/roles/<newrole> 
```

In the next two steps you will be creating a user, and assigning previously created roles to it. These steps are only needed in the following cases: 
                                                                                                 
 - Your LDAP server does not support the `memberOf` attribute, or 
 - You want to configure a user with additional roles that are not mapped to the group(s) that the user is a member of
 
 If this is not the case for your scenario, you can skip these steps.

### Step 4. Create a user

Once LDAP is enabled, only user passwords are verified with LDAP. You add the LDAP user to Druid as follows: 

```
curl -i -v  -H "Content-Type: application/json" -u internal -X POST http://localhost:8081/druid-ext/basic-security/authentication/db/ldap/users/<AD user> 
```

### Step 5. Assign the role to the user 

The following command shows how to assign a role to a user:

```
curl -i -v  -H "Content-Type: application/json" -u internal -X POST http://localhost:8081/druid-ext/basic-security/authorization/db/ldapauth/users/<AD user>/roles/<rolename> 
```

For more information about security and the basic security extension, see [Security Overview](security-overview.md). 
