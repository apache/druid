---
id: auth-ldap
title: "LDAP auth"
---

This page describes how to set up Druid user authentication and authorizion through LDAP. The first step is to enable LDAP authentication and authorization for Druid. You can then map an LDAP group to roles and assign permissions and users to roles.

## Enable LDAP in Druid

Before starting, verify that the active directory is reachable from the Druid Master servers. Command line tools such as `ldapsearch` and `ldapwhoami`, a part of OpenLDAP, are useful for this testing. 

### Check the connection

First test that the basic connection and user credential works. For example, given a user `uuser1@example.com`, try:

```bash
ldapwhoami -vv -H ldap://<ip_address>:389  -D"uuser1@example.com" -W
```

Enter the password associated with the user when prompted and verify command success. If unsuccessful, note the following frequently occuring issues:  

* Verify that you've used the correct port for your LDAP instance. By default, the LDAP port is 389, but doublecheck with your LDAP admin if unable to connect. 
* Check whether a network firewall is not preventing connections to the LDAP port.
* Check whether LDAP clients need to be specifically whitelisted at the LDAP server to be able to reach it. If so, add the Druid Coordinator server to the AD whitelist. 


### Check the search criteria

After verifying basic connectivity, check your search criteria. For example, the command for searching for user `uuser1@example.com ` is the following: 

```bash
ldapsearch -x -W -H ldap://<ldap_server>  -D"uuser1@example.com" -b "dc=example,dc=com" "(sAMAccountName=uuser1)"
```

In the results, note the `memberOf` attribute; it shows the groups the user belongs. You will use this value to map from the LDAP group to the Druid roles later. The sAMAccountName attribute provides for user authentication. 

## Configure Druid user authentication with LDAP/Active Directory 

Enable druid-basic-security  under common.runtime.properties and need to be updated in all the nodes in the druid cluster.  This file is located under conf dir . For the quickstart , this file is present under conf-quickstart/druid/_common/common.runtime.properties
Make sure druid.extensions.loadList have druid-basic-security 
Update the following properties  in the common.runtime.properties 
Create a user in LDAP which is used for internal communication . In the config below we used a user named "internal@example.com". Use the same user as the initial admin user.

```
druid.auth.authenticatorChain=["ldap"]
druid.auth.authenticator.ldap.type=basic
druid.auth.authenticator.ldap.enableCacheNotifications=true
druid.auth.authenticator.ldap.credentialsValidator.type=ldap
druid.auth.authenticator.ldap.credentialsValidator.url=ldap://<AD host>:<AD port>
druid.auth.authenticator.ldap.credentialsValidator.bindUser=<AD admin user eg: Administrator@example.com>
druid.auth.authenticator.ldap.credentialsValidator.bindPassword=<AD admin password>
druid.auth.authenticator.ldap.credentialsValidator.baseDn=<base dn eg: dc=example,dc=com>
druid.auth.authenticator.ldap.credentialsValidator.userSearch=< this we get the from ldap search eg:(&(sAMAccountName=%s)(objectClass=user))>
druid.auth.authenticator.ldap.credentialsValidator.userAttribute=sAMAccountName
druid.auth.authenticator.ldap.authorizerName=ldapauth
druid.escalator.type=basic
druid.escalator.internalClientUsername=<AD interal user eg:internal>
druid.escalator.internalClientPassword=Welcome123
druid.escalator.authorizerName=ldapauth
druid.auth.authorizers=["ldapauth"]
druid.auth.authorizer.ldapauth.type=basic
druid.auth.authorizer.ldapauth.initialAdminUser=AD user which can act as initial admin user eg: internal>
druid.auth.authorizer.ldapauth.initialAdminRole=admin
druid.auth.authorizer.ldapauth.roleProvider.type=ldap
```

3. Make use of LDAP groups to assign roles.
LDAP groups can be mapped to a role in Druid. Members in a group will get access to the permissions of the corresponding role. 

Before creating the group mapping in druid we need to have the role created .  Use the druid rest api to create the role.  Use a user with admin user role to create the role. In the sample config we have used user : internal@example.com as the admin user, hence we can use the same user to create the role.

Step 1: Creating a Role if not exists

Creating a role is a POST request to the coordinator.  In this article, we make use of curl command to call the rest API. Alternatively, you can use other REST clients like postman, etc.

Below are the REST APIs to create the role to read access for datasource, config, state.

Note:  The REST API need to call the coordinator node. The examples used in this article use localhost as the coordinator host and 8081 as coordinator port. Please change this according to your deployment.

Call the following API to create role `readRole` . 

curl -i -v  -H "Content-Type: application/json" -u internal -X POST  http://localhost:8081>/druid-ext/basic-security/authorization/db/ldapauth/roles/readRole 
For advanced role permission for each resourceAction please ref to the documentation link

Check the role is created successfully by executing the below rest api

curl -i -v  -H "Content-Type: application/json" -u internal -X GET  http://localhost:8081/druid-ext/basic-security/authorization/db/ldapauth/roles
Step 2: Add permission to Role 

Add multiple permission to the role . In this example, we are going to add a read-only access to a `wikipedia` data source. 

Execute the following rest API to assign permission to the role. 

Save the following JSON into a file perm.json

```
[{ "resource": { "name": "wikipedia", "type": "DATASOURCE" }, "action": "READ" }
,{ "resource": { "name": ".*", "type": "STATE" }, "action": "READ" },
{ "resource": {"name": ".*", "type": "CONFIG"}, "action": "READ"}]
curl -i -v  -H "Content-Type: application/json" -u internal -X POST -d@perm.json  http://localhost:8081/druid-ext/basic-security/authorization/db/ldapauth/roles/readRole/permissions
```

Execute the above command to assign  the permission to role.

The state and config permission is only if you want to see the data source in the druid console. Otherwise for querying the data source only  below entry is enough 

```
[{ "resource": { "name": "wikipedia", "type": "DATASOURCE" }, "action": "READ" }]
```

You can also provide the name in the form of regular expression . eg to give access to all the data sources starting with wiki give name as  { "name": "wiki.*", .....


Step 3: Create group Mapping 

In this article we make use of LDAP group : "group1" to assign the role.  The "group1" is created in the directory and is the prerequisite for this step.

Use the Druid API to create the group mapping and allocate initial roles.  Creating a group mapping requires the below payload and saved it in a file  groupmap.json 

```
{
    "name": "group1map",
    "groupPattern": "CN=group1,CN=Users,DC=example,DC=com",
    "roles": [
        "readRole"
    ]
}
```
Call the API for groupMapping as below

```
curl -i -v  -H "Content-Type: application/json" -u internal -X POST -d @groupmap.json http://localhost:8081/druid-ext/basic-security/authorization/db/ldapauth/groupMappings/group1map
```

Check if the group mapping is created successfully by executing the following API. This will list all the group mappings.

```
curl -i -v  -H "Content-Type: application/json" -u internal -X GET http://localhost:8081/druid-ext/basic-security/authorization/db/ldapauth/groupMappings
```

To check the details of a specific group mapping use the following API

```
curl -i -v  -H "Content-Type: application/json" -u internal -X GET http://localhost:8081/druid-ext/basic-security/authorization/db/ldapauth/groupMappings/group1map
```

To add additional roles to the group mapping use the following API 

```
curl -i -v  -H "Content-Type: application/json" -u internal -X POST http://localhost:8081/druid-ext/basic-security/authorization/db/ldapauth/groupMappings/group1/roles/<newrole> 
```

Assign roles for individual LDAP user 
Once security is configured to authenticate the LDAP the user will only verify the user password with LDAP. One of the prerequisites is to add the LDAP user to Druid. 

To add a user use the below authentication API

```
curl -i -v  -H "Content-Type: application/json" -u internal -X POST http://localhost:8081/druid-ext/basic-security/authentication/db/ldap/users/<AD user> 
```

Role Assignment : 
Use the below API to assign the role to a user 

```
curl -i -v  -H "Content-Type: application/json" -u internal -X POST http://localhost:8081/druid-ext/basic-security/authorization/db/ldapauth/users/<AD user>/roles/<rolename> 
```

Troubleshooting

1. Unable to reach LDAP from druid node :
   * The default LDAP port is 389, check if the LDAP/AD server is running in 389
   * Check if the network firewall allows connecting to the LDAP port.
   * Check with the LDAP admin if whitelisting the LDAP client is required. In that case, add the coordinator node to AD whitelist. 
