---
id: druid-opa-authorizer
title: "Open Policy Agent (OPA) Authorizer"
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

To use this Apache Druid extension, [include](../../configuration/extensions.md#loading-extensions) `druid-opa-authorizer` in the extensions load list.

This extension requests policy decisions from [Open Policy Agent](https://www.openpolicyagent.org/) (OPA).

## Configuration

The OPA authorizer needs to be referenced in your authenticator. The OPA authorizer is configured like so:

|Property|Description|Default|Required|
|--------|-----------|-------|--------|
|`druid.auth.authorizer.<authorizerName>.type`|Must be `opa`.|none|yes|
|`druid.auth.authorizer.<authorizerName>.opaUri`|The URI for the OPA server (e.g. `http://<host>:<port>/v1/data/my/druid/allow`).|none|yes|

## Write Rego rules

The authorizer will send a request to the `opaUri` specified in the configuration. The input will be:

```json
{
    "authenticationResult": {
        "identity": "username",
        "authorizerName": "authorizerName",
        "authenticatedBy": "authenticatorName",
        "context": null
    },
    "action": "READ|WRITE",
    "resource": {
        "name": "resourceName",
        "type": "resourceType"
    }
}
```

For the details, especially the resources types, consult the Druid documentation on the [Authentication and Authorization Model](../../operations/security-user-auth.md#authentication-and-authorization-model).

Inside your Rego rules, this snippet of data will be available as `input`. 
For the details on how to write Rego rules, have a look at the [OPA documentation](https://www.openpolicyagent.org/docs/latest/).

## Example: Setting up OPA locally to test

The `druid-opa-authorizer` extension source contains an [example](https://github.com/apache/druid/tree/{{DRUIDVERSION}}/extensions-contrib/druid-opa-authorizer/example/) directory with some test files.

### Run a local OPA server with example files

Download the [OPA binary](https://www.openpolicyagent.org/docs/latest/#running-opa) into the `extensions-contrib/druid-opa-authorizer/example/` directory and follow the instructions (setting execute permissions).

Start the server with the provided example files: `./opa run -s druid.rego druid.json`.
By default, the server will then run on port `8181`.

The example files define 6 users and 4 roles, with rules that work based on the roles. 
There is a fifth special `admin` role that grants full access to everything.

### Configure Druid

In Druid, in your common `runtime.properties`:
- Add `druid-opa-authorizer` and `druid-basic-security` extensions to the load list:
```properties
druid.extensions.loadList=[..., "druid-opa-authorizer", "druid-basic-security"]
```
- Add the following entries:
```properties
# Druid basic security
druid.auth.authenticatorChain=["basicAuthenticator"]
druid.auth.authenticator.basicAuthenticator.type=basic

# Default password for 'admin' user, should be changed for production.
druid.auth.authenticator.basicAuthenticator.initialAdminPassword=password1

# Default password for internal 'druid_system' user, should be changed for production.
druid.auth.authenticator.basicAuthenticator.initialInternalClientPassword=password2

# Uses the metadata store for storing users, you can use authentication API to create new users and grant permissions
druid.auth.authenticator.basicAuthenticator.credentialsValidator.type=metadata

# If true and the request credential doesn't exist in this credentials store, the request will proceed to next Authenticator in the chain.
druid.auth.authenticator.basicAuthenticator.skipOnFailure=false
druid.auth.authenticator.basicAuthenticator.authorizerName=opaAuthorizer

# Escalator
druid.escalator.type=basic
druid.escalator.internalClientUsername=druid_system
druid.escalator.internalClientPassword=password2
druid.escalator.authorizerName=opaAuthorizer

druid.auth.authorizers=["opaAuthorizer"]
druid.auth.authorizer.opaAuthorizer.type=opa
druid.auth.authorizer.opaAuthorizer.opaUri=http://localhost:8181/v1/data/app/druid/allow
```

### Setup Users and Verify

Run the `setup.sh` script in the `extensions-contrib/druid-opa-authorizer/example/setup/` directory to create the example users.
Five users will be created: alice, bob, christy, dylan, and eve. The password for each user is the same as their username.

When connecting to the dashboard you will now be prompted to log in. 
- If you log in with alice (admin), you will be able to access everything. 
- If you log in with eve (no access grants) you should see 403 errors inside the Druid Console.
- If you log in with christy (Datasource and State read grants), you should be able to see the Services tab in the Druid Console, but not the cluster configs.


## Troubleshooting

If you get 401/403 type errors, check the OPA logs (you might need to enable debug level).

If you get 500 type errors, it might be that the internal `druid_system` user doesn't have full permissions.

You can increase log output for the authorizer by adding this snippet to your `log4j2.xml`:

```xml
<Logger name="org.apache.druid.security.opa.OpaAuthorizer" level="trace" additivity="false">
  <Appender-ref ref="Console"/>
</Logger>
```

## Example: Testing with LDAP Authentication

To test LDAP authentication with the OPA authorizer, you can use the provided LDAP example files in the `extensions-contrib/druid-opa-authorizer/example/ldap/` directory.

### Run a local LDAP server

Navigate to the `extensions-contrib/druid-opa-authorizer/example/ldap/` directory and run the `run-ldap.sh` script to start a mock LDAP server:

```bash
./run-ldap.sh
```

This will start an OpenLDAP server on port `8389` with the users `alice`, `bob`, `christy`, `dylan`, `eve`, and `druid_system`.

### Configure Druid for LDAP and OPA

In your common `runtime.properties`, replace the `basicAuthenticator` configuration with the following:

```properties
# Druid basic security with LDAP
druid.auth.authenticatorChain=["ldapAuthenticator"]
druid.auth.authenticator.ldapAuthenticator.type=basic

# LDAP Validator Configuration
druid.auth.authenticator.ldapAuthenticator.credentialsValidator.type=ldap
druid.auth.authenticator.ldapAuthenticator.credentialsValidator.url=ldap://localhost:8389
druid.auth.authenticator.ldapAuthenticator.credentialsValidator.bindUser=cn=admin,dc=example,dc=org
druid.auth.authenticator.ldapAuthenticator.credentialsValidator.bindPassword=admin
druid.auth.authenticator.ldapAuthenticator.credentialsValidator.baseDn=ou=Users,dc=example,dc=org
druid.auth.authenticator.ldapAuthenticator.credentialsValidator.userSearch=(&(uid=%s)(objectClass=inetOrgPerson))
druid.auth.authenticator.ldapAuthenticator.credentialsValidator.userAttribute=uid

# Redirect to OPA authorizer
druid.auth.authenticator.ldapAuthenticator.authorizerName=opaAuthorizer

# Escalator using LDAP system user
druid.escalator.type=basic
druid.escalator.internalClientUsername=druid_system
druid.escalator.internalClientPassword=password2
druid.escalator.authorizerName=opaAuthorizer

# OPA Authorizer
druid.auth.authorizers=["opaAuthorizer"]
druid.auth.authorizer.opaAuthorizer.type=opa
druid.auth.authorizer.opaAuthorizer.opaUri=http://localhost:8181/v1/data/app/druid/allow
```

### Verify

Log in to the Druid Console as `alice` with password `alice`. OPA will receive the identity `alice` and authorize based on the roles defined in your OPA policy.

