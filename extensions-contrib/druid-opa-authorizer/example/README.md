# Setting up OPA locally to test

This document will guide you through downloading OPA, configuring it with a sample config and configuring your Druid instance to connect to it.

## Run a local OPA server with example files

1. Download the [OPA binary](https://www.openpolicyagent.org/docs/latest/#running-opa) into this directory and follow the instructions (setting execute permissions).
2. Start the server with the provided example files: `./opa run -s druid.rego druid.json`.

By default the server will then run on port `8181`.

The example files define 6 users and 4 roles, with rules that work based on the roles. There is a fifth special `admin` role that grants full access to everything.

## Install the Extension

TODO

## Configure Druid & Setup Users

In Druid in your common `runtime.properties` add the following:

    # Druid basic security
    druid.auth.authenticatorChain=["basicAuthenticator"]
    druid.auth.authenticator.basicAuthenticator.type=basic
    
    # Default password for 'admin' user, should be changed for production.
    druid.auth.authenticator.basicAuthenticator.initialAdminPassword=password1
    
    # Default password for internal 'druid_system' user, should be changed for production.
    druid.auth.authenticator.basicAuthenticator.initialInternalClientPassword=password2
    
    # Uses the metadata store for storing users, you can use authentication API to create new users and grant permissions
    druid.auth.authenticator.basicAuthenticator.credentialsValidator.type=metadata
    
    # If true and the request credential doesn't exists in this credentials store, the request will proceed to next Authenticator in the chain.
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

Then run the `setup.sh` script in the `setup` directory to create the example users. 5 users will be created: alice, bob, christy, dylan and eve. The password for each user is the same as their username.

When connecting to the dashboard you will now be prompted to log in. If you log in with Alice - the admin - you will be able to access everything. If you login with Eve (she wasn't granted access to the dashboard) you should see 403 errors in the info boxes.
