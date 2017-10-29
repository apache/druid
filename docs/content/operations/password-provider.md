#### Password Provider

Druid needs some passwords for accessing various secured systems like metadata store, Key Store containing server certificates etc.
All these passwords have corresponding runtime properties associated with them, for example `druid.metadata.storage.connector.password` corresponds to the metadata store password.

By default users can directly set the passwords in plaintext for these runtime properties, for example `druid.metadata.storage.connector.password=pwd` sets the metadata store password
to be used by Druid to connect to metadata store to `pwd`. Apart from this, users can use environment variables to get password in following way -

Environment variable password provider provides password by looking at specified environment variable. Use this in order to avoid specifying password in runtime.properties file.
e.g

```json
{ "type": "environment", "variable": "METADATA_STORAGE_PASSWORD" }
```

The values are described below.

|Field|Type|Description|Required|
|-----|----|-----------|--------|
|`type`|String|password provider type|Yes: `environment`|
|`variable`|String|environment variable to read password from|Yes|

However, many times users may want their own way to optionally securely fetch password during runtime of the Druid process.
Druid allows this by users to implement their own `PasswordProvider` interface and create a Druid extension to register this implementation at Druid process startup.
Please have a look at "Adding a new Password Provider implementation" on this [page](../development/modules.html) to learn more.

To use this implementation, simply set the relevant password runtime property to something similar as was done for Environment variable password provider like -

```json
{ "type": "<registered_password_provider_name>", "<jackson_property>": "<value>", ... }
```