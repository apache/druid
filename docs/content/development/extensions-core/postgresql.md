---
layout: doc_page
---

# PostgreSQL Metadata Store

Make sure to [include](../../operations/including-extensions.html) `postgresql-metadata-storage` as an extension.

## Setting up PostgreSQL

1. Install PostgreSQL

  Use your favorite package manager to install PostgreSQL, e.g.:
  - on Ubuntu/Debian using apt `apt-get install postgresql`
  - on OS X, using [Homebrew](http://brew.sh/) `brew install postgresql`

2. Create a druid database and user

  On the machine where PostgreSQL is installed, using an account with proper
  postgresql permissions:

  Create a druid user, enter `diurd` when prompted for the password.

  ```bash
  createuser druid -P
  ```

  Create a druid database owned by the user we just created

  ```bash
  createdb druid -O druid
  ```

  *Note:* On Ubuntu / Debian you may have to prefix the `createuser` and
  `createdb` commands with `sudo -u postgres` in order to gain proper
  permissions.

3. Configure your Druid metadata storage extension:

  Add the following parameters to your Druid configuration, replacing `<host>`
  with the location (host name and port) of the database.

  ```properties
  druid.extensions.loadList=["postgresql-metadata-storage"]
  druid.metadata.storage.type=postgresql
  druid.metadata.storage.connector.connectURI=jdbc:postgresql://<host>/druid
  druid.metadata.storage.connector.user=druid
  druid.metadata.storage.connector.password=diurd
  ```

## Configuration
In most cases, the configuration options map directly to the [postgres jdbc connection options](https://jdbc.postgresql.org/documentation/head/connect.html).

|Property|Description|Default|Required|
|--------|-----------|-------|--------|
| useSSL | Enables SSL | `false` | no |
| sslPassword | The [Password Provider](../../operations/password-provider.html) or String password for the client's key. | none | no |
| sslFactory | The class name to as as the `SSLSocketFactory` | none | no |
| sslFactoryArg | An optional argument passed to the sslFactory's constructor | none | no |
| sslMode | The sslMode. Possible values are "disable", "require", "verify-ca", "verify-full", "allow" and "prefer"| none | no |
| sslCert | The full path to the certificate file. | none | no |
| sslKey | The full path to the key file. | none | no |
| sslRootCert | The full path to the root certificate. | none | no |
| sslHostNameVerifier | The classname of the hostname verifier. | none | no |
| sslPasswordCallback | The classname of the SSL password provider. | none | no |
