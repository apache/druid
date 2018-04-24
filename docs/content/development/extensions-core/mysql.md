---
layout: doc_page
---

# MySQL Metadata Store

Make sure to [include](../../operations/including-extensions.html) `mysql-metadata-storage` as an extension.

## Setting up MySQL

1. Install MySQL

  Use your favorite package manager to install mysql, e.g.:
  - on Ubuntu/Debian using apt `apt-get install mysql-server`
  - on OS X, using [Homebrew](http://brew.sh/) `brew install mysql`

  Alternatively, download and follow installation instructions for MySQL
  Community Server here:
  [http://dev.mysql.com/downloads/mysql/](http://dev.mysql.com/downloads/mysql/)

2. Create a druid database and user

  Connect to MySQL from the machine where it is installed.

  ```bash
  > mysql -u root
  ```

  Paste the following snippet into the mysql prompt:

  ```sql
  -- create a druid database, make sure to use utf8mb4 as encoding
  CREATE DATABASE druid DEFAULT CHARACTER SET utf8mb4;

  -- create a druid user, and grant it all permission on the database we just created
  GRANT ALL ON druid.* TO 'druid'@'localhost' IDENTIFIED BY 'diurd';
  ```

3. Configure your Druid metadata storage extension:

  Add the following parameters to your Druid configuration, replacing `<host>`
  with the location (host name and port) of the database.

  ```properties
  druid.extensions.loadList=["mysql-metadata-storage"]
  druid.metadata.storage.type=mysql
  druid.metadata.storage.connector.connectURI=jdbc:mysql://<host>/druid
  druid.metadata.storage.connector.user=druid
  druid.metadata.storage.connector.password=diurd
  ```

  Note: the metadata storage extension is not packaged within the main Druid tarball; it is
  packaged in a separate tarball that can be downloaded from [here](http://druid.io/downloads.html).
  You can also get it using [pull-deps](../../operations/pull-deps.html), or you can build
  it from source code; see [Build from Source](../build.html).


## Encrypting MySQL connections
  This extension provides support for encrypting MySQL connections. To get more information about encrypting MySQL connections using TLS/SSL in general, please refer to this [guide](https://dev.mysql.com/doc/refman/5.7/en/using-encrypted-connections.html).

## Configuration

|Property|Description|Default|Required|
|--------|-----------|-------|--------|
|`druid.metadata.mysql.ssl.useSSL`|Enable SSL|`false`|no|
|`druid.metadata.mysql.ssl.clientCertificateKeyStoreUrl`|The file path URL to the client certificate key store.|none|no|
|`druid.metadata.mysql.ssl.clientCertificateKeyStoreType`|The type of the key store where the client certificate is stored.|none|no|
|`druid.metadata.mysql.ssl.clientCertificateKeyStorePassword`|The [Password Provider](../../operations/password-provider.html) or String password for the client key store.|none|no|
|`druid.metadata.mysql.ssl.verifyServerCertificate`|Enables server certificate verification.|false|no|
|`druid.metadata.mysql.ssl.trustCertificateKeyStoreUrl`|The file path to the trusted root certificate key store.|Default trust store provided by MySQL|yes if `verifyServerCertificate` is set to true and a custom trust store is used|
|`druid.metadata.mysql.ssl.trustCertificateKeyStoreType`|The type of the key store where trusted root certificates are stored.|JKS|yes if `verifyServerCertificate` is set to true and keystore type is not JKS|
|`druid.metadata.mysql.ssl.trustCertificateKeyStorePassword`|The [Password Provider](../../operations/password-provider.html) or String password for the trust store.|none|yes if `verifyServerCertificate` is set to true and password is not null|
|`druid.metadata.mysql.ssl.enabledSSLCipherSuites`|Overrides the existing cipher suites with these cipher suites.|none|no|
|`druid.metadata.mysql.ssl.enabledTLSProtocols`|Overrides the TLS protocols with these protocols.|none|no|

