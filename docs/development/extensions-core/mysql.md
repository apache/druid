---
id: mysql
title: "MySQL Metadata Store"
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


To use this Apache Druid extension, [include](../../configuration/extensions.md#loading-extensions) `mysql-metadata-storage` in the extensions load list.

:::info
 The MySQL extension requires the MySQL Connector/J library or MariaDB Connector/J library, neither of which are included in the Druid distribution.
 Refer to the following section for instructions on how to install this library.
:::

## Installing the MySQL connector library

This extension can use Oracle's MySQL JDBC driver which is not included in the Druid distribution. You must
install it separately. There are a few ways to obtain this library:

- It can be downloaded from the MySQL site at: https://dev.mysql.com/downloads/connector/j/
- It can be fetched from Maven Central at: https://repo1.maven.org/maven2/mysql/mysql-connector-java/5.1.49/mysql-connector-java-5.1.49.jar
- It may be available through your package manager, e.g. as `libmysql-java` on APT for a Debian-based OS

This fetches the MySQL connector JAR file with a name like `mysql-connector-java-5.1.49.jar`.

Copy or symlink this file inside the folder `extensions/mysql-metadata-storage` under the distribution root directory.

## Alternative: Installing the MariaDB connector library

This extension also supports using the MariaDB connector jar, though it is also not included in the Druid distribution, so you must install it separately.

- Download from the MariaDB site: https://mariadb.com/downloads/connector
- Download from Maven Central: https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/2.7.3/mariadb-java-client-2.7.3.jar

This fetches the MariaDB connector JAR file with a name like `maria-java-client-2.7.3.jar`.

Copy or symlink this file to `extensions/mysql-metadata-storage` under the distribution root directory.

To configure the `mysql-metadata-storage` extension to use the MariaDB connector library instead of MySQL, set `druid.metadata.mysql.driver.driverClassName=org.mariadb.jdbc.Driver`.

Depending on the MariaDB client library version, the connector supports both `jdbc:mysql:` and `jdbc:mariadb:` connection URIs. However, the parameters to configure the connection vary between implementations, so be sure to [check the documentation](https://mariadb.com/kb/en/about-mariadb-connector-j/#connection-strings) for details.


## Setting up MySQL

To avoid issues with upgrades that require schema changes to a large metadata table, consider a MySQL version that supports instant ADD COLUMN semantics. For example, MySQL 8.

1. Install MySQL

  Use your favorite package manager to install mysql, e.g.:
  - on Ubuntu/Debian using apt `apt-get install mysql-server`
  - on OS X, using [Homebrew](http://brew.sh/) `brew install mysql`

  Alternatively, download and follow installation instructions for MySQL
  Community Server here:
  [http://dev.mysql.com/downloads/mysql/](http://dev.mysql.com/downloads/mysql/).

This extension also supports using MariaDB server, https://mariadb.org/download/, substituting for MariaDB in the following instructions where appropriate.

2. Create a druid database and user

  Connect to MySQL from the machine where it is installed.

  ```bash
  mysql -u root
  ```

  Paste the following snippet into the mysql prompt:

  ```sql
  -- create a druid database, make sure to use utf8mb4 as encoding
  CREATE DATABASE druid DEFAULT CHARACTER SET utf8mb4;

  -- create a druid user
  CREATE USER 'druid'@'localhost' IDENTIFIED BY 'diurd';

  -- grant the user all the permissions on the database we just created
  GRANT ALL PRIVILEGES ON druid.* TO 'druid'@'localhost';
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

If using the MariaDB connector library, set `druid.metadata.mysql.driver.driverClassName=org.mariadb.jdbc.Driver`.

## Encrypting MySQL connections
  This extension provides support for encrypting MySQL connections. To get more information about encrypting MySQL connections using TLS/SSL in general, please refer to this [guide](https://dev.mysql.com/doc/refman/5.7/en/using-encrypted-connections.html).

## Configuration

|Property|Description|Default|Required|
|--------|-----------|-------|--------|
|`druid.metadata.mysql.ssl.useSSL`|Enable SSL|`false`|no|
|`druid.metadata.mysql.ssl.clientCertificateKeyStoreUrl`|The file path URL to the client certificate key store.|none|no|
|`druid.metadata.mysql.ssl.clientCertificateKeyStoreType`|The type of the key store where the client certificate is stored.|none|no|
|`druid.metadata.mysql.ssl.clientCertificateKeyStorePassword`|The [Password Provider](../../operations/password-provider.md) or String password for the client key store.|none|no|
|`druid.metadata.mysql.ssl.verifyServerCertificate`|Enables server certificate verification.|false|no|
|`druid.metadata.mysql.ssl.trustCertificateKeyStoreUrl`|The file path to the trusted root certificate key store.|Default trust store provided by MySQL|yes if `verifyServerCertificate` is set to true and a custom trust store is used|
|`druid.metadata.mysql.ssl.trustCertificateKeyStoreType`|The type of the key store where trusted root certificates are stored.|JKS|yes if `verifyServerCertificate` is set to true and keystore type is not JKS|
|`druid.metadata.mysql.ssl.trustCertificateKeyStorePassword`|The [Password Provider](../../operations/password-provider.md) or String password for the trust store.|none|yes if `verifyServerCertificate` is set to true and password is not null|
|`druid.metadata.mysql.ssl.enabledSSLCipherSuites`|Overrides the existing cipher suites with these cipher suites.|none|no|
|`druid.metadata.mysql.ssl.enabledTLSProtocols`|Overrides the TLS protocols with these protocols.|none|no|

### MySQL InputSource

```json
{
  "type": "index_parallel",
  "spec": {
    "dataSchema": {
      "dataSource": "some_datasource",
      "dimensionsSpec": {
        "dimensionExclusions": [],
        "dimensions": [
          "dim1",
          "dim2",
          "dim3"
        ]
      },
      "timestampSpec": {
        "format": "auto",
        "column": "ts"
      },
      "metricsSpec": [],
      "granularitySpec": {
        "type": "uniform",
        "segmentGranularity": "DAY",
        "queryGranularity": {
          "type": "none"
        },
        "rollup": false,
        "intervals": null
      },
      "transformSpec": {
        "filter": null,
        "transforms": []
      }
    },
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "sql",
        "database": {
          "type": "mysql",
          "connectorConfig": {
            "connectURI": "jdbc:mysql://some-rds-host.us-west-1.rds.amazonaws.com:3306/druid",
            "user": "admin",
            "password": "secret"
          }
        },
        "sqls": [
          "SELECT * FROM some_table"
        ]
      },
      "inputFormat": {
        "type": "json"
      }
    },
    "tuningConfig": {
      "type": "index_parallel"
    }
  }
}
```
