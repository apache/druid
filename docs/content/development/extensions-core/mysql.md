---
layout: doc_page
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

# MySQL Metadata Store

Make sure to [include](../../operations/including-extensions.html) `mysql-metadata-storage` as an extension.

<div class="note caution">
The MySQL extension requires the MySQL Connector/J library which is not included in the Druid distribution. 
Refer to the following section for instructions on how to install this library.
</div>

## Installing the MySQL connector library

This extension uses Oracle's MySQL JDBC driver which is not included in the Druid distribution and must be
installed separately. There are a few ways to obtain this library:

- It can be downloaded from the MySQL site at: https://dev.mysql.com/downloads/connector/j/
- It can be fetched from Maven Central at: http://central.maven.org/maven2/mysql/mysql-connector-java/5.1.38/mysql-connector-java-5.1.38.jar
- It may be available through your package manager, e.g. as `libmysql-java` on APT for a Debian-based OS

This should fetch a JAR file named similar to 'mysql-connector-java-x.x.xx.jar'.

Copy or symlink this file to `extensions/mysql-metadata-storage` under the distribution root directory.

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
  druid.metadata.storage.connector.password=druid
  ```

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
