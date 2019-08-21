---
id: sqlserver
title: "Microsoft SQLServer"
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


To use this Apache Druid (incubating) extension, make sure to [include](../../development/extensions.md#loading-extensions) `sqlserver-metadata-storage` as an extension.

## Setting up SQLServer

1. Install Microsoft SQLServer

2. Create a druid database and user

  Create the druid user
  - Microsoft SQL Server Management Studio - Security - Logins - New Login...
  - Create a druid user, enter `diurd` when prompted for the password.

  Create a druid database owned by the user we just created
  - Databases - New Database
  - Database Name: druid, Owner: druid

3. Add the Microsoft JDBC library to the Druid classpath
  - To ensure the com.microsoft.sqlserver.jdbc.SQLServerDriver class is loaded you will have to add the appropriate Microsoft JDBC library (sqljdbc*.jar) to the Druid classpath.
  - For instance, if all jar files in your "druid/lib" directory are automatically added to your Druid classpath, then manually download the Microsoft JDBC drivers from ( https://www.microsoft.com/en-ca/download/details.aspx?id=11774) and drop it into my druid/lib directory.

4. Configure your Druid metadata storage extension:

  Add the following parameters to your Druid configuration, replacing `<host>`
  with the location (host name and port) of the database.

  ```properties
  druid.metadata.storage.type=sqlserver
  druid.metadata.storage.connector.connectURI=jdbc:sqlserver://<host>;databaseName=druid
  druid.metadata.storage.connector.user=druid
  druid.metadata.storage.connector.password=diurd
  ```
