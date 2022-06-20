---
id: sql-jdbc
title: "SQL JDBC driver API"
sidebar_label: "JDBC driver API"
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

> Apache Druid supports two query languages: Druid SQL and [native queries](querying.md).
> This document describes the SQL language.


You can make [Druid SQL](./sql.md) queries using the [Avatica JDBC driver](https://calcite.apache.org/avatica/downloads/). We recommend using Avatica JDBC driver version 1.17.0 or later. Note that as of the time of this writing, Avatica 1.17.0, the latest version, does not support passing connection string parameters from the URL to Druid, so you must pass them using a `Properties` object. Once you've downloaded the Avatica client jar, add it to your classpath and use the connect string `jdbc:avatica:remote:url=http://BROKER:8082/druid/v2/sql/avatica/`.

Example code:

```java
// Connect to /druid/v2/sql/avatica/ on your Broker.
String url = "jdbc:avatica:remote:url=http://localhost:8082/druid/v2/sql/avatica/";

// Set any connection context parameters you need here
// Or leave empty for default behavior.
Properties connectionProperties = new Properties();

try (Connection connection = DriverManager.getConnection(url, connectionProperties)) {
  try (
      final Statement statement = connection.createStatement();
      final ResultSet resultSet = statement.executeQuery(query)
  ) {
    while (resultSet.next()) {
      // process result set
    }
  }
}
```

It is also possible to use a protocol buffers JDBC connection with Druid, this offer reduced bloat and potential performance
improvements for larger result sets. To use it apply the following connection url instead, everything else remains the same
```
String url = "jdbc:avatica:remote:url=http://localhost:8082/druid/v2/sql/avatica-protobuf/;serialization=protobuf";
```

> The protobuf endpoint is also known to work with the official [Golang Avatica driver](https://github.com/apache/calcite-avatica-go)

Table metadata is available over JDBC using `connection.getMetaData()` or by querying the
["INFORMATION_SCHEMA" tables](sql-metadata-tables.md).

## Connection stickiness

Druid's JDBC server does not share connection state between Brokers. This means that if you're using JDBC and have
multiple Druid Brokers, you should either connect to a specific Broker, or use a load balancer with sticky sessions
enabled. The Druid Router process provides connection stickiness when balancing JDBC requests, and can be used to achieve
the necessary stickiness even with a normal non-sticky load balancer. Please see the
[Router](../design/router.md) documentation for more details.

Note that the non-JDBC [JSON over HTTP](sql-api.md#submit-a-query) API is stateless and does not require stickiness.

## Dynamic parameters

You can use [parameterized queries](sql.md#dynamic-parameters) in JDBC code, as in this example:

```java
PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) AS cnt FROM druid.foo WHERE dim1 = ? OR dim1 = ?");
statement.setString(1, "abc");
statement.setString(2, "def");
final ResultSet resultSet = statement.executeQuery();
```
