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

When using the JDBC connector for the [examples](#examples) or in general, it's helpful to understand the parts of the connect string stored in the `url` variable:

  - `jdbc:avatica:remote:url=` is prepended to the hostname and port.
  - The hostname and port number for your Druid deployment depends on whether you want to connect to the Router or a specific Broker. For more information, see [Connection stickiness](#connection-stickiness). In the case of the quickstart deployment, the hostname and port are `http://localhost:8888`, which connects to the Router running on your local machine.
  - The SQL endpoint in Druid for the Avatica driver is  `/druid/v2/sql/avatica/`.

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

For a runnable example that includes a query that you might run, see [Examples](#examples).

It is also possible to use a protocol buffers JDBC connection with Druid, this offer reduced bloat and potential performance
improvements for larger result sets. To use it apply the following connection url instead, everything else remains the same
```
String url = "jdbc:avatica:remote:url=http://localhost:8082/druid/v2/sql/avatica-protobuf/;serialization=protobuf";
```

> The protobuf endpoint is also known to work with the official [Golang Avatica driver](https://github.com/apache/calcite-avatica-go)

Table metadata is available over JDBC using `connection.getMetaData()` or by querying the
["INFORMATION_SCHEMA" tables](sql-metadata-tables.md). For an example of this, see [Get the metadata for a datasource](#get-the-metadata-for-a-datasource).

## Connection stickiness

Druid's JDBC server does not share connection state between Brokers. This means that if you're using JDBC and have
multiple Druid Brokers, you should either connect to a specific Broker or use a load balancer with sticky sessions
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

## Examples

<!-- docs/tutorial-jdbc.md redirects here -->

The following section contains two complete samples that use the JDBC connector:

- [Get the metadata for a datasource](#get-the-metadata-for-a-datasource) shows you how to query the `INFORMATION_SCHEMA` to get metadata like column names. 
- [Query data](#query-data) runs a select query against the datasource.

You can try out these examples after verifying that you meet the [prerequisites](#prerequisites).

For more information about the connection options, see [Client Reference](https://calcite.apache.org/avatica/docs/client_reference.html).

### Prerequisites 

Make sure you meet the following requirements before trying these examples:

- A supported Java version, such as Java 8

- [Avatica JDBC driver](https://calcite.apache.org/avatica/downloads/). You can add the JAR  to your `CLASSPATH` directly or manage it externally, such as through Maven and a `pom.xml` file.

- An available Druid instance. You can use the `micro-quickstart` configuration described in [Quickstart (local)](../tutorials/index.md). The examples assume that you are using the quickstart, so no authentication or authorization is expected unless explicitly mentioned. 

- The example `wikipedia` datasource from the quickstart is loaded on your Druid instance. If you have a different datasource loaded, you can still try these examples. You'll have to update the table name and column names to match your datasource.

### Get the metadata for a datasource

Metadata, such as column names, is available either through the [`INFORMATION_SCHEMA`](../querying/sql-metadata-tables.md) table or through `connect.getMetaData()`. The following example uses the `INFORMATION_SCHEMA` table to retrieve and print the list of column names for the `wikipedia` datasource that you loaded during a previous tutorial.

```java
import java.sql.*;
import java.util.Properties;

public class JdbcListColumns {

    public static void main(String args[]) throws SQLException
    {
        // Connect to /druid/v2/sql/avatica/ on your Router. 
        // You can connect to a Broker but must configure connection stickiness if you do. 
        String url = "jdbc:avatica:remote:url=http://localhost:8888/druid/v2/sql/avatica/";

        String query = "SELECT COLUMN_NAME,* FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'wikipedia' and TABLE_SCHEMA='druid'";
        // Set any connection context parameters you need here
        // Or leave empty for default behavior.
        Properties connectionProperties = new Properties();

        try (Connection connection = DriverManager.getConnection(url, connectionProperties)) {
            try (
                    final Statement statement = connection.createStatement();
                    final ResultSet rs = statement.executeQuery(query)
            ) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    System.out.println(columnName);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }
}
```

### Query data

Now that you know what columns are available, you can start querying the data. The following example queries the datasource named `wikipedia` for the timestamps and comments from Japan. It also sets the [query context parameter](../querying/sql-query-context.md) `sqlTimeZone` . Optionally,  you can also parameterize queries by using [dynamic parameters](#dynamic-parameters).

```java
import java.sql.*;
import java.util.Properties;

public class JdbcCountryAndTime {

    public static void main(String args[]) throws SQLException
    {
        // Connect to /druid/v2/sql/avatica/ on your Router. 
        // You can connect to a Broker but must configure connection stickiness if you do. 
        String url = "jdbc:avatica:remote:url=http://localhost:8888/druid/v2/sql/avatica/";

        //The query you want to run.
        String query = "SELECT __time, isRobot, countryName, comment FROM wikipedia WHERE countryName='Japan'";
        // Set any connection context parameters you need here
        // Or leave empty for default behavior.
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("sqlTimeZone", "America/Los_Angeles");

        try (Connection connection = DriverManager.getConnection(url, connectionProperties)) {
            try (
                    final Statement statement = connection.createStatement();
                    final ResultSet rs = statement.executeQuery(query)
            ) {
                while (rs.next()) {
                    Timestamp timeStamp = rs.getTimestamp("__time");
                    String comment = rs.getString("comment");
                    System.out.println(timeStamp);
                    System.out.println(comment);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }
}
`````


