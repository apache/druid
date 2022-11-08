---
id: tutorial-jdbc
title: "Tutorial: Using the JDBC driver to query Druid"
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

Learn to connect to your Druid instance using the [Avatica JDBC driver](https://calcite.apache.org/avatica/downloads/) to make Druid SQL queries. You can run queries that return metadata about a datasource or run queries against a datasource.

For more information about the JDBC driver, see [SQL JDBC driver API](../querying/sql-jdbc).

## Before you start

Make sure you meet the following requirements before starting the tutorial:

- A supported Java version, such as Java 8

- [Avatica JDBC driver](https://calcite.apache.org/avatica/downloads/). The jar can be added to your classpath directly or managed externally, such as through Maven and a `pom.xml` file.

- An available Druid instance. You can use the `micro-quickstart` configuration described in [Quickstart (local)](./index.md). The tutorials assume that you are using the quickstart, so no authentication or authorization is expected unless explicitly mentioned. 

- The example `wikipedia` datasource from the quickstart is loaded on your Druid instance. If you have a different datasource loaded, you can still try this tutorial. You'll have to update the tablename and column names though.

Keep the following in mind when trying these  the Java examples:
- The `url` variable includes the following:
  - `jdbc:avatica:remote:url=` prepended to the hostname and port
  - the hostname and port. In the case of the quickstart deployment, `http:localhost:8888`.
  - the SQL endpoint in Druied for the Avatica driver, `/druid/v2/sql/avatica/`
  - For more information about the connection options, see [Client Reference](https://calcite.apache.org/avatica/docs/client_reference.html).
- The `query` variable contains the SQL query you want to submit to Druid.

## Get the metadata for a datasource

Metadata, such as column names, is available either through the [`INFORMATION_SCHEMA`](../querying/sql-metadata-tables.md) table or through `connect.getMetaData()`. The following example uses the `INFORMATION_SCHEMA` table to retrieve and print the list of column names for the `wikipedia` datasource that you loaded during a previous tutorial.

```java
import java.sql.*;
import java.util.Properties;

public class JdbcListColumns {

    public static void main(String args[]) throws SQLException
    {
        // Connect to /druid/v2/sql/avatica/ on your Broker.

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

## Query data

Now that you know what columns are available, you can start querying the data. The following example queries the datasource named `wikipedia` for the timestamps and comments from Japan:

```java
import java.sql.*;
import java.util.Properties;

public class JdbcCountryAndTime {

    public static void main(String args[]) throws SQLException
    {
        // Connect to /druid/v2/sql/avatica/ on your Broker.
        String url = "jdbc:avatica:remote:url=http://localhost:8888/druid/v2/sql/avatica/";

        //The query you want to run.
        String query = "SELECT __time, isRobot, countryName, comment FROM wikipedia WHERE countryName='Japan'";
        // Set any connection context parameters you need here
        // Or leave empty for default behavior.
        Properties connectionProperties = new Properties();

        try (Connection connection = DriverManager.getConnection(url, connectionProperties)) {
            try (
                    final Statement statement = connection.createStatement();
                    final ResultSet rs = statement.executeQuery(query)
            ) {
                while (rs.next()) {
                    String timeStamp = rs.getString("__time");
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

