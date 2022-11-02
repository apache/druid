---
id: tutorial-jdbc
title: "JDBC tutorial"
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

You can connect to your Druid instance using the [Avatica JDBC driver](https://calcite.apache.org/avatica/downloads/). The JDBC driver connects to the Broker service.

## Before you start

Make sure you meet the following requirements before starting the tutorial:

- A supported Java version, such as Java 8 or 11

- The Avatica JDBC driver jar. The jar can be added to your classpath directly or managed externally, such as through Maven and a `pom.xml` file.

- An available Druid instance. You can use the `micro-quickstart` configuration described in [Quickstart (local)](./index.md). The tutorials assume that you are using the quickstart, so no authentication or authorization is expected unless explicitly mentioned. 

- The example `wikipedia` datasource from the quickstart is loaded on your Druid instance

## Java examples

Keep the following in mind when using the Java samples:
- The `url` variable includes the following:
  - `jdbc:avatica:remote:url=` prepended to the hostname and port
  - the hostname and port. In the case of the quickstart deployment, `http:localhost:8888`.
  - the SQL endpoint for the Avatica driver connection `/druid/v2/sql/avatica/`

### Get the list of column names

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

### Query data

The following example queries the datasource named `wikipedia` in your micro-quickstart deployment for the timestamps from contributors whose country is Japan:


```java
import java.sql.*;
import java.util.Properties;

public class JdbcExample {

    public static void main(String args[]) throws SQLException
    {
        // Connect to /druid/v2/sql/avatica/ on your Broker.
        String url = "jdbc:avatica:remote:url=http://localhost:8888/druid/v2/sql/avatica/";

        //The query you want to run.
        String query = "SELECT __time, countryName FROM wikipedia WHERE countryName='Japan'";
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
                    System.out.println(timeStamp);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }
}
`````