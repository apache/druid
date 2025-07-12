---
id: set-query-context
title: "Set query context"
sidebar_label: "Set query context"
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
  

The query context gives you fine-grained control over how Apache Druid executes your individual queries. While the default settings in Druid work well for most queries, you can set query context to handle specific requirements and optimize performance.

Common use cases for the query context include:
- Override default timeouts for long-running queries or complex aggregations.
- Control resource usage to prevent expensive queries from overwhelming your cluster.
- Debug query performance by disabling caching during testing.
- Configure SQL-specific behaviors like time zones for accurate time-based analysis.
- Set priorities to ensure critical queries get computational resources first.
- Adjust memory limits for queries that process large datasets.

Druid provides several ways to set query context, and the method you use depends on how and where you're submitting your query.
This guide lists how to set query context for each method of submitting queries.

Before you begin, identify which context parameters you need to configure in order to establish your query context as query context carriers. For available parameters and their descriptions, see [Query context reference](query-context-reference.md).

## Druid web console

The most straightforward method to configure query context parameters is via the Druid web console. In web console, you can set up context parameters for both Druid SQL and native queries.

The following steps outline how to define query context parameters:

1. Open the **Query** tab in the web console.

1. **Click** the **Engine** selector next to the **Run** button to choose the appropriate query type:

- Selects the **JSON (native) engine** for native queries.
- Selects the **SQL (native) engine** for Druid SQL queries.
- Selects the **SQL (task) engine** for Multi-stage queries (MSQ).
- Selects the **Auto engine** to let the console detect the query type automatically. You just paste your query into the **Query** view, and the web console chooses the right engine for you.

2. Enter the query you want to run in the web console.

3. Select **Edit context** from the menu.
4. In the **Edit query context** dialog, add your context parameters as JSON key-value pairs:
   ```json
   {
     "timeout": 300000,
     "useCache": false
   }
   ```
5. Click **Save** to apply the context to your query.
6. Click **Run** to execute your query with the specified context parameters.

The web console validates your JSON and highlights any syntax errors before you run the query.

For more information about using the Druid SQL Web Console Query view, see [Query view](../operations/web-console.md#query).

## Druid SQL

When you use Druid SQL programmatically—such as in applications, automated scripts, or database tools, you can set query context using three different methods depending on how you execute your queries beyond [Druid Web Console](./set-query-context.md#druid-web-console). 

### HTTP API

When using the HTTP API, you include query context parameters in the `context` object of your JSON request. 

The following example sets the `sqlTimeZone` parameter:

   ```json
   {
     "query" : "SELECT COUNT(*) FROM data_source WHERE foo = 'bar' AND __time > TIMESTAMP '2000-01-01 00:00:00'",
     "context" : {
       "sqlTimeZone" : "America/Los_Angeles"
     }
   }
   ```

Druid will execute your query with the specified context parameters and return the results.

You can set multiple context parameters in a single request:

```json
{
  "query" : "SELECT COUNT(*) FROM data_source WHERE foo = 'bar'",
  "context" : {
    "timeout" : 30000,
    "useCache" : false,
    "sqlTimeZone" : "America/Los_Angeles"
  }
}
```
For more information on how to format Druid SQL API requests and handle responses, see [Druid SQL API](../api-reference/sql-api.md).


### JDBC driver API

When connecting to Druid through JDBC, you set query context parameters a JDBC connection properties object. This approach is useful when integrating Druid with BI tools or Java applications.

Druid uses the Avatica JDBC driver (version 1.23.0 or later recommended). Note that Avatica does not support passing connection context parameters from the JDBC connection string—you must use a `Properties` object instead.


You can set query context parameters when creating your JDBC connection:

```java
String url = "jdbc:avatica:remote:url=http://localhost:8082/druid/v2/sql/avatica/";

// Set any query context parameters you need here.
Properties connectionProperties = new Properties();
connectionProperties.setProperty("sqlTimeZone", "America/Los_Angeles");
connectionProperties.setProperty("useCache", "false");

try (Connection connection = DriverManager.getConnection(url, connectionProperties)) {
  // create and execute statements, process result sets, etc
}
```

For more details on how to use JDBC driver API, see [Druid SQL JDBC driver API](../api-reference/sql-jdbc.md).

### SET statements

Beyond using the `context` parameter, you can use `SET` command to specify SQL query context parameters that modify the behavior of a Druid SQL query. You can include one or more SET statements before the main SQL query. You can use `SET` in the both web console and Druid SQL HTTP API. 

In the web console, you can write your `SET` statements followed by your query directly. For example, 

```sql
SET useApproximateTopN = false;
SET sqlTimeZone = 'America/Los_Angeles';
SET timeout = 90000;
SELECT some_column, COUNT(*) 
FROM druid.foo 
WHERE other_column = 'foo' 
GROUP BY 1 
ORDER BY 2 DESC
```

You can also include your SET statements as part of the query string in your HTTP API call. For example,

```bash
curl -X POST 'http://localhost:8888/druid/v2/sql' \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "SET useApproximateTopN = false; SET sqlTimeZone = '\''America/Los_Angeles'\''; SET timeout = 90000; SELECT some_column, COUNT(*) FROM druid.foo WHERE other_column = '\''foo'\'' GROUP BY 1 ORDER BY 2 DESC"
  }'
```

You can also combine SET statements with the `context` field. If you include both, the parameter value in SET takes precedence:

```json
{
  "query": "SET timeout = 90000; SELECT COUNT(*) FROM data_source",
  "context": {
    "timeout": 30000,  // This will be overridden by SET
    "priority": 100    // This will still apply
  }
}
```

SET statements only apply to the query in the same request. Subsequent requests are not affected. 

SET statements work with SELECT, INSERT, and REPLACE queries.

For more details on how to use the SET command in your SQL query, see [SET](sql.md#set).

:::info
 You cannot use SET statements when using Druid SQL JDBC connections.
:::


## Native queries

For native queries, you can include query context parameters in a JSON object named `context` within your query structure or through [Druid Web Console](./set-query-context.md#druid-web-console).

Here's an example of how to set up context parameters using JSON for native queries:

```json
{
  "queryType": "timeseries",
  "dataSource": "sample_datasource",
  "granularity": "day",
  "descending": "true",
  "filter": {
    "type": "and",
    "fields": [
      { "type": "selector", "dimension": "sample_dimension1", "value": "sample_value1" },
      { "type": "or",
        "fields": [
          { "type": "selector", "dimension": "sample_dimension2", "value": "sample_value2" },
          { "type": "selector", "dimension": "sample_dimension3", "value": "sample_value3" }
        ]
      }
    ]
  },
  "aggregations": [
    { "type": "longSum", "name": "sample_name1", "fieldName": "sample_fieldName1" },
    { "type": "doubleSum", "name": "sample_name2", "fieldName": "sample_fieldName2" }
  ],
  "postAggregations": [
    { "type": "arithmetic",
      "name": "sample_divide",
      "fn": "/",
      "fields": [
        { "type": "fieldAccess", "name": "postAgg__sample_name1", "fieldName": "sample_name1" },
        { "type": "fieldAccess", "name": "postAgg__sample_name2", "fieldName": "sample_name2" }
      ]
    }
  ],
  "intervals": [ "2012-01-01T00:00:00.000/2012-01-03T00:00:00.000" ],
  // Add context parameters here
  "context": {
    "timeout": 30000,        // Query timeout in milliseconds
    "priority": 100,         // Higher priority queries get more resources
    "useCache": false,       // Disable cache for testing
  }
}
```

For more information about native query structure and how to submit native queries, see [Native queries](querying.md).


## Learn more
- [Query context reference](query-context-reference.md) for available query context parameters.
- [SQL query context](sql-query-context.md) for SQL-specific context parameters.
- [Multi-stage query context](../multi-stage-query/reference.md#context-parameters) for context parameters specific to SQL-based ingestion.
- [Native queries](querying.md) for details on constructing native queries with context.
- [SET](sql.md#set) for complete syntax and usage of SET statements.
