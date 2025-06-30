---
id: set-query-context
title: "Set query context"
sidebar_label: "Set query context"
---

Query context gives you fine-grained control over how Apache Druid executes your individual queries. While Druid's default settings work well for most queries, you can set query context to handle specific requirements and optimize performance.

You'll use query context when you need to:
- Override default timeouts for long-running queries or complex aggregations.
- Control resource usage to prevent expensive queries from overwhelming your cluster.
- Debug query performance by disabling caching during testing.
- Configure SQL-specific behaviors like time zones for accurate time-based analysis.
- Set priorities to ensure critical queries get computational resources first.
- Adjust memory limits for queries that process large datasets.

Druid offers multiple approaches for you to set up your query context. This guide covers all the approaches—from the web console to programmatic APIs with practical examples you can adapt for your use case.

Before you begin, identify which context parameters you need to configure in order to establish your query context as context carriers. For a complete list of available parameters and their descriptions, see [Query context reference](query-context-reference.md).

## Druid web console

The easiest way to set query context is through the Druid web console. You can define context parameters using the Query context editor with these steps:

1. Open the **Query** tab in the Web Console.
2. Write your SQL query in the query editor. For example:

   ```sql
   SELECT COUNT(*) FROM data_source 
   WHERE foo = 'bar' AND __time >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
   ```
3. Click the **Engine: native, sql-native** selector (or current engine) next to the **Run** button.
4. Select **Edit context** from the menu.
5. In the **Edit query context** dialog, add your context parameters as JSON key-value pairs:
   ```json
   {
     "timeout": 300000,
     "priority": 100,
     "useCache": false
   }
   ```
6. Click **Save** to apply the context to your query.
7. Click **Run** to execute your query with the specified context parameters.

The web console validates your JSON and highlights any syntax errors before you run the query.

For more information about using the Druid SQL Web Console Query view, see [Query view](../operations/web-console.md#query).

## Druid SQL

When you use Druid SQL programmatically—such as in applications, automated scripts, or database tools, you can set query context using three different methods depending on how you execute your queries beyond web console. 

### HTTP API

When using the HTTP API, you include query context parameters in the `context` object of your JSON request. 

To set query context via the HTTP API, You can start with your SQL query:

   ```sql
   SELECT COUNT(*) FROM data_source WHERE foo = 'bar' AND __time > TIMESTAMP '2000-01-01 00:00:00'
   ```

Then you need to create a JSON request with your query and context parameters. The following example sets the `sqlTimeZone` parameter:

   ```json
   {
     "query" : "SELECT COUNT(*) FROM data_source WHERE foo = 'bar' AND __time > TIMESTAMP '2000-01-01 00:00:00'",
     "context" : {
       "sqlTimeZone" : "America/Los_Angeles"
     }
   }
   ```

You can then send the request to Druid's SQL API HTTP endpoint:

   ```bash
   curl -X POST 'http://localhost:8888/druid/v2/sql' \
     -H 'Content-Type: application/json' \
     -d '{
       "query": "SELECT COUNT(*) FROM data_source WHERE foo = '\''bar'\'' AND __time > TIMESTAMP '\''2000-01-01 00:00:00'\''",
       "context": {
         "sqlTimeZone": "America/Los_Angeles"
       }
     }'
   ```

Druid will execute your query with the specified context parameters and return the results.

You can set multiple context parameters in a single request:

```json
{
  "query" : "SELECT COUNT(*) FROM data_source WHERE foo = 'bar'",
  "context" : {
    "timeout" : 30000,
    "priority" : 100,
    "useCache" : false,
    "sqlTimeZone" : "America/Los_Angeles"
  }
}
```
For more information on how to format Druid SQL API requests and handle responses, see [Druid SQL API](../api-reference/sql-api.md).


### JDBC driver API

When connecting to Druid through JDBC, you set query context parameters as connection properties. This approach is useful when integrating Druid with BI tools or Java applications.

Druid uses the Avatica JDBC driver (version 1.23.0 or later recommended). Note that Avatica does not support passing connection context parameters from the JDBC connection string—you must use a `Properties` object instead.

**Basic example:**

You can set query context parameters when creating your JDBC connection:

```java
// Connect to /druid/v2/sql/avatica/ on your Broker
String url = "jdbc:avatica:remote:url=http://localhost:8888/druid/v2/sql/avatica/;transparent_reconnection=true";

// Set any query context parameters you need here
Properties connectionProperties = new Properties();
connectionProperties.setProperty("sqlTimeZone", "America/Los_Angeles");
connectionProperties.setProperty("useCache", "false");
connectionProperties.setProperty("priority", "100");

try (Connection connection = DriverManager.getConnection(url, connectionProperties)) {
    try (
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM data_source WHERE foo = 'bar'")
    ) {
        while (resultSet.next()) {
            // Process result set
            System.out.println("Count: " + resultSet.getLong(1));
        }
    }
}
```

**With authentication:**

If your Druid cluster requires authentication, you can include both context parameters and credentials in the same Properties object:

```java
Properties connectionProperties = new Properties();
// Query context parameters
connectionProperties.setProperty("sqlTimeZone", "Etc/UTC");
connectionProperties.setProperty("useCache", "false");

// Authentication parameters
connectionProperties.setProperty("user", "admin");
connectionProperties.setProperty("password", "password1");

try (Connection connection = DriverManager.getConnection(url, connectionProperties)) {
    // Execute queries with both context and authentication
}
```

For more details on how to use JDBC driver API, see [Druid SQL JDBC driver API](../api-reference/sql-jdbc.md).

### SET statements

Beyond using the `context` parameter, you can use `SET` command to specify SQL query context parameters that modify the behavior of a Druid SQL query. You can include one or more SET statements before the main SQL query. You can use `SET` in the both web console and Druid SQL HTTP API. 

**Using SET in the Web Console:**

In the Query tab of web console, you can write your SET statements followed by your query directly. For example, 

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

**Using SET via the HTTP API:**

You can also include your SET statements as part of the query string in your API call. For example,

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

SET statements work with `SELECT`, `INSERT`, and `REPLACE` queries.

For more details on how to use the SET command in your SQL query, see [SET](sql.md#set).

:::info
 You cannot use SET statements when using Druid SQL JDBC connections.
:::


## Native queries

For native queries, you can include query context parameters in a JSON object named `context` within your query structure.

Native queries in Druid are JSON objects that you typically send to the Broker or Router processes. You can submit native queries in two ways:

**Using curl with a query file:**

First, you need to create a JSON file with your query and context parameters. The following example is the content from `query.json` file.

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
    "finalize": true         // Finalize aggregation results
  }
}
```

Then you can submit the query using curl:

```bash
# Replace localhost:8888 with your Druid Broker or Router address
curl -X POST 'http://localhost:8888/druid/v2/?pretty' \
  -H 'Content-Type:application/json' \
  -H 'Accept:application/json' \
  -d @query.json
```

**Using the web console:**

For this approach, you  can simply paste your native query JSON into the Query view, which automatically switches to JSON mode.

The context parameters you set here will apply to this specific query execution. Each native query can have its context settings, allowing you to optimize performance on a per-query basis. See [Druid Web Console](./set-query-context.md#druid-web-console) for more detailed instruction.

For more information about native query structure and available query types, see [Native queries](querying.md).


## Learn more
- [Query context reference](query-context-reference.md) for a complete list of all available query context parameters.
- [SQL query context](sql-query-context.md) for SQL-specific context parameters.
- [Multi-stage query context](../multi-stage-query/reference.md#context-parameters) for context parameters specific to SQL-based ingestion.
- [Native queries](querying.md) for details on constructing native queries with context.
- [SET](sql.md#set) for complete syntax and usage of SET statements.