---
id: connect-external-data
title: Tutorial - Connect external data for SQL-based ingestion
description: How to generate a query that references externally hosted data
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

> SQL-based ingestion using the multi-stage query task engine is our recommended solution starting in Druid 24.0. Alternative ingestion solutions, such as native batch and Hadoop-based ingestion systems, will still be supported. We recommend you read all [known issues](./msq-known-issues.md) and test the feature in a development environment before rolling it out in production. Using the multi-stage query task engine with `SELECT` statements that do not write to a datasource is experimental.

This tutorial demonstrates how to generate a query that references externally hosted data using the **Connect external data** wizard.

The following example uses EXTERN to query a JSON file located at https://static.imply.io/data/wikipedia.json.gz.

Although you can manually create a query in the UI, you can use Druid to generate a base query for you that you can modify to meet your requirements.

To generate a query from external data, do the following:

1. In the **Query** view of the Druid console, click **Connect external data**.
2. On the **Select input type** screen, choose **HTTP(s)** and enter the following value in the **URIs** field: `https://static.imply.io/data/wikipedia.json.gz`. Leave the HTTP auth username and password blank.
3. Click **Connect data**.
4. On the **Parse** screen, you can perform additional actions before you load the data into Druid:
   - Expand a row to see what data it corresponds to from the source.
   - Customize how Druid handles the data by selecting the **Input format** and its related options, such as adding **JSON parser features** for JSON files.
5. When you're ready, click **Done**. You're returned to the **Query** view where you can see the newly generated query:

   - The query inserts the data from the external source into a table named `wikipedia`.
   - Context parameters appear before the query in the syntax unique to the Druid console: `--: context {key}: {value}`. When submitting queries to Druid directly, set the `context` parameters in the context section of the SQL query object. For more information about context parameters, see [Context parameters](./msq-reference.md#context-parameters).

   <details><summary>Show the query</summary>

   ```sql
   REPLACE INTO "wikipedia" OVERWRITE ALL
   WITH ext AS (SELECT *
   FROM TABLE(
     EXTERN(
       '{"type":"http","uris":["https://static.imply.io/data/wikipedia.json.gz"]}',
       '{"type":"json"}',
       '[{"name":"isRobot","type":"string"},{"name":"channel","type":"string"},{"name":"timestamp","type":"string"},{"name":"flags","type":"string"},{"name":"isUnpatrolled","type":"string"},{"name":"page","type":"string"},{"name":"diffUrl","type":"string"},{"name":"added","type":"long"},{"name":"comment","type":"string"},{"name":"commentLength","type":"long"},{"name":"isNew","type":"string"},{"name":"isMinor","type":"string"},{"name":"delta","type":"long"},{"name":"isAnonymous","type":"string"},{"name":"user","type":"string"},{"name":"deltaBucket","type":"long"},{"name":"deleted","type":"long"},{"name":"namespace","type":"string"},{"name":"cityName","type":"string"},{"name":"countryName","type":"string"},{"name":"regionIsoCode","type":"string"},{"name":"metroCode","type":"long"},{"name":"countryIsoCode","type":"string"},{"name":"regionName","type":"string"}]'
     )
   ))
   SELECT
     TIME_PARSE("timestamp") AS __time,
     isRobot,
     channel,
     flags,
     isUnpatrolled,
     page,
     diffUrl,
     added,
     comment,
     commentLength,
     isNew,
     isMinor,
     delta,
     isAnonymous,
     user,
     deltaBucket,
     deleted,
     namespace,
     cityName,
     countryName,
     regionIsoCode,
     metroCode,
     countryIsoCode,
     regionName
   FROM ext
   PARTITIONED BY DAY
   ```
   </details>

6. Review and modify the query to meet your needs. For example, you can rename the table or change segment granularity. To partition by something other than ALL, include `TIME_PARSE("timestamp") AS __time` in your SELECT statement.

   For example, to specify day-based segment granularity, change the partitioning to `PARTITIONED BY DAY`:
      
     ```sql
      ...
      SELECT
        TIME_PARSE("timestamp") AS __time,
      ...
      ...
       PARTITIONED BY DAY
     ```

1. Optionally, select **Preview** to review the data before you ingest it. A preview runs the query without the INSERT INTO clause and with an added LIMIT to the main query and to all helper queries. You can see the general shape of the data before you commit to inserting it. The LIMITs make the query run faster but can cause incomplete results.
2. Click **Run** to launch your query. The query returns information including its duration and the number of rows inserted into the table.

## Query the data

You can query the `wikipedia` table after the ingestion completes.
For example, you can analyze the data in the table to produce a list of top channels:

```sql
SELECT
  channel,
  COUNT(*)
FROM "wikipedia"
GROUP BY channel
ORDER BY COUNT(*) DESC
```

With the EXTERN function, you could run the same query on the external data directly without ingesting it first:

<details><summary>Show the query</summary>

```sql
SELECT
  channel,
  COUNT(*)
FROM TABLE(
  EXTERN(
    '{"type": "http", "uris": ["https://static.imply.io/data/wikipedia.json.gz"]}',
    '{"type": "json"}',
    '[{"name": "added", "type": "long"}, {"name": "channel", "type": "string"}, {"name": "cityName", "type": "string"}, {"name": "comment", "type": "string"}, {"name": "commentLength", "type": "long"}, {"name": "countryIsoCode", "type": "string"}, {"name": "countryName", "type": "string"}, {"name": "deleted", "type": "long"}, {"name": "delta", "type": "long"}, {"name": "deltaBucket", "type": "string"}, {"name": "diffUrl", "type": "string"}, {"name": "flags", "type": "string"}, {"name": "isAnonymous", "type": "string"}, {"name": "isMinor", "type": "string"}, {"name": "isNew", "type": "string"}, {"name": "isRobot", "type": "string"}, {"name": "isUnpatrolled", "type": "string"}, {"name": "metroCode", "type": "string"}, {"name": "namespace", "type": "string"}, {"name": "page", "type": "string"}, {"name": "regionIsoCode", "type": "string"}, {"name": "regionName", "type": "string"}, {"name": "timestamp", "type": "string"}, {"name": "user", "type": "string"}]'
  )
)
GROUP BY channel
ORDER BY COUNT(*) DESC
```

</details>

## Further reading

See the following topics to learn more:

* [MSQ task engine query syntax](./index.md#msq-task-engine-query-syntax) for information about the different query components.
* [Reference](./msq-reference.md) for reference on context parameters, functions, and error codes.
