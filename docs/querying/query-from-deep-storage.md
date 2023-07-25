---
id: query-deep-storage
title: "Query from deep storage"
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

> Query from deep storage is an experimental feature.

## Segments in deep storage

Any data you ingest into Druid is already stored in deep storage, so you don't need to perform any additional configuration from that perspective. However, to take advantage of the space savings that querying from deep storage provides, make sure not all your segments get loaded onto Historical processes.

To do this, configure [load rules](../operations/rule-configuration.md#load-rules) to load only the segments you do want on Historical processes. 

For example, use the `loadByInterval` load rule and set  `tieredReplicants.YOUR_TIER` (such as `tieredReplicants._default_tier`) to 0 for a specific interval. If the default tier is the only tier in your cluster, this results in that interval only being available from deep storage.

For example, the following interval load rule assigns 0 replicants for the specified interval to the tier `_default_tier`:

```
  {
    "interval": "2017-01-19T00:00:00.000Z/2017-09-20T00:00:00.000Z",
    "tieredReplicants": {
      "_default_tier": 0
    },
    "useDefaultTierForNull": true,
    "type": "loadByInterval"
  }
```

This means that any segments within that interval don't get loaded onto `_default_tier`. Then, create a corresponding drop rule so that Druid drops the segments from Historical tiers if they were previously loaded.

You can verify that a segment is not loaded on any Historical tiers by querying the Druid metadata table:

```sql
SELECT "segment_id", "replication_factor" FROM sys."segments" WHERE "replication_factor" = 0 AND "datasource" = YOUR_DATASOURCE
```

Segments with a `replication_factor` of `0` are not assigned to any Historical tiers. Queries against these segments are run directly against the segment in deep storage. 

You can also confirm this through the Druid console. On the **Segments** page, see the **Replication factor** column.

Keep the following in mind when working with load rules to control what exists only in deep storage:

- At least one of the segments in a datasource must be loaded onto a Historical process so that Druid can plan the query. The segment on the Historical process can be any segment from the datasource. It does not need to be a specific segment. 
- The actual number of replicas may differ from the replication factor temporarily as Druid processes your load rules.

## Run a query from deep storage

### Submit a query

You can query data from deep storage by submitting a query to the API using `POST /sql/statements`  or the Druid console. Druid uses the multi-stage query (MSQ) task engine to perform the query.

To run a query from deep storage, send your query to the Router using the POST method:

```
POST https://ROUTER:8888/druid/v2/sql/statements
```

Submitting a query from deep storage uses the same syntax as any other Druid SQL query where the query is contained in the "query" field in the JSON object within the request payload. For example:

```json
{"query" : "SELECT COUNT(*) FROM data_source WHERE foo = 'bar'"}
```  

Generally, the request body fields are the same between the `sql` and `sql/statements` endpoints.

There are additional context parameters for `sql/statements` specifically: 

   - `executionMode`  determines how query results are fetched. The currently supported mode is `ASYNC`. 
   - `selectDestination` set to `DURABLE_STORAGE` instructs Druid to write the results from SELECT queries to durable storage. Note that this requires you to have [durable storage for MSQ enabled](../operations/durable-storage.md).

The following sample query includes the two additional context parameters that querying from deep storage supports:

```
curl --location 'http://localhost:8888/druid/v2/sql/statements' \
--header 'Content-Type: application/json' \
--data '{
    "query":"SELECT * FROM \"YOUR_DATASOURCE\" where \"__time\" >TIMESTAMP'\''2017-09-01'\'' and \"__time\" <= TIMESTAMP'\''2017-09-02'\''",
    "context":{
        "executionMode":"ASYNC",
        "selectDestination": "DURABLE_STORAGE"

    }  
}'
```

The response for submitting a query includes the query ID along with basic information, such as when you submitted the query and the schema of the results:

```json
{
  "queryId": "query-ALPHANUMBERIC-STRING",
  "state": "ACCEPTED",
  "createdAt": CREATION_TIMESTAMP,
"schema": [
  {
    "name": COLUMN_NAME,
    "type": COLUMN_TYPE,
    "nativeType": COLUMN_TYPE
  },
  ...
],
"durationMs": DURATION_IN_MS,
}
```


### Get query status

You can check the status of a query with the following API call:

```
GET https://ROUTER:8888/druid/v2/sql/statements/QUERYID
```

The query returns the status of the query, such as `ACCEPTED` or `RUNNING`. Before you attempt to get results, make sure the state is `SUCCESS`. 

When you check the status on a successful query,  it includes useful information about your query results including a sample record and information about how the results are organized by `pages`. The information for each page includes the following:

- `numRows`: the number of rows in that page of results
- `sizeInBytes`: the size of the page
- `id`: the indexed page number that you can use to reference a specific page when you get query results

You can use `page` as a parameter to refine the results you retrieve. 

The following snippet shows the structure of the `result` object:

```json
{
  ...
  "result": {
    "numTotalRows": INTEGER,
    "totalSizeInBytes": INTEGER,
    "dataSource": "__query_select",
    "sampleRecords": [
      [
        RECORD_1,
        RECORD_2,
        ...
      ]
    ],
    "pages": [
      {
        "numRows": INTEGER,
        "sizeInBytes": INTEGER,
        "id": INTEGER_PAGE_NUMBER
      }
      ...
    ]
}
}
```

### Get query results

Only the user who submitted a query can retrieve the results for the query.

Use the following endpoint to retrieve results:

```
GET https://ROUTER:8888/druid/v2/sql/statements/QUERYID/results?page=PAGENUMBER&size=RESULT_SIZE&timeout=TIMEOUT_MS
```

Results are returned in JSON format.

You can use the optional `page`, `size`, and `timeout` parameters to refine your results. You can retrieve the `page` information for your results by fetching the status of the completed query.

When you try to get results for a query from deep storage, you may receive an error that states the query is still running. Wait until the query completes before you try again.


