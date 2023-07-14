---
id: query-deep-storage
title: "Query from deep storage"
---

> Query from deep storage is an experimental feature.

## Segments in deep storage

Any data you ingest into Druid is already stored in deep storage, so you don't need to perform any additional configuration from that perspective. To take advantage of the space savings that querying from deep storage provides though, you need to make sure not all your segments get loaded onto Historical processes.

To do this, configure [load rules](../operations/rule-configuration.md#load-rules) to load only the segments you do want on Historical processes. 

For example, use the `loadByInterval` load rule and set  `tieredReplicants.YOUR_TIER` (such as `tieredReplicants._default_tier`) to 0 for a specific interval. This results in that interval only being available from deep storage.

You can verify that a segment is not loaded on any Historical tiers by querying the Druid metadata table:

```sql
SELECT "segment_id", "replication_factor" FROM sys."segments" 
```

Segments with a `replication_factor` of `0` are not assigned to any Historical tiers and won't get loaded onto them. Queries you run against these segments are run directly against the segment in deep storage.

Note that at least one of the segments in a datasource must be loaded onto a Historical process so that Druid can plan the query. But the segment on the Historical process can be any segment from the datasource. It does not need to be any specific segment. It 

## Run a query from deep storage

### Submit a query

You can query data from deep storage bv submitting a query to the the API using `POST /sql/statements`  or the Druid console. Druid uses the multi-stage query (MSQ) task engine to perform the query.

To run a query from deep storage, send your query to the Router using the POST method:

```
POST https://ROUTER:8888/druid/v2/sql/statements
```

Submitting a query from deep storage uses the same syntax as any other Druid SQL query where the query is contained in the "query" field in the JSON object within the request payload. For example:

```json
{"query" : "SELECT COUNT(*) FROM data_source WHERE foo = 'bar'"}
```  

Generally, the request body fields are the same between the `sql` and `sql/statements` endpoints.

The response body includes the query ID, which you use to get the results.

### Get query status

You can check the status of a query with the following API call:

```
/druid/v2/sql/statements/QUERYID
```

In addition to the state of the query, such as `ACCEPTED` or `RUNNING`. Before you attempt to get results, make sure the state is `SUCCESS`. 

When you check the status on a successful query,  it includes useful information about your query results including a sample record and information about how the results are organized by `pages`. The information for each page includes the following:

- `numRows`: the number of rows in that page of results
- `sizeInBytes`: the size of the page
- `id`: the 0 indexed page number that you can use to reference a specific page when you get query results

You can use this as a parameter to refine the results you retrieve. 

### Get query results

Only the user who submitted a query can retrieve the results for the query.

Use the following endpoint to retrieve results:

```
GET /druid/v2/sq/statements/QUERYID/results?page=PAGENUMBER&size=RESULT_SIZE&timeout=TIMEOUT_MS
```

You can use the `page`, `size`, and `timeout` parameters to refine your results. You can view the information for your `pages` by getting the status of a completed query.

When you try to get results for a query from deep storage, you may receive an error that states the query is still running. Wait until the query completes before you try again.


