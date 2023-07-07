---
id: query-deep-storage
title: "Query from deep storage"
---

## Make data available to query from deep storage

All data is available to be queried from deep storage, so you don't need to perform any additional configuration from that perspective. To take advantage of the space savings that querying from deep storage enables though, you need to unload data from Historical processes that you only want to query from deep storage.

To do this, you configure the following [load rules](../operations/rule-configuration.md#load-rules):

- `IntervalLoadOnDemandRule`
- `PeriodLoadOnDemandRule`
- `ForeverLoadOnDemandRule`

Note that some of the segments in a datasource must be loaded onto a Historical process so that Druid can plan the query.

## Run a query from deep storage

### Submit a query

You can query data from deep storage bv submitting a Druid SQL  to the the API using the `POST /sql/statements`  or the Druid console. Druid uses the multi-stage query (MSQ) task engine to perform the query.

Querying from deep storage uses the same syntax as any other Druid SQL query. 


Additionally, there is a `mode` query context parameter that determines how query results are fetched. The currently supported mode is "ASYNC".

### Retrieve query results

When you retrieve results for a query from deep storage, you may receive partial results (status code `206`) while the query completes or full results once the query completes (status code `200`). 

Use the following endpoint to retrieve results:

```
GET /sq/statements/ID/results?offset=ROW_OFFSET,size=RESULT_SIZE,timeout=TIMEOUT_MS
```

- `ID`: the query ID from the response when you submitted the query
- `offset`:
- `size`
- `timeout`