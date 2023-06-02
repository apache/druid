### Peon

`GET /druid/worker/v1/chat/{taskId}/rowStats`

Retrieve a live row stats report from a Peon. See [task reports](../ingestion/tasks.md#task-reports) for more details.

`GET /druid/worker/v1/chat/{taskId}/unparseableEvents`

Retrieve an unparseable events report from a Peon. See [task reports](../ingestion/tasks.md#task-reports) for more details.


### Router

> Note: Much of this information is available in a simpler, easier-to-use form through the Druid SQL
> [`INFORMATION_SCHEMA.TABLES`](../querying/sql-metadata-tables.md#tables-table),
> [`INFORMATION_SCHEMA.COLUMNS`](../querying/sql-metadata-tables.md#columns-table), and
> [`sys.segments`](../querying/sql-metadata-tables.md#segments-table) tables.

`GET /druid/v2/datasources`

Returns a list of queryable datasources.

`GET /druid/v2/datasources/{dataSourceName}`

Returns the dimensions and metrics of the datasource.

`GET /druid/v2/datasources/{dataSourceName}/dimensions`

Returns the dimensions of the datasource.

`GET /druid/v2/datasources/{dataSourceName}/metrics`

Returns the metrics of the datasource.