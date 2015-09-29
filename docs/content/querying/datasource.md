---
layout: doc_page
---

## Datasources

A data source is the Druid equivalent of a database table. However, a query can also masquerade as a data source, providing subquery-like functionality. Query data sources are currently supported only by [GroupBy](../querying/groupbyquery.html) queries.

### Table Data Source
The table data source is the most common type. It's represented by a string, or by the full structure:

```json
{
	"type": "table",
	"name": "<string_value>"
}
```

### Union Data Source

This data source unions two or more table data sources.

```json
{
       "type": "union",
       "dataSources": ["<string_value1>", "<string_value2>", "<string_value3>", ... ]
}
```

Note that the data sources being unioned should have the same schema.
Union Queries should be always sent to the broker/router node and are *NOT* supported directly by the historical nodes. 

### Query Data Source

This is used for nested groupBys and is only currently supported for groupBys.

```json
{
	"type": "query",
	"query": {
		"type": "groupBy",
		...
	}
}
```
