---
layout: doc_page
---
A data source is the Druid equivalent of a database table. However, a query can also masquerade as a data source, providing subquery-like functionality. Query data sources are currently only supported by [GroupBy](GroupByQuery.html) queries.

### Table Data Source
The table data source the most common type. It's represented by a string, or by the full structure:

```json
{
	"type": "table",
	"name": <string_value>
}
```

### Query Data Source
```json
{
	"type": "query",
	"query": {
		"type": "groupBy",
		...
	}
}
```
