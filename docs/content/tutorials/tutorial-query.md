---
layout: doc_page
---

# Tutorial: Querying data

This tutorial will demonstrate how to query data in Druid, with examples for Druid's native query format and Druid SQL.

The tutorial assumes that you've already completed one of the 4 ingestion tutorials, as we will be querying the sample Wikipedia edits data.

* [Tutorial: Loading a file](/docs/VERSION/tutorials/tutorial-batch.html)
* [Tutorial: Loading stream data from Kafka](/docs/VERSION/tutorials/tutorial-kafka.html)
* [Tutorial: Loading a file using Hadoop](/docs/VERSION/tutorials/tutorial-batch-hadoop.html)
* [Tutorial: Loading stream data using Tranquility](/docs/VERSION/tutorials/tutorial-tranquility.html)

## Native JSON queries

Druid's native query format is expressed in JSON. We have included a sample native TopN query under `examples/wikipedia-top-pages.json`:

```json
{
  "queryType" : "topN",
  "dataSource" : "wikipedia",
  "intervals" : ["2015-09-12/2015-09-13"],
  "granularity" : "all",
  "dimension" : "page",
  "metric" : "count",
  "threshold" : 10,
  "aggregations" : [
    {
      "type" : "count",
      "name" : "count"
    }
  ]
}
```

This query retrieves the 10 Wikipedia pages with the most page edits on 2015-09-12.

Let's submit this query to the Druid broker:

```bash
curl -X 'POST' -H 'Content-Type:application/json' -d @examples/wikipedia-top-pages.json http://localhost:8082/druid/v2?pretty
```

You should see the following query results:

```json
[ {
  "timestamp" : "2015-09-12T00:46:58.771Z",
  "result" : [ {
    "count" : 33,
    "page" : "Wikipedia:Vandalismusmeldung"
  }, {
    "count" : 28,
    "page" : "User:Cyde/List of candidates for speedy deletion/Subpage"
  }, {
    "count" : 27,
    "page" : "Jeremy Corbyn"
  }, {
    "count" : 21,
    "page" : "Wikipedia:Administrators' noticeboard/Incidents"
  }, {
    "count" : 20,
    "page" : "Flavia Pennetta"
  }, {
    "count" : 18,
    "page" : "Total Drama Presents: The Ridonculous Race"
  }, {
    "count" : 18,
    "page" : "User talk:Dudeperson176123"
  }, {
    "count" : 18,
    "page" : "Wikipédia:Le Bistro/12 septembre 2015"
  }, {
    "count" : 17,
    "page" : "Wikipedia:In the news/Candidates"
  }, {
    "count" : 17,
    "page" : "Wikipedia:Requests for page protection"
  } ]
} ]
```

## Druid SQL queries

Druid also supports a dialect of SQL for querying. Let's run a SQL query that is equivalent to the native JSON query shown above:

```
SELECT page, COUNT(*) AS Edits FROM wikipedia WHERE "__time" BETWEEN TIMESTAMP '2015-09-12 00:00:00' AND TIMESTAMP '2015-09-13 00:00:00' GROUP BY page ORDER BY Edits DESC LIMIT 10;
```

The SQL queries are submitted as JSON over HTTP.

### TopN query example

The tutorial package includes an example file that contains the SQL query shown above at `examples/wikipedia-top-pages-sql.json`. Let's submit that query to the Druid broker:

```bash
curl -X 'POST' -H 'Content-Type:application/json' -d @examples/wikipedia-top-pages-sql.json http://localhost:8082/druid/v2/sql
```

The following results should be returned:

```
[
  {
    "page": "Wikipedia:Vandalismusmeldung",
    "Edits": 33
  },
  {
    "page": "User:Cyde/List of candidates for speedy deletion/Subpage",
    "Edits": 28
  },
  {
    "page": "Jeremy Corbyn",
    "Edits": 27
  },
  {
    "page": "Wikipedia:Administrators' noticeboard/Incidents",
    "Edits": 21
  },
  {
    "page": "Flavia Pennetta",
    "Edits": 20
  },
  {
    "page": "Total Drama Presents: The Ridonculous Race",
    "Edits": 18
  },
  {
    "page": "User talk:Dudeperson176123",
    "Edits": 18
  },
  {
    "page": "Wikipédia:Le Bistro/12 septembre 2015",
    "Edits": 18
  },
  {
    "page": "Wikipedia:In the news/Candidates",
    "Edits": 17
  },
  {
    "page": "Wikipedia:Requests for page protection",
    "Edits": 17
  }
]
```

### Pretty Printing

Note that Druid SQL does not support pretty printing of results. The output above was formatted using the [jq](https://stedolan.github.io/jq/) JSON processing tool. Other instances of SQL queries in these tutorials will also use `jq` formatting.

### Additional Druid SQL queries

The following section provides additional SQL examples that plan to various types of native Druid queries.

#### Timeseries

An example SQL timeseries query is available at `examples/wikipedia-timeseries-sql.json`. This query retrieves the total number of lines deleted from pages for each hour of 2015-09-12.

```bash
curl -X 'POST' -H 'Content-Type:application/json' -d @examples/wikipedia-timeseries-sql.json http://localhost:8082/druid/v2/sql
```

The following results should be returned:

```
[
  {
    "HourTime": "2015-09-12T00:00:00.000Z",
    "LinesDeleted": 1761
  },
  {
    "HourTime": "2015-09-12T01:00:00.000Z",
    "LinesDeleted": 16208
  },
  {
    "HourTime": "2015-09-12T02:00:00.000Z",
    "LinesDeleted": 14543
  },
  {
    "HourTime": "2015-09-12T03:00:00.000Z",
    "LinesDeleted": 13101
  },
  {
    "HourTime": "2015-09-12T04:00:00.000Z",
    "LinesDeleted": 12040
  },
  {
    "HourTime": "2015-09-12T05:00:00.000Z",
    "LinesDeleted": 6399
  },
  {
    "HourTime": "2015-09-12T06:00:00.000Z",
    "LinesDeleted": 9036
  },
  {
    "HourTime": "2015-09-12T07:00:00.000Z",
    "LinesDeleted": 11409
  },
  {
    "HourTime": "2015-09-12T08:00:00.000Z",
    "LinesDeleted": 11616
  },
  {
    "HourTime": "2015-09-12T09:00:00.000Z",
    "LinesDeleted": 17509
  },
  {
    "HourTime": "2015-09-12T10:00:00.000Z",
    "LinesDeleted": 19406
  },
  {
    "HourTime": "2015-09-12T11:00:00.000Z",
    "LinesDeleted": 16284
  },
  {
    "HourTime": "2015-09-12T12:00:00.000Z",
    "LinesDeleted": 18672
  },
  {
    "HourTime": "2015-09-12T13:00:00.000Z",
    "LinesDeleted": 30520
  },
  {
    "HourTime": "2015-09-12T14:00:00.000Z",
    "LinesDeleted": 18025
  },
  {
    "HourTime": "2015-09-12T15:00:00.000Z",
    "LinesDeleted": 26399
  },
  {
    "HourTime": "2015-09-12T16:00:00.000Z",
    "LinesDeleted": 24759
  },
  {
    "HourTime": "2015-09-12T17:00:00.000Z",
    "LinesDeleted": 19634
  },
  {
    "HourTime": "2015-09-12T18:00:00.000Z",
    "LinesDeleted": 17345
  },
  {
    "HourTime": "2015-09-12T19:00:00.000Z",
    "LinesDeleted": 19305
  },
  {
    "HourTime": "2015-09-12T20:00:00.000Z",
    "LinesDeleted": 22265
  },
  {
    "HourTime": "2015-09-12T21:00:00.000Z",
    "LinesDeleted": 16394
  },
  {
    "HourTime": "2015-09-12T22:00:00.000Z",
    "LinesDeleted": 16379
  },
  {
    "HourTime": "2015-09-12T23:00:00.000Z",
    "LinesDeleted": 15289
  }
]
```
#### GroupBy

An example SQL GroupBy query is available at `examples/wikipedia-groupby-sql.json`. This query retrieves the total number of lines added for each Wikipedia language category (the channel) on 2015-09-12.

```bash
curl -X 'POST' -H 'Content-Type:application/json' -d @examples/wikipedia-groupby-sql.json http://localhost:8082/druid/v2/sql
```

The following results should be returned:

```
[
  {
    "channel": "#en.wikipedia",
    "EXPR$1": 3045299
  },
  {
    "channel": "#it.wikipedia",
    "EXPR$1": 711011
  },
  {
    "channel": "#fr.wikipedia",
    "EXPR$1": 642555
  },
  {
    "channel": "#ru.wikipedia",
    "EXPR$1": 640698
  },
  {
    "channel": "#es.wikipedia",
    "EXPR$1": 634670
  }
]
```

#### Scan

An example SQL scan query is available at `examples/wikipedia-scan-sql.json`. This query retrieves 5 user-page pairs from the datasource.


```bash
curl -X 'POST' -H 'Content-Type:application/json' -d @examples/wikipedia-scan-sql.json http://localhost:8082/druid/v2/sql
```

The following results should be returned:

```
[
  {
    "user": "Thiago89",
    "page": "Campeonato Mundial de Voleibol Femenino Sub-20 de 2015"
  },
  {
    "user": "91.34.200.249",
    "page": "Friede von Schönbrunn"
  },
  {
    "user": "TuHan-Bot",
    "page": "Trĩ vàng"
  },
  {
    "user": "Lowercase sigmabot III",
    "page": "User talk:ErrantX"
  },
  {
    "user": "BattyBot",
    "page": "Hans W. Jung"
  }
]
```

#### EXPLAIN PLAN FOR

By prepending `EXPLAIN PLAN FOR ` to a Druid SQL query, it is possible to see what native Druid queries a SQL query will plan into.

An example query that explains the top pages query shown earlier has been provided at `examples/wikipedia-explain-sql.json`:

```bash
curl -X 'POST' -H 'Content-Type:application/json' -d @examples/wikipedia-explain-top-pages-sql.json http://localhost:8082/druid/v2/sql
```

This will return the following plan:
```
[
  {
    "PLAN": "DruidQueryRel(query=[{\"queryType\":\"topN\",\"dataSource\":{\"type\":\"table\",\"name\":\"wikipedia\"},\"virtualColumns\":[],\"dimension\":{\"type\":\"default\",\"dimension\":\"page\",\"outputName\":\"d0\",\"outputType\":\"STRING\"},\"metric\":{\"type\":\"numeric\",\"metric\":\"a0\"},\"threshold\":10,\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"2015-09-12T00:00:00.000Z/2015-09-13T00:00:00.001Z\"]},\"filter\":null,\"granularity\":{\"type\":\"all\"},\"aggregations\":[{\"type\":\"count\",\"name\":\"a0\"}],\"postAggregations\":[],\"context\":{},\"descending\":false}], signature=[{d0:STRING, a0:LONG}])\n"
  }
]
```

## Further reading

The [Queries documentation](/docs/VERSION/querying/querying.html) has more information on Druid's native JSON queries.

The [Druid SQL documentation](/docs/VERSION/querying/sql.html) has more information on using Druid SQL queries.