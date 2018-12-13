---
layout: doc_page
title: "Tutorial: Querying data"
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

# Tutorial: Querying data

This tutorial will demonstrate how to query data in Druid, with examples for Druid's native query format and Druid SQL.

The tutorial assumes that you've already completed one of the 4 ingestion tutorials, as we will be querying the sample Wikipedia edits data.

* [Tutorial: Loading a file](../tutorials/tutorial-batch.html)
* [Tutorial: Loading stream data from Kafka](../tutorials/tutorial-kafka.html)
* [Tutorial: Loading a file using Hadoop](../tutorials/tutorial-batch-hadoop.html)
* [Tutorial: Loading stream data using Tranquility](../tutorials/tutorial-tranquility.html)

## Native JSON queries

Druid's native query format is expressed in JSON. We have included a sample native TopN query under `quickstart/tutorial/wikipedia-top-pages.json`:

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
curl -X 'POST' -H 'Content-Type:application/json' -d @quickstart/tutorial/wikipedia-top-pages.json http://localhost:8082/druid/v2?pretty
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

The tutorial package includes an example file that contains the SQL query shown above at `quickstart/tutorial/wikipedia-top-pages-sql.json`. Let's submit that query to the Druid broker:

```bash
curl -X 'POST' -H 'Content-Type:application/json' -d @quickstart/tutorial/wikipedia-top-pages-sql.json http://localhost:8082/druid/v2/sql
```

The following results should be returned:

```json
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

### dsql client

For convenience, the Druid package includes a SQL command-line client, located at `bin/dsql` from the Druid package root.

Let's now run `bin/dsql`; you should see the following prompt:

```bash
Welcome to dsql, the command-line client for Druid SQL.
Type "\h" for help.
dsql> 
```

To submit the query, paste it to the `dsql` prompt and press enter:

```bash
dsql> SELECT page, COUNT(*) AS Edits FROM wikipedia WHERE "__time" BETWEEN TIMESTAMP '2015-09-12 00:00:00' AND TIMESTAMP '2015-09-13 00:00:00' GROUP BY page ORDER BY Edits DESC LIMIT 10;
┌──────────────────────────────────────────────────────────┬───────┐
│ page                                                     │ Edits │
├──────────────────────────────────────────────────────────┼───────┤
│ Wikipedia:Vandalismusmeldung                             │    33 │
│ User:Cyde/List of candidates for speedy deletion/Subpage │    28 │
│ Jeremy Corbyn                                            │    27 │
│ Wikipedia:Administrators' noticeboard/Incidents          │    21 │
│ Flavia Pennetta                                          │    20 │
│ Total Drama Presents: The Ridonculous Race               │    18 │
│ User talk:Dudeperson176123                               │    18 │
│ Wikipédia:Le Bistro/12 septembre 2015                    │    18 │
│ Wikipedia:In the news/Candidates                         │    17 │
│ Wikipedia:Requests for page protection                   │    17 │
└──────────────────────────────────────────────────────────┴───────┘
Retrieved 10 rows in 0.06s.
```

### Additional Druid SQL queries

#### Timeseries

`SELECT FLOOR(__time to HOUR) AS HourTime, SUM(deleted) AS LinesDeleted FROM wikipedia WHERE "__time" BETWEEN TIMESTAMP '2015-09-12 00:00:00' AND TIMESTAMP '2015-09-13 00:00:00' GROUP BY FLOOR(__time to HOUR);`

```bash
dsql> SELECT FLOOR(__time to HOUR) AS HourTime, SUM(deleted) AS LinesDeleted FROM wikipedia WHERE "__time" BETWEEN TIMESTAMP '2015-09-12 00:00:00' AND TIMESTAMP '2015-09-13 00:00:00' GROUP BY FLOOR(__time to HOUR);
┌──────────────────────────┬──────────────┐
│ HourTime                 │ LinesDeleted │
├──────────────────────────┼──────────────┤
│ 2015-09-12T00:00:00.000Z │         1761 │
│ 2015-09-12T01:00:00.000Z │        16208 │
│ 2015-09-12T02:00:00.000Z │        14543 │
│ 2015-09-12T03:00:00.000Z │        13101 │
│ 2015-09-12T04:00:00.000Z │        12040 │
│ 2015-09-12T05:00:00.000Z │         6399 │
│ 2015-09-12T06:00:00.000Z │         9036 │
│ 2015-09-12T07:00:00.000Z │        11409 │
│ 2015-09-12T08:00:00.000Z │        11616 │
│ 2015-09-12T09:00:00.000Z │        17509 │
│ 2015-09-12T10:00:00.000Z │        19406 │
│ 2015-09-12T11:00:00.000Z │        16284 │
│ 2015-09-12T12:00:00.000Z │        18672 │
│ 2015-09-12T13:00:00.000Z │        30520 │
│ 2015-09-12T14:00:00.000Z │        18025 │
│ 2015-09-12T15:00:00.000Z │        26399 │
│ 2015-09-12T16:00:00.000Z │        24759 │
│ 2015-09-12T17:00:00.000Z │        19634 │
│ 2015-09-12T18:00:00.000Z │        17345 │
│ 2015-09-12T19:00:00.000Z │        19305 │
│ 2015-09-12T20:00:00.000Z │        22265 │
│ 2015-09-12T21:00:00.000Z │        16394 │
│ 2015-09-12T22:00:00.000Z │        16379 │
│ 2015-09-12T23:00:00.000Z │        15289 │
└──────────────────────────┴──────────────┘
Retrieved 24 rows in 0.08s.
```

#### GroupBy

`SELECT channel, SUM(added) FROM wikipedia WHERE "__time" BETWEEN TIMESTAMP '2015-09-12 00:00:00' AND TIMESTAMP '2015-09-13 00:00:00' GROUP BY channel ORDER BY SUM(added) DESC LIMIT 5;`

```bash
dsql> SELECT channel, SUM(added) FROM wikipedia WHERE "__time" BETWEEN TIMESTAMP '2015-09-12 00:00:00' AND TIMESTAMP '2015-09-13 00:00:00' GROUP BY channel ORDER BY SUM(added) DESC LIMIT 5;
┌───────────────┬─────────┐
│ channel       │ EXPR$1  │
├───────────────┼─────────┤
│ #en.wikipedia │ 3045299 │
│ #it.wikipedia │  711011 │
│ #fr.wikipedia │  642555 │
│ #ru.wikipedia │  640698 │
│ #es.wikipedia │  634670 │
└───────────────┴─────────┘
Retrieved 5 rows in 0.05s.
```

#### Scan

` SELECT user, page FROM wikipedia WHERE "__time" BETWEEN TIMESTAMP '2015-09-12 02:00:00' AND TIMESTAMP '2015-09-12 03:00:00' LIMIT 5;`

```bash
 dsql> SELECT user, page FROM wikipedia WHERE "__time" BETWEEN TIMESTAMP '2015-09-12 02:00:00' AND TIMESTAMP '2015-09-12 03:00:00' LIMIT 5;
┌────────────────────────┬────────────────────────────────────────────────────────┐
│ user                   │ page                                                   │
├────────────────────────┼────────────────────────────────────────────────────────┤
│ Thiago89               │ Campeonato Mundial de Voleibol Femenino Sub-20 de 2015 │
│ 91.34.200.249          │ Friede von Schönbrunn                                  │
│ TuHan-Bot              │ Trĩ vàng                                               │
│ Lowercase sigmabot III │ User talk:ErrantX                                      │
│ BattyBot               │ Hans W. Jung                                           │
└────────────────────────┴────────────────────────────────────────────────────────┘
Retrieved 5 rows in 0.04s.
```

#### EXPLAIN PLAN FOR

By prepending `EXPLAIN PLAN FOR ` to a Druid SQL query, it is possible to see what native Druid queries a SQL query will plan into.

Using the TopN query above as an example:

`EXPLAIN PLAN FOR SELECT page, COUNT(*) AS Edits FROM wikipedia WHERE "__time" BETWEEN TIMESTAMP '2015-09-12 00:00:00' AND TIMESTAMP '2015-09-13 00:00:00' GROUP BY page ORDER BY Edits DESC LIMIT 10;`

```bash
dsql> EXPLAIN PLAN FOR SELECT page, COUNT(*) AS Edits FROM wikipedia WHERE "__time" BETWEEN TIMESTAMP '2015-09-12 00:00:00' AND TIMESTAMP '2015-09-13 00:00:00' GROUP BY page ORDER BY Edits DESC LIMIT 10;
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ PLAN                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    │
├─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ DruidQueryRel(query=[{"queryType":"topN","dataSource":{"type":"table","name":"wikipedia"},"virtualColumns":[],"dimension":{"type":"default","dimension":"page","outputName":"d0","outputType":"STRING"},"metric":{"type":"numeric","metric":"a0"},"threshold":10,"intervals":{"type":"intervals","intervals":["2015-09-12T00:00:00.000Z/2015-09-13T00:00:00.001Z"]},"filter":null,"granularity":{"type":"all"},"aggregations":[{"type":"count","name":"a0"}],"postAggregations":[],"context":{},"descending":false}], signature=[{d0:STRING, a0:LONG}]) │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
Retrieved 1 row in 0.03s.
```

## Further reading

The [Queries documentation](../querying/querying.html) has more information on Druid's native JSON queries.

The [Druid SQL documentation](../querying/sql.html) has more information on using Druid SQL queries.
