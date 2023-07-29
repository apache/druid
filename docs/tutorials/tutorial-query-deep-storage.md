---
id: tutorial-query-deep-storage
title: "Tutorial: Query from deep storage"
---

> Query from deep storage is an [experimental feature](../development/experimental.md).

Query from deep storage allows you to query segments that are stored only in deep storage, which provides lower costs than if you were to load everything onto Historical processes. The tradeoff is that queries from deep storage may take longer to complete. 

This tutorial walks you through loading example data, configuring load rules so that not all the segments are loaded onto Historical processes, and querying data from deep storage.

To run the queries in this tutorial, replace `ROUTER:PORT` with the location of the Router process and its port number. For example, use `localhost:8888` for the quickstart deployment.

## Load example data

Use the **Load data** wizard or the following SQL query to ingest the `wikipedia` sample datasource bundled with Druid. If you use the wizard, make sure you change the partitioning to be by hour.

Partitioning by hour provides more segment granularity, so can selectively load segments onto Historicals or keep in deep storage.

<details><summary>Show the query</summary>

```sql
REPLACE INTO "wikipedia" OVERWRITE ALL
WITH "ext" AS (SELECT *
FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://druid.apache.org/data/wikipedia.json.gz"]}',
    '{"type":"json"}'
  )
) EXTEND ("isRobot" VARCHAR, "channel" VARCHAR, "timestamp" VARCHAR, "flags" VARCHAR, "isUnpatrolled" VARCHAR, "page" VARCHAR, "diffUrl" VARCHAR, "added" BIGINT, "comment" VARCHAR, "commentLength" BIGINT, "isNew" VARCHAR, "isMinor" VARCHAR, "delta" BIGINT, "isAnonymous" VARCHAR, "user" VARCHAR, "deltaBucket" BIGINT, "deleted" BIGINT, "namespace" VARCHAR, "cityName" VARCHAR, "countryName" VARCHAR, "regionIsoCode" VARCHAR, "metroCode" BIGINT, "countryIsoCode" VARCHAR, "regionName" VARCHAR))
SELECT
  TIME_PARSE("timestamp") AS "__time",
  "isRobot",
  "channel",
  "flags",
  "isUnpatrolled",
  "page",
  "diffUrl",
  "added",
  "comment",
  "commentLength",
  "isNew",
  "isMinor",
  "delta",
  "isAnonymous",
  "user",
  "deltaBucket",
  "deleted",
  "namespace",
  "cityName",
  "countryName",
  "regionIsoCode",
  "metroCode",
  "countryIsoCode",
  "regionName"
FROM "ext"
PARTITIONED BY HOUR
```

</details>

## Configure a load rule

The load rules in this section do two things:

- Any segments that fall within the following interval are only available from deep storage:

```
2016-06-27T00:00:00.000Z/2016-06-27T02:00:00.000Z
```

- The segments that fall within the following interval get loaded onto Historical tiers:

```
2016-06-27T02:00:00.001Z/2016-06-27T23:00:00.000Z
```

```json
[
  {
    "interval": "2016-06-27T00:00:00.000Z/2016-06-27T02:00:00.000Z",
    "tieredReplicants": {},
    "useDefaultTierForNull": false,
    "type": "loadByInterval"
  },
  {
    "interval": "2016-06-27T02:00:00.001Z/2016-06-27T23:00:00.000Z",
    "tieredReplicants": {
      "_default_tier": 2
    },
    "useDefaultTierForNull": false,
    "type": "loadByInterval"
  }
]
```

You can configure the load rules through the API or the Druid console. For example,

### Verify the replication factor

Segments that are only available from deep storage have a `replication_factor` of 0 in the Druid system table. You can verify that your load rule worked as intended using the following query:

```sql
SELECT "segment_id", "replication_factor", "num_replicas"  FROM sys."segments" WHERE datasource = 'wikipedia'
```

You can also verify it through the Druid console on the **Segments** page.

Note that the number of replicas and replication factor may differ temporarily as Druid processes your retention rules.

## Query from deep storage

Now that there is a segment that's only available from deep storage, run the following query:

```sql
SELECT * FROM "page" WHERE "__time" <  TIMESTAMP'2016-06-27 00:10:00'"
```

With the context parameter:

```json
"executionMode": "ASYNC"
```

```
curl --location 'http://ROUTER:PORT/druid/v2/sql/statements' \
--header 'Content-Type: application/json' \
--data '{
    "query":"SELECT page FROM \"wikipedia\" WHERE \"__time\" <  TIMESTAMP'\''2016-06-27 00:10:00'\''",
    "context":{
        "executionMode":"ASYNC"
    }
    
}'
```

This query looks for records with timestamps that precede `01:00:00`. Based on the load rule you configured earlier, this data is only available from deep storage.

When you submit the query through the API, you get the following response

<details><summary>Show the response</summary>

```json
{
    "queryId": "query-6888b6f6-e597-456c-9004-222b05b97051",
    "state": "ACCEPTED",
    "createdAt": "2023-07-28T21:59:02.334Z",
    "schema": [
        {
            "name": "page",
            "type": "VARCHAR",
            "nativeType": "STRING"
        }
    ],
    "durationMs": -1
}
```

</details>

Compare this to if you were to submit the query to Druid SQL's regular endpoint, `POST /sq/`: 

```
curl --location 'http://ROUTER:PORT/druid/v2/sql/' \
--header 'Content-Type: application/json' \
--data '{
    "query":"SELECT * FROM \"wikipedia\" WHERE \"__time\" <  TIMESTAMP'\''2016-06-27 00:10:00'\''"
    
}'
```


The response you get back is an empty response cause there are no records on the Historicals

## Get query status

### Response for a running query

The response for a running query is the same as the response from when you submitted the query except the `state` is `RUNNING` instead of `ACCEPTED`.



### Response for a completed query

```json
{
    "queryId": "query-6888b6f6-e597-456c-9004-222b05b97051",
    "state": "SUCCESS",
    "createdAt": "2023-07-28T21:59:02.334Z",
    "schema": [
        {
            "name": "page",
            "type": "VARCHAR",
            "nativeType": "STRING"
        }
    ],
    "durationMs": 87351,
    "result": {
        "numTotalRows": 152,
        "totalSizeInBytes": 9036,
        "dataSource": "__query_select",
        "sampleRecords": [
            [
                "Salo Toraut"
            ],
            [
                "利用者:ワーナー成増/放送ウーマン賞"
            ],
            [
                "Bailando 2015"
            ],
            ...
            ...
            ...
            [
                "Talk:The Zeitgeist Movement"
            ]
        ],
        "pages": [
            {
                "id": 0,
                "numRows": 152,
                "sizeInBytes": 9036
            }
        ]
    }
}
```

## Get query results


```json

```