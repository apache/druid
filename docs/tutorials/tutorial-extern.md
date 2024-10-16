---
id: tutorial-extern
title: Export query results
sidebar_label: Export results
description: How to use EXTERN to export query results.
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

This tutorial demonstrates how to use the [EXTERN](..multi-stage-query/reference#extern-function) function Apache Druid&circledR; to export data.

## Prerequisites

Before you follow the steps in this tutorial, download Druid as described in the [Local quickstart](index.md).
Do not start Druid, you'll do that as part of the tutorial.

You should be familiar with ingesting and querying data in Druid.
If you haven't already, go through the [Query data](../tutorials/tutorial-query.md) tutorial first.

## Export query results to the local file system

This example demonstrates how to configure Druid to export to the local file system.
It is OK to learn about EXTERN syntax for exporting data.
It is not suitable for production scenarios.

### Configure Druid local export directory 

The following commands set the base path for the Druid exports to `/tmp/druid/`.
If the account running Druid does not have access to `/tmp/druid/`, change the path.
For example: `/Users/Example/druid`.
If you change the path in this step, use the updated path in all subsequent steps.

From the root of the Druid distribution, run the following:

```bash
export export_path="/tmp/druid"
sed -i -e $'$a\\\n\\\n\\\n#\\\n###Local export\\\n#\\\ndruid.export.storage.baseDir='$export_path conf/druid/auto/_common/common.runtime.properties
```

This adds the following section to the Druid quicstart `common.runtime.properties`:

```
#
###Local export
#
druid.export.storage.baseDir=/tmp/druid/
```

### Start Druid and load sample data

From the root of the Druid distribution, launch Druid as follows:

```bash
./bin/start-druid
```

From the [Query view](http://localhost:8888/unified-console.html#workbench), run the following command to load the Wikipedia example data set:

```sql
REPLACE INTO "wikipedia" OVERWRITE ALL
WITH "ext" AS (
  SELECT *
  FROM TABLE(
    EXTERN(
      '{"type":"http","uris":["https://druid.apache.org/data/wikipedia.json.gz"]}',
      '{"type":"json"}'
    )
  ) EXTEND ("isRobot" VARCHAR, "channel" VARCHAR, "timestamp" VARCHAR, "flags" VARCHAR, "isUnpatrolled" VARCHAR, "page" VARCHAR, "diffUrl" VARCHAR, "added" BIGINT, "comment" VARCHAR, "commentLength" BIGINT, "isNew" VARCHAR, "isMinor" VARCHAR, "delta" BIGINT, "isAnonymous" VARCHAR, "user" VARCHAR, "deltaBucket" BIGINT, "deleted" BIGINT, "namespace" VARCHAR, "cityName" VARCHAR, "countryName" VARCHAR, "regionIsoCode" VARCHAR, "metroCode" BIGINT, "countryIsoCode" VARCHAR, "regionName" VARCHAR)
)
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
PARTITIONED BY DAY
```

### Query to export data

Run the following query to export query results to the path:
`/tmp/druid/wiki_example`.
The path must be a subdirectory of the `druid.export.storage.baseDir`.


```sql
INSERT INTO
  EXTERN(
    local(exportPath => '/tmp/druid/wiki_example')
        )
AS CSV
SELECT "channel",
  SUM("delta") AS "changes"
FROM "wikipedia"
GROUP BY 1
LIMIT 10
```

Druid exports the results of the qurey to the `/tmp/druid/wiki_example` dirctory.
Run the following comannd to list the contents of 

```bash
ls '/tmp/druid/wiki_example'
```

The results are a csv file export of the data and a directory   

## Learn more

See the following topics for more information:

* [Update data](./tutorial-update-data.md) for a tutorial on updating data in Druid.
* [Data updates](../data-management/update.md) for an overview of updating data in Druid.
