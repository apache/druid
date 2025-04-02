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

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This tutorial demonstrates how to use the Apache Druid&circledR; SQL [EXTERN](../multi-stage-query/reference.md#extern-function) function to export data.

## Prerequisites

Before you follow the steps in this tutorial, download Druid as described in the [Local quickstart](index.md).
Don't start Druid, you'll do that as part of the tutorial.

You should be familiar with ingesting and querying data in Druid.
If you haven't already, go through the [Query data](../tutorials/tutorial-query.md) tutorial first.

## Export query results to the local file system

This example demonstrates how to configure Druid to export data to the local file system.
While you can use this approach to learn about EXTERN syntax for exporting data, it's not suitable for production scenarios.

### Configure Druid local export directory 

The following commands set the base path for the Druid exports to `/tmp/druid/`.
If the account running Druid doesn't have access to `/tmp/druid/`, change the path.
For example: `/Users/Example/druid`.
If you change the path in this step, use the updated path in all subsequent steps.

From the root of the Druid distribution, run the following:

```bash
export export_path="/tmp/druid"
sed -i -e $'$a\\\n\\\n\\\n#\\\n###Local export\\\n#\\\ndruid.export.storage.baseDir='$export_path' conf/druid/auto/_common/common.runtime.properties
```

This adds the following section to the Druid `common.runtime.properties` configuration file located in `conf/druid/auto/_common`:

```
#
###Local export
#
druid.export.storage.baseDir=/tmp/druid/
```

### Start Druid and load sample data

1. From the root of the Druid distribution, launch Druid as follows:

     ```bash
    ./bin/start-druid
     ```
1. After Druid starts, open [http://localhost:8888/](http://localhost:8888/) in your browser to access the Web Console.
1. From the [Query view](http://localhost:8888/unified-console.html#workbench), run the following command to load the Wikipedia example data set:
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

Open a new tab and run the following query to export query results to the path:
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

Druid exports the results of the query to the `/tmp/druid/wiki_example` directory.
Run the following command to list the contents of 

```bash
ls /tmp/druid/wiki_example
```

The results are a CSV file export of the data and a directory.

## Export query results to cloud storage

The steps to export to cloud storage are similar to exporting to the local file system.
Druid supports Amazon S3 or Google Cloud Storage (GCS) as cloud storage destinations.

1. Enable the extension for your cloud storage destination. See [Loading core extensions](../configuration/extensions.md#loading-core-extensions).
   - **Amazon S3**: `druid-s3-extensions`
   - **GCS**: `google-extensions`
  See [Loading core extensions](../configuration/extensions.md#loading-core-extensions) for more information.
1. Configure the additional properties for your cloud storage destination. Replace `{CLOUD}` with `s3` or `google` accordingly:
   - `druid.export.storage.{CLOUD}.tempLocalDir`:  Local temporary directory where the query engine stages files to export.
   - `druid.export.storage.{CLOUD}.allowedExportPaths`: S3 or GS prefixes allowed as Druid export locations. For example `[\"s3://bucket1/export/\",\"s3://bucket2/export/\"]` or `[\"gs://bucket1/export/\", \"gs://bucket2/export/\"]`.
   - `druid.export.storage.{CLOUD}.maxRetry`: Maximum number of times to attempt cloud API calls to avoid failures from transient errors.
   - `druid.export.storage.s3.chunkSize`: Maximum size of individual data chunks to store in the temporary directory.
1. Verify the instance role has the correct permissions to the bucket and folders: read, write, create, and delete. See [Permissions for durable storage](../multi-stage-query/security.md#permissions-for-durable-storage).
1. Use the query syntax for your cloud storage type. For example:

   <Tabs>

   <TabItem value="1" label="S3">

    ```sql
    INSERT INTO
    EXTERN(
      s3(bucket => 'your_bucket', prefix => 'prefix/to/files'))
    AS CSV
    SELECT "channel",
    SUM("delta") AS "changes"
    FROM "wikipedia"
    GROUP BY 1
    LIMIT 10
    ```

   </TabItem>

   <TabItem value="2" label="GCS">

   ```sql
   INSERT INTO
   EXTERN
    google(bucket => 'your_bucket', prefix => 'prefix/to/files')
   AS CSV
   SELECT "channel",
   SUM("delta") AS "changes"
   FROM "wikipedia"
   GROUP BY 1
   LIMIT 10
   ``` 

   </TabItem>

   </Tabs>

1. When querying, use the `rowsPerPage` query context parameter to restrict the output file size. While it's possible to add a very large LIMIT at the end of your query to force Druid to create a single file, we don't recommend this technique.

## Learn more

See the following topics for more information:

* [Export to a destination](../multi-stage-query/reference.md#extern-to-export-to-a-destination) for a reference of the EXTERN.
* [SQL-based ingestion security](../multi-stage-query/security.md#permissions-for-durable-storage) for cloud permission requirements for MSQ.
