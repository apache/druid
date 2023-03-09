---
id: migrate-from-firehose
title: "Migrate from firehose to input source ingestion"
sidebar_label: "Migrate from firehose"
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

Apache deprecated support for Druid firehoses in version 0.17. Support for firehose ingestion will be removed in version 26.0.

If you're using a firehose for batch ingestion, we strongly recommend that you follow the instructions on this page to transition to using native batch ingestion input sources as soon as possible. 

Firehose ingestion doesn't work with newer Druid versions, so you must be using an ingestion spec with a defined input source before you upgrade. 

## Migrate from firehose ingestion to an input source

To migrate from firehose ingestion, you can use the Druid console to update your ingestion spec, or you can update it manually.

### Use the Druid console

To update your ingestion spec using the Druid console, open the console and copy your spec into the **Edit spec** stage of the data loader.

Druid converts the spec into one with a defined input source. For example, it converts the [example firehose ingestion spec](#example-firehose-ingestion-spec) below into the [example ingestion spec after migration](#example-ingestion-spec-after-migration).

If you're unable to use the console or you have problems with the console method, the alternative is to update your ingestion spec manually.

### Update your ingestion spec manually

To update your ingestion spec manually, copy your existing spec into a new file. Refer to [Native batch ingestion with firehose (Deprecated)](./native-batch-firehose.md) for a description of firehose properties.

Edit the new file as follows:

1. In the `ioConfig` component, replace the `firehose` definition with an `inputSource` definition for your chosen input source. See [Native batch input sources](./native-batch-input-source.md) for details.
2. Move the `timeStampSpec` definition from `parser.parseSpec` to the `dataSchema` component.
3. Move the `dimensionsSpec` definition from `parser.parseSpec` to the `dataSchema` component.
4. Move the `format` definition from `parser.parseSpec` to an `inputFormat` definition in `ioConfig`.
5. Delete the `parser` definition.
6. Save the file.
You can check the format of your new ingestion file against the [migrated example](#example-ingestion-spec-after-migration) below.
7. Test the new ingestion spec with a temporary data source.
8. Once you've successfully ingested sample data with the new spec, stop firehose ingestion and switch to the new spec.

When the transition is complete, you can upgrade Druid to the latest version. See the [Druid release notes](https://druid.apache.org/downloads.html) for upgrade instructions.

### Example firehose ingestion spec

An example firehose ingestion spec is as follows:

```json
{
  "type" : "index",
  "spec" : {
     "dataSchema" : {
        "dataSource" : "wikipedia",
        "metricsSpec" : [
           {
              "type" : "count",
              "name" : "count"
           },
           {
              "type" : "doubleSum",
              "name" : "added",
              "fieldName" : "added"
           },
           {
              "type" : "doubleSum",
              "name" : "deleted",
              "fieldName" : "deleted"
           },
           {
              "type" : "doubleSum",
              "name" : "delta",
              "fieldName" : "delta"
           }
        ],
        "granularitySpec" : {
           "type" : "uniform",
           "segmentGranularity" : "DAY",
           "queryGranularity" : "NONE",
           "intervals" : [ "2013-08-31/2013-09-01" ]
        },
        "parser": {
           "type": "string",
           "parseSpec": {
              "format": "json",
              "timestampSpec" : {
                 "column" : "timestamp",
                 "format" : "auto"
              },
              "dimensionsSpec" : {
                 "dimensions": ["country", "page","language","user","unpatrolled","newPage","robot","anonymous","namespace","continent","region","city"],
                 "dimensionExclusions" : []
              }
           }
        }
     },
     "ioConfig" : {
        "type" : "index",
        "firehose" : {
           "type" : "local",
           "baseDir" : "examples/indexing/",
           "filter" : "wikipedia_data.json"
        }
     },
     "tuningConfig" : {
        "type" : "index",
        "partitionsSpec": {
           "type": "single_dim",
           "partitionDimension": "country",
           "targetRowsPerSegment": 5000000
        }
     }
  }
}
```

### Example ingestion spec after migration

The following example illustrates the result of migrating the [example firehose ingestion spec](#example-firehose-ingestion-spec) to a spec with an input source:

```json
{
 "type" : "index",
 "spec" : {
   "dataSchema" : {
     "dataSource" : "wikipedia",
     "timestampSpec" : {
       "column" : "timestamp",
       "format" : "auto"
     },
     "dimensionsSpec" : {
       "dimensions": ["country", "page","language","user","unpatrolled","newPage","robot","anonymous","namespace","continent","region","city"],
       "dimensionExclusions" : []
     },
     "metricsSpec" : [
       {
         "type" : "count",
         "name" : "count"
       },
       {
         "type" : "doubleSum",
         "name" : "added",
         "fieldName" : "added"
       },
       {
         "type" : "doubleSum",
         "name" : "deleted",
         "fieldName" : "deleted"
       },
       {
         "type" : "doubleSum",
         "name" : "delta",
         "fieldName" : "delta"
       }
     ],
     "granularitySpec" : {
       "type" : "uniform",
       "segmentGranularity" : "DAY",
       "queryGranularity" : "NONE",
       "intervals" : [ "2013-08-31/2013-09-01" ]
     }
   },
   "ioConfig" : {
     "type" : "index",
     "inputSource" : {
       "type" : "local",
       "baseDir" : "examples/indexing/",
       "filter" : "wikipedia_data.json"
      },
      "inputFormat": {
        "type": "json"
      }
   },
   "tuningConfig" : {
     "type" : "index",
     "partitionsSpec": {
       "type": "single_dim",
       "partitionDimension": "country",
       "targetRowsPerSegment": 5000000
     }
   }
 }
}
```

## Learn more

For more information, see the following pages:

- [Ingestion](./index.md): Overview of the Druid ingestion process.
- [Native batch ingestion](./native-batch.md): Description of the supported native batch indexing tasks.
- [Ingestion spec reference](./ingestion-spec.md): Description of the components and properties in the ingestion spec.
