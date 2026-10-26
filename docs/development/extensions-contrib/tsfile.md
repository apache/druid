---
id: tsfile
title: "Apache TsFile input format"
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

To use this Apache Druid extension, [include](../../configuration/extensions.md#loading-extensions) `druid-tsfile-extensions` in the extensions load list.

This extension enables Druid to ingest [Apache TsFile](https://tsfile.apache.org/) files. TsFile is the columnar
storage format for time series data used by [Apache IoTDB](https://iotdb.apache.org/). It is read through the
TsFile Java library, version 2.5.0.

The extension supports batch ingestion from local files and from any input source that Druid can read as a file
(for example, S3). Local files are read in place; files from remote input sources are first fetched to the task's
temporary directory, so a non-local input source needs enough local disk for one file at a time.

## TsFile input format

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| type | String | Must be `tsfile` | yes |
| tables | Array of String | Tables to read. When omitted or empty, all tables in the file are read. | no |
| columns | Array of String | Columns to read. When omitted or empty, all columns of each selected table are read. | no |
| interval | String | Time range to read, in Druid interval format (`start/end`). The range is end-exclusive. When omitted, each file is read in full. | no |

### Time range pruning

When `interval` is set, it is pushed down to the TsFile reader, which uses the time statistics stored with each
chunk to skip chunks that fall outside the range. Only matching rows are returned.

Druid does not pass the task's time window to input formats, so the range has to be set explicitly here. Rows are
still filtered by the granularity spec as usual.

### Timestamp

TsFile stores its own timestamp, which is exposed to Druid under the column name `Time`. The ingestion spec must
therefore select `Time` as the timestamp column with a numeric format, for example:

```json
"timestampSpec": {
  "column": "Time",
  "format": "millis"
}
```

If `timestampSpec.column` is set to another name, the TsFile timestamp is also placed under that name so that
existing ingestion specs keep working. In that case a TsFile column with the same name is shadowed.

### Column types

Tag, field, and attribute columns are all converted to input rows. The mapping of TsFile data types is:

| TsFile type | Druid value |
|-------------|-------------|
| INT32 | Integer |
| INT64 | Long |
| FLOAT | Float |
| DOUBLE | Double |
| BOOLEAN | Boolean |
| TEXT, STRING | String |
| TIMESTAMP | Long (epoch milliseconds) |
| DATE | Integer (epoch day) |
| BLOB | Base64-encoded String |

`VECTOR`, `OBJECT`, `ROW`, and `UNKNOWN` columns are not supported; requesting one of them fails the ingestion task.
Null values are omitted from the row.

## Example: local batch ingestion

Suppose a TsFile file `sensor.tsfile` contains a table `sensor` with the tag columns `region` and `device` and the
value columns `temperature`, `humidity`, and `status`. The following spec ingests it:

```json
{
  "type": "index_parallel",
  "spec": {
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "local",
        "baseDir": "/data/tsfile",
        "filter": "*.tsfile"
      },
      "inputFormat": {
        "type": "tsfile",
        "tables": ["sensor"],
        "columns": ["region", "device", "temperature", "humidity", "status"]
      }
    },
    "dataSchema": {
      "dataSource": "sensor",
      "timestampSpec": {
        "column": "Time",
        "format": "millis"
      },
      "dimensionsSpec": {
        "dimensions": ["region", "device"]
      },
      "metricsSpec": [
        {"type": "doubleSum", "name": "temperature", "fieldName": "temperature"},
        {"type": "doubleSum", "name": "humidity", "fieldName": "humidity"}
      ],
      "granularitySpec": {
        "type": "uniform",
        "segmentGranularity": "DAY",
        "queryGranularity": "MINUTE",
        "rollup": true
      }
    },
    "tuningConfig": {
      "type": "index_parallel"
    }
  }
}
```

## Limitations

- The input format is not splittable. With a splittable input source such as `local`, `s3`, or `gcs`, Druid still
  parallelizes ingestion one file per task.
- Druid's segment granularity is not pushed down to TsFile. To read only part of a file, set `interval`.
- A single ingestion spec reads one row schema. If a file contains several tables with different columns, either
  select one table with `tables` or ingest each table separately.
