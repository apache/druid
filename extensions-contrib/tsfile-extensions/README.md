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

# druid-tsfile-extensions

Community extension that lets Druid batch ingestion read [Apache TsFile](https://tsfile.apache.org/) files.

User documentation: [docs/development/extensions-contrib/tsfile.md](../../docs/development/extensions-contrib/tsfile.md).

## Building

```bash
mvn -pl extensions-contrib/tsfile-extensions -am package
```

## Compatibility

| Item | Version |
|------|---------|
| TsFile Java library | `org.apache.tsfile:tsfile:2.5.0` |
| TsFile format | Files written by the TsFile 2.x table model (table schemas in the file footer) |
| Druid | 39.0.0 |

The extension reads the table model. Files that only contain the legacy tree model (device paths such as
`root.sg.d1.s1` without a table schema in the footer) are not supported.

## Dependencies and licenses

The extension adds the following runtime dependencies, all Apache-2.0 compatible:

| Dependency | License |
|------------|---------|
| `org.apache.tsfile:tsfile` | Apache-2.0 |
| `org.apache.tsfile:common` | Apache-2.0 |
| `com.github.luben:zstd-jni` | BSD-2-Clause (managed by Druid) |
| `org.xerial.snappy:snappy-java` | Apache-2.0 (managed by Druid) |
| `at.yawk.lz4:lz4-java` | Apache-2.0 (managed by Druid) |
| `org.tukaani:xz` | Public domain (managed by Druid) |
| `org.antlr:antlr4-runtime` | BSD-3-Clause (managed by Druid) |

Only `org.apache.tsfile:tsfile` and `org.apache.tsfile:common` are new to Druid; the compression and parser
libraries are already shipped by Druid.

## Sample data

`examples/data/sensor.tsfile` is a three-row TsFile written by `TsFileTestData`, and `examples/sensor-ingest.json`
is an ingestion spec for it. The file contains one table, `sensor`:

| Column | TsFile category | TsFile type |
|--------|-----------------|-------------|
| `region` | TAG | STRING |
| `device` | TAG | STRING |
| `temperature` | FIELD | FLOAT |
| `humidity` | FIELD | DOUBLE |
| `status` | FIELD | BOOLEAN |

Expected input rows:

| `Time` | `region` | `device` | `temperature` | `humidity` | `status` |
|--------|----------|----------|---------------|------------|----------|
| 1000 | beijing | d1 | 21.5 | 0.5 | true |
| 2000 | beijing | d2 | 22.5 | *(null)* | false |
| 3000 | shanghai | d3 | 23.5 | 0.7 | true |

`Time` is in epoch milliseconds. The second row has no humidity value; TsFile returns it as null and the extension
omits it from the row.

To regenerate the sample file from this module:

```bash
mvn exec:java -Dexec.classpathScope=test \
  -Dexec.mainClass=org.apache.druid.data.input.tsfile.TsFileTestData
```

## Test plan

`TsFileInputFormatTest` covers:

| Test | What it checks |
|------|----------------|
| `testReadAllColumns` | All tag and field columns, timestamps, and a missing (null) value are read correctly |
| `testReadWithDefaultTimestampColumn` | The TsFile timestamp is also available under a non-default `timestampSpec` column |
| `testReadSelectedTableAndColumns` | `tables` and `columns` restrict the read |
| `testIntervalPushdown` | An `interval` restricts the read to rows inside it |
| `testIntervalIsEndExclusive` | A row at the end boundary of `interval` is excluded |
| `testEmptyIntervalReadsNothing` | An empty `interval` yields no rows |
| `testReadCommittedSampleFile` | The committed `examples/data/sensor.tsfile` matches the expected rows below |
| `testUnknownTable` | A missing table produces an `InvalidInputException` naming the table |
| `testUnknownColumn` | A missing column produces an `InvalidInputException` naming the column |
| `testSerde` | The input format round-trips through Jackson with `"type": "tsfile"` |

To run them:

```bash
mvn -pl extensions-contrib/tsfile-extensions -am test -Dtest=TsFileInputFormatTest
```

## Maintenance

This is a community extension. The Apache TsFile community owns maintenance and compatibility testing, as agreed
with the Druid community on [dev@druid.apache.org](https://lists.apache.org/thread/qmrkhobp7p07wkmcf4y4myq36k866zz0).
