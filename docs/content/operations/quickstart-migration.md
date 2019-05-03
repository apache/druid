---
layout: doc_page
title: "Migrating Derby Metadata and Local Deep Storage"
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

# Migrating Derby Metadata and Local Deep Storage

If you have been running an evaluation Druid cluster using the built-in Derby metadata storage and local
deep storage (configurations used by the tutorial and single-machine quickstarts), and wish to migrate to a 
more production-capable metadata store such as MySQL or PostgreSQL, and/or migrate your deep storage from local
to S3 or HDFS, Druid provides the `export-metadata` tool to assist with such migrations.

This tool exports the contents of the following Druid tables:
- segments
- rules
- config
- datasource
- supervisors

Additionally, the tool can rewrite the local deep storage location descriptors in the rows of the segments table 
to point to new deep storage locations (S3, HDFS, and local rewrite paths are supported).

## `export-metadata` Options

The `export-metadata` tool provides the following options:

### Output Path

`--output-path`, `-o`: The output directory of the tool. CSV files for the Druid segments, rules, config, datasource, and supervisors tables will be written to this directory.

### S3 Migration

By setting the options below, the tool will rewrite the segment load specs to point to a new S3 deep storage location.

This helps users migrate segments stored in local deep storage to S3.

`--s3bucket`, `-b`: The S3 bucket that will hold the migrated segments
`--s3baseKey`, `-k`: The base S3 key where the migrated segments will be stored

When copying the local deep storage segments to S3, the rewrite performed by this tool requires that the directory structure of the segments be unchanged.

For example, if the cluster had the following local deep storage configuration:

```
druid.storage.type=local
druid.storage.storageDirectory=/druid/segments
```

If the target S3 bucket was `migration`, with a base key of `example`, the contents of `s3://migration/example/` must be identical to that of `/druid/segments` on the old local filesystem.

### HDFS Migration

By setting the options below, the tool will rewrite the segment load specs to point to a new HDFS deep storage location.

This helps users migrate segments stored in local deep storage to HDFS.

`--hadoopStorageDirectory`, `h`: The HDFS path that will hold the migrated segments

When copying the local deep storage segments to HDFS, the rewrite performed by this tool requires that the directory structure of the segments be unchanged, with the exception of directory names containing colons (`:`).

For example, if the cluster had the following local deep storage configuration:

```
druid.storage.type=local
druid.storage.storageDirectory=/druid/segments
```

If the target hadoopStorageDirectory was `/migration/example`, the contents of `hdfs:///migration/example/` must be identical to that of `/druid/segments` on the old local filesystem.

Additionally, the segments paths in local deep storage contain colons(`:`) in their names, e.g.:

`wikipedia/2016-06-27T02:00:00.000Z_2016-06-27T03:00:00.000Z/2019-05-03T21:57:15.950Z/1/index.zip`

HDFS cannot store files containing colons, and this tool expects the colons to be replaced with underscores (`_`) in HDFS.

In this example, the `wikipedia` segment above under `/druid/segments` in local deep storage would need to be migrated to HDFS under `hdfs:///migration/example/` with the following path:

`wikipedia/2016-06-27T02_00_00.000Z_2016-06-27T03_00_00.000Z/2019-05-03T21_57_15.950Z/1/index.zip`

### Local to Local Migration

By setting the options below, the tool will rewrite the segment load specs to point to a new local deep storage location.

This helps users migrate segments stored in local deep storage to a new path (e.g., a new NFS mount).

`--newLocalPath`, `-n`: The new path on the local filesystem that will hold the migrated segments

When copying the local deep storage segments to a new path, the rewrite performed by this tool requires that the directory structure of the segments be unchanged.

For example, if the cluster had the following local deep storage configuration:

```
druid.storage.type=local
druid.storage.storageDirectory=/druid/segments
```

If the new path  was `/migration/example`, the contents of `/migration/example/` must be identical to that of `/druid/segments` on the local filesystem.

## Running the tool

To use the tool, you can run the following command:

```bash
java -classpath "lib/*:conf/druid/single-server/micro-quickstart/_common" org.apache.druid.cli.Main tools export-metadata -o /tmp/csv
```

In the example command above:
- `lib` is the the Druid lib directory
- `conf/druid/single-server/micro-quickstart/_common` is the directory containing the `common.runtime.properties` file used by the existing deployment.
- `/tmp/csv` is the output directory

## Importing to MySQL/PostgreSQL

After running the tool, the output directory will contain `<table-name>_raw.csv` and `<table-name>.csv` files.

The `<table-name>_raw.csv` files are intermediate files used by the tool, containing the table data as exported by Derby without modification.

The `<table-name>.csv` files are used for import into another database such as MySQL and PostgreSQL and have any configured deep storage location rewrites applied.

Example import commands for MySQL and PostgreSQL are shown below.

These example import commands expect `/tmp/csv` and its contents to be accessible from the server. For other options, such as importing from the client filesystem, please refer to the MySQL or PostgreSQL documentation.

### MySQL

```sql
LOAD DATA INFILE '/tmp/csv/druid_segments.csv' INTO TABLE druid_segments FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' (id,dataSource,created_date,start,end,partitioned,version,used,payload); SHOW WARNINGS;

LOAD DATA INFILE '/tmp/csv/druid_rules.csv' INTO TABLE druid_rules FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' (id,dataSource,version,payload); SHOW WARNINGS;

LOAD DATA INFILE '/tmp/csv/druid_config.csv' INTO TABLE druid_config FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' (name,payload); SHOW WARNINGS;

LOAD DATA INFILE '/tmp/csv/druid_dataSource.csv' INTO TABLE druid_dataSource FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' (dataSource,created_date,commit_metadata_payload,commit_metadata_sha1); SHOW WARNINGS;

LOAD DATA INFILE '/tmp/csv/druid_supervisors.csv' INTO TABLE druid_supervisors FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' (id,spec_id,created_date,payload); SHOW WARNINGS;
```

### PostgreSQL

```sql
COPY druid_segments(id,dataSource,created_date,start,"end",partitioned,version,used,payload) FROM '/tmp/csv/druid_segments.csv' DELIMITER ',' CSV;

COPY druid_rules(id,dataSource,version,payload) FROM '/tmp/csv/druid_rules.csv' DELIMITER ',' CSV;

COPY druid_config(name,payload) FROM '/tmp/csv/druid_config.csv' DELIMITER ',' CSV;

COPY druid_dataSource(dataSource,created_date,commit_metadata_payload,commit_metadata_sha1) FROM '/tmp/csv/druid_dataSource.csv' DELIMITER ',' CSV;

COPY druid_supervisors(id,spec_id,created_date,payload) FROM '/tmp/csv/druid_supervisors.csv' DELIMITER ',' CSV;
```