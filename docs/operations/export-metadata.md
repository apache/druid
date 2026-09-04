---
id: export-metadata
title: "Export Metadata Tool"
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


Druid includes an `export-metadata` tool for assisting with migration of cluster metadata and deep storage.

This tool exports the contents of the following Druid metadata tables:

- segments
- rules
- config
- datasource
- supervisors

Additionally, the tool can rewrite the local deep storage location descriptors in the rows of the segments table
to point to new deep storage locations (S3, HDFS, and local rewrite paths are supported).

The tool supports exporting from both Derby and PostgreSQL metadata stores.

The tool has the following limitations:

- If rewriting load specs for deep storage migration, only migrating from local deep storage is currently supported.

## `export-metadata` Options

The `export-metadata` tool provides the following options:

### Connection Properties

- `--connectURI`: The URI of the metadata database, e.g. `jdbc:derby://localhost:1527/var/druid/metadata.db;create=true` for Derby or `jdbc:postgresql://localhost:5432/druid` for PostgreSQL
- `--user`: Username
- `--password`: Password
- `--base`: corresponds to the value of `druid.metadata.storage.tables.base` in the configuration, `druid` by default.

### Output Path

- `--output-path`, `-o`: The output directory of the tool. CSV files for the Druid segments, rules, config, datasource, and supervisors tables will be written to this directory.

### Export Format Options

- `--use-hex-blobs`, `-x`: If set, export BLOB payload columns as hexadecimal strings. This needs to be set if importing back into Derby. Default is false.
- `--booleans-as-strings`, `-t`: If set, write boolean values as "true" or "false" instead of "1" and "0". This needs to be set if importing back into Derby. Default is false.

### Deep Storage Migration

#### Migration to S3 Deep Storage

By setting the options below, the tool will rewrite the segment load specs to point to a new S3 deep storage location.

This helps users migrate segments stored in local deep storage to S3.

- `--s3bucket`, `-b`: The S3 bucket that will hold the migrated segments
- `--s3baseKey`, `-k`: The base S3 key where the migrated segments will be stored

When copying the local deep storage segments to S3, the rewrite performed by this tool requires that the directory structure of the segments be unchanged.

For example, if the cluster had the following local deep storage configuration:

```
druid.storage.type=local
druid.storage.storageDirectory=/druid/segments
```

If the target S3 bucket was `migration`, with a base key of `example`, the contents of `s3://migration/example/` must be identical to that of `/druid/segments` on the old local filesystem.

#### Migration to HDFS Deep Storage

By setting the options below, the tool will rewrite the segment load specs to point to a new HDFS deep storage location.

This helps users migrate segments stored in local deep storage to HDFS.

`--hadoopStorageDirectory`, `-h`: The HDFS path that will hold the migrated segments

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

#### Migration to New Local Deep Storage Path

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

To use the tool, you can run the following from the root of the Druid package.

### Exporting from Derby

```bash
cd ${DRUID_ROOT}
mkdir -p /tmp/csv
java -classpath "lib/*" -Dlog4j.configurationFile=conf/druid/cluster/_common/log4j2.xml -Ddruid.extensions.directory="extensions" -Ddruid.extensions.loadList=[] org.apache.druid.cli.Main tools export-metadata --connectURI "jdbc:derby://localhost:1527/var/druid/metadata.db;" -o /tmp/csv
```

### Exporting from PostgreSQL

When exporting from PostgreSQL, you must load the `postgresql-metadata-storage` extension and set the storage type to `postgresql`:

```bash
cd ${DRUID_ROOT}
mkdir -p /tmp/csv
java -classpath "lib/*" -Dlog4j.configurationFile=conf/druid/cluster/_common/log4j2.xml -Ddruid.extensions.directory="extensions" -Ddruid.extensions.loadList='["postgresql-metadata-storage"]' -Ddruid.metadata.storage.type=postgresql org.apache.druid.cli.Main tools export-metadata --connectURI "jdbc:postgresql://localhost:5432/druid" --user druid --password druid -o /tmp/csv
```

If the cluster sets `druid.metadata.postgres.dbTableSchema`, pass that property as well, since the tool looks the tables up in the configured schema and defaults to `public`.

In the example commands above:

- `lib` is the Druid lib directory
- `extensions` is the Druid extensions directory
- `/tmp/csv` is the output directory. Please make sure that this directory exists.

## Importing Metadata

After running the tool, the output directory contains one `<table-name>.csv` file per exported table, with any deep storage rewrites applied. Example import commands for Derby, MySQL, and PostgreSQL are shown below. They expect `/tmp/csv` and its contents to be accessible from the database server; for other options, such as importing from the client filesystem, see your database's documentation.

### Before you import

#### Adjust the column lists

Every table is exported in a fixed column order, whatever the physical column order of the source table is. For the segments table this is `id`, `dataSource`, `created_date`, `start`, `end`, `partitioned`, `version`, `used`, `payload`, followed by whichever of `used_status_last_updated`, `indexing_state_fingerprint`, `upgraded_from_segment_id`, `schema_fingerprint`, and `num_rows` the source table has, in that order.

Make the segments column list in the commands below match the source table exactly: drop the optional columns it does not have, and add `schema_fingerprint,num_rows` at the end if it has them. Segments tables from older Druid versions may have only the first nine columns. Adjust the `FORCE_NULL` list in the PostgreSQL command in the same way. In the MySQL command, each optional column needs both a user variable in the column list and an assignment in `SET`, for example `@num_rows` with `SET num_rows=NULLIF(@num_rows,'')`. A user variable that is not assigned discards the value.

#### Import the fingerprint tables as well

`druid_segmentSchemas` and `druid_indexingStates` hold the schemas and indexing states that the `schema_fingerprint` and `indexing_state_fingerprint` of a segment refer to. They are exported when the source metadata store has them, and must be imported along with the segments table, so that imported segments do not refer to missing rows. The `id` of `druid_segmentSchemas` is a generated identity column that nothing refers to, so it is not exported and the target assigns new values.

Both tables must exist in the target before their import commands run. Druid creates them at startup, and so does the `metadata-init` tool described in [Metadata migration](metadata-migration.md). Neither creates `druid_segmentSchemas`, or the `schema_fingerprint` and `num_rows` columns of `druid_segments`, unless `druid.centralizedDatasourceSchema.enabled` is `true`, and `metadata-init` does not read the runtime properties of the target cluster. So pass `-Ddruid.centralizedDatasourceSchema.enabled=true` to it whenever the export contains those artifacts, that is, whenever the output directory has a `druid_segmentSchemas.csv` or the segments CSV has the two extra columns. Turning the feature off does not drop the table or the columns, so a source cluster that has it disabled today can still export them.

#### Fill in `used_status_last_updated`

Druid creates this column as `NOT NULL` in a new segments table, but adds it as nullable to an existing one and only sets it on the segments it marks unused. The import therefore fails both when the source table does not have the column and when it has the column with NULL rows. In either case, make the column nullable, import, then fill in the missing values:

```sql
ALTER TABLE druid_segments ALTER COLUMN used_status_last_updated NULL;
-- run the import command for your database here
-- replace <migration-time> with the current UTC time, for example 2026-09-02T19:00:00.000Z
UPDATE druid_segments SET used_status_last_updated = '<migration-time>' WHERE used_status_last_updated IS NULL;
ALTER TABLE druid_segments ALTER COLUMN used_status_last_updated NOT NULL;
```

Use the current UTC time of the migration, in ISO 8601 as above, and not `created_date`. `created_date` is when a segment was published, while `used_status_last_updated` is when it became unused, and the Coordinator permanently deletes an unused segment, including from deep storage, once it has been unused for longer than the kill task's `durationToRetain`. With `created_date`, an old segment that was marked unused recently would be deleted straight away.

The `ALTER TABLE` syntax above is Derby's. On PostgreSQL, use `ALTER COLUMN used_status_last_updated DROP NOT NULL` and `SET NOT NULL`; on MySQL, `MODIFY used_status_last_updated VARCHAR(255) NULL` and `MODIFY used_status_last_updated VARCHAR(255) NOT NULL`.

#### Empty fields and backslashes

A NULL is exported as an unquoted empty field and an empty string as a quoted one (`""`). Each database needs to be told how to read them:

- Derby reads an unquoted empty field as NULL and a quoted one as an empty string, so it needs no extra handling.
- PostgreSQL `COPY` reads an empty field as an empty string, which fails for non-string columns such as `num_rows`, so the command below lists the nullable columns in `FORCE_NULL`. That applies to unquoted empty fields only, so empty strings survive.
- MySQL `LOAD DATA` also reads an empty field as an empty string, and turns it into `0` for numeric columns such as `num_rows`, so the command below reads the nullable columns into user variables and applies `NULLIF`. MySQL cannot tell a quoted empty field from an unquoted one, so an empty string in such a column becomes NULL.

The exported CSV follows RFC 4180, where a backslash is an ordinary character. MySQL `LOAD DATA` treats it as an escape character by default, which would corrupt payloads and segment ids containing one, so the commands below turn that off with `ESCAPED BY ''`.

### Derby

`SYSCS_IMPORT_DATA` matches its insert-column list against the stored column names, which are case-sensitive and must not be quoted. Derby stores the unquoted column names of the metadata tables in uppercase, so the lists below use uppercase names. The exception is `end`, a reserved word that Druid creates as a quoted lowercase identifier: `"END"` and `END` both fail with `XIE08 There is no column named`.

The `ID` of `DRUID_SUPERVISORS` and `DRUID_SEGMENTSCHEMAS` is `GENERATED ALWAYS AS IDENTITY`, which Derby refuses to insert a value into, so it is left out of the lists below and Derby assigns new ids. The segment schemas CSV has no `id` and imports directly; the supervisors CSV starts with its `ID` and needs the staging table shown after the commands.

```sql
CALL SYSCS_UTIL.SYSCS_IMPORT_DATA (null,'DRUID_SEGMENTS','ID,DATASOURCE,CREATED_DATE,START,end,PARTITIONED,VERSION,USED,PAYLOAD,USED_STATUS_LAST_UPDATED,INDEXING_STATE_FINGERPRINT,UPGRADED_FROM_SEGMENT_ID',null,'/tmp/csv/druid_segments.csv',',','"',null,0);

CALL SYSCS_UTIL.SYSCS_IMPORT_DATA (null,'DRUID_RULES','ID,DATASOURCE,VERSION,PAYLOAD',null,'/tmp/csv/druid_rules.csv',',','"',null,0);

CALL SYSCS_UTIL.SYSCS_IMPORT_DATA (null,'DRUID_CONFIG','NAME,PAYLOAD',null,'/tmp/csv/druid_config.csv',',','"',null,0);

CALL SYSCS_UTIL.SYSCS_IMPORT_DATA (null,'DRUID_DATASOURCE','DATASOURCE,CREATED_DATE,COMMIT_METADATA_PAYLOAD,COMMIT_METADATA_SHA1',null,'/tmp/csv/druid_dataSource.csv',',','"',null,0);

CALL SYSCS_UTIL.SYSCS_IMPORT_DATA (null,'DRUID_SEGMENTSCHEMAS','FINGERPRINT,CREATED_DATE,DATASOURCE,PAYLOAD,USED,USED_STATUS_LAST_UPDATED,VERSION',null,'/tmp/csv/druid_segmentSchemas.csv',',','"',null,0);

CALL SYSCS_UTIL.SYSCS_IMPORT_DATA (null,'DRUID_INDEXINGSTATES','FINGERPRINT,CREATED_DATE,DATASOURCE,PAYLOAD,USED,PENDING,USED_STATUS_LAST_UPDATED',null,'/tmp/csv/druid_indexingStates.csv',',','"',null,0);
```

Import the supervisors through a staging table, which drops the exported `ID` and lets Derby generate new ones. Druid reads the latest version of a supervisor spec by `ID`, so insert the rows ordered by the exported id:

```sql
CREATE TABLE DRUID_SUPERVISORS_IMPORT (ID BIGINT, SPEC_ID VARCHAR(255), CREATED_DATE VARCHAR(255), PAYLOAD BLOB);
CALL SYSCS_UTIL.SYSCS_IMPORT_DATA (null,'DRUID_SUPERVISORS_IMPORT','ID,SPEC_ID,CREATED_DATE,PAYLOAD',null,'/tmp/csv/druid_supervisors.csv',',','"',null,0);
INSERT INTO DRUID_SUPERVISORS (SPEC_ID, CREATED_DATE, PAYLOAD) SELECT SPEC_ID, CREATED_DATE, PAYLOAD FROM DRUID_SUPERVISORS_IMPORT ORDER BY ID;
DROP TABLE DRUID_SUPERVISORS_IMPORT;
```

### MySQL

```sql
LOAD DATA INFILE '/tmp/csv/druid_segments.csv' INTO TABLE druid_segments FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '' (id,dataSource,created_date,start,end,partitioned,version,used,payload,@used_status_last_updated,@indexing_state_fingerprint,@upgraded_from_segment_id) SET used_status_last_updated=NULLIF(@used_status_last_updated,''), indexing_state_fingerprint=NULLIF(@indexing_state_fingerprint,''), upgraded_from_segment_id=NULLIF(@upgraded_from_segment_id,''); SHOW WARNINGS;

LOAD DATA INFILE '/tmp/csv/druid_rules.csv' INTO TABLE druid_rules FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '' (id,dataSource,version,payload); SHOW WARNINGS;

LOAD DATA INFILE '/tmp/csv/druid_config.csv' INTO TABLE druid_config FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '' (name,payload); SHOW WARNINGS;

LOAD DATA INFILE '/tmp/csv/druid_dataSource.csv' INTO TABLE druid_dataSource FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '' (dataSource,created_date,commit_metadata_payload,commit_metadata_sha1); SHOW WARNINGS;

LOAD DATA INFILE '/tmp/csv/druid_supervisors.csv' INTO TABLE druid_supervisors FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '' (id,spec_id,created_date,payload); SHOW WARNINGS;

LOAD DATA INFILE '/tmp/csv/druid_segmentSchemas.csv' INTO TABLE druid_segmentSchemas FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '' (fingerprint,created_date,datasource,payload,used,used_status_last_updated,version); SHOW WARNINGS;

LOAD DATA INFILE '/tmp/csv/druid_indexingStates.csv' INTO TABLE druid_indexingStates FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '' (fingerprint,created_date,dataSource,payload,used,pending,used_status_last_updated); SHOW WARNINGS;
```

The supervisors command keeps the exported `id` values; InnoDB advances its auto-increment counter past them, so no reseeding is needed.

### PostgreSQL

The payload columns are `BYTEA` in PostgreSQL, and `COPY` reads a `BYTEA` field with the `bytea` input syntax, where a backslash starts an escape sequence. Exported payloads are JSON text that commonly contains backslashes, such as escaped quotes or Windows deep storage paths, so loading them straight into a `BYTEA` column either fails or silently changes the payload.

Run the export with `--use-hex-blobs` (`-x`) to keep the payloads hex-encoded, load the CSV files into staging tables whose payload columns are `TEXT`, and decode with `decode(payload, 'hex')`:

```sql
CREATE TEMP TABLE druid_segments_import(id text,dataSource text,created_date text,start text,"end" text,partitioned boolean,version text,used boolean,payload text,used_status_last_updated text,indexing_state_fingerprint text,upgraded_from_segment_id text);
COPY druid_segments_import FROM '/tmp/csv/druid_segments.csv' WITH (FORMAT csv, FORCE_NULL (used_status_last_updated,indexing_state_fingerprint,upgraded_from_segment_id));
INSERT INTO druid_segments(id,dataSource,created_date,start,"end",partitioned,version,used,payload,used_status_last_updated,indexing_state_fingerprint,upgraded_from_segment_id) SELECT id,dataSource,created_date,start,"end",partitioned,version,used,decode(payload,'hex'),used_status_last_updated,indexing_state_fingerprint,upgraded_from_segment_id FROM druid_segments_import;

CREATE TEMP TABLE druid_rules_import(id text,dataSource text,version text,payload text);
COPY druid_rules_import FROM '/tmp/csv/druid_rules.csv' WITH (FORMAT csv);
INSERT INTO druid_rules(id,dataSource,version,payload) SELECT id,dataSource,version,decode(payload,'hex') FROM druid_rules_import;

CREATE TEMP TABLE druid_config_import(name text,payload text);
COPY druid_config_import FROM '/tmp/csv/druid_config.csv' WITH (FORMAT csv);
INSERT INTO druid_config(name,payload) SELECT name,decode(payload,'hex') FROM druid_config_import;

CREATE TEMP TABLE druid_dataSource_import(dataSource text,created_date text,commit_metadata_payload text,commit_metadata_sha1 text);
COPY druid_dataSource_import FROM '/tmp/csv/druid_dataSource.csv' WITH (FORMAT csv);
INSERT INTO druid_dataSource(dataSource,created_date,commit_metadata_payload,commit_metadata_sha1) SELECT dataSource,created_date,decode(commit_metadata_payload,'hex'),commit_metadata_sha1 FROM druid_dataSource_import;

CREATE TEMP TABLE druid_supervisors_import(id text,spec_id text,created_date text,payload text);
COPY druid_supervisors_import FROM '/tmp/csv/druid_supervisors.csv' WITH (FORMAT csv);
INSERT INTO druid_supervisors(id,spec_id,created_date,payload) SELECT id::bigint,spec_id,created_date,decode(payload,'hex') FROM druid_supervisors_import;
-- druid_supervisors.id is a BIGSERIAL, whose sequence must be advanced past the imported ids
SELECT setval(pg_get_serial_sequence('druid_supervisors','id'), (SELECT MAX(id) FROM druid_supervisors));

CREATE TEMP TABLE druid_segmentSchemas_import(fingerprint text,created_date text,datasource text,payload text,used boolean,used_status_last_updated text,version integer);
COPY druid_segmentSchemas_import FROM '/tmp/csv/druid_segmentSchemas.csv' WITH (FORMAT csv);
INSERT INTO druid_segmentSchemas(fingerprint,created_date,datasource,payload,used,used_status_last_updated,version) SELECT fingerprint,created_date,datasource,decode(payload,'hex'),used,used_status_last_updated,version FROM druid_segmentSchemas_import;

CREATE TEMP TABLE druid_indexingStates_import(fingerprint text,created_date text,dataSource text,payload text,used boolean,pending boolean,used_status_last_updated text);
COPY druid_indexingStates_import FROM '/tmp/csv/druid_indexingStates.csv' WITH (FORMAT csv);
INSERT INTO druid_indexingStates(fingerprint,created_date,dataSource,payload,used,pending,used_status_last_updated) SELECT fingerprint,created_date,dataSource,decode(payload,'hex'),used,pending,used_status_last_updated FROM druid_indexingStates_import;
```

The staging tables declare the columns the export writes; adjust the segments columns as described above if the source table has a different set of optional ones. A staging column is `text` wherever the exported value needs a cast or a decode, such as the `id` of `druid_supervisors`, which is a `BIGSERIAL` in the target table.
