---
id: iceberg 
title: "Iceberg extension"
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

## Iceberg Ingest extension

Apache Iceberg is an open table format for huge analytic datasets. [IcebergInputSource](../../ingestion/input-sources.md#iceberg-input-source) lets you ingest data stored in the Iceberg table format into Apache Druid. To use the iceberg extension, add the `druid-iceberg-extensions` to the list of loaded extensions. See [Loading extensions](../../configuration/extensions.md#loading-extensions) for more information.

Iceberg manages most of its metadata in metadata files in the object storage. However, it is still dependent on a metastore to manage a certain amount of metadata.
Iceberg refers to these metastores as catalogs. The Iceberg extension lets you connect to the following Iceberg catalog types:
* Hive metastore catalog
* Local catalog

Druid does not support AWS Glue and REST based catalogs yet.

For a given catalog, Iceberg input source reads the table name from the catalog, applies the filters, and extracts all the underlying live data files up to the latest snapshot.
The data files can be in Parquet, ORC, or Avro formats. The data files typically reside in a warehouse location, which can be in HDFS, S3, or the local filesystem.
The `druid-iceberg-extensions` extension relies on the existing input source connectors in Druid to read the data files from the warehouse. Therefore, the Iceberg input source can be considered as an intermediate input source, which provides the file paths for other input source implementations.

## Hive metastore catalog

For Druid to seamlessly talk to the Hive metastore, ensure that the Hive configuration files such as `hive-site.xml` and `core-site.xml` are available in the Druid classpath for peon processes.  
You can also specify Hive properties under the `catalogProperties` object in the ingestion spec. 

The `druid-iceberg-extensions` extension presently only supports HDFS, S3 and local warehouse directories.

### Read from HDFS warehouse 

To read from a HDFS warehouse, load the `druid-hdfs-storage` extension. Druid extracts data file paths from the Hive metastore catalog and uses [HDFS input source](../../ingestion/input-sources.md#hdfs-input-source) to ingest these files.
The `warehouseSource` type in the ingestion spec should be `hdfs`.

For authenticating with Kerberized clusters, include `principal` and `keytab` properties in the `catalogProperties` object:

```json
"catalogProperties": {
  "principal": "krb_principal",
  "keytab": "/path/to/keytab"
}
```
Only Kerberos based authentication is supported as of now.

### Read from S3 warehouse

To read from a S3 warehouse, load the `druid-s3-extensions` extension. Druid extracts the data file paths from the Hive metastore catalog and uses `S3InputSource` to ingest these files.
Set the `type` property of the `warehouseSource` object to `s3` in the ingestion spec. If the S3 endpoint for the warehouse is different from the endpoint configured as the deep storage, include the following properties in the `warehouseSource` object to define the S3 endpoint settings:

```json
"warehouseSource": {
  "type": "s3",
  "endpointConfig": {
    "url": "S3_ENDPOINT_URL",
    "signingRegion": "us-east-1"
  },
  "clientConfig": {
    "protocol": "http",
    "disableChunkedEncoding": true,
    "enablePathStyleAccess": true,
    "forceGlobalBucketAccessEnabled": false
  },
  "properties": {
    "accessKeyId": {
      "type": "default",
      "password": "<ACCESS_KEY_ID"
    },
    "secretAccessKey": {
      "type": "default",
      "password": "<SECRET_ACCESS_KEY>"
    }
  }
}
```

This extension uses the [Hadoop AWS module](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/) to connect to S3 and retrieve the metadata and data file paths.
The following properties are required in the `catalogProperties`:

```json
"catalogProperties": {
  "fs.s3a.access.key" : "S3_ACCESS_KEY",
  "fs.s3a.secret.key" : "S3_SECRET_KEY",
  "fs.s3a.endpoint" : "S3_API_ENDPOINT"
}
```
Since the Hadoop AWS connector uses the `s3a` filesystem client, specify the warehouse path with the `s3a://` protocol instead of `s3://`.

## Local catalog

The local catalog type can be used for catalogs configured on the local filesystem. Set the `icebergCatalog` type to `local`. You can use this catalog for demos or localized tests. It is not recommended for production use cases.
The `warehouseSource` is set to `local` because this catalog only supports reading from a local filesystem.

## Downloading Iceberg extension

To download `druid-iceberg-extensions`, run the following command after replacing `<VERSION>` with the desired
Druid version:

```shell
java \
  -cp "lib/*" \
  -Ddruid.extensions.directory="extensions" \
  -Ddruid.extensions.hadoopDependenciesDir="hadoop-dependencies" \
  org.apache.druid.cli.Main tools pull-deps \
  --no-default-hadoop \
  -c "org.apache.druid.extensions.contrib:druid-iceberg-extensions:<VERSION>"
```

See [Loading community extensions](../../configuration/extensions.md#loading-community-extensions) for more information.

## Known limitations

This section lists the known limitations that apply to the Iceberg extension.

- This extension does not fully utilize the Iceberg features such as snapshotting or schema evolution.
- The Iceberg input source reads every single live file on the Iceberg table up to the latest snapshot, which makes the table scan less performant. It is recommended to use Iceberg filters on partition columns in the ingestion spec in order to limit the number of data files being retrieved. Since, Druid doesn't store the last ingested iceberg snapshot ID, it cannot identify the files created between that snapshot and the latest snapshot on Iceberg.
- It does not handle Iceberg [schema evolution](https://iceberg.apache.org/docs/latest/evolution/) yet. In cases where an existing Iceberg table column is deleted and recreated with the same name, ingesting this table into Druid may bring the data for this column before it was deleted.
- The Hive catalog has not been tested on Hadoop 2.x.x and is not guaranteed to work with Hadoop 2.