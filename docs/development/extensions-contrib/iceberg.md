---
id: iceberg 
title: "Iceberg"
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

## Iceberg Ingest Extension

This extension provides [IcebergInputSource](../../ingestion/input-sources.md#iceberg-input-source) which enables ingestion of data stored in the Iceberg table format into Druid.

Apache Iceberg is an open table format for huge analytic datasets. Even though iceberg manages most of its metadata on metadata files, it is still dependent on a metastore for managing a certain amount of metadata.
These metastores are defined as Iceberg catalogs and this extension supports connecting to the following catalog types:
* Hive metastore catalog
* Local catalog

Support for AWS Glue and REST based catalogs are not available yet.

For a given catalog, iceberg table name and filters, The IcebergInputSource works by reading the table from the catalog, applying the filters and extracting all the underlying live data files up to the latest snapshot.
The data files are in either Parquet, ORC or Avro formats and all of these have InputFormat support in Druid. The data files typically reside in a warehouse location which could be in HDFS, S3 or the local filesystem.
This extension relies on the existing InputSource connectors in Druid to read the data files from the warehouse. Therefore, the IcebergInputSource can be considered as an intermediate InputSource which provides the file paths for other InputSource implementations.

### Load the Iceberg Ingest extension

To use the iceberg extension, add the `druid-iceberg-extensions` to the list of loaded extensions. See [Loading extensions](../../configuration/extensions.md#loading-extensions) for more information.


### Hive Metastore catalog

For Druid to seamlessly talk to the Hive Metastore, ensure that the Hive specific configuration files such as `hive-site.xml` and `core-site.xml` are available in the Druid classpath.  
Hive specific properties can also be specified under the `catalogProperties` object in the ingestion spec. 

Hive metastore catalogs can be associated with different types of warehouses, but this extension presently only supports HDFS and S3 warehouse directories.

#### Reading from HDFS warehouse 

Ensure that the extension `druid-hdfs-storage` is loaded. The data file paths are extracted from the Hive metastore catalog and [HDFS input source](../../ingestion/input-sources.md#hdfs-input-source) is used to ingest these files.
The `warehouseSource` type in the ingestion spec should be `hdfs`.

If the Hive metastore supports Kerberos authentication, the following properties will be required in the `catalogProperties`:

```json
"catalogProperties": {
  "principal": "krb_principal",
  "keytab": "/path/to/keytab"
}
```

#### Reading from S3 warehouse

Ensure that the extension `druid-s3-extensions` is loaded. The data file paths are extracted from the Hive metastore catalog and `S3InputSource` is used to ingest these files.
The `warehouseSource` type in the ingestion spec should be `s3`. If the S3 endpoint for the warehouse is different from the endpoint configured as the deep storage, the following properties are required in the `warehouseSource` section to define the S3 endpoint settings:

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
The following properties will be required in the `catalogProperties`:

```json
"catalogProperties": {
  "fs.s3a.access.key" : "S3_ACCESS_KEY",
  "fs.s3a.secret.key" : "S3_SECRET_KEY",
  "fs.s3a.endpoint" : "S3_API_ENDPOINT"
}
```
Since the AWS connector uses the `s3a` filesystem based client, the warehouse path should be specified with the `s3a://` protocol instead of `s3://`.

### Local Catalog

The local catalog type can be used for catalogs configured on the local filesystem. The `icebergCatalog` type should be set as `local`. This catalog is useful for demos or localized tests and is not recommended for production use cases.
This catalog only supports reading from a local filesystem and so the `warehouseSource` is defined as `local`.

### Known limitations

This extension does not presently fully utilize the iceberg features such as snapshotting or schema evolution. Following are the current limitations of this extension:

- The `IcebergInputSource` reads every single live file on the iceberg table up to the latest snapshot, which makes the table scan less performant. It is recommended to use iceberg filters on partition columns in the ingestion spec in order to limit the number of data files being retrieved. Since, Druid doesn't store the last ingested iceberg snapshot ID, it cannot identify the files created between that snapshot and the latest snapshot on iceberg.
- It does not handle iceberg [schema evolution](https://iceberg.apache.org/docs/latest/evolution/) yet. In cases where an existing iceberg table column is deleted and recreated with the same name, ingesting this table into Druid may bring the data for this column before it was deleted.
- The Hive catalog has not been tested on Hadoop 2.x.x and therefore is not guaranteed to work with Hadoop 2.