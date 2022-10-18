---
id: native-batch-input-sources
title: "Native batch input sources"
sidebar_label: "Native batch: input sources"
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

The input source defines where your index task reads data for Apache Druid native batch ingestion. Only the native parallel task and simple task support the input source.

For general information on native batch indexing and parallel task indexing, see [Native batch ingestion](./native-batch.md).

## S3 input source

> You need to include the [`druid-s3-extensions`](../development/extensions-core/s3.md) as an extension to use the S3 input source.

The S3 input source reads objects directly from S3. You can specify either:
- a list of S3 URI strings
- a list of S3 location prefixes that attempts to list the contents and ingest
all objects contained within the locations.

The S3 input source is splittable. Therefore, you can use it with the [Parallel task](./native-batch.md). Each worker task of `index_parallel` reads one or multiple objects.

Sample specs:

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "s3",
        "filter": "*.json",
        "uris": ["s3://foo/bar/file.json", "s3://bar/foo/file2.json"]
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "s3",
        "filter": "*.parquet",
        "prefixes": ["s3://foo/bar/", "s3://bar/foo/"]
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```


```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "s3",
        "filter": "*.json",
        "objects": [
          { "bucket": "foo", "path": "bar/file1.json"},
          { "bucket": "bar", "path": "foo/file2.json"}
        ]
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "s3",
        "filter": "*.json",
        "uris": ["s3://foo/bar/file.json", "s3://bar/foo/file2.json"],
        "properties": {
          "accessKeyId": "KLJ78979SDFdS2",
          "secretAccessKey": "KLS89s98sKJHKJKJH8721lljkd"
        }
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "s3",
        "filter": "*.json",
        "uris": ["s3://foo/bar/file.json", "s3://bar/foo/file2.json"],
        "properties": {
          "accessKeyId": "KLJ78979SDFdS2",
          "secretAccessKey": "KLS89s98sKJHKJKJH8721lljkd",
          "assumeRoleArn": "arn:aws:iam::2981002874992:role/role-s3"
        }
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "s3",
        "uris": ["s3://foo/bar/file.json", "s3://bar/foo/file2.json"],
        "endpointConfig": {
             "url" : "s3-store.aws.com",
             "signingRegion" : "us-west-2"
         },
         "clientConfig": {
             "protocol" : "http",
             "disableChunkedEncoding" : true,
             "enablePathStyleAccess" : true,
             "forceGlobalBucketAccessEnabled" : false
         },
         "proxyConfig": {
             "host" : "proxy-s3.aws.com",
             "port" : 8888,
             "username" : "admin",
             "password" : "admin"
         },

        "properties": {
          "accessKeyId": "KLJ78979SDFdS2",
          "secretAccessKey": "KLS89s98sKJHKJKJH8721lljkd",
          "assumeRoleArn": "arn:aws:iam::2981002874992:role/role-s3"
        }
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```

|Property|Description|Default|Required|
|--------|-----------|-------|---------|
|type|Set the value to `s3`.|None|yes|
|uris|JSON array of URIs where S3 objects to be ingested are located.|None|`uris` or `prefixes` or `objects` must be set|
|prefixes|JSON array of URI prefixes for the locations of S3 objects to be ingested. Empty objects starting with one of the given prefixes will be skipped.|None|`uris` or `prefixes` or `objects` must be set|
|objects|JSON array of S3 Objects to be ingested.|None|`uris` or `prefixes` or `objects` must be set|
|filter|A wildcard filter for files. See [here](http://commons.apache.org/proper/commons-io/apidocs/org/apache/commons/io/filefilter/WildcardFileFilter) for more information. Files matching the filter criteria are considered for ingestion. Files not matching the filter criteria are ignored.|None|no|
| endpointConfig |Config for overriding the default S3 endpoint and signing region. This would allow ingesting data from a different S3 store. Please see [s3 config](../development/extensions-core/s3.md#connecting-to-s3-configuration) for more information.|None|No (defaults will be used if not given)
| clientConfig |S3 client properties for the overridden s3 endpoint. This is used in conjunction with `endPointConfig`. Please see [s3 config](../development/extensions-core/s3.md#connecting-to-s3-configuration) for more information.|None|No (defaults will be used if not given)
| proxyConfig |Properties for specifying proxy information for the overridden s3 endpoint. This is used in conjunction with `clientConfig`. Please see [s3 config](../development/extensions-core/s3.md#connecting-to-s3-configuration) for more information.|None|No (defaults will be used if not given)
|properties|Properties Object for overriding the default S3 configuration. See below for more information.|None|No (defaults will be used if not given)

Note that the S3 input source will skip all empty objects only when `prefixes` is specified.

S3 Object:

|Property|Description|Default|Required|
|--------|-----------|-------|---------|
|bucket|Name of the S3 bucket|None|yes|
|path|The path where data is located.|None|yes|

Properties Object:

|Property|Description|Default|Required|
|--------|-----------|-------|---------|
|accessKeyId|The [Password Provider](../operations/password-provider.md) or plain text string of this S3 input source access key|None|yes if secretAccessKey is given|
|secretAccessKey|The [Password Provider](../operations/password-provider.md) or plain text string of this S3 input source secret key|None|yes if accessKeyId is given|
|assumeRoleArn|AWS ARN of the role to assume [see](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_request.html). **assumeRoleArn** can be used either with the ingestion spec AWS credentials or with the default S3 credentials|None|no|
|assumeRoleExternalId|A unique identifier that might be required when you assume a role in another account [see](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_request.html)|None|no|

> **Note:** If `accessKeyId` and `secretAccessKey` are not given, the default [S3 credentials provider chain](../development/extensions-core/s3.md#s3-authentication-methods) is used.

## Google Cloud Storage input source

> You need to include the [`druid-google-extensions`](../development/extensions-core/google.md) as an extension to use the Google Cloud Storage input source.

The Google Cloud Storage input source is to support reading objects directly
from Google Cloud Storage. Objects can be specified as list of Google
Cloud Storage URI strings. The Google Cloud Storage input source is splittable
and can be used by the [Parallel task](./native-batch.md), where each worker task of `index_parallel` will read
one or multiple objects.

Sample specs:

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "google",
        "filter": "*.json",
        "uris": ["gs://foo/bar/file.json", "gs://bar/foo/file2.json"]
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "google",
        "filter": "*.parquet",
        "prefixes": ["gs://foo/bar/", "gs://bar/foo/"]
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```


```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "google",
        "filter": "*.json",
        "objects": [
          { "bucket": "foo", "path": "bar/file1.json"},
          { "bucket": "bar", "path": "foo/file2.json"}
        ]
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```

|Property|Description|Default|Required|
|--------|-----------|-------|---------|
|type|Set the value to `google`.|None|yes|
|uris|JSON array of URIs where Google Cloud Storage objects to be ingested are located.|None|`uris` or `prefixes` or `objects` must be set|
|prefixes|JSON array of URI prefixes for the locations of Google Cloud Storage objects to be ingested. Empty objects starting with one of the given prefixes will be skipped.|None|`uris` or `prefixes` or `objects` must be set|
|objects|JSON array of Google Cloud Storage objects to be ingested.|None|`uris` or `prefixes` or `objects` must be set|
|filter|A wildcard filter for files. See [here](http://commons.apache.org/proper/commons-io/apidocs/org/apache/commons/io/filefilter/WildcardFileFilter) for more information. Files matching the filter criteria are considered for ingestion. Files not matching the filter criteria are ignored.|None|no|

Note that the Google Cloud Storage input source will skip all empty objects only when `prefixes` is specified.

Google Cloud Storage object:

|Property|Description|Default|Required|
|--------|-----------|-------|---------|
|bucket|Name of the Google Cloud Storage bucket|None|yes|
|path|The path where data is located.|None|yes|

## Azure input source

> You need to include the [`druid-azure-extensions`](../development/extensions-core/azure.md) as an extension to use the Azure input source.

The Azure input source reads objects directly from Azure Blob store or Azure Data Lake sources. You can
specify objects as a list of file URI strings or prefixes. You can split the Azure input source for use with [Parallel task](./native-batch.md) indexing and each worker task reads one chunk of the split data.

Sample specs:

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "azure",
        "filter": "*.json",
        "uris": ["azure://container/prefix1/file.json", "azure://container/prefix2/file2.json"]
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "azure",
        "filter": "*.parquet",
        "prefixes": ["azure://container/prefix1/", "azure://container/prefix2/"]
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```


```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "azure",
        "filter": "*.json",
        "objects": [
          { "bucket": "container", "path": "prefix1/file1.json"},
          { "bucket": "container", "path": "prefix2/file2.json"}
        ]
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```

|Property|Description|Default|Required|
|--------|-----------|-------|---------|
|type|Set the value to `azure`.|None|yes|
|uris|JSON array of URIs where the Azure objects to be ingested are located, in the form `azure://<container>/<path-to-file>`|None|`uris` or `prefixes` or `objects` must be set|
|prefixes|JSON array of URI prefixes for the locations of Azure objects to ingest, in the form `azure://<container>/<prefix>`. Empty objects starting with one of the given prefixes are skipped.|None|`uris` or `prefixes` or `objects` must be set|
|objects|JSON array of Azure objects to ingest.|None|`uris` or `prefixes` or `objects` must be set|
|filter|A wildcard filter for files. See [here](http://commons.apache.org/proper/commons-io/apidocs/org/apache/commons/io/filefilter/WildcardFileFilter) for more information. Files matching the filter criteria are considered for ingestion. Files not matching the filter criteria are ignored.|None|no|

Note that the Azure input source skips all empty objects only when `prefixes` is specified.

The `objects` property is:

|Property|Description|Default|Required|
|--------|-----------|-------|---------|
|bucket|Name of the Azure Blob Storage or Azure Data Lake container|None|yes|
|path|The path where data is located.|None|yes|

## HDFS input source

> You need to include the [`druid-hdfs-storage`](../development/extensions-core/hdfs.md) as an extension to use the HDFS input source.

The HDFS input source is to support reading files directly
from HDFS storage. File paths can be specified as an HDFS URI string or a list
of HDFS URI strings. The HDFS input source is splittable and can be used by the [Parallel task](./native-batch.md),
where each worker task of `index_parallel` will read one or multiple files.

Sample specs:

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "hdfs",
        "paths": "hdfs://namenode_host/foo/bar/", "hdfs://namenode_host/bar/foo"
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "hdfs",
        "paths": "hdfs://namenode_host/foo/bar/", "hdfs://namenode_host/bar/foo"
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "hdfs",
        "paths": "hdfs://namenode_host/foo/bar/file.json", "hdfs://namenode_host/bar/foo/file2.json"
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "hdfs",
        "paths": ["hdfs://namenode_host/foo/bar/file.json", "hdfs://namenode_host/bar/foo/file2.json"]
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```

|Property|Description|Default|Required|
|--------|-----------|-------|---------|
|type|Set the value to `hdfs`.|None|yes|
|paths|HDFS paths. Can be either a JSON array or comma-separated string of paths. Wildcards like `*` are supported in these paths. Empty files located under one of the given paths will be skipped.|None|yes|

You can also ingest from other storage using the HDFS input source if the HDFS client supports that storage.
However, if you want to ingest from cloud storage, consider using the service-specific input source for your data storage.
If you want to use a non-hdfs protocol with the HDFS input source, include the protocol
in `druid.ingestion.hdfs.allowedProtocols`. See [HDFS input source security configuration](../configuration/index.md#hdfs-input-source) for more details.

## HTTP input source

The HTTP input source is to support reading files directly from remote sites via HTTP.

> **Security notes:** Ingestion tasks run under the operating system account that runs the Druid processes, for example the Indexer, Middle Manager, and Peon. This means any user who can submit an ingestion task can specify an input source referring to any location that the Druid process can access. For example, using `http` input source, users may have access to internal network servers.
>
> The `http` input source is not limited to the HTTP or HTTPS protocols. It uses the Java URI class that supports HTTP, HTTPS, FTP, file, and jar protocols by default.

For more information about security best practices, see [Security overview](../operations/security-overview.md#best-practices).

The HTTP input source is _splittable_ and can be used by the [Parallel task](./native-batch.md),
where each worker task of `index_parallel` will read only one file. This input source does not support Split Hint Spec.

Sample specs:

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "http",
        "uris": ["http://example.com/uri1", "http://example2.com/uri2"]
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```

Example with authentication fields using the DefaultPassword provider (this requires the password to be in the ingestion spec):

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "http",
        "uris": ["http://example.com/uri1", "http://example2.com/uri2"],
        "httpAuthenticationUsername": "username",
        "httpAuthenticationPassword": "password123"
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
```

You can also use the other existing Druid PasswordProviders. Here is an example using the EnvironmentVariablePasswordProvider:

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "http",
        "uris": ["http://example.com/uri1", "http://example2.com/uri2"],
        "httpAuthenticationUsername": "username",
        "httpAuthenticationPassword": {
          "type": "environment",
          "variable": "HTTP_INPUT_SOURCE_PW"
        }
      },
      "inputFormat": {
        "type": "json"
      },
      ...
    },
...
}
```

|Property|Description|Default|Required|
|--------|-----------|-------|---------|
|type|Set the value to `http`.|None|yes|
|uris|URIs of the input files. See below for the protocols allowed for URIs.|None|yes|
|httpAuthenticationUsername|Username to use for authentication with specified URIs. Can be optionally used if the URIs specified in the spec require a Basic Authentication Header.|None|no|
|httpAuthenticationPassword|PasswordProvider to use with specified URIs. Can be optionally used if the URIs specified in the spec require a Basic Authentication Header.|None|no|

You can only use protocols listed in the `druid.ingestion.http.allowedProtocols` property as HTTP input sources.
The `http` and `https` protocols are allowed by default. See [HTTP input source security configuration](../configuration/index.md#http-input-source) for more details.

## Inline input source

The Inline input source can be used to read the data inlined in its own spec.
It can be used for demos or for quickly testing out parsing and schema.

Sample spec:

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "inline",
        "data": "0,values,formatted\n1,as,CSV"
      },
      "inputFormat": {
        "type": "csv"
      },
      ...
    },
...
```

|Property|Description|Required|
|--------|-----------|---------|
|type|Set the value to `inline`.|yes|
|data|Inlined data to ingest.|yes|

## Local input source

The Local input source is to support reading files directly from local storage,
and is mainly intended for proof-of-concept testing.
The Local input source is _splittable_ and can be used by the [Parallel task](./native-batch.md),
where each worker task of `index_parallel` will read one or multiple files.

Sample spec:

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "local",
        "filter" : "*.csv",
        "baseDir": "/data/directory",
        "files": ["/bar/foo", "/foo/bar"]
      },
      "inputFormat": {
        "type": "csv"
      },
      ...
    },
...
```

|Property|Description|Required|
|--------|-----------|---------|
|type|Set the value to `local`.|yes|
|filter|A wildcard filter for files. See [here](http://commons.apache.org/proper/commons-io/apidocs/org/apache/commons/io/filefilter/WildcardFileFilter) for more information. Files matching the filter criteria are considered for ingestion. Files not matching the filter criteria are ignored.|yes if `baseDir` is specified|
|baseDir|Directory to search recursively for files to be ingested. Empty files under the `baseDir` will be skipped.|At least one of `baseDir` or `files` should be specified|
|files|File paths to ingest. Some files can be ignored to avoid ingesting duplicate files if they are located under the specified `baseDir`. Empty files will be skipped.|At least one of `baseDir` or `files` should be specified|

## Druid input source

The Druid input source is to support reading data directly from existing Druid segments,
potentially using a new schema and changing the name, dimensions, metrics, rollup, etc. of the segment.
The Druid input source is _splittable_ and can be used by the [Parallel task](./native-batch.md).
This input source has a fixed input format for reading from Druid segments;
no `inputFormat` field needs to be specified in the ingestion spec when using this input source.

|Property|Description|Required|
|--------|-----------|---------|
|type|Set the value to `druid`.|yes|
|dataSource|A String defining the Druid datasource to fetch rows from|yes|
|interval|A String representing an ISO-8601 interval, which defines the time range to fetch the data over.|yes|
|filter| See [Filters](../querying/filters.md). Only rows that match the filter, if specified, will be returned.|no|

The Druid input source can be used for a variety of purposes, including:

- Creating new datasources that are rolled-up copies of existing datasources.
- Changing the [partitioning or sorting](./partitioning.md) of a datasource to improve performance.
- Updating or removing rows using a [`transformSpec`](./ingestion-spec.md#transformspec).

When using the Druid input source, the timestamp column shows up as a numeric field named `__time` set to the number
of milliseconds since the epoch (January 1, 1970 00:00:00 UTC). It is common to use this in the timestampSpec, if you
want the output timestamp to be equivalent to the input timestamp. In this case, set the timestamp column to `__time`
and the format to `auto` or `millis`.

It is OK for the input and output datasources to be the same. In this case, newly generated data will overwrite the
previous data for the intervals specified in the `granularitySpec`. Generally, if you are going to do this, it is a good
idea to test out your reindexing by writing to a separate datasource before overwriting your main one. Alternatively, if
your goals can be satisfied by [compaction](../data-management/compaction.md), consider that instead as a simpler
approach.

An example task spec is shown below. It reads from a hypothetical raw datasource `wikipedia_raw` and creates a new
rolled-up datasource `wikipedia_rollup` by grouping on hour, "countryName", and "page".

```json
{
  "type": "index_parallel",
  "spec": {
    "dataSchema": {
      "dataSource": "wikipedia_rollup",
      "timestampSpec": {
        "column": "__time",
        "format": "millis"
      },
      "dimensionsSpec": {
        "dimensions": [
          "countryName",
          "page"
        ]
      },
      "metricsSpec": [
        {
          "type": "count",
          "name": "cnt"
        }
      ],
      "granularitySpec": {
        "type": "uniform",
        "queryGranularity": "HOUR",
        "segmentGranularity": "DAY",
        "intervals": ["2016-06-27/P1D"],
        "rollup": true
      }
    },
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "druid",
        "dataSource": "wikipedia_raw",
        "interval": "2016-06-27/P1D"
      }
    },
    "tuningConfig": {
      "type": "index_parallel",
      "partitionsSpec": {
        "type": "hashed"
      },
      "forceGuaranteedRollup": true,
      "maxNumConcurrentSubTasks": 1
    }
  }
}
```

> Note: Older versions (0.19 and earlier) did not respect the timestampSpec when using the Druid input source. If you
> have ingestion specs that rely on this and cannot rewrite them, set
> [`druid.indexer.task.ignoreTimestampSpecForDruidInputSource`](../configuration/index.md#indexer-general-configuration)
> to `true` to enable a compatibility mode where the timestampSpec is ignored.

## SQL input source

The SQL input source is used to read data directly from RDBMS.
The SQL input source is _splittable_ and can be used by the [Parallel task](./native-batch.md), where each worker task will read from one SQL query from the list of queries.
This input source does not support Split Hint Spec.
Since this input source has a fixed input format for reading events, no `inputFormat` field needs to be specified in the ingestion spec when using this input source.
Please refer to the Recommended practices section below before using this input source.

|Property|Description|Required|
|--------|-----------|---------|
|type|Set the value to `sql`.|Yes|
|database|Specifies the database connection details. The database type corresponds to the extension that supplies the `connectorConfig` support. The specified extension must be loaded into Druid:<br/><br/><ul><li>[mysql-metadata-storage](../development/extensions-core/mysql.md) for `mysql`</li><li> [postgresql-metadata-storage](../development/extensions-core/postgresql.md) extension for `postgresql`.</li></ul><br/><br/>You can selectively allow JDBC properties in `connectURI`. See [JDBC connections security config](../configuration/index.md#jdbc-connections-to-external-databases) for more details.|Yes|
|foldCase|Toggle case folding of database column names. This may be enabled in cases where the database returns case insensitive column names in query results.|No|
|sqls|List of SQL queries where each SQL query would retrieve the data to be indexed.|Yes|

The following is an example of an SQL input source spec:

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "sql",
        "database": {
            "type": "mysql",
            "connectorConfig": {
                "connectURI": "jdbc:mysql://host:port/schema",
                "user": "user",
                "password": "password"
            }
        },
        "sqls": ["SELECT * FROM table1 WHERE timestamp BETWEEN '2013-01-01 00:00:00' AND '2013-01-01 11:59:59'", "SELECT * FROM table2 WHERE timestamp BETWEEN '2013-01-01 00:00:00' AND '2013-01-01 11:59:59'"]
      }
    },
...
```

The spec above will read all events from two separate SQLs for the interval `2013-01-01/2013-01-02`.
Each of the SQL queries will be run in its own sub-task and thus for the above example, there would be two sub-tasks.

**Recommended practices**

Compared to the other native batch input sources, SQL input source behaves differently in terms of reading the input data. Therefore, consider the following points before using this input source in a production environment:

* During indexing, each sub-task would execute one of the SQL queries and the results are stored locally on disk. The sub-tasks then proceed to read the data from these local input files and generate segments. Presently, there isn’t any restriction on the size of the generated files and this would require the MiddleManagers or Indexers to have sufficient disk capacity based on the volume of data being indexed.

* Filtering the SQL queries based on the intervals specified in the `granularitySpec` can avoid unwanted data being retrieved and stored locally by the indexing sub-tasks. For example, if the `intervals` specified in the `granularitySpec` is `["2013-01-01/2013-01-02"]` and the SQL query is `SELECT * FROM table1`, `SqlInputSource` will read all the data for `table1` based on the query, even though only data between the intervals specified will be indexed into Druid.

* Pagination may be used on the SQL queries to ensure that each query pulls a similar amount of data, thereby improving the efficiency of the sub-tasks.

* Similar to file-based input formats, any updates to existing data will replace the data in segments specific to the intervals specified in the `granularitySpec`.


## Combining input source

The Combining input source lets you read data from multiple input sources.
It identifies the splits from delegate input sources and uses a worker task to process each split.
Use the Combining input source only if all the delegates are splittable and can be used by the [Parallel task](./native-batch.md). 

Similar to other input sources, the Combining input source supports a single `inputFormat`.
Delegate input sources that require an `inputFormat` must have the same format for input data.

|Property|Description|Required|
|--------|-----------|---------|
|type|Set the value to `combining`.|Yes|
|delegates|List of splittable input sources to read data from.|Yes|

The following is an example of a Combining input source spec:

```json
...
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "combining",
        "delegates" : [
         {
          "type": "local",
          "filter" : "*.csv",
          "baseDir": "/data/directory",
          "files": ["/bar/foo", "/foo/bar"]
         },
         {
          "type": "druid",
          "dataSource": "wikipedia",
          "interval": "2013-01-01/2013-01-02"
         }
        ]
      },
      "inputFormat": {
        "type": "csv"
      },
      ...
    },
...
```

The [secondary partitioning method](native-batch.md#partitionsspec) determines the requisite number of concurrent worker tasks that run in parallel to complete ingestion with the Combining input source.
Set this value in `maxNumConcurrentSubTasks` in `tuningConfig` based on the secondary partitioning method:
- `range` or `single_dim` partitioning: greater than or equal to 1
- `hashed` or `dynamic` partitioning: greater than or equal to 2

For more information on the `maxNumConcurrentSubTasks` field, see [Implementation considerations](native-batch.md#implementation-considerations).