---
id: postgresql
title: "PostgreSQL Metadata Store"
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


To use this Apache Druid extension, [include](../../configuration/extensions.md#loading-extensions) `postgresql-metadata-storage` in the extensions load list.

## Setting up PostgreSQL

To avoid issues with upgrades that require schema changes to a large metadata table, consider a PostgreSQL version that supports instant ADD COLUMN semantics.

1. Install PostgreSQL

  Use your favorite package manager to install PostgreSQL, e.g.:
  - on Ubuntu/Debian using apt `apt-get install postgresql`
  - on OS X, using [Homebrew](http://brew.sh/) `brew install postgresql`

2. Create a druid database and user

  On the machine where PostgreSQL is installed, using an account with proper
  postgresql permissions:

  Create a druid user, enter `diurd` when prompted for the password.

  ```bash
  createuser druid -P
  ```

  Create a druid database owned by the user we just created

  ```bash
  createdb druid -O druid
  ```

  *Note:* On Ubuntu / Debian you may have to prefix the `createuser` and
  `createdb` commands with `sudo -u postgres` in order to gain proper
  permissions.

3. Configure your Druid metadata storage extension:

  Add the following parameters to your Druid configuration, replacing `<host>`
  with the location (host name and port) of the database.

  ```properties
  druid.extensions.loadList=["postgresql-metadata-storage"]
  druid.metadata.storage.type=postgresql
  druid.metadata.storage.connector.connectURI=jdbc:postgresql://<host>/druid
  druid.metadata.storage.connector.user=druid
  druid.metadata.storage.connector.password=diurd
  ```

## Configuration

In most cases, the configuration options map directly to the [postgres JDBC connection options](https://jdbc.postgresql.org/documentation/use/#connecting-to-the-database).

|Property|Description|Default|Required|
|--------|-----------|-------|--------|
| `druid.metadata.postgres.ssl.useSSL` | Enables SSL | `false` | no |
| `druid.metadata.postgres.ssl.sslPassword` | The [Password Provider](../../operations/password-provider.md) or String password for the client's key. | none | no |
| `druid.metadata.postgres.ssl.sslFactory` | The class name to use as the `SSLSocketFactory` | none | no |
| `druid.metadata.postgres.ssl.sslFactoryArg` | An optional argument passed to the sslFactory's constructor | none | no |
| `druid.metadata.postgres.ssl.sslMode` | The sslMode. Possible values are "disable", "require", "verify-ca", "verify-full", "allow" and "prefer"| none | no |
| `druid.metadata.postgres.ssl.sslCert` | The full path to the certificate file. | none | no |
| `druid.metadata.postgres.ssl.sslKey` | The full path to the key file. | none | no |
| `druid.metadata.postgres.ssl.sslRootCert` | The full path to the root certificate. | none | no |
| `druid.metadata.postgres.ssl.sslHostNameVerifier` | The classname of the hostname verifier. | none | no |
| `druid.metadata.postgres.ssl.sslPasswordCallback` | The classname of the SSL password provider. | none | no |
| `druid.metadata.postgres.dbTableSchema` | druid meta table schema | `public` | no |

### PostgreSQL Firehose

The PostgreSQL extension provides an implementation of an [SQL input source](../../ingestion/input-sources.md) which can be used to ingest data into Druid from a PostgreSQL database.

```json
{
  "type": "index_parallel",
  "spec": {
    "dataSchema": {
      "dataSource": "some_datasource",
      "dimensionsSpec": {
        "dimensionExclusions": [],
        "dimensions": [
          "dim1",
          "dim2",
          "dim3"
        ]
      },
      "timestampSpec": {
        "format": "auto",
        "column": "ts"
      },
      "metricsSpec": [],
      "granularitySpec": {
        "type": "uniform",
        "segmentGranularity": "DAY",
        "queryGranularity": {
          "type": "none"
        },
        "rollup": false,
        "intervals": null
      },
      "transformSpec": {
        "filter": null,
        "transforms": []
      }
    },
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "sql",
        "database": {
          "type": "postgresql",
          "connectorConfig": {
            "connectURI": "jdbc:postgresql://some-rds-host.us-west-1.rds.amazonaws.com:5432/druid",
            "user": "admin",
            "password": "secret"
          }
        },
        "sqls": [
          "SELECT * FROM some_table"
        ]
      },
      "inputFormat": {
        "type": "json"
      }
    },
    "tuningConfig": {
      "type": "index_parallel"
    }
  }
}
```
