---
layout: doc_page
---
# Metadata Storage

The Metadata Storage is an external dependency of Druid. Druid uses it to store
various metadata about the system, but not to store the actual data. There are
a number of tables used for various purposes described below.

## Supported Metadata Storages

The following metadata storage engines are supported:

* Derby (default - only works if you have all processes running on the same node)
* MySQL (io.druid.extensions:mysql-metadata-storage)
* PostgreSQL (io.druid.extensions:postgresql-metadata-storage)

To choose a metadata storage, set the `druid.extensions` configuration to
include the extension for the metadata storage you plan to use.


## Setting up MySQL

1. Install MySQL

  Use your favorite package manager to install mysql, e.g.:
  - on Ubuntu/Debian using apt `apt-get install mysql-server`
  - on OS X, using [Homebrew](http://brew.sh/) `brew install mysql`

  Alternatively, download and follow installation instructions for MySQL
  Community Server here:
  [http://dev.mysql.com/downloads/mysql/](http://dev.mysql.com/downloads/mysql/)

2. Create a druid database and user

  Connect to MySQL from the machine where it is installed.

  ```bash
  > mysql -u root
  ```

  Paste the following snippet into the mysql prompt:

  ```sql
  -- create a druid database, make sure to use utf8 as encoding
  CREATE DATABASE druid DEFAULT CHARACTER SET utf8;

  -- create a druid user, and grant it all permission on the database we just created
  GRANT ALL ON druid.* TO 'druid'@'localhost' IDENTIFIED BY 'diurd';
  ```

3. Configure your Druid metadata storage extension:

  Add the following parameters to your Druid configuration, replacing `<host>`
  with the hostname of the database.

  ```properties
  druid.extensions.coordinates=[\"io.druid.extensions:mysql-metadata-storage"]
  druid.metadata.storage.type=mysql
  druid.metadata.storage.connector.connectURI=jdbc:mysql://<host>/druid_test
  druid.metadata.storage.connector.user=druid
  druid.metadata.storage.connector.password=diurd
  ```

## Setting up PostgreSQL

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
  with the hostname of the database.

  ```properties
  druid.extensions.coordinates=[\"io.druid.extensions:postgresql-metadata-storage"]
  druid.metadata.storage.type=postgresql
  druid.metadata.storage.connector.connectURI=jdbc:postgresql://<host>/druid_test
  druid.metadata.storage.connector.user=druid
  druid.metadata.storage.connector.password=diurd
  ```

## Metadata Storage Tables

### Segments Table

This is dictated by the `druid.metadata.storage.tables.segments` property.

This table stores metadata about the segments that are available in the system.
The table is polled by the [Coordinator](../design/coordinator.html) to
determine the set of segments that should be available for querying in the
system. The table has two main functional columns, the other columns are for
indexing purposes.

The `used` column is a boolean "tombstone". A 1 means that the segment should
be "used" by the cluster (i.e. it should be loaded and available for requests).
A 0 means that the segment should not be actively loaded into the cluster. We
do this as a means of removing segments from the cluster without actually
removing their metadata (which allows for simpler rolling back if that is ever
an issue).

The `payload` column stores a JSON blob that has all of the metadata for the segment (some of the data stored in this payload is redundant with some of the columns in the table, that is intentional). This looks something like

```
{
 "dataSource":"wikipedia",
 "interval":"2012-05-23T00:00:00.000Z/2012-05-24T00:00:00.000Z",
 "version":"2012-05-24T00:10:00.046Z",
 "loadSpec":{"type":"s3_zip",
             "bucket":"bucket_for_segment",
             "key":"path/to/segment/on/s3"},
 "dimensions":"comma-delimited-list-of-dimension-names",
 "metrics":"comma-delimited-list-of-metric-names",
 "shardSpec":{"type":"none"},
 "binaryVersion":9,
 "size":size_of_segment,
 "identifier":"wikipedia_2012-05-23T00:00:00.000Z_2012-05-24T00:00:00.000Z_2012-05-23T00:10:00.046Z"
}
```

Note that the format of this blob can and will change from time-to-time.

### Rule Table

The rule table is used to store the various rules about where segments should
land. These rules are used by the [Coordinator](../design/coordinator.html)
  when making segment (re-)allocation decisions about the cluster.

### Config Table

The config table is used to store runtime configuration objects. We do not have
many of these yet and we are not sure if we will keep this mechanism going
forward, but it is the beginnings of a method of changing some configuration
parameters across the cluster at runtime.

### Task-related Tables

There are also a number of tables created and used by the [Indexing
Service](../design/indexing-service.html) in the course of its work.

### Audit Table

The Audit table is used to store the audit history for configuration changes
e.g rule changes done by [Coordinator](../design/coordinator.html) and other
config changes.
