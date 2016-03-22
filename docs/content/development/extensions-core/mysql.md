---
layout: doc_page
---

# MySQL Metadata Store

Make sure to [include](../../operations/including-extensions.html) `mysql-metadata-storage` as an extension.

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
  with the location (host name and port) of the database.

  ```properties
  druid.extensions.loadList=["mysql-metadata-storage"]
  druid.metadata.storage.type=mysql
  druid.metadata.storage.connector.connectURI=jdbc:mysql://<host>/druid
  druid.metadata.storage.connector.user=druid
  druid.metadata.storage.connector.password=diurd
  ```

  Note: the metadata storage extension is not packaged within the main Druid tarball; it is
  packaged in a separate tarball that can be downloaded from [here](http://druid.io/downloads.html).
  You can also get it using [pull-deps](../pull-deps.html), or you can build
  it from source code; see [Build from Source](../development/build.html).
