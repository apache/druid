---
layout: doc_page
---

# Microsoft SQLServer

Make sure to [include](../../operations/including-extensions.html) `sqlserver-metadata-storage` as an extension.

## Setting up SQLServer

1. Install Microsoft SQLServer

2. Create a druid database and user

  Create the druid user
  - Microsoft SQL Server Management Studio - Security - Logins - New Login...
  - Create a druid user, enter `diurd` when prompted for the password.

  Create a druid database owned by the user we just created
  - Databases - New Database
  - Database Name: druid, Owner: druid

3. Add the Microsoft JDBC library to the Druid classpath
  - To ensure the com.microsoft.sqlserver.jdbc.SQLServerDriver class is loaded you will have to add the appropriate Microsoft JDBC library (sqljdbc*.jar) to the Druid classpath.
  - For instance, if all jar files in your "druid/lib" directory are automatically added to your Druid classpath, then manually download the Microsoft JDBC drivers from ( https://www.microsoft.com/en-ca/download/details.aspx?id=11774) and drop it into my druid/lib directory.

4. Configure your Druid metadata storage extension:

  Add the following parameters to your Druid configuration, replacing `<host>`
  with the location (host name and port) of the database.

  ```properties
  druid.metadata.storage.type=sqlserver
  druid.metadata.storage.connector.connectURI=jdbc:sqlserver://<host>;databaseName=druid
  druid.metadata.storage.connector.user=druid
  druid.metadata.storage.connector.password=diurd
  ```
