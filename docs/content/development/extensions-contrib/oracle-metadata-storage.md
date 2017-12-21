---
layout: doc_page
---

# Oracle Metadata Storage

Make sure to [include](../../operations/including-extensions.html) `oracle-metadata-storage` as an extension.

## Setting up Extension

1. Add the OJDBC library to the Druid classpath
  - To ensure the oracle.jdbc.OracleDriver class is loaded you will have to add the appropriate OJDBC library (ojdbc6.jar or ojdbc7.jar) to the Druid classpath.
  - For instance, if all jar files in your "druid/lib" directory are automatically added to your Druid classpath, then manually download the OJDBC drivers from oracle official website and drop it into my druid/lib directory.

2. Configure your Druid metadata storage extension:

  Add the following parameters to your Druid configuration, replacing `<host>`, `<port>`, `<service_name>`, `<user>`, `<password>`
  with the actual values of the database.

  ```properties
  druid.metadata.storage.type=oracle
  druid.metadata.storage.connector.connectURI=jdbc:oracle:thin:@//<host>:<port>/<service_name>
  druid.metadata.storage.connector.user=<user>
  druid.metadata.storage.connector.password=<password>
  ```
