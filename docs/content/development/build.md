---
layout: doc_page
---

### Build from Source

Druid can be set up by building from source via git.

Building Druid requires the following:
- [JDK 7](http://www.oracle.com/technetwork/java/javase/downloads/jdk7-downloads-1880260.html)
  or [JDK 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
- [Maven version 3.x](http://maven.apache.org/download.cgi)

To do so, run these commands:

```
git clone git@github.com:druid-io/druid.git
cd druid
mvn clean install
```

This will compile the project and create the Druid binary distribution tar under
`distribution/target/druid-VERSION-bin.tar.gz`.

This will also create `distribution/target/mysql-metadata-storage-bin.tar.gz`,
which is a tarball that contains the `mysql-metadata-storage` extension.

You can find the example executables in the examples/bin directory:

* run_example_server.sh
* run_example_client.sh
