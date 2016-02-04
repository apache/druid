---
layout: doc_page
---

### Build from Source

You can build Druid directly from source. Please note that these instructions are for building the latest stable of Druid. 
For building the latest code in master, follow the instructions [here](https://github.com/druid-io/druid/blob/master/docs/content/development/build.md).

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

This will also create a tarball that contains `mysql-metadata-storage` extension under 
`distribution/target/mysql-metadata-storage-bin.tar.gz`. If you want Druid to load `mysql-metadata-storage`, you can 
first untar `druid-VERSION-bin.tar.gz`, then go to ```druid-<version>/extensions```, untar `mysql-metadata-storage-bin.tar.gz` 
there. Now just specifiy `mysql-metadata-storage` in `druid.extensions.loadList` so that Druid will pick it up. 
See [Including Extensions](../operations/including-extensions.html) for more information.
