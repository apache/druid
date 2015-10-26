---
layout: doc_page
---

### Build from Source

The other way to setup Druid is from source via git.

Builing Druid requires the following:
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
 `distribution/target/mysql-metdata-storage-bin.tar.gz`. If you want Druid to load `mysql-metadata-storage`, you can first untar `druid-VERSION-bin.tar.gz`, then go to ```druid-<version>/druid_extensions```, untar `mysql-metdata-storage-bin.tar.gz` there. Now just specifiy `mysql-metadata-storage` in `druid.extensions.loadList` so that Druid will pick it up. See [Including Extensions](../operations/including-extensions.html) for more infomation.

You can find the example executables in the examples/bin directory:

* run_example_server.sh
* run_example_client.sh
