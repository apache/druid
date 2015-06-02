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
mvn clean package
```

This will compile the project and create the Druid binary distribution tar under
`services/target/druid-VERSION-bin.tar.gz`.

You can find the example executables in the examples/bin directory:

* run_example_server.sh
* run_example_client.sh
