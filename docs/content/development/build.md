---
layout: doc_page
---

### Build from Source

You can build Druid directly from source. Please note that these instructions are for building the latest stable of Druid. 
For building the latest code in master, follow the instructions [here](https://github.com/apache/incubator-druid/blob/master/docs/content/development/build.md).


#### Prerequisites

##### Installing Java and Maven:
- [JDK 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
- [Maven version 3.x](http://maven.apache.org/download.cgi)

##### Downloading the source:

```bash
git clone git@github.com:apache/incubator-druid.git
cd druid
```


#### Building the source

##### Building the Druid binary distribution tarball:

```bash
mvn clean install -Pdist -Dtar -DskipTests
```

Once it succeeds, you can find the Druid binary (`druid-VERSION-bin.tar.gz`)
and `mysql-metadata-storage` extension under `${DRUID_ROOT}/distribution/target/`.

If you want Druid to load `mysql-metadata-storage`, you can 
first untar `druid-VERSION-bin.tar.gz`, then go to ```druid-<version>/extensions```, untar `mysql-metadata-storage-bin.tar.gz` 
there. Now just specifiy `mysql-metadata-storage` in `druid.extensions.loadList` so that Druid will pick it up. 
See [Including Extensions](../operations/including-extensions.html) for more information.

##### Building the source code only:

```bash
mvn clean install -DskipTests
```