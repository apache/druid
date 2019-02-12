---
layout: doc_page
title: "Build from Source"
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

# Build from Source

You can build Druid directly from source. Please note that these instructions are for building the latest stable version of Druid.
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

The basic command to build Druid from source is:

```bash
mvn clean install
```

This will run static analysis, unit tests, compile classes, and package the projects into JARs. It will _not_ generate the source or binary distribution tarball.

In addition to the basic stages, you may also want to add the following profiles and properties:

- **-Pdist** - Distribution profile: Generates the binary distribution tarball by pulling in core extensions and dependencies and packaging the files as `distribution/target/apache-druid-x.x.x-bin.tar.gz`
- **-Papache-release** - Apache release profile: Generates GPG signature and checksums, and builds the source distribution tarball as `distribution/target/apache-druid-x.x.x-src.tar.gz`
- **-Prat** - Apache Rat profile: Runs the Apache Rat license audit tool
- **-DskipTests** - Skips unit tests (which reduces build time)

Putting these together, if you wish to build the source and binary distributions with signatures and checksums, audit licenses, and skip the unit tests, you would run:

```bash
mvn clean install -Papache-release,dist,rat -DskipTests
```

#### Testing

- To run in parallel, add `-Pparallel-test -Dmaven.fork.count={parallelism-level}` arguments to the end of the command. For example:

	```bash
	# Run unit tests in processing module, in parallel with parallelism of 2.
	mvn test -pl processing -Pparallel-test -Dmaven.fork.count=2
	```

- Testing s3-extensions module requires `AWS_REGION` environment variable. If this variable is not defined, please define it explicitly. For example:

	```bash
	# Run all unit test with `AWS_REGION` environment variable of `us-east-1`.
	export AWS_REGION=us-east-1 && mvn test
	```

- For integration tests, see [here](https://github.com/apache/incubator-druid/blob/master/integration-tests/README.md).

##### Run all unit tests

```bash
mvn test
```

##### Run specific module unit tests

```bash
# Run unit tests in processing module
mvn test -pl processing
```

##### Run s3-extensions module unit tests

```bash
# Run unit tests with defined AWS_REGION environment variable.
mvn test -pl s3-extensions

# Run unit tests with AWS_REGION environment variable of us-east-1.
export AWS_REGION=us-east-1 && mvn test -pl s3-extensions
```

##### Run specific test suites

```bash
mvn test -pl common -pl sql -Dtest=BlockingPoolTest,ExpressionsTest
```

##### Run specific test methods

```bash
mvn test -pl common -Dtest=BlockingPoolTest#testTake+testTakeTimeout
```
