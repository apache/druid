---
id: build
title: "Build from source"
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


You can build Apache Druid directly from source. Use the version of this page
that matches the version you want to build.
For building the latest code in master, follow the latest version of this page
[here](https://github.com/apache/druid/blob/master/docs/development/build.md):
make sure it has `/master/` in the URL.

#### Prerequisites

##### Installing Java and Maven

- JDK 8, 8u92+ or JDK 11. See our [Java documentation](../operations/java.md) for information about obtaining a JDK.
- [Maven version 3.x](http://maven.apache.org/download.cgi)

##### Other dependencies

- Distribution builds require Python 3.x and the `pyyaml` module

##### Downloading the source

```bash
git clone git@github.com:apache/druid.git
cd druid
```

#### Building from source

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
- **-Dweb.console.skip=true** - Skip front end project

Putting these together, if you wish to build the source and binary distributions with signatures and checksums, audit licenses, and skip the unit tests, you would run:

```bash
mvn clean install -Papache-release,dist,rat -DskipTests
```

### Building hadoop 3 distribution

By default, druid ships hadoop 2.x.x jars along with the distribution. Exact version can be found in the
main [pom](https://github.com/apache/druid/blob/master/pom.xml). To build druid with hadoop 3.x.x jars, hadoop3 profile
needs to be activated.

To generate build with hadoop 3 dependencies, run:

```bash
mvn clean install -Phadoop3
```

To generate distribution with hadoop3 dependencies, run :

```bash
mvn clean install -Papache-release,dist-hadoop3,rat,hadoop3 -DskipTests 
```

#### Potential issues

##### Missing `pyyaml`

You are building Druid from source following the instructions on this page but you get
```
[ERROR] Failed to execute goal org.codehaus.mojo:exec-maven-plugin:1.6.0:exec (generate-binary-license) on project distribution: Command execution failed.: Process exited with an error: 1 (Exit value: 1) -> [Help 1]
```

Resolution: Make sure you have Python installed as well as the `yaml` module:

```bash
pip install pyyaml
```

On some systems, ensure you use the Python 3.x version of `pip`:

```bash
pip3 install pyyaml
```
