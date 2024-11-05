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

## Prerequisites

### Installing Java and Maven

- See our [Java documentation](../operations/java.md) for information about obtaining a supported JDK
- [Maven version 3.x](http://maven.apache.org/download.cgi)

### Other Dependencies

- Distribution builds require Python 3.x and the `pyyaml` module.
- Integration tests require `pyyaml` version 5.1 or later.

## Downloading the Source Code

```bash
git clone git@github.com:apache/druid.git
cd druid
```

## Building from Source

The basic command to build Druid from source is:

```bash
mvn clean install
```

This will run static analysis, unit tests, compile classes, and package the projects into JARs. It will _not_ generate the source or binary distribution tarball. Note that this build may take some time to complete.

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

### Building for Development

For development, use only the dist profile and skip the Apache release and Apache rat profiles.

```bash
mvn clean install -Pdist -DskipTests
```

If you want to speed up the build even more, you can enable parallel building with the `-T1C` option and skip some static analysis checks.

```bash
mvn clean install -Pdist -T1C -DskipTests -Dforbiddenapis.skip=true -Dcheckstyle.skip=true -Dpmd.skip=true -Dmaven.javadoc.skip=true -Denforcer.skip=true
```

You will expect to find your distribution tar file under the `distribution/target` directory.

## Potential issues

### Missing `pyyaml`

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
