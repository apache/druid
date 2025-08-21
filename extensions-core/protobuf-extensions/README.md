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

# Protobuf Extension

This extension provides support to ingest and understand the Protobuf data format. For more details, read the [Protobuf extension docs](../../docs/development/extensions-core/protobuf.md).

## Test Resources and Code Generation

The `src/test/resources/` directory contains Protocol Buffer (`.proto`) files used in unit tests. For each `.proto` file, we make use of two generated files:

1. **Java wrapper classes**: Generated Java code for working with the protobuf messages, automatically generated during the build process.
2. **Descriptor files** (`.desc`): Binary representation of the protobuf schema with all dependencies, in source-control and manually generated using the process outlined below.

### Automatic Code Generation

The project uses the `io.github.ascopes.protobuf-maven-plugin` to automatically generate Java wrapper classes from `.proto` files. This happens automatically when you run:

```sh
mvn generate-test-sources
# or any lifecycle phase that includes it, like:
mvn test
```

### Descriptor File Generation

Unit tests may require manually generated **descriptor files** (`.desc`) which contain binary representations of the protobuf schema with all dependencies.

#### Prerequisites

- Docker installed and running
- The `Dockerfile` in `src/test/resources/` creates a minimal Alpine Linux image with the correct protoc version

#### Build Docker Image

**Important**: Run all commands from the `protobuf-extensions` project root directory.

```sh
# Build the custom protoc image (only needed once or when Dockerfile changes)
docker build -t protoc-druid src/test/resources/
```

#### Generate Descriptor Files

```sh
docker run --rm -v $PWD:/workspace protoc-druid protoc \
  /workspace/src/test/resources/your_file.proto \
  --proto_path=/workspace/src/test/resources \
  --descriptor_set_out=/workspace/src/test/resources/your_file.desc \
  --include_imports
```
