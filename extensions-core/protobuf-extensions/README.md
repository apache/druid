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

This extension provides support to ingest and understand the Protobuf data format. For more details, read the [Protobuf extension docs](../../docs/development/extensions-core/protobuf.md).

## Test Resources

The `src/test/resources/` directory contains Protocol Buffer (`.proto`) files and their generated artifacts used in unit tests. For each `.proto` file, two generated files are required:

1. **Descriptor files** (`.desc`): Binary representation of the protobuf schema with all dependencies
2. **Java wrapper classes**: Generated Java code for working with the protobuf messages

### Prerequisites

- Docker installed and running
- The `Dockerfile` in this directory creates a minimal Alpine Linux image with the correct protoc version, matching the project's Java protobuf dependency.

### Build the Docker Image

**Important**: Run all commands from the `protobuf-extensions` project root directory.

```sh
# Build the custom protoc image (only needed once or when Dockerfile changes)
docker build -t protoc-druid src/test/resources/
```

### Generate Descriptor Files

For any `.proto` file, generate its corresponding `.desc` file:

```sh
docker run --rm -v $PWD:/workspace protoc-druid protoc \
  /workspace/src/test/resources/your_file.proto \
  --proto_path=/workspace/src/test/resources \
  --descriptor_set_out=/workspace/src/test/resources/your_file.desc \
  --include_imports
```

### Generate Java Wrapper Classes

Generate Java classes from proto files:

```sh
docker run --rm -v $PWD:/workspace protoc-druid protoc \
  /workspace/src/test/resources/your_file.proto \
  --proto_path=/workspace/src/test/resources \
  --java_out=/workspace/src/test/java
```
