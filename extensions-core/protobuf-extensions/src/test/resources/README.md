# Test Resources

## Protocol Buffer Files

This directory contains Protocol Buffer (`.proto`) files and their generated artifacts used in unit tests. For each `.proto` file, two generated files are required:

1. **Descriptor files** (`.desc`): Binary representation of the protobuf schema with all dependencies
2. **Java wrapper classes**: Generated Java code for working with the protobuf messages

### Prerequisites

- Docker installed and running
- The `Dockerfile` in this directory creates a minimal Alpine Linux image with the correct protoc version, matching the project's Java protobuf dependency.

### Build the Docker Image

```sh
# Build the custom protoc image (only needed once or when Dockerfile changes)
docker build -t protoc-druid .
```

### Generate Descriptor Files

For any `.proto` file, generate its corresponding `.desc` file:

```sh
docker run --rm -v $PWD:/workspace protoc-druid protoc \
  --descriptor_set_out=your_file.desc \
  --include_imports your_file.proto
```

### Generate Java Wrapper Classes

Generate Java classes from proto files:

```sh
docker run --rm \
  -v $PWD:/workspace \
  -w /workspace/src/test/resources \
  protoc-druid protoc --java_out=../java your_file.proto
```

## Naming Conventions

This project follows Google's Protocol Buffer style guide:

- **Proto files**: Use `snake_case` naming (e.g., `my_message.proto`)
- **Messages**: Use `PascalCase` naming (e.g., `MyMessage`)
- **Descriptor files**: Match the proto filename (e.g., `my_message.desc`)

The file and message names should be related and ideally match the main message type.
