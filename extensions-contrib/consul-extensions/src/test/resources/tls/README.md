<!--

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

-->

# Consul TLS/mTLS Testing

**Integration tests for Consul with TLS/mTLS have been moved to the `embedded-tests` module.**

## Running the Tests

```bash
# Run all Consul discovery tests (Plain HTTP, TLS, and mTLS)
mvn verify -P docker-tests -pl embedded-tests -Ddruid.testing.consul.enabled=true
```

## Test Classes

The following test classes in `embedded-tests` provide comprehensive coverage:

- **`ConsulDiscoveryPlainDockerTest`** - Plain HTTP (no encryption)
- **`ConsulDiscoveryTLSDockerTest`** - Server-side TLS only
- **`ConsulDiscoveryMTLSDockerTest`** - Mutual TLS (client certificates required)

Each test class:
- ✅ Automatically generates TLS certificates at runtime using Bouncy Castle
- ✅ Starts Consul in a Testcontainers Docker container with appropriate security config
- ✅ Tests service discovery, leader election, and ingestion workflows
- ✅ Cleans up certificates and containers after tests complete

## Implementation Details

See:
- `embedded-tests/src/test/java/org/apache/druid/testing/embedded/consul/ConsulClusterResource.java` - Consul container management with TLS support
- `embedded-tests/src/test/java/org/apache/druid/testing/embedded/docker/BaseConsulDiscoveryDockerTest.java` - Shared test logic
- `services/src/test/java/org/apache/druid/testing/utils/TLSCertificateGenerator.java` - Certificate generation utility
