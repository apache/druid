<!--
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
-->

# Consul Discovery Extension

This extension provides Consul-based service discovery and leader election for Apache Druid, enabling a complete ZooKeeper replacement.

## Features

- Service registration and discovery via Consul
- Leader election for Coordinator and Overlord (replaces ZooKeeper)
- Health checking via TTL checks
- Support for Consul ACL
- HTTP Basic Auth support
- Multi-datacenter support (DC usually maps to a region; election and discovery are DC-scoped)
- TLS/mTLS support for secure Consul communication

## Usage

Add to your `extensions.loadList`:

```properties
druid.extensions.loadList=["druid-consul-extensions", ...]
```

Configure Consul discovery:

```properties
# Consul connection
druid.discovery.type=consul
druid.discovery.consul.connection.host=localhost
druid.discovery.consul.connection.port=8500
druid.discovery.consul.service.servicePrefix=druid

# Required HTTP-based configurations
druid.serverview.type=http
druid.indexer.runner.type=httpRemote

# Leader election using Consul (replaces ZooKeeper)
druid.coordinator.selector.type=consul
druid.indexer.selector.type=consul
```

## Documentation

See [docs/development/extensions-contrib/consul.md](../../docs/development/extensions-contrib/consul.md) for full documentation including TLS configuration, ACL permissions, and production tuning.

## Building

```bash
mvn clean install -DskipTests
```

## Testing

Unit tests:
```bash
mvn test
```

Integration tests (requires Docker):
```bash
# Run all Consul discovery tests (Plain HTTP, TLS, and mTLS)
mvn verify -P docker-tests -pl embedded-tests -Ddruid.testing.consul.enabled=true

# Test classes:
# - ConsulDiscoveryPlainDockerTest
# - ConsulDiscoveryTLSDockerTest
# - ConsulDiscoveryMTLSDockerTest
```

## License

Apache License, Version 2.0
