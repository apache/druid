---
id: consul
title: "Consul-based Service Discovery"
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

Apache Druid extension to enable using HashiCorp Consul for node discovery. This extension allows Druid clusters to use Consul's service catalog for discovering nodes, as an alternative to ZooKeeper or Kubernetes-based discovery.

## Quick Start

To use this Apache Druid extension, [include](../../configuration/extensions.md#loading-extensions) `druid-consul-extensions` in the extensions load list.

```properties
# Load the extension
druid.extensions.loadList=["druid-consul-extensions"]

# Minimal Consul configuration
druid.discovery.type=consul
druid.discovery.consul.connection.host=localhost
druid.discovery.consul.connection.port=8500
druid.discovery.consul.service.servicePrefix=druid

# Required HTTP-based configurations
druid.serverview.type=http
druid.indexer.runner.type=httpRemote

# Enable Consul-based leader election
druid.coordinator.selector.type=consul
druid.indexer.selector.type=consul
```

For production deployments, see the [Configuration](#configuration) section below for security, TLS, and advanced options.

## Configuration

This extension works together with HTTP-based segment and task management in Druid. The following configurations must be set on all Druid nodes:

```
druid.serverview.type=http
druid.indexer.runner.type=httpRemote
druid.discovery.type=consul
druid.coordinator.selector.type=consul
druid.indexer.selector.type=consul
```

**Note:** This extension provides complete ZooKeeper replacement, including both service discovery and leader election for Coordinator and Overlord roles.

### Properties

| Property | Possible Values | Description | Default | Required |
|----------|-----------------|-------------|---------|--------|
| `druid.discovery.consul.connection.host` | String | Consul agent hostname or IP address. | `localhost` | No |
| `druid.discovery.consul.connection.port` | Integer | Consul agent HTTP API port. | `8500` | No |
| `druid.discovery.consul.connection.connectTimeout` | ISO8601 Duration | Connection timeout for Consul client. | `PT10S` | No |
| `druid.discovery.consul.connection.socketTimeout` | ISO8601 Duration | Socket read timeout for Consul client. Must be greater than watchSeconds. | `PT75S` (75s) | No |
| `druid.discovery.consul.connection.maxTotalConnections` | Integer | Maximum total HTTP connections in the connection pool. | `50` | No |
| `druid.discovery.consul.connection.maxConnectionsPerRoute` | Integer | Maximum HTTP connections per route (host). | `20` | No |
| `druid.discovery.consul.service.servicePrefix` | String | Prefix for Consul service names; namespaces clusters. | None | Yes |
| `druid.discovery.consul.service.datacenter` | String | Consul datacenter for registration and discovery. | Default datacenter | No |
| `druid.discovery.consul.service.healthCheckInterval` | ISO8601 Duration | Update interval for Consul health checks. | `PT10S` | No |
| `druid.discovery.consul.service.deregisterAfter` | ISO8601 Duration | Deregister service after health check fails. | `PT90S` | No |
| `druid.discovery.consul.service.serviceTags.*` | String | Additional Consul service tags as key/value pairs (rendered as `key:value`). Useful for AZ/tier/version. | None | No |
| `druid.discovery.consul.auth.aclToken` | String | Consul ACL token for authentication. | None | No |
| `druid.discovery.consul.auth.basicAuthUser` | String | Username for HTTP basic authentication (preemptive Basic Auth). | None | No |
| `druid.discovery.consul.auth.basicAuthPassword` | String | Password for HTTP basic authentication. | None | No |
| `druid.discovery.consul.auth.allowBasicAuthOverHttp` | Boolean | Allow basic auth credentials over unencrypted HTTP. By default, basic auth requires TLS and will fail fast if TLS is not configured. Set to `true` only for sidecar TLS termination scenarios. | `false` | No |
| `druid.discovery.consul.watch.watchSeconds` | ISO8601 Duration | Blocking query timeout for service changes. | `PT60S` | No |
| `druid.discovery.consul.watch.maxWatchRetries` | Long | Max consecutive watch failures before circuit breaker activates. Use `-1` or `0` for unlimited retries (default). | `unlimited` | No |
| `druid.discovery.consul.watch.watchRetryDelay` | ISO8601 Duration | Wait time before retrying failed watch. | `PT10S` | No |
| `druid.discovery.consul.watch.circuitBreakerSleep` | ISO8601 Duration | Sleep duration when circuit breaker trips after exceeding maxWatchRetries. | `PT2M` (2 minutes) | No |
| `druid.discovery.consul.leader.coordinatorLeaderLockPath` | String | Consul KV path for Coordinator leader lock. | `druid/leader/coordinator` | No |
| `druid.discovery.consul.leader.overlordLeaderLockPath` | String | Consul KV path for Overlord leader lock. | `druid/leader/overlord` | No |
| `druid.discovery.consul.leader.leaderMaxErrorRetries` | Long | Max consecutive leader-election errors before stopping election attempts. Use `-1` or `0` for unlimited retries (default). Leader election retries indefinitely by default because giving up breaks cluster operation, and exponential backoff (up to `leaderRetryBackoffMax`) prevents overwhelming Consul during transient failures. | `unlimited` | No |
| `druid.discovery.consul.leader.leaderRetryBackoffMax` | ISO8601 Duration | Maximum backoff applied between leader-election retries. | `PT5M` | No |
| `druid.discovery.consul.leader.leaderSessionTtl` | ISO8601 Duration | TTL for leader election session. Auto-calculated if not set. | `max(45s, 3 * healthCheckInterval)` | No |

### TLS Configuration

This extension uses Druid's standard TLS configuration for secure HTTPS connections to Consul. To enable TLS, configure the SSL client properties under `druid.discovery.consul.connection.sslClientConfig.*`.

**Security Notes:**
- If `sslClientConfig` is provided but TLS initialization fails (e.g., invalid truststore), the Consul client fails fast and does not fall back to plain HTTP.
- **Basic Auth Security:** By default, configuring basic authentication (`basicAuthUser` and `basicAuthPassword`) without TLS will cause the client to fail fast with an error. This prevents accidental transmission of credentials in cleartext. To explicitly allow basic auth over HTTP (e.g., for sidecar TLS termination scenarios), set `allowBasicAuthOverHttp=true`. When enabled, a warning will be logged about cleartext credential transmission.

TLS properties:

| Property | Description | Default |
|----------|-------------|---------|
| `druid.discovery.consul.connection.sslClientConfig.protocol` | SSL/TLS protocol to use. | `TLSv1.2` |
| `druid.discovery.consul.connection.sslClientConfig.trustStoreType` | Type of truststore (PKCS12, JKS, etc.). | `java.security.KeyStore.getDefaultType()` |
| `druid.discovery.consul.connection.sslClientConfig.trustStorePath` | Path to truststore for verifying Consul server certificates. [Required for TLS] | None |
| `druid.discovery.consul.connection.sslClientConfig.trustStoreAlgorithm` | Algorithm for TrustManagerFactory. | `javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm()` |
| `druid.discovery.consul.connection.sslClientConfig.trustStorePassword` | Password for truststore. Can use [password providers](../../operations/password-provider.md). | None |
| `druid.discovery.consul.connection.sslClientConfig.keyStoreType` | Type of keystore for client certificate (PKCS12, JKS, etc.). | `java.security.KeyStore.getDefaultType()` |
| `druid.discovery.consul.connection.sslClientConfig.keyStorePath` | Path to keystore with client certificate for mTLS. [Optional] | None |
| `druid.discovery.consul.connection.sslClientConfig.keyStorePassword` | Password for keystore. Can use [password providers](../../operations/password-provider.md). | None |
| `druid.discovery.consul.connection.sslClientConfig.keyManagerPassword` | Password for key manager within keystore. Can use [password providers](../../operations/password-provider.md). | None |
| `druid.discovery.consul.connection.sslClientConfig.keyManagerFactoryAlgorithm` | Algorithm for KeyManagerFactory. | `javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm()` |
| `druid.discovery.consul.connection.sslClientConfig.certAlias` | Alias of certificate to use from keystore for mTLS. [Optional] | None |
| `druid.discovery.consul.connection.sslClientConfig.validateHostnames` | Verify Consul server hostname in certificate. | `true` |

For details on supported keystore formats and additional configuration options, see the [Simple Client SSL Context extension](../../development/extensions-core/simple-client-sslcontext.md) documentation.

### Example Configuration

For a typical deployment:

```properties
# Extension loading
druid.extensions.loadList=["druid-consul-extensions", ...]

# Discovery configuration
druid.discovery.type=consul
druid.discovery.consul.connection.host=consul.example.com
druid.discovery.consul.connection.port=8500
druid.discovery.consul.service.servicePrefix=druid-prod

# HTTP-based segment and task management
druid.serverview.type=http
druid.indexer.runner.type=httpRemote

# Leader election using Consul (replaces ZooKeeper)
druid.coordinator.selector.type=consul
druid.indexer.selector.type=consul

# Optional: discovery watch retry behavior
# Use -1 for unlimited retries (default). Set a positive number to stop after N consecutive failures.
# druid.discovery.consul.watch.maxWatchRetries=-1

# Optional: leader election retry behavior (also defaults to -1/unlimited)
# druid.discovery.consul.leader.leaderMaxErrorRetries=-1
# druid.discovery.consul.leader.leaderRetryBackoffMax=PT5M
```

For a secure Consul cluster with ACL:

```properties
druid.discovery.type=consul
druid.discovery.consul.connection.host=consul.example.com
druid.discovery.consul.connection.port=8500
druid.discovery.consul.service.servicePrefix=druid-prod
druid.discovery.consul.auth.aclToken=your-secret-acl-token
druid.discovery.consul.service.datacenter=dc1
```

For TLS-enabled Consul with server certificate verification:

```properties
druid.discovery.type=consul
druid.discovery.consul.connection.host=consul.example.com
druid.discovery.consul.connection.port=8501
druid.discovery.consul.service.servicePrefix=druid-prod
druid.discovery.consul.auth.aclToken=your-secret-acl-token

# TLS configuration (standard Druid properties)
druid.discovery.consul.connection.sslClientConfig.trustStorePath=/etc/druid/certs/consul-ca-truststore.jks
druid.discovery.consul.connection.sslClientConfig.trustStorePassword=truststore-password
druid.discovery.consul.connection.sslClientConfig.validateHostnames=true
```

For Consul with mutual TLS (mTLS) authentication:

```properties
druid.discovery.type=consul
druid.discovery.consul.connection.host=consul.example.com
druid.discovery.consul.connection.port=8501
druid.discovery.consul.service.servicePrefix=druid-prod
druid.discovery.consul.auth.aclToken=your-secret-acl-token

# TLS with client certificates (mutual TLS)
druid.discovery.consul.connection.sslClientConfig.trustStorePath=/etc/druid/certs/consul-ca-truststore.jks
druid.discovery.consul.connection.sslClientConfig.trustStorePassword=truststore-password
druid.discovery.consul.connection.sslClientConfig.keyStorePath=/etc/druid/certs/druid-client-keystore.p12
druid.discovery.consul.connection.sslClientConfig.keyStorePassword=keystore-password
druid.discovery.consul.connection.sslClientConfig.validateHostnames=true
```

### Datacenter Scope

Consul datacenter scope: service catalog, KV, sessions, locks.
- Leader election is DC-scoped
- Multiple DC support via `datacenter` configuration
- Single DC per region recommended

### Health Check TTL Behavior

Service health check TTL = 3 * `healthCheckInterval` (minimum 30s). 
Heartbeats sent every `healthCheckInterval`.

## Operational Verification

After enabling the extension, run a quick “sanity walk” against Consul to ensure everything is wired up:

1. **Confirm registration**
   ```bash
   consul catalog services -service druid-prod-broker -tag role:broker
   ```
   Replace `druid-prod-broker` with `servicePrefix-role`. You should see the node’s `host:port` and any custom tags.

2. **Check TTL status**
   ```bash
   consul health service druid-prod-broker
   ```
   Healthy checks should show `Status: passing` and a TTL note (“Druid node is healthy”).

3. **Inspect leader KV**
   ```bash
   consul kv get -detailed druid/leader/coordinator
   ```
   The output includes the session ID and the stored leader URI (e.g., `http://host:8081`). If no value is returned, no leader is registered yet.

4. **Watch for updates**
   ```bash
   consul watch -type=service -service=druid-prod-broker
   ```
   As you start/stop Druid nodes, Consul should emit change notifications that line up with the extension’s logs and metrics.

If any command returns empty output, double-check network reachability to the local Consul agent, ACL permissions, and `servicePrefix` spelling.

## Implementation Details

### Service Registration

- Service name: `{servicePrefix}-{nodeRole}`
- Service ID: `{servicePrefix}-{nodeRole}-{host}-{port}`
- Service tags: `druid`, `role:{nodeRole}`
- Service metadata: `DiscoveryDruidNode` JSON
- Health check: TTL-based, updated every `healthCheckInterval`

### Service Discovery

1. Query Consul health service API for services by role
2. Use blocking queries to watch for changes
3. Notify listeners of node additions/removals

### Leader Election

Uses Consul sessions and KV locks:
- Session TTL: `max(45s, healthCheckInterval * 3)`
- Lock delay: 5 seconds
- Failover time: 15-45 seconds (depends on healthCheckInterval)

## Authentication Methods

The extension supports multiple authentication methods for securing communication with Consul:

### 1. ACL Token Authentication (Recommended)

Most common for production deployments:

```properties
druid.discovery.consul.auth.aclToken=your-secret-token
```

The token must have appropriate permissions (see Consul ACL Permissions section below).

### 2. TLS/HTTPS with Certificate Verification

For encrypted communication and server verification, configure a truststore containing Consul's CA certificate:

```properties
# TLS configuration using truststore
druid.discovery.consul.connection.sslClientConfig.protocol=TLSv1.3
druid.discovery.consul.connection.sslClientConfig.trustStoreType=PKCS12
druid.discovery.consul.connection.sslClientConfig.trustStorePath=/path/to/truststore.p12
druid.discovery.consul.connection.sslClientConfig.trustStorePassword=truststore-password
druid.discovery.consul.connection.sslClientConfig.validateHostnames=true
```

### 3. Mutual TLS (mTLS) Authentication

For strongest security, use client certificates in addition to server verification:

```properties
# TLS with both truststore and keystore (mTLS)
druid.discovery.consul.connection.sslClientConfig.protocol=TLSv1.3

# Server verification (truststore with Consul CA)
druid.discovery.consul.connection.sslClientConfig.trustStoreType=PKCS12
druid.discovery.consul.connection.sslClientConfig.trustStorePath=/path/to/truststore.p12
druid.discovery.consul.connection.sslClientConfig.trustStorePassword=truststore-password

# Client authentication (keystore with client certificate)
druid.discovery.consul.connection.sslClientConfig.keyStoreType=PKCS12
druid.discovery.consul.connection.sslClientConfig.keyStorePath=/path/to/client-keystore.p12
druid.discovery.consul.connection.sslClientConfig.keyStorePassword=keystore-password
druid.discovery.consul.connection.sslClientConfig.certAlias=client

# Hostname verification
druid.discovery.consul.connection.sslClientConfig.validateHostnames=true
```

**Creating Keystores from PEM Files:**

If you have PEM certificate and key files, convert them to PKCS12 keystores:

```bash
# 1. Create client keystore from PEM certificate and private key
openssl pkcs12 -export \
  -in client-cert.pem \
  -inkey client-key.pem \
  -out client-keystore.p12 \
  -name client \
  -passout pass:keystore-password

# 2. Create truststore from Consul CA certificate
keytool -import \
  -file consul-ca.pem \
  -alias consul-ca \
  -keystore truststore.p12 \
  -storetype PKCS12 \
  -storepass truststore-password \
  -noprompt
```

**Notes:**
- Both JKS and PKCS12 keystore types are supported
- PKCS12 is recommended as the industry standard
- Keystores can contain both encrypted and unencrypted private keys
- The `certAlias` must match the alias used when creating the keystore (e.g., "client" in the example above)
- If hostname verification is not desired (e.g., lab environments), set `validateHostnames=false`

### 4. Basic Authentication

For simple HTTP basic auth (less common). **Security requirement:** Basic authentication requires TLS by default to prevent credential exposure.

**With TLS (recommended):**
```properties
# Basic auth over TLS (secure)
druid.discovery.consul.auth.basicAuthUser=username
druid.discovery.consul.auth.basicAuthPassword=password
druid.discovery.consul.connection.sslClientConfig.trustStorePath=/path/to/truststore.p12
druid.discovery.consul.connection.sslClientConfig.trustStorePassword=truststore-password
```

**Without TLS (only for sidecar TLS termination):**
```properties
# Basic auth over HTTP (requires explicit opt-in)
druid.discovery.consul.auth.basicAuthUser=username
druid.discovery.consul.auth.basicAuthPassword=password
druid.discovery.consul.auth.allowBasicAuthOverHttp=true
```

**Important:** If you configure basic auth credentials without TLS and without setting `allowBasicAuthOverHttp=true`, the client will fail to start with an error message. This fail-fast behavior prevents accidental cleartext credential transmission. Only use `allowBasicAuthOverHttp=true` when you have sidecar TLS termination (e.g., Envoy, nginx) handling encryption before traffic reaches Consul.

### 5. Combined Authentication

You can combine methods for defense-in-depth. For example, TLS/mTLS + ACL token (recommended):

```properties
# TLS + ACL token
druid.discovery.consul.auth.aclToken=your-token
druid.discovery.consul.connection.sslClientConfig.trustStorePath=/path/to/truststore.p12
druid.discovery.consul.connection.sslClientConfig.trustStorePassword=truststore-password

# Optional: mTLS (client certificate)
druid.discovery.consul.connection.sslClientConfig.keyStorePath=/path/to/client-keystore.p12
druid.discovery.consul.connection.sslClientConfig.keyStorePassword=keystore-password
druid.discovery.consul.connection.sslClientConfig.certAlias=client
```

## Requirements

- Consul 1.0.0 or higher
- Network connectivity from all Druid nodes to Consul agent
- If using Consul ACL, appropriate ACL token with permissions to:
  - Register and deregister services
  - Read service catalog
  - Update health checks
- If using TLS/mTLS:
  - Valid certificates issued by trusted CA
  - Certificate files accessible to Druid processes
  - Consul configured to accept TLS connections

## Consul ACL Permissions

If Consul ACL is enabled, the ACL token must have the following permissions:

### For Service Discovery Only

```hcl
service "{servicePrefix}-" {
  policy = "write"
}

service_prefix "" {
  policy = "read"
}
```

### For Leader Election (Coordinator/Overlord)

In addition to the service permissions above, the Coordinator and Overlord nodes require KV and session permissions for leader election:

```hcl
# KV permissions for leader election locks
key_prefix "druid/leader/" {
  policy = "write"
}

# Session permissions for creating and managing Consul sessions
session_prefix "" {
  policy = "write"
}
```

### Complete ACL Policy Example

For a complete Druid deployment using Consul for both discovery and leader election:

```hcl
# Service discovery permissions (all nodes)
service "druid-prod-" {
  policy = "write"
}

service_prefix "" {
  policy = "read"
}

# Leader election permissions (Coordinator and Overlord only)
key_prefix "druid/leader/" {
  policy = "write"
}

session_prefix "" {
  policy = "write"
}
```

**Note:** You can create separate ACL tokens for different node types:
- Broker, Historical, MiddleManager: Only need service permissions
- Coordinator, Overlord: Need both service and leader election permissions

## Monitoring

### Consul Monitoring

Monitor the following in your Consul cluster:
- Service registrations for each Druid node role
- Health check status for all Druid services
- Consul agent connectivity
- Leader election locks in KV store (keys under `druid/leader/coordinator` and `druid/leader/overlord`)
- Session status for Coordinator and Overlord nodes

### Druid Logs

Check Druid logs for:
- `Successfully announced DiscoveryDruidNode` - Node registered successfully
- `Failed to announce` - Registration errors
- `Exception while watching for role` - Discovery errors
- `Created Consul session [%s] for leader election` - Leader election session created
- `Failed to renew session` - Session renewal failures (may indicate network issues)
- `Became leader` / `Lost leadership` - Leader election state changes

### Metrics and Observability

The extension emits lightweight Druid metrics when a ServiceEmitter is available:
- consul/announce/success|failure — node announce/unannounce outcomes
- consul/healthcheck/failure — TTL heartbeat update failures
- consul/watch/error|added|removed — discovery watch errors and changes
- consul/watch/lifecycle — watcher thread start/stop events
- consul/leader/become|stop|renew/fail — leader election transitions
- consul/leader/ownership_mismatch — promotion skipped due to session mismatch
- consul/leader/loop — leader election loop lifecycle
- consul/leader/giveup — leader loop exceeded retry budget

You should also monitor:
1. Consul's built-in metrics and health checks
2. Druid application logs
3. Consul UI for visualizing service health and registrations

**Recommended Alerts:**
- Alert when Druid services disappear from Consul catalog
- Alert when health checks fail for extended periods
- Alert on frequent leader election changes (indicates instability)
- Alert when Consul agent becomes unreachable from Druid nodes

### Prometheus Mapping (example)

If you use the Prometheus emitter, add these to your metrics config (dimension map) so our Consul metrics are exposed with useful labels:

```json
{
  "consul/announce/success": {
    "dimensions": ["role"],
    "type": "count",
    "help": "Druid Consul announce success"
  },
  "consul/announce/failure": {
    "dimensions": ["role"],
    "type": "count",
    "help": "Druid Consul announce failure"
  },
  "consul/healthcheck/failure": {
    "dimensions": [],
    "type": "count",
    "help": "Druid Consul TTL heartbeat failures"
  },
  "consul/watch/error": {
    "dimensions": ["role"],
    "type": "count",
    "help": "Druid Consul discovery watch errors"
  },
  "consul/watch/added": {
    "dimensions": ["role"],
    "type": "count",
    "help": "Druid nodes added by Consul watch"
  },
  "consul/watch/removed": {
    "dimensions": ["role"],
    "type": "count",
    "help": "Druid nodes removed by Consul watch"
  },
  "consul/watch/lifecycle": {
    "dimensions": ["role", "state"],
    "type": "count",
    "help": "Consul watch thread start/stop events"
  },
  "consul/leader/become": {
    "dimensions": ["lock"],
    "type": "count",
    "help": "Leader elected"
  },
  "consul/leader/stop": {
    "dimensions": ["lock"],
    "type": "count",
    "help": "Leadership lost/stopped"
  },
  "consul/leader/loop": {
    "dimensions": ["lock", "state"],
    "type": "count",
    "help": "Leader election loop lifecycle events"
  },
  "consul/leader/renew/fail": {
    "dimensions": ["lock"],
    "type": "count",
    "help": "Leader session renew failures"
  },
  "consul/leader/ownership_mismatch": {
    "dimensions": ["lock"],
    "type": "count",
    "help": "Attempts blocked because Consul lock session changed unexpectedly"
  },
  "consul/leader/giveup": {
    "dimensions": ["lock"],
    "type": "count",
    "help": "Leader election aborted after exceeding retry budget"
  }
}
```

Lifecycle metrics (`consul/watch/lifecycle`, `consul/leader/loop`) emit a `state` dimension (`start` or `stop`) so you can build gauges in Prometheus by subtracting stop counters from start counters.

In Prometheus, you can then alert on spikes, for example:

```promql
sum by (role) (rate(druid_consul_watch_error[5m])) > 0
sum by (lock) (rate(druid_consul_leader_renew_fail[5m])) > 0
```

Note: Names are normalized by the Prometheus emitter; `consul/watch/error` becomes `druid_consul_watch_error` with your configured namespace.

### Production Checklist

- Run Consul servers across AZs within a single DC; all Druid nodes talk to the local agent in that DC
- Use ACL tokens with least-privilege policies (separate tokens for leaders vs others if desired)
- Enable TLS/mTLS for Consul HTTP API; distribute CA truststore and optional client keystores
- Set healthCheckInterval and deregisterAfter appropriate to network latency (e.g., 30s/180s for high latency)
- Increase watchSeconds (e.g., 120s) in large clusters to reduce Consul load
- Ensure time sync (NTP) on all nodes; session TTLs depend on accurate clocks
- Consider adding service tags (AZ, tier, version) for observability and filtering

## Limitations

- All Druid nodes must be able to reach the Consul agent
- Service metadata size is limited by Consul's limits (typically 512KB)
- Leader election paths must be unique per cluster (configure via `coordinatorLeaderLockPath` and `overlordLeaderLockPath` if running multiple clusters)
- For TLS configuration, Java keystores (PKCS12 or JKS) are recommended over PEM files for better compatibility

## Implementation Details

### Current Approach: Service Registration

The extension uses Consul's **Service Catalog** with the following design:

- Each Druid node registers as a Consul service
- Service name format: `{servicePrefix}-{nodeRole}` (e.g., `druid-prod-broker`)
- Full `DiscoveryDruidNode` JSON stored in service metadata
- TTL-based health checks with automatic updates
- Blocking queries for efficient change detection

**Advantages:**
- Native Consul integration
- Visible in Consul UI
- Built-in health checking
- Standard Consul patterns

**Limitations:**
- Service metadata size limits (~512KB typically)
- Requires regular health check updates

### Alternative Approaches

Other valid implementation patterns that could be considered:

#### 1. Key-Value Store Approach
Store node information in Consul's KV store:
```
/druid/{cluster}/{role}/{host:port} = DiscoveryDruidNode JSON
```

- Use Consul sessions for ephemeral keys (auto-cleanup)
- Watch KV prefix for changes
- More ZooKeeper-like behavior
- No metadata size limits

#### 2. Hybrid Approach
Combine services for discovery + KV for detailed metadata:
- Register service with minimal info
- Store full details in KV store
- Best of both worlds, but more complex

#### 3. Service + Tags
Use extensive Consul service tags for filtering:
- Faster queries with tag-based filtering
- Limited metadata in service itself
- Scales better for very large clusters

The current implementation (Service Catalog) was chosen for its simplicity, native Consul integration, and alignment with Consul best practices.
