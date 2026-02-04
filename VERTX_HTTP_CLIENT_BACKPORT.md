# Vertx HTTP Client Backport Implementation

## Overview

This document describes the backport of Vertx HTTP client support from Druid 35 PR #18540 to Druid 30. This change enables non-blocking I/O for Kubernetes API calls, addressing thread pool exhaustion issues under high task concurrency.

**Goal:** Replace the default OkHttp client (blocking I/O) with Vertx (non-blocking I/O) for K8s API communication in the Overlord.

**Risk Level:** Medium - we're adding a new HTTP client option while keeping OkHttp available as fallback.

**Upstream Alignment:** This implementation was verified against the actual Druid 35 PR #18540 source code (commit `cabada6209`). The core Vertx integration matches upstream exactly, with additional logging added for debugging.

---

## Problem Statement

When running many concurrent MSQ tasks on Kubernetes, the Overlord can experience thread pool exhaustion because:
- OkHttp uses a thread-per-request model
- Each K8s API call (pod status checks, log streaming, etc.) blocks a thread
- With 50+ concurrent tasks, thread pools can become saturated

Vertx solves this by using event loops (non-blocking I/O), which can handle many concurrent connections with fewer threads.

---

## Changes Made

### New Files Created

| File | Location | Description |
|------|----------|-------------|
| `HttpClientType.java` | `common/` | Enum with `VERTX` (default) and `OKHTTP` options |
| `DruidKubernetesHttpClientFactory.java` | `common/` | Interface extending Fabric8's `HttpClient.Factory` |
| `DruidKubernetesVertxHttpClientConfig.java` | `common/httpclient/vertx/` | Configuration class for Vertx thread pools |
| `DruidKubernetesVertxHttpClientFactory.java` | `common/httpclient/vertx/` | Factory that creates Vertx-backed HTTP clients |

### Files Modified

| File | Changes |
|------|---------|
| `pom.xml` | Added `kubernetes-httpclient-vertx` dependency (v6.7.2) |
| `KubernetesTaskRunnerConfig.java` | Added `httpClientType` and `vertxHttpClientConfig` fields with getters |
| `KubernetesOverlordModule.java` | Updated `makeKubernetesClient()` to select HTTP client based on configuration |
| `DruidKubernetesClient.java` | Added new constructor accepting `DruidKubernetesHttpClientFactory` |
| `KubernetesWorkItem.java` | Fixed pre-existing indentation issue |
| `K8sTaskAdapter.java` | Fixed pre-existing bug - added `taskType` parameter to `buildJob()` |
| `MultiContainerTaskAdapter.java` | Updated `buildJob()` call to pass task type |
| `SingleContainerTaskAdapter.java` | Updated `buildJob()` call to pass task type |

---

## Configuration Reference

### Default Behavior (Vertx enabled by default)

No configuration needed - Vertx is the default HTTP client, matching Druid 35 behavior.

### Optional: Tune Vertx Thread Pools

Add to `overlord/runtime.properties`:

```properties
# Vertx thread pool settings (these are the defaults)
druid.indexer.runner.vertxHttpClientConfig.workerPoolSize=20
druid.indexer.runner.vertxHttpClientConfig.eventLoopPoolSize=0
druid.indexer.runner.vertxHttpClientConfig.internalBlockingPoolSize=20
```

**Pool Size Descriptions:**
- `workerPoolSize`: Thread pool for blocking operations (default: 20)
- `eventLoopPoolSize`: Event loop threads (default: 0 = 2 * CPU cores)
- `internalBlockingPoolSize`: Internal blocking pool (default: 20)

### Production Recommendation (High Concurrency)

For environments with 50+ concurrent tasks:

```properties
druid.indexer.runner.vertxHttpClientConfig.workerPoolSize=40
druid.indexer.runner.vertxHttpClientConfig.internalBlockingPoolSize=40
```

### Fallback to OkHttp

If you encounter issues with Vertx, fallback to OkHttp:

```properties
druid.indexer.runner.httpClientType=OKHTTP
```

---

## Logging

Extensive logging was added to help verify Vertx is being used and debug issues:

### Startup Logs (Vertx - Default)

```
INFO [main] KubernetesOverlordModule - Kubernetes HTTP client type configured: [VERTX]
INFO [main] KubernetesOverlordModule - Creating Kubernetes client with VERTX HTTP client - workerPoolSize=[20], eventLoopPoolSize=[0], internalBlockingPoolSize=[20]
INFO [main] DruidKubernetesVertxHttpClientFactory - Initializing Vertx HTTP client factory with config: DruidKubernetesVertxHttpClientConfig{workerPoolSize=20, eventLoopPoolSize=0, internalBlockingPoolSize=20}
INFO [main] DruidKubernetesVertxHttpClientFactory - Vertx instance created successfully with daemon threads
INFO [main] DruidKubernetesVertxHttpClientFactory - Vertx HTTP client initialized - workerPoolSize=[20], eventLoopPoolSize=[16], internalBlockingPoolSize=[20]
INFO [main] DruidKubernetesClient - Creating Kubernetes client with custom HTTP client factory: DruidKubernetesVertxHttpClientFactory
```

### Startup Logs (OkHttp Fallback)

```
INFO [main] KubernetesOverlordModule - Kubernetes HTTP client type configured: [OKHTTP]
INFO [main] KubernetesOverlordModule - Creating Kubernetes client with OKHTTP HTTP client
INFO [main] DruidKubernetesClient - Creating Kubernetes client with default HTTP client (OkHttp)
```

### Shutdown Logs

```
INFO [main] KubernetesOverlordModule - Stopping Vertx HTTP client factory
INFO [main] DruidKubernetesVertxHttpClientFactory - Closing Vertx HTTP client factory
INFO [main] DruidKubernetesVertxHttpClientFactory - Vertx instance closed
INFO [main] KubernetesOverlordModule - Stopping overlord Kubernetes client
```

### Runtime Verification

To verify Vertx is being used at runtime, check for Vertx threads in a thread dump:

```bash
# On the Overlord pod/container
jstack <overlord_pid> | grep -i "vert.x"

# Expected output with Vertx:
"vert.x-eventloop-thread-0" daemon prio=5 ...
"vert.x-eventloop-thread-1" daemon prio=5 ...
"vert.x-worker-thread-0" daemon prio=5 ...

# With OkHttp, you'd see:
"OkHttp ConnectionPool" daemon prio=5 ...
```

---

## Build Instructions

### Prerequisites

- Maven 3.9.x
- Java 8 or 11 (for compilation)

### Compile Only

```bash
cd /Users/ronshub/workspace/druid
./apache-maven-3.9.11/bin/mvn -pl extensions-contrib/kubernetes-overlord-extensions -am compile -DskipTests
```

### Full Package (with JAR)

```bash
cd /Users/ronshub/workspace/druid
./apache-maven-3.9.11/bin/mvn -pl extensions-contrib/kubernetes-overlord-extensions -am package -DskipTests
```

### Verify JAR Contains New Classes

```bash
jar tf extensions-contrib/kubernetes-overlord-extensions/target/druid-kubernetes-overlord-extensions-30.0.0.jar | grep -E "HttpClientType|DruidKubernetesHttpClientFactory|Vertx"
```

Expected output:
```
org/apache/druid/k8s/overlord/common/DruidKubernetesHttpClientFactory.class
org/apache/druid/k8s/overlord/common/HttpClientType.class
org/apache/druid/k8s/overlord/common/httpclient/vertx/DruidKubernetesVertxHttpClientConfig.class
org/apache/druid/k8s/overlord/common/httpclient/vertx/DruidKubernetesVertxHttpClientFactory.class
```

---

## Deployment Steps

### Step 1: Build the Extension JAR

```bash
cd /Users/ronshub/workspace/druid
./apache-maven-3.9.11/bin/mvn -pl extensions-contrib/kubernetes-overlord-extensions -am clean package -DskipTests
```

### Step 2: Copy JAR to Deployment

The built JAR is located at:
```
extensions-contrib/kubernetes-overlord-extensions/target/druid-kubernetes-overlord-extensions-30.0.0.jar
```

Copy this JAR to your Druid deployment's extensions directory, replacing the existing one.

### Step 3: Deploy to Staging/Test Environment

1. Deploy the updated JAR to the staging Overlord
2. Restart the Overlord service
3. Check Overlord logs for Vertx initialization messages (see Logging section above)

### Step 4: Verify Vertx is Active

1. Check Overlord startup logs for:
   ```
   Kubernetes HTTP client type configured: [VERTX]
   Creating Kubernetes client with VERTX HTTP client
   ```

2. Run a test MSQ task and verify it completes successfully

3. Optional: Take a thread dump and verify Vertx threads are present:
   ```bash
   jstack <pid> | grep "vert.x"
   ```

### Step 5: Load Testing

1. Run 50+ concurrent MSQ tasks
2. Monitor:
   - Thread count (should stay bounded with Vertx)
   - Task completion times
   - Overlord CPU/memory usage

### Step 6: Rollback Test

Before production deployment, verify the OkHttp fallback works:

1. Add to `overlord/runtime.properties`:
   ```properties
   druid.indexer.runner.httpClientType=OKHTTP
   ```
2. Restart Overlord
3. Verify logs show OkHttp is being used
4. Run a test task to ensure everything works

### Step 7: Production Deployment

1. Deploy to production Overlord(s)
2. Monitor for any issues
3. If problems occur, rollback by setting `httpClientType=OKHTTP`

---

## Using Existing Deploy Scripts

If you have existing deploy scripts (like `rons_deploy_to_prodft.sh`), ensure they:
1. Build the kubernetes-overlord-extensions module
2. Copy the JAR to the correct location
3. Restart the Overlord service

Example addition to deploy script:
```bash
# Build the kubernetes extension with Vertx support
./apache-maven-3.9.11/bin/mvn -pl extensions-contrib/kubernetes-overlord-extensions -am package -DskipTests

# Copy to deployment (adjust paths as needed)
cp extensions-contrib/kubernetes-overlord-extensions/target/druid-kubernetes-overlord-extensions-30.0.0.jar $DEPLOY_DIR/extensions/druid-kubernetes-overlord-extensions/
```

---

## Troubleshooting

### Issue: Vertx Not Initializing

**Symptom:** Logs show OkHttp instead of Vertx
**Check:**
1. Verify `httpClientType` is not set to `OKHTTP` in config
2. Check if `kubernetes-httpclient-vertx` dependency is in the classpath
3. Look for any Vertx initialization errors in logs

### Issue: Connection Timeouts

**Symptom:** K8s API calls timing out more frequently
**Solution:** Adjust timeout settings:
```properties
druid.indexer.runner.k8sApiRequestTimeoutMs=60000
druid.indexer.runner.k8sApiConnectionTimeoutMs=30000
```

### Issue: Thread Pool Exhaustion Still Occurring

**Symptom:** High thread count even with Vertx
**Solution:** Increase Vertx pool sizes:
```properties
druid.indexer.runner.vertxHttpClientConfig.workerPoolSize=40
druid.indexer.runner.vertxHttpClientConfig.internalBlockingPoolSize=40
```

### Issue: Need to Rollback

**Solution:** Add to config and restart:
```properties
druid.indexer.runner.httpClientType=OKHTTP
```

---

## Future Upgrade Path to Druid 35

When upgrading to Druid 35:

1. **Remove backported files:**
   - `common/HttpClientType.java`
   - `common/DruidKubernetesHttpClientFactory.java`
   - `common/httpclient/vertx/DruidKubernetesVertxHttpClientConfig.java`
   - `common/httpclient/vertx/DruidKubernetesVertxHttpClientFactory.java`

2. **Revert modified files** to upstream Druid 35 versions:
   - `DruidKubernetesClient.java`
   - `KubernetesOverlordModule.java`
   - `KubernetesTaskRunnerConfig.java`

3. **Update config property names:**
   ```properties
   # Our backport:
   druid.indexer.runner.httpClientType=VERTX

   # Druid 35 native:
   druid.indexer.runner.k8sAndWorker.http.httpClientType=vertx
   ```

---

## Comparison with Upstream Druid 35

This implementation was verified against Druid 35 PR #18540 (commit `cabada6209`).

### What Matches Upstream Exactly

| Component | Details |
|-----------|---------|
| `DruidKubernetesVertxHttpClientConfig` | Uses `VertxOptions.DEFAULT_*` constants for defaults |
| `DruidKubernetesVertxHttpClientFactory.createVertxInstance()` | Identical Vertx initialization logic |
| `DruidKubernetesHttpClientFactory` | Same interface extending `HttpClient.Factory` |
| VertxOptions settings | Same: daemon threads, disabled file caching, disabled classpath resolving |

### Differences from Upstream (Intentional)

| Aspect | Upstream Druid 35 | Our Backport | Reason |
|--------|-------------------|--------------|--------|
| Logging | None | Extensive | Helps debug and verify Vertx is working |
| `close()` method | None | Added | Ensures clean Vertx shutdown on lifecycle stop |
| Client selection | PolyBind | if/else | Simpler for backport with only 2 options |
| Config paths | `k8sAndWorker.http.*` | `indexer.runner.*` | Match existing Druid 30 patterns |
| JDK client option | Yes | No | Not needed for our use case |

### Upstream Files Reference

The upstream implementation in Druid 35 is located at:
- `extensions-core/kubernetes-overlord-extensions/src/main/java/org/apache/druid/k8s/overlord/common/httpclient/`

Note: In Druid 35, the kubernetes extension moved from `extensions-contrib` to `extensions-core`.

---

## Technical Details

### Why Vertx?

| Aspect | OkHttp (Blocking) | Vertx (Non-Blocking) |
|--------|-------------------|----------------------|
| Thread Model | Thread per request | Event loops |
| Scalability | Limited by thread pool | Can handle 1000s of connections |
| Memory | Higher (thread stacks) | Lower |
| Best For | Low concurrency | High concurrency |

### Fabric8 Compatibility

- Fabric8 6.7.2 (Druid 30's version) includes `kubernetes-httpclient-vertx` support
- No Fabric8 upgrade required
- Vertx integration is provided by Fabric8's `VertxHttpClientBuilder`

### Thread Safety

- `DruidKubernetesVertxHttpClientFactory` creates a single `Vertx` instance
- The `Vertx` instance is thread-safe and designed for sharing
- Daemon threads are used to prevent blocking JVM shutdown

---

## References

- [Druid PR #18540](https://github.com/apache/druid/pull/18540) - Original Vertx HTTP client implementation
- [Fabric8 Kubernetes Client](https://github.com/fabric8io/kubernetes-client)
- [Vertx Documentation](https://vertx.io/docs/)
