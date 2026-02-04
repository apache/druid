# Implementation Plan: Vertx HTTP Client & MSQ Polling Configuration

## Overview

This document outlines the detailed implementation plan for two changes to improve Overlord scalability:

1. **Vertx HTTP Client Backport** - Cherry-pick minimal Vertx support from Druid 35 PR #18540
2. **MSQ Polling Frequency Configuration** - Make polling intervals configurable (defaults unchanged)

---

## Change 1: Vertx HTTP Client Backport

### Goal

Enable Vertx as the HTTP client for Kubernetes API communication, providing non-blocking I/O that won't exhaust thread pools under high load.

---

### Source PR Reference (CRITICAL FOR FUTURE UPGRADE)

**Pull Request:** [apache/druid#18540](https://github.com/apache/druid/pull/18540)  
**Title:** "Add pluggable HTTP client for Kubernetes extension"  
**Merged:** Druid 35.0.0  
**Author:** Apache Druid contributors

#### PR #18540 Files We Are Backporting (Vertx-only subset)

| Druid 35 File | Our Backport File | Status |
|---------------|-------------------|--------|
| `extensions-core/kubernetes-overlord-extensions/src/main/java/org/apache/druid/k8s/overlord/common/httpclient/vertx/DruidKubernetesVertxHttpClientConfig.java` | `extensions-contrib/kubernetes-overlord-extensions/.../common/DruidKubernetesVertxHttpClientConfig.java` | **Backport** |
| `extensions-core/kubernetes-overlord-extensions/src/main/java/org/apache/druid/k8s/overlord/common/httpclient/vertx/DruidKubernetesVertxHttpClientFactory.java` | `extensions-contrib/kubernetes-overlord-extensions/.../common/DruidKubernetesVertxHttpClientFactory.java` | **Backport** |
| `extensions-core/kubernetes-overlord-extensions/src/main/java/org/apache/druid/k8s/overlord/common/httpclient/DruidKubernetesClientConfig.java` | Merged into `KubernetesTaskRunnerConfig.java` | **Simplified** |
| `extensions-core/kubernetes-overlord-extensions/src/main/java/org/apache/druid/k8s/overlord/KubernetesOverlordModule.java` | Same file path | **Modify** |

#### PR #18540 Files We Are NOT Backporting

| Druid 35 File | Reason |
|---------------|--------|
| `okhttp/DruidKubernetesOkHttpHttpClientConfig.java` | Not needed - Vertx only |
| `okhttp/DruidKubernetesOkHttpHttpClientFactory.java` | Not needed - Vertx only |
| `jdk/DruidKubernetesJdkHttpClientConfig.java` | Not needed - Vertx only |
| `jdk/DruidKubernetesJdkHttpClientFactory.java` | Not needed - Vertx only |
| PolyBind infrastructure for runtime switching | Simplified - we just use if/else |

#### Configuration Property Naming (MUST MATCH DRUID 35)

To ensure smooth upgrade path, our config properties should align with Druid 35 naming:

| Druid 35 Property | Our Backport Property | Match? |
|-------------------|----------------------|--------|
| `druid.indexer.runner.k8sAndWorker.http.httpClientType` | `druid.indexer.runner.httpClientType` | **Similar** (shortened) |
| `druid.indexer.runner.k8sAndWorker.http.vertx.workerPoolSize` | `druid.indexer.runner.vertxHttpClientConfig.workerPoolSize` | **Similar** (flattened) |
| `druid.indexer.runner.k8sAndWorker.http.vertx.eventLoopPoolSize` | `druid.indexer.runner.vertxHttpClientConfig.eventLoopPoolSize` | **Similar** (flattened) |

**Note:** Druid 35 uses `k8sAndWorker` prefix because the extension was moved from `extensions-contrib` to `extensions-core` and renamed. Our Druid 30 uses the older `extensions-contrib` location.

#### Upgrade Path When Moving to Druid 35

When you upgrade to Druid 35:

1. **Remove our custom code:** Delete our backported Vertx files
2. **Update config properties:** Change from our naming to Druid 35 naming:
   ```properties
   # Our Druid 30 backport config:
   druid.indexer.runner.httpClientType=VERTX
   druid.indexer.runner.vertxHttpClientConfig.workerPoolSize=40
   
   # Druid 35 native config (after upgrade):
   druid.indexer.runner.k8sAndWorker.http.httpClientType=vertx
   druid.indexer.runner.k8sAndWorker.http.vertx.workerPoolSize=40
   ```
3. **Benefit:** Same Vertx functionality, now natively supported

#### Key Code Differences: Our Backport vs Druid 35

| Aspect | Druid 35 (PR #18540) | Our Backport |
|--------|---------------------|--------------|
| HTTP client selection | PolyBind with runtime injection | Simple if/else in module |
| Supported clients | Vertx, OkHttp, JDK | Vertx only |
| Config location | Separate `DruidKubernetesClientConfig` | Inline in `KubernetesTaskRunnerConfig` |
| Package structure | `common/httpclient/vertx/` | `common/` (flat) |
| Extension location | `extensions-core` | `extensions-contrib` |

**Design Decision:** We're intentionally keeping the backport simpler than Druid 35's full implementation. This reduces code complexity while achieving the same result (Vertx for K8s API calls).

---

### Scope of Changes

| Component | Changes Required | Deployment Impact |
|-----------|------------------|-------------------|
| `druid-kubernetes-overlord-extensions` | Add Vertx dependencies + 3-4 new Java files | **Overlord only** |
| Core Druid | None | N/A |
| MSQ Extension | None | N/A |
| Worker pods | None | N/A |

**Key insight:** This change only affects the Overlord's Kubernetes client. Worker pods don't use this code path - they communicate with the Overlord, not directly with K8s API.

### Files to Create/Modify

#### New Files (4 files)

| File | Purpose | Lines (est.) |
|------|---------|--------------|
| `DruidKubernetesVertxHttpClientConfig.java` | Configuration properties for Vertx | ~60 |
| `DruidKubernetesVertxHttpClientFactory.java` | Creates Vertx-backed HTTP clients | ~80 |
| `VertxHttpClientBuilderHelper.java` | Helper to integrate with Fabric8 | ~40 |
| `HttpClientType.java` | Enum for client type selection | ~20 |

#### Modified Files (2 files)

| File | Changes |
|------|---------|
| `pom.xml` | Add Vertx dependencies |
| `KubernetesOverlordModule.java` | Bind Vertx factory, add client type selection |
| `KubernetesTaskRunnerConfig.java` | Add `httpClientType` config property |

### Detailed Tasks

#### Task 1.1: Add Maven Dependencies

**File:** `extensions-contrib/kubernetes-overlord-extensions/pom.xml`

**Add:**
```xml
<dependency>
  <groupId>io.fabric8</groupId>
  <artifactId>kubernetes-httpclient-vertx</artifactId>
  <version>6.7.2</version>
</dependency>
<dependency>
  <groupId>io.vertx</groupId>
  <artifactId>vertx-core</artifactId>
  <version>4.4.4</version>
</dependency>
```

**Verification:**
- [ ] Run `mvn dependency:tree` to verify no conflicts
- [ ] Verify Vertx version is compatible with Fabric8 6.7.2

**Subtasks:**
1. Check current Vertx transitive dependencies (if any)
2. Verify no version conflicts with existing dependencies
3. Add dependencies to pom.xml
4. Run `mvn compile` to verify dependency resolution

---

#### Task 1.2: Create HTTP Client Type Enum

**File:** `extensions-contrib/kubernetes-overlord-extensions/src/main/java/org/apache/druid/k8s/overlord/common/HttpClientType.java`

**Purpose:** Allow configuration-based selection of HTTP client backend.

```java
package org.apache.druid.k8s.overlord.common;

public enum HttpClientType
{
  VERTX,
  DEFAULT  // Uses Fabric8 default (OkHttp)
}
```

**Subtasks:**
1. Create enum class
2. Add DEFAULT option to maintain backward compatibility

---

#### Task 1.3: Create Vertx Configuration Class

**File:** `extensions-contrib/kubernetes-overlord-extensions/src/main/java/org/apache/druid/k8s/overlord/common/DruidKubernetesVertxHttpClientConfig.java`

**Purpose:** Hold Vertx-specific configuration options.

**Reference:** Druid 35's `DruidKubernetesVertxHttpClientConfig.java`

**Properties to include:**
```java
@JsonProperty
private int workerPoolSize = 20;  // VertxOptions.DEFAULT_WORKER_POOL_SIZE

@JsonProperty
private int eventLoopPoolSize = 0;  // 0 = use default (2 * cores)

@JsonProperty
private int internalBlockingPoolSize = 20;

@JsonProperty
private boolean useDaemonThread = true;
```

**Subtasks:**
1. Create config class with Jackson annotations
2. Add sensible defaults matching Druid 35
3. Add getters for all properties
4. Add `@JsonCreator` constructor if needed for immutability

---

#### Task 1.4: Create Vertx HTTP Client Factory

**File:** `extensions-contrib/kubernetes-overlord-extensions/src/main/java/org/apache/druid/k8s/overlord/common/DruidKubernetesVertxHttpClientFactory.java`

**Purpose:** Create Vertx-backed HTTP clients for Fabric8 Kubernetes client.

**Key implementation points:**
```java
public class DruidKubernetesVertxHttpClientFactory implements HttpClient.Factory
{
  private final Vertx vertx;
  private final DruidKubernetesVertxHttpClientConfig config;

  public DruidKubernetesVertxHttpClientFactory(DruidKubernetesVertxHttpClientConfig config)
  {
    this.config = config;
    VertxOptions options = new VertxOptions()
        .setWorkerPoolSize(config.getWorkerPoolSize())
        .setInternalBlockingPoolSize(config.getInternalBlockingPoolSize())
        .setUseDaemonThread(config.isUseDaemonThread());
    
    if (config.getEventLoopPoolSize() > 0) {
      options.setEventLoopPoolSize(config.getEventLoopPoolSize());
    }
    
    this.vertx = Vertx.vertx(options);
  }

  @Override
  public VertxHttpClientBuilder<HttpClientFactory> newBuilder(Config config)
  {
    return new VertxHttpClientBuilder<>(this, vertx, config);
  }
  
  // Implement close() to shut down Vertx instance
}
```

**Subtasks:**
1. Implement `HttpClient.Factory` interface from Fabric8
2. Create Vertx instance with configured options
3. Implement `newBuilder()` method
4. Implement `close()` for clean shutdown
5. Add logging for debugging

---

#### Task 1.5: Add Config Property to KubernetesTaskRunnerConfig

**File:** `extensions-contrib/kubernetes-overlord-extensions/src/main/java/org/apache/druid/k8s/overlord/KubernetesTaskRunnerConfig.java`

**Add:**
```java
@JsonProperty
private HttpClientType httpClientType = HttpClientType.DEFAULT;

@JsonProperty
@Nullable
private DruidKubernetesVertxHttpClientConfig vertxHttpClientConfig;

public HttpClientType getHttpClientType()
{
  return httpClientType;
}

public DruidKubernetesVertxHttpClientConfig getVertxHttpClientConfig()
{
  return vertxHttpClientConfig != null 
      ? vertxHttpClientConfig 
      : new DruidKubernetesVertxHttpClientConfig();
}
```

**Subtasks:**
1. Add `httpClientType` property with DEFAULT
2. Add nested `vertxHttpClientConfig` for Vertx-specific settings
3. Add getters
4. Verify Jackson deserialization works with nested config

---

#### Task 1.6: Modify KubernetesOverlordModule to Use Vertx

**File:** `extensions-contrib/kubernetes-overlord-extensions/src/main/java/org/apache/druid/k8s/overlord/KubernetesOverlordModule.java`

**Current code creates client like:**
```java
Config config = new ConfigBuilder().build();
client = new DruidKubernetesClient(config);
```

**Modified to:**
```java
Config config = new ConfigBuilder().build();

KubernetesClient kubernetesClient;
if (kubernetesTaskRunnerConfig.getHttpClientType() == HttpClientType.VERTX) {
  HttpClient.Factory httpClientFactory = new DruidKubernetesVertxHttpClientFactory(
      kubernetesTaskRunnerConfig.getVertxHttpClientConfig()
  );
  kubernetesClient = new KubernetesClientBuilder()
      .withConfig(config)
      .withHttpClientFactory(httpClientFactory)
      .build();
} else {
  kubernetesClient = new KubernetesClientBuilder()
      .withConfig(config)
      .build();
}
client = new DruidKubernetesClient(kubernetesClient);
```

**Subtasks:**
1. Import Vertx factory and config classes
2. Check `httpClientType` from config
3. Create appropriate HTTP client factory
4. Use `KubernetesClientBuilder.withHttpClientFactory()` for Vertx
5. Ensure backward compatibility when `httpClientType=DEFAULT`

---

#### Task 1.7: Update DruidKubernetesClient if Needed

**File:** `extensions-contrib/kubernetes-overlord-extensions/src/main/java/org/apache/druid/k8s/overlord/common/DruidKubernetesClient.java`

**Check if:**
- Constructor accepts `KubernetesClient` directly
- Need to add new constructor overload

**Subtasks:**
1. Review current constructors
2. Add constructor that accepts `KubernetesClient` if not present
3. Ensure `close()` method properly closes the underlying client

---

### Configuration Reference

After implementation, enable Vertx with:

```properties
# overlord/runtime.properties

# Enable Vertx HTTP client
druid.indexer.runner.httpClientType=VERTX

# Optional: Tune Vertx settings
druid.indexer.runner.vertxHttpClientConfig.workerPoolSize=40
druid.indexer.runner.vertxHttpClientConfig.eventLoopPoolSize=8
druid.indexer.runner.vertxHttpClientConfig.internalBlockingPoolSize=40
```

---

### Testing Plan for Vertx

#### Unit Tests

| Test | File | Purpose |
|------|------|---------|
| Config deserialization | `DruidKubernetesVertxHttpClientConfigTest.java` | Verify JSON config parsing |
| Factory creation | `DruidKubernetesVertxHttpClientFactoryTest.java` | Verify Vertx instance creation |
| Module binding | `KubernetesOverlordModuleTest.java` | Verify correct client type selection |

#### Integration Tests

| Test | Purpose | How to Run |
|------|---------|------------|
| Start Overlord with Vertx | Verify Overlord starts successfully | Local Docker/K8s |
| Run single MSQ task | Verify task execution works end-to-end | Dev cluster |
| Run 10 concurrent MSQ tasks | Verify basic concurrency | Dev cluster |
| Run 100 concurrent MSQ tasks | Stress test | Staging cluster |

#### Verification Steps

1. **Verify Vertx is being used:**
   - Check Overlord logs for Vertx initialization messages
   - Look for thread names containing "vertx" in thread dumps
   - Monitor with: `jstack <pid> | grep -i vertx`

2. **Verify thread pool behavior:**
   - Under load, thread count should remain low (~20-30) instead of thousands
   - Monitor: `jstack <pid> | grep -c "vert.x-worker"`

3. **Verify K8s API calls work:**
   - Tasks should launch successfully
   - Pod status updates should work
   - Log streaming should work

#### Rollback Plan

If issues occur:
1. Remove `druid.indexer.runner.httpClientType=VERTX` from config
2. Restart Overlord
3. System falls back to default OkHttp client

---

### Flow Verification: Where Kubernetes Client is Used

The Kubernetes client is used in these code paths (verify all work with Vertx):

| Class | Method | K8s Operation |
|-------|--------|---------------|
| `KubernetesTaskRunner` | `run()` | Create Job/Pod |
| `KubernetesTaskRunner` | `shutdown()` | Delete Pod |
| `KubernetesTaskRunner` | `getRunnerTaskState()` | Get Pod status |
| `KubernetesWorkItem` | `streamTaskLogs()` | Stream pod logs |
| `K8sTaskAdapter` | `toTask()` | Read pod annotations |
| `PodTemplateTaskAdapter` | `fromTask()` | Create pod spec |
| `JobStatusObserver` | `run()` | Watch job events |

**Test each of these operations with Vertx enabled.**

---

## Change 2: MSQ Polling Frequency Configuration

### Goal

Make MSQ worker status polling intervals configurable via query context, while keeping current defaults for backward compatibility.

### Scope of Changes

| Component | Changes Required | Deployment Impact |
|-----------|------------------|-------------------|
| `druid-multi-stage-query` (core extension) | Modify 3 Java files | **All MSQ-capable services** |
| Core Druid | None | N/A |
| Kubernetes Extension | None | N/A |

**Key insight:** MSQ code runs on:
- **Overlord** - ControllerImpl runs here for Overlord-based MSQ
- **Middle Manager / Worker pods** - If using indexer-based MSQ

For your setup (K8s-based MSQ), the controller runs on pods, not Overlord. You need to rebuild the MSQ extension and include it in your **worker pod images**.

### Deployment Requirements

| Your Setup | Component to Update |
|------------|---------------------|
| MSQ with K8s Task Runner | Worker pod image (contains MSQ extension) |
| Overlord (for receiving reduced polls) | Overlord image (optional, same build) |

**Recommendation:** Rebuild entire Druid distribution and update all images to same version.

### Files to Modify

| File | Changes | Lines Changed |
|------|---------|---------------|
| `MultiStageQueryContext.java` | Add context parameter definitions | ~30 |
| `MSQWorkerTaskLauncher.java` | Make polling intervals instance variables | ~20 |
| `ControllerImpl.java` | Read context and pass to launcher | ~10 |

### Detailed Tasks

#### Task 2.1: Add Context Parameters to MultiStageQueryContext

**File:** `extensions-core/multi-stage-query/src/main/java/org/apache/druid/msq/util/MultiStageQueryContext.java`

**Add:**
```java
// Context parameter names
public static final String CTX_WORKER_STATUS_POLL_INTERVAL_MS = "workerStatusPollIntervalMs";
public static final String CTX_WORKER_STATUS_POLL_INTERVAL_HIGH_MS = "workerStatusPollIntervalHighMs";

// Default values (match current hardcoded values for backward compatibility)
public static final long DEFAULT_WORKER_STATUS_POLL_INTERVAL_MS = 2000;
public static final long DEFAULT_WORKER_STATUS_POLL_INTERVAL_HIGH_MS = 100;

// Getter methods
public static long getWorkerStatusPollIntervalMs(final QueryContext queryContext)
{
  return queryContext.getLong(
      CTX_WORKER_STATUS_POLL_INTERVAL_MS,
      DEFAULT_WORKER_STATUS_POLL_INTERVAL_MS
  );
}

public static long getWorkerStatusPollIntervalHighMs(final QueryContext queryContext)
{
  return queryContext.getLong(
      CTX_WORKER_STATUS_POLL_INTERVAL_HIGH_MS,
      DEFAULT_WORKER_STATUS_POLL_INTERVAL_HIGH_MS
  );
}
```

**Subtasks:**
1. Add constant definitions
2. Add getter methods with defaults
3. Verify method signatures match other context getters in same file

---

#### Task 2.2: Make MSQWorkerTaskLauncher Use Configurable Intervals

**File:** `extensions-core/multi-stage-query/src/main/java/org/apache/druid/msq/indexing/MSQWorkerTaskLauncher.java`

**Current code:**
```java
private static final long HIGH_FREQUENCY_CHECK_MILLIS = 100;
private static final long LOW_FREQUENCY_CHECK_MILLIS = 2000;
```

**Change to:**
```java
// Keep static constants as defaults for backward compatibility
private static final long DEFAULT_HIGH_FREQUENCY_CHECK_MILLIS = 100;
private static final long DEFAULT_LOW_FREQUENCY_CHECK_MILLIS = 2000;

// Instance variables for actual values
private final long highFrequencyCheckMillis;
private final long lowFrequencyCheckMillis;
```

**Modify constructor:**
```java
public MSQWorkerTaskLauncher(
    // ... existing parameters ...,
    long highFrequencyCheckMillis,
    long lowFrequencyCheckMillis
)
{
  // ... existing initialization ...
  this.highFrequencyCheckMillis = highFrequencyCheckMillis;
  this.lowFrequencyCheckMillis = lowFrequencyCheckMillis;
}
```

**Update `computeSleepTime()`:**
```java
private long computeSleepTime(final long loopStartTime, final long loopDurationMillis)
{
  if (loopDurationMillis < highFrequencyCheckMillis) {
    // Use instance variable instead of static constant
    final long timeSinceStart = System.currentTimeMillis() - runStartTime;
    if (timeSinceStart < SWITCH_TO_LOW_FREQUENCY_CHECK_AFTER_MILLIS) {
      return highFrequencyCheckMillis - loopDurationMillis;
    }
  }
  return lowFrequencyCheckMillis - loopDurationMillis;
}
```

**Subtasks:**
1. Change static constants to instance variables
2. Add constructor parameters
3. Update `computeSleepTime()` to use instance variables
4. Keep static defaults for any code that might reference them (unlikely)
5. Review all usages of the constants

---

#### Task 2.3: Verify Constructor Call Sites

**Find all places that instantiate `MSQWorkerTaskLauncher`:**

```bash
grep -r "new MSQWorkerTaskLauncher" extensions-core/multi-stage-query/
```

**Expected location:** `ControllerImpl.java`

**Subtasks:**
1. Find all call sites
2. Update each to pass configured values

---

#### Task 2.4: Update ControllerImpl to Pass Configured Values

**File:** `extensions-core/multi-stage-query/src/main/java/org/apache/druid/msq/exec/ControllerImpl.java`

**Find where `MSQWorkerTaskLauncher` is created and update:**

```java
// Read from query context
final long highFrequencyPollMs = MultiStageQueryContext.getWorkerStatusPollIntervalHighMs(queryContext());
final long lowFrequencyPollMs = MultiStageQueryContext.getWorkerStatusPollIntervalMs(queryContext());

// Pass to launcher
this.workerTaskLauncher = new MSQWorkerTaskLauncher(
    // ... existing parameters ...,
    highFrequencyPollMs,
    lowFrequencyPollMs
);
```

**Subtasks:**
1. Import `MultiStageQueryContext` if not already
2. Get poll intervals from query context
3. Pass to `MSQWorkerTaskLauncher` constructor
4. Add logging at DEBUG level for configured values

---

#### Task 2.5: Check for Other Usages of Polling Constants

**Search for any other references:**

```bash
grep -r "HIGH_FREQUENCY_CHECK_MILLIS\|LOW_FREQUENCY_CHECK_MILLIS" extensions-core/multi-stage-query/
```

**Subtasks:**
1. Search for references
2. Update any that need access to configured values
3. If only used in `MSQWorkerTaskLauncher`, no additional changes needed

---

### Configuration Reference

After implementation, configure polling via:

**Option 1: Cluster-wide defaults (recommended)**
```properties
# common.runtime.properties
druid.query.default.context.workerStatusPollIntervalHighMs=2000
druid.query.default.context.workerStatusPollIntervalMs=10000
```

**Option 2: Per-query context**
```sql
INSERT INTO my_table
SELECT * FROM source
PARTITIONED BY DAY
CONTEXT {
  "workerStatusPollIntervalHighMs": 2000,
  "workerStatusPollIntervalMs": 10000
}
```

---

### Testing Plan for MSQ Polling

#### Unit Tests

| Test | File | Purpose |
|------|------|---------|
| Context parameter parsing | `MultiStageQueryContextTest.java` | Verify defaults and overrides |
| Launcher sleep time | `MSQWorkerTaskLauncherTest.java` | Verify configurable intervals |

#### Integration Tests

| Test | Purpose | What to Observe |
|------|---------|-----------------|
| Run MSQ with default config | Verify backward compatibility | Behavior unchanged |
| Run MSQ with custom intervals | Verify config is respected | Log messages showing configured values |
| Run MSQ with very high intervals | Verify still works | Slower startup detection, but works |

#### Verification Steps

1. **Verify config is being read:**
   - Add DEBUG logging in `ControllerImpl` when reading context
   - Check controller logs for: "Using workerStatusPollIntervalMs=X"

2. **Verify polling frequency changed:**
   - Monitor Overlord HTTP access logs
   - Count requests per minute to `/druid/indexer/v1/task/{id}/status`
   - With `workerStatusPollIntervalMs=10000`, expect ~6 requests/min per worker (vs 30 at default)

3. **Verify no functionality regression:**
   - Worker failures should still be detected (within configured interval)
   - Tasks should complete successfully
   - Stage transitions should work correctly

#### Compatibility Verification

| Scenario | Expected Behavior |
|----------|-------------------|
| Old workers, new controller | Works - context params passed to launcher |
| New workers, old controller | Works - defaults used when context missing |
| Mixed versions during rollout | Works - each controller uses its own launcher |

---

## Build & Deployment Plan

### Build Steps

```bash
# 1. Apply all code changes

# 2. Build the extensions
cd /Users/ronshub/workspace/druid
mvn clean package -pl extensions-contrib/kubernetes-overlord-extensions -am -DskipTests
mvn clean package -pl extensions-core/multi-stage-query -am -DskipTests

# 3. Run unit tests
mvn test -pl extensions-contrib/kubernetes-overlord-extensions
mvn test -pl extensions-core/multi-stage-query

# 4. Build full distribution (for integration testing)
mvn clean package -DskipTests -Pdist
```

### Deployment Steps

#### For Vertx (Overlord Only)

1. **Build:** Rebuild `druid-kubernetes-overlord-extensions` JAR
2. **Deploy:** Replace JAR in Overlord's extensions folder
3. **Config:** Add Vertx config to `overlord/runtime.properties`
4. **Restart:** Restart Overlord
5. **Verify:** Check logs for Vertx initialization

**Rollback:** Remove config, restart Overlord

#### For MSQ Polling (All MSQ Services)

1. **Build:** Rebuild `druid-multi-stage-query` JAR
2. **Deploy:** Replace JAR in:
   - Overlord extensions folder
   - Worker pod base image extensions folder
3. **Config:** Add polling config to `common.runtime.properties`
4. **Restart:** 
   - Restart Overlord
   - New worker pods will automatically use new image
5. **Verify:** Check controller logs for configured values

**Rollback:** Remove config (defaults will be used), no code rollback needed

---

## Deployment Matrix

| Change | Overlord JAR | Worker Pod Image | Config Change |
|--------|--------------|------------------|---------------|
| Vertx HTTP Client | Yes (`kubernetes-overlord-extensions`) | No | Yes (`overlord/runtime.properties`) |
| MSQ Polling | Yes (`multi-stage-query`) | Yes (`multi-stage-query`) | Yes (`common.runtime.properties`) |

### Recommended Deployment Order

1. **Phase 1: MSQ Polling** (lower risk)
   - Deploy with defaults (no behavior change)
   - Verify everything works
   - Gradually increase intervals

2. **Phase 2: Vertx** (after MSQ polling is stable)
   - Deploy to staging Overlord first
   - Run load tests
   - Deploy to production

---

## Task Checklist

### Vertx HTTP Client

- [ ] **1.1** Add Maven dependencies to pom.xml
- [ ] **1.2** Create `HttpClientType` enum
- [ ] **1.3** Create `DruidKubernetesVertxHttpClientConfig` class
- [ ] **1.4** Create `DruidKubernetesVertxHttpClientFactory` class
- [ ] **1.5** Add config properties to `KubernetesTaskRunnerConfig`
- [ ] **1.6** Modify `KubernetesOverlordModule` to use Vertx
- [ ] **1.7** Update `DruidKubernetesClient` if needed
- [ ] **1.8** Write unit tests
- [ ] **1.9** Test locally with minikube/kind
- [ ] **1.10** Test in staging with load

### MSQ Polling

- [ ] **2.1** Add context parameters to `MultiStageQueryContext`
- [ ] **2.2** Make `MSQWorkerTaskLauncher` use configurable intervals
- [ ] **2.3** Verify all constructor call sites
- [ ] **2.4** Update `ControllerImpl` to pass configured values
- [ ] **2.5** Check for other usages of polling constants
- [ ] **2.6** Write unit tests
- [ ] **2.7** Test with default values (verify no regression)
- [ ] **2.8** Test with custom values
- [ ] **2.9** Deploy to staging
- [ ] **2.10** Deploy to production

---

## Appendix A: Druid 35 PR #18540 Code Reference

### How to View the Original PR

```bash
# Option 1: View on GitHub
# https://github.com/apache/druid/pull/18540

# Option 2: Checkout Druid 35 locally to examine code
git stash
git fetch origin
git checkout druid-35.0.1
# ... examine files ...
git checkout singular-druid-30-changes
git stash pop
```

### Druid 35 Vertx Files (Full Paths)

These are the exact files from PR #18540 that we're basing our backport on:

#### 1. DruidKubernetesVertxHttpClientConfig.java

**Druid 35 Location:**
```
extensions-core/kubernetes-overlord-extensions/src/main/java/org/apache/druid/k8s/overlord/common/httpclient/vertx/DruidKubernetesVertxHttpClientConfig.java
```

**Key properties to replicate:**
```java
@JsonProperty
private int workerPoolSize = VertxOptions.DEFAULT_WORKER_POOL_SIZE;  // 20

@JsonProperty
private int eventLoopPoolSize = VertxOptions.DEFAULT_EVENT_LOOP_POOL_SIZE;  // 2 * cores

@JsonProperty
private int internalBlockingPoolSize = VertxOptions.DEFAULT_INTERNAL_BLOCKING_POOL_SIZE;  // 20
```

#### 2. DruidKubernetesVertxHttpClientFactory.java

**Druid 35 Location:**
```
extensions-core/kubernetes-overlord-extensions/src/main/java/org/apache/druid/k8s/overlord/common/httpclient/vertx/DruidKubernetesVertxHttpClientFactory.java
```

**Key implementation pattern:**
```java
public class DruidKubernetesVertxHttpClientFactory implements HttpClient.Factory
{
  private final Vertx vertx;

  public DruidKubernetesVertxHttpClientFactory(DruidKubernetesVertxHttpClientConfig config)
  {
    VertxOptions options = new VertxOptions()
        .setWorkerPoolSize(config.getWorkerPoolSize())
        .setEventLoopPoolSize(config.getEventLoopPoolSize())
        .setInternalBlockingPoolSize(config.getInternalBlockingPoolSize())
        .setUseDaemonThread(true);
    this.vertx = Vertx.vertx(options);
  }

  @Override
  public VertxHttpClientBuilder<HttpClientFactory> newBuilder(Config config)
  {
    return new VertxHttpClientBuilder<>(this, vertx, config);
  }

  @Override
  public void close()
  {
    vertx.close();
  }
}
```

#### 3. KubernetesOverlordModule.java Changes

**Druid 35 Location:**
```
extensions-core/kubernetes-overlord-extensions/src/main/java/org/apache/druid/k8s/overlord/KubernetesOverlordModule.java
```

**Key change pattern (Druid 35 uses PolyBind, we simplify to if/else):**

Druid 35:
```java
// Uses Guice PolyBind for runtime injection
PolyBind.optionBinder(binder, Key.get(HttpClient.Factory.class, K8sHttpClientBindingAnnotation.class))
    .addBinding(DruidKubernetesClientConfig.HttpClientType.VERTX.name())
    .to(DruidKubernetesVertxHttpClientFactory.class)
    .in(LazySingleton.class);
```

Our backport (simplified):
```java
// Simple if/else based on config
if (config.getHttpClientType() == HttpClientType.VERTX) {
  HttpClient.Factory factory = new DruidKubernetesVertxHttpClientFactory(config.getVertxConfig());
  return new KubernetesClientBuilder().withHttpClientFactory(factory).build();
} else {
  return new KubernetesClientBuilder().build();  // Default OkHttp
}
```

### Druid 35 File Tree (PR #18540 additions)

```
extensions-core/kubernetes-overlord-extensions/src/main/java/org/apache/druid/k8s/overlord/
├── KubernetesOverlordModule.java                           (MODIFIED)
├── common/
│   └── httpclient/
│       ├── DruidKubernetesClientConfig.java                (NEW - client type enum + config root)
│       ├── vertx/
│       │   ├── DruidKubernetesVertxHttpClientConfig.java   (NEW - we backport this)
│       │   └── DruidKubernetesVertxHttpClientFactory.java  (NEW - we backport this)
│       ├── okhttp/
│       │   ├── DruidKubernetesOkHttpHttpClientConfig.java  (NEW - we skip)
│       │   └── DruidKubernetesOkHttpHttpClientFactory.java (NEW - we skip)
│       └── jdk/
│           ├── DruidKubernetesJdkHttpClientConfig.java     (NEW - we skip)
│           └── DruidKubernetesJdkHttpClientFactory.java    (NEW - we skip)
```

---

## Appendix B: MSQ Polling - No Upstream PR Reference

### Current Status in Druid 35

The MSQ polling intervals remain **hardcoded in Druid 35.0.1**:

```java
// File: extensions-core/multi-stage-query/src/main/java/org/apache/druid/msq/indexing/MSQWorkerTaskLauncher.java
// Still hardcoded in Druid 35.0.1!
private static final long HIGH_FREQUENCY_CHECK_MILLIS = 100;
private static final long LOW_FREQUENCY_CHECK_MILLIS = 2000;
```

### Implications

- **No upstream PR to reference** - this is a custom enhancement
- **Will need to maintain** after Druid 35 upgrade
- **Consider contributing upstream** after validating in production

### Potential Upstream Contribution

If this works well, consider opening a PR to Apache Druid:
1. Title: "Make MSQ worker status polling intervals configurable"
2. Approach: Same as our implementation (context parameters)
3. Benefit: Reduces maintenance burden after contribution is merged

---

## Appendix C: Our Target Files (Druid 30)

### Vertx Backport Files

```
extensions-contrib/kubernetes-overlord-extensions/src/main/java/org/apache/druid/k8s/overlord/
├── KubernetesOverlordModule.java                    (MODIFY)
├── KubernetesTaskRunnerConfig.java                  (MODIFY)
└── common/
    ├── HttpClientType.java                          (CREATE - enum)
    ├── DruidKubernetesVertxHttpClientConfig.java    (CREATE - from Druid 35)
    └── DruidKubernetesVertxHttpClientFactory.java   (CREATE - from Druid 35)
```

### MSQ Polling Files

```
extensions-core/multi-stage-query/src/main/java/org/apache/druid/msq/
├── util/
│   └── MultiStageQueryContext.java                  (MODIFY)
├── indexing/
│   └── MSQWorkerTaskLauncher.java                   (MODIFY)
└── exec/
    └── ControllerImpl.java                          (MODIFY)
```

---

## Appendix D: Commands to Examine Druid 35 Code

Before implementing, run these commands to examine the exact Druid 35 code:

```bash
# Save current work
git stash

# Checkout Druid 35
git fetch origin druid-35.0.1
git checkout druid-35.0.1

# View the Vertx config class
cat extensions-core/kubernetes-overlord-extensions/src/main/java/org/apache/druid/k8s/overlord/common/httpclient/vertx/DruidKubernetesVertxHttpClientConfig.java

# View the Vertx factory class
cat extensions-core/kubernetes-overlord-extensions/src/main/java/org/apache/druid/k8s/overlord/common/httpclient/vertx/DruidKubernetesVertxHttpClientFactory.java

# View the module changes
cat extensions-core/kubernetes-overlord-extensions/src/main/java/org/apache/druid/k8s/overlord/KubernetesOverlordModule.java

# View MSQ polling code (still hardcoded in D35)
grep -n "FREQUENCY_CHECK_MILLIS" extensions-core/multi-stage-query/src/main/java/org/apache/druid/msq/indexing/MSQWorkerTaskLauncher.java

# Return to your branch
git checkout singular-druid-30-changes
git stash pop
```

---

## Appendix E: Upgrade Checklist (Druid 30 → 35)

When you eventually upgrade to Druid 35, follow this checklist:

### Vertx HTTP Client

- [ ] **Remove** our custom `DruidKubernetesVertxHttpClientConfig.java`
- [ ] **Remove** our custom `DruidKubernetesVertxHttpClientFactory.java`
- [ ] **Remove** our custom `HttpClientType.java`
- [ ] **Revert** changes to `KubernetesOverlordModule.java`
- [ ] **Revert** changes to `KubernetesTaskRunnerConfig.java`
- [ ] **Update config** from our naming to Druid 35 naming:
  ```properties
  # Old (our backport):
  druid.indexer.runner.httpClientType=VERTX
  druid.indexer.runner.vertxHttpClientConfig.workerPoolSize=40
  
  # New (Druid 35 native):
  druid.indexer.runner.k8sAndWorker.http.httpClientType=vertx
  druid.indexer.runner.k8sAndWorker.http.vertx.workerPoolSize=40
  ```

### MSQ Polling

- [ ] **Keep** our changes (not in Druid 35)
- [ ] **OR** contribute upstream and remove once merged

---

*Plan created: February 2026*  
*Source PR: [apache/druid#18540](https://github.com/apache/druid/pull/18540)*
