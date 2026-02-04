# MSQ Worker Polling Configuration

## Overview

This document describes the MSQ worker status polling configuration feature, which allows tuning the frequency at which MSQ controllers poll the Overlord for worker task status updates.

**Goal:** Reduce Overlord HTTP request load when running many concurrent MSQ tasks.

**Risk Level:** Low - only affects polling frequency, not core functionality.

**Upstream Status:** Custom enhancement (not in upstream Druid as of 35.0.1).

---

## Problem Statement

When running many concurrent MSQ tasks (e.g., 100+ workers):
- Each MSQ controller polls the Overlord for worker task status
- Default high-frequency polling: 100ms (10 requests/sec/worker) for first 10 seconds after any worker starts
- Default low-frequency polling: 2000ms (0.5 requests/sec/worker) thereafter
- With 100 workers in low-frequency mode: ~50 requests/sec to Overlord
- Can contribute to Overlord HTTP thread exhaustion

---

## Configuration Reference

### Context Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `workerStatusPollIntervalMs` | Long | 2000 | Low-frequency polling interval in milliseconds (used after 10 seconds) |
| `workerStatusPollIntervalHighMs` | Long | 100 | High-frequency polling interval in milliseconds (used during first 10 seconds) |

**Note:** The 10-second threshold for switching from high to low frequency is hardcoded (`SWITCH_TO_LOW_FREQUENCY_CHECK_AFTER_MILLIS = 10000`).

### Cluster-Wide Defaults (Recommended)

Add to `common.runtime.properties` on all services that run MSQ controllers (typically worker pods):

```properties
# Reduce Overlord polling load for MSQ tasks
druid.query.default.context.workerStatusPollIntervalMs=5000
druid.query.default.context.workerStatusPollIntervalHighMs=500
```

### Per-Query Configuration

```sql
INSERT INTO my_table
SELECT * FROM source
PARTITIONED BY DAY
CONTEXT {
  "workerStatusPollIntervalMs": 5000,
  "workerStatusPollIntervalHighMs": 500
}
```

---

## Logging

### Startup Logs (INFO level - Always Visible)

When MSQ controller initializes, you will see these logs in the **worker pod** logs (where the MSQ controller runs):

```
INFO [task-runner-0-priority-0] ControllerImpl - Task[query-abc-123-worker0_0] configured worker polling intervals from context: highFrequencyPollIntervalMs=500, lowFrequencyPollIntervalMs=5000
INFO [task-runner-0-priority-0] MSQWorkerTaskLauncher - MSQ worker task launcher initialized for controller[query-abc-123-worker0_0]: highFrequencyPollMs=500, lowFrequencyPollMs=5000, switchToLowFreqAfterMs=10000 (CUSTOM CONFIG)
```

With default settings:
```
INFO [task-runner-0-priority-0] MSQWorkerTaskLauncher - MSQ worker task launcher initialized for controller[query-abc-123-worker0_0]: highFrequencyPollMs=100, lowFrequencyPollMs=2000, switchToLowFreqAfterMs=10000 (defaults)
```

### Runtime Logs (INFO level - Always Visible)

Mode transition and periodic statistics logging:

```
INFO [multi-stage-query-task-launcher] MSQWorkerTaskLauncher - Controller[query-abc-123-worker0_0] entering high-frequency polling mode: interval=500ms, duration=10000ms, trackedWorkers=1
INFO [multi-stage-query-task-launcher] MSQWorkerTaskLauncher - Controller[query-abc-123-worker0_0] switching to low-frequency polling mode: interval=5000ms, afterHighFreqPolls=20, trackedWorkers=5
INFO [multi-stage-query-task-launcher] MSQWorkerTaskLauncher - Controller[query-abc-123-worker0_0] polling stats: totalPolls=150, highFreqPolls=20, lowFreqPolls=130, activeWorkers=5, pendingWorkers=0, configuredIntervals=[high=500ms, low=5000ms]
```

### Shutdown Logs (INFO level)

When the task launcher stops:
```
INFO [multi-stage-query-task-launcher] MSQWorkerTaskLauncher - Controller[query-abc-123-worker0_0] task launcher stopping. Final polling stats: totalPolls=200, highFreqPolls=20, lowFreqPolls=180, trackedWorkers=5
```

---

## Verification Steps

### 1. Verify Configuration is Applied

Check worker pod logs for:
```
configured worker polling intervals from context: highFrequencyPollIntervalMs=XXX, lowFrequencyPollIntervalMs=XXX
```

### 2. Calculate Expected Load Reduction

| Workers | Default (2000ms) | Custom (5000ms) | Reduction |
|---------|------------------|-----------------|-----------|
| 50 | 25 req/sec | 10 req/sec | 60% |
| 100 | 50 req/sec | 20 req/sec | 60% |
| 200 | 100 req/sec | 40 req/sec | 60% |

### 3. Monitor Overlord HTTP Thread Pool

Before and after deployment, monitor:
- Overlord HTTP thread count
- `/druid/indexer/v1/task/{id}/status` request rate
- Overlord response times

### 4. Verify No Functionality Regression

- Worker failures should still be detected (within configured interval)
- Tasks should complete successfully
- Stage transitions should work correctly

---

## Recommended Settings by Environment

| Environment | Workers | High-Freq (ms) | Low-Freq (ms) | Rationale |
|-------------|---------|----------------|---------------|-----------|
| Development | <10 | 100 (default) | 2000 (default) | Fast feedback |
| Production (light) | 10-50 | 100 (default) | 2000 (default) | Default is fine |
| Production (medium) | 50-200 | 250 | 5000 | Reduce load by ~60% |
| Production (heavy) | 200+ | 500 | 10000 | Significant load reduction |

**Production Heavy Configuration:**
```properties
druid.query.default.context.workerStatusPollIntervalMs=10000
druid.query.default.context.workerStatusPollIntervalHighMs=500
```

---

## Trade-offs

| Setting | Advantage | Disadvantage |
|---------|-----------|--------------|
| Lower intervals (faster polling) | Faster failure detection, quicker stage transitions | Higher Overlord HTTP load |
| Higher intervals (slower polling) | Lower Overlord load, better scalability | Slower failure detection (up to interval + network latency) |

**Important:** The 10-second high-frequency window ensures startup failures are detected quickly, then switches to more conservative polling. This provides a good balance for most workloads.

---

## Architecture Notes

### Where the Controller Runs

The MSQ controller runs on the **worker pod** (not the Overlord). The polling configuration affects:
1. Worker pod → Overlord HTTP requests for task status
2. Each MSQ query spawns one controller
3. The controller polls status for all workers it manages

### Polling Flow

```
MSQ Query Submitted
    ↓
Controller starts on worker pod
    ↓
Controller creates MSQWorkerTaskLauncher
    ↓
Launcher polls Overlord for worker status:
    - First 10 seconds: highFrequencyPollIntervalMs (100ms default)
    - After 10 seconds: lowFrequencyPollIntervalMs (2000ms default)
    ↓
MSQ Query completes
```

---

## Files Changed

| File | Changes |
|------|---------|
| `extensions-core/multi-stage-query/src/main/java/org/apache/druid/msq/util/MultiStageQueryContext.java` | Context parameter definitions and getter methods |
| `extensions-core/multi-stage-query/src/main/java/org/apache/druid/msq/exec/ControllerImpl.java` | Reads context parameters, passes to launcher, INFO logging |
| `extensions-core/multi-stage-query/src/main/java/org/apache/druid/msq/indexing/MSQWorkerTaskLauncher.java` | Configurable intervals, statistics tracking, DEBUG logging |

---

## Deployment Requirements

### What to Deploy

Rebuild and deploy the `druid-multi-stage-query` extension JAR:
```bash
mvn -pl extensions-core/multi-stage-query -am clean package -DskipTests
```

The JAR is located at:
```
extensions-core/multi-stage-query/target/druid-multi-stage-query-30.0.0.jar
```

### Where to Deploy

| Component | Required | Reason |
|-----------|----------|--------|
| **Worker pods** | Yes | MSQ controllers run here |
| Overlord | Optional | Same build for consistency |
| Broker | Optional | Same build for consistency |

**Recommendation:** Rebuild the entire Druid distribution and update all images to the same version.

---

## Future Considerations

### Potential Enhancements (Not Implemented)

1. **Configurable high-frequency duration**: Currently hardcoded at 10 seconds
   ```
   workerStatusHighFreqDurationMs (default: 10000)
   ```

2. **Metrics integration**: Emit polling statistics as Druid metrics
   ```java
   emitter.emit("msq/worker/polling/count", totalPollCount);
   ```

3. **Adaptive polling**: Automatically adjust intervals based on Overlord response times

---

## Troubleshooting

### Issue: Configuration Not Applied

**Symptom:** Logs show default values instead of configured values

**Check:**
1. Verify configuration in `common.runtime.properties`:
   ```properties
   druid.query.default.context.workerStatusPollIntervalMs=5000
   ```
2. Ensure configuration is on the **worker pod** (not just Overlord)
3. Restart worker pods after configuration change

### Issue: Tasks Failing Due to Slow Status Detection

**Symptom:** Worker failures not detected quickly enough

**Solution:** Reduce polling intervals:
```properties
druid.query.default.context.workerStatusPollIntervalMs=2000
druid.query.default.context.workerStatusPollIntervalHighMs=100
```

### Issue: Overlord Still Overloaded

**Additional Steps:**
1. Check if Vertx HTTP client is enabled (see VERTX_HTTP_CLIENT_BACKPORT.md)
2. Increase polling intervals further
3. Scale Overlord horizontally (if supported)
4. Review other sources of Overlord load

---

## References

- [IMPLEMENTATION_PLAN_VERTX_AND_MSQ_POLLING.md](IMPLEMENTATION_PLAN_VERTX_AND_MSQ_POLLING.md) - Original implementation plan
- [VERTX_HTTP_CLIENT_BACKPORT.md](VERTX_HTTP_CLIENT_BACKPORT.md) - Vertx HTTP client documentation
- [Apache Druid MSQ Documentation](https://druid.apache.org/docs/latest/multi-stage-query/)
