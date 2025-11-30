# Live Log Streaming Fix for Kubernetes Tasks

## 🐛 Problem

When calling `GET /druid/indexer/v1/task/{taskId}/log` while a Kubernetes task is running, no logs are returned until the task completes.

## 🔍 Root Cause

The `KubernetesPeonLifecycle.streamLogs()` method was using the wrong Kubernetes API call:

**BEFORE (Bug):**
```java
protected Optional<InputStream> streamLogs()
{
  if (!State.RUNNING.equals(state.get())) {
    return Optional.absent();
  }
  return kubernetesClient.getPeonLogs(taskId);  // ❌ Returns snapshot only!
}
```

`getPeonLogs()` internally calls Kubernetes' `.getLogInputStream()` which returns a **static snapshot** of logs at that moment, not a continuous stream.

---

## ✅ Fix

Changed to use `getPeonLogWatcher()` which uses Kubernetes' `.watchLog()` API for live streaming:

**AFTER (Fixed):**
```java
protected Optional<InputStream> streamLogs()
{
  if (!State.RUNNING.equals(state.get())) {
    return Optional.absent();
  }
  
  // Use LogWatch for live streaming instead of getLogInputStream which returns a snapshot
  Optional<LogWatch> maybeLogWatch = kubernetesClient.getPeonLogWatcher(taskId);
  if (maybeLogWatch.isPresent()) {
    return Optional.of(maybeLogWatch.get().getOutput());
  }
  
  return Optional.absent();
}
```

---

## 📊 Comparison

| Method | Kubernetes API | Behavior | Use Case |
|--------|---------------|----------|----------|
| `getPeonLogs()` | `.getLogInputStream()` | ❌ Static snapshot | Not suitable for live streaming |
| `getPeonLogWatcher()` | `.watchLog()` | ✅ Continuous stream | Live log streaming |

---

## 🎯 Impact

**Before Fix:**
- API returns empty/incomplete logs during task execution
- Logs only appear after task completes
- Poor user experience for monitoring long-running tasks

**After Fix:**
- ✅ API streams logs in real-time as they're generated
- ✅ Users can monitor task progress live
- ✅ Works like Middle Manager tasks (HTTP-based streaming)

---

## 🧪 Testing

### 1. Submit a Long-Running Task

```bash
RESPONSE=$(curl -s -X POST http://prodft30-broker0.druid.singular.net:8082/druid/v2/sql/task \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "INSERT INTO test_live_logs SELECT __time, country FROM buffer_tracker_stats_1728 WHERE __time >= TIMESTAMP '\''2025-08-01'\'' LIMIT 10000 PARTITIONED BY ALL",
    "context": {
      "tags": {"userProvidedTag": "medium"},
      "maxNumTasks": 2
    }
  }')

TASK_ID=$(echo $RESPONSE | jq -r '.taskId')
echo "Task ID: $TASK_ID"
```

### 2. Stream Logs While Running

```bash
# Should show logs IMMEDIATELY as they're generated
curl "http://prodft30-overlord0.druid.singular.net:8090/druid/indexer/v1/task/${TASK_ID}/log"
```

### 3. Watch Logs Continuously

```bash
# Update every second
watch -n 1 "curl -s http://prodft30-overlord0.druid.singular.net:8090/druid/indexer/v1/task/${TASK_ID}/log | tail -20"
```

**Expected:** Logs appear and update in real-time during task execution.

---

## 🔧 Deployment

### Build

```bash
./apache-maven-3.9.11/bin/mvn \
  -pl extensions-contrib/kubernetes-overlord-extensions \
  -DskipTests \
  -am \
  clean package
```

### Deploy to Overlord

```bash
# Backup current extension
sudo cp -r /opt/druid/extensions/druid-kubernetes-overlord-extensions \
          /opt/druid-backups/k8s-ext-$(date +%Y%m%d_%H%M%S)

# Copy new JAR
scp extensions-contrib/kubernetes-overlord-extensions/target/druid-kubernetes-overlord-extensions-30.0.0.jar \
    ubuntu@prodft30-overlord0:/tmp/

# On Overlord server
sudo cp /tmp/druid-kubernetes-overlord-extensions-30.0.0.jar \
     /opt/druid/extensions/druid-kubernetes-overlord-extensions/
sudo chown ubuntu:ubuntu /opt/druid/extensions/druid-kubernetes-overlord-extensions/*.jar

# Restart Overlord
sudo supervisorctl restart overlord
```

---

## 📝 Related Code

### File Modified
- `extensions-contrib/kubernetes-overlord-extensions/src/main/java/org/apache/druid/k8s/overlord/KubernetesPeonLifecycle.java`

### Methods Involved
- `KubernetesPeonLifecycle.streamLogs()` - **Fixed**
- `KubernetesWorkItem.streamTaskLogs()` - Calls streamLogs()
- `KubernetesTaskRunner.streamTaskLog()` - API entry point
- `KubernetesPeonClient.getPeonLogWatcher()` - Returns LogWatch (live stream)
- `KubernetesPeonClient.getPeonLogs()` - Returns InputStream (snapshot) - **Not used anymore for live streaming**

---

## 🎓 Technical Details

### Kubernetes Log Streaming APIs

**Snapshot API:**
```java
InputStream logs = client.batch()
    .v1()
    .jobs()
    .inNamespace(namespace)
    .withName(jobName)
    .inContainer("main")
    .getLogInputStream();  // Returns all logs up to this moment
```

**Watch API:**
```java
LogWatch logWatch = client.batch()
    .v1()
    .jobs()
    .inNamespace(namespace)
    .withName(jobName)
    .inContainer("main")
    .watchLog();  // Continuously streams new log lines
```

The watch API keeps the connection open and streams new log lines as they're written to the container's stdout/stderr.

---

## ⚠️ Important Notes

1. **LogWatch Resource Management:** The `LogWatch` object opens a persistent connection to the Kubernetes API server. It's important that these are properly closed when the client disconnects.

2. **State Check:** The method still checks if the task state is `RUNNING` before attempting to stream. This prevents errors when trying to stream logs from tasks that haven't started yet.

3. **Backward Compatibility:** This fix doesn't change any APIs or interfaces - it only fixes the implementation to work as originally intended.

4. **S3 Final Logs:** This fix does NOT change how final logs are saved to S3. The `saveLogs()` method in the `finally` block still works the same way, creating a complete log file after task completion.

---

## 📚 References

- **Fabric8 Kubernetes Client:** https://github.com/fabric8io/kubernetes-client
- **LogWatch Javadoc:** Shows that `.watchLog()` returns a continuous stream
- **Original Issue:** Live log streaming not working during task execution

---

**Created:** December 2024  
**Status:** ✅ Fixed  
**Affects:** Druid 30.0.0 with Kubernetes overlord extension

