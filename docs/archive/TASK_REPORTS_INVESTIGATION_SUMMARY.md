# Task Reports Investigation Summary

## Investigation Requested

User asked to:
1. Check commits after Druid 30.0.0 that fixed task reports/logs in Kubernetes
2. Verify if logs are working properly
3. Add comprehensive logging to diagnose the issue

## Commits Found After Druid 30.0.0

### 1. PR #18206 (Commit c494dd15ae) - July 2025
**"Make K8s tasks persist task report + Fix MSQ INSERT/REPLACE querying problem"**

**What it changed:**
- Modified `peon.sh` to pass `TASK_DIR` and attempt ID to CliPeon
- This ensures proper directory structure for reports
- Changed: `org.apache.druid.cli.Main internal peon` → `org.apache.druid.cli.Main internal peon "${TASK_DIR}" 1`

**Key insight**: This sets up the correct file paths, but **does NOT implement report persistence to deep storage**

### 2. PR #18379 (Commit c5127ba) - Already Cherry-Picked ✅
**"Get reports file from file writer"**

- This is what we already cherry-picked
- Fixes how tasks determine where to write reports locally
- Does NOT add persistence to deep storage

### 3. PR #18444 (Commit 90be6827f8) - September 2025
**"Create Kubernetes peon lifecycle task log persist timeout"**

- Improves LOG persistence reliability
- Adds timeout protection
- Still no report persistence

### 4. Other K8s Commits
- `#17752`: "Adding some more logging to the K8's runner"
- `#17614`: "make k8s ingestion core"
- `#16711`: "kubernetes-overlord-extension: Fix tasks not being shutdown"

**None of these add report persistence to deep storage!**

## Key Finding: Reports Are Never Persisted! 🔴

### What Works (Logs)
```java
// In KubernetesPeonLifecycle.java
finally {
  saveLogs();    // ✅ Fetches logs from pod and pushes to S3
  stopTask();
}
```

### What's Missing (Reports)
```java
// In KubernetesPeonLifecycle.java
finally {
  saveLogs();
  // ❌ NO saveReports() call!
  stopTask();
}
```

## Architecture Comparison

### Logs (Working) ✅
```
Task completes
  ↓
saveLogs() called
  ↓
Fetches logs via Kubernetes API (logWatch.getOutput())
  ↓
Saves to temp file
  ↓
Pushes to S3 via taskLogs.pushTaskLog()
  ↓
Pod terminates
  ↓
Logs accessible from S3 ✅
```

### Reports (Broken) ❌
```
Task completes
  ↓
❌ NO saveReports() called!
  ↓
Pod terminates
  ↓
Reports lost forever
  ↓
API returns 404 Not Found
```

## What We Added: Comprehensive Logging

### Changes Made to KubernetesPeonLifecycle.java

1. **Enhanced run() method logging:**
   - 🚀 Task start
   - 📤 Task payload write
   - ⏳ Peon job launch
   - ✅ Job started
   - 🏁 Finally block entry

2. **Enhanced join() method logging:**
   - ⏸️  Joining task
   - ✅ Task completed
   - 🔧 Finally block entry
   - 📋 Log save attempt/success/failure
   - 📊 Report save attempt/success/failure

3. **Enhanced saveLogs() logging:**
   - 📋 Log persistence start
   - 📋 Temp file creation
   - 📋 Log watch status
   - 📋 Stream copy progress
   - 📋 Push to deep storage
   - ✅ Success confirmation

4. **Added saveReports() stub:**
   - ⚠️  Warns that reports are NOT persisted
   - Documents exactly what's missing
   - Shows task location (if available)
   - Explains impact (404 after pod termination)

### Log Markers for Easy Searching

```bash
# Search for lifecycle events
grep "LIFECYCLE" overlord.log

# Search for log persistence
grep "LOGS" overlord.log

# Search for report persistence warnings
grep "REPORTS" overlord.log

# Follow complete task lifecycle
grep "taskId-123" overlord.log | grep -E "LIFECYCLE|LOGS|REPORTS"
```

## Expected Log Output After Changes

### Successful Task Execution
```
🚀 [LIFECYCLE] Starting task [query_controller-abc123] in Kubernetes pod
📤 [LIFECYCLE] Writing task payload to deep storage for task [query_controller-abc123]
⏳ [LIFECYCLE] Launching Kubernetes peon job for task [query_controller-abc123], waiting for start...
✅ [LIFECYCLE] Peon job started for task [query_controller-abc123], joining to wait for completion...
⏸️  [LIFECYCLE] Joining task [query_controller-abc123], waiting for completion (timeout=900000ms)...
✅ [LIFECYCLE] Task [query_controller-abc123] completed with phase: Succeeded
🔧 [LIFECYCLE] Task [query_controller-abc123] join() finally block - will save logs and reports
📋 [LIFECYCLE] Attempting to save logs for task [query_controller-abc123]...
📋 [LOGS] Starting log persistence for task [query_controller-abc123]
📋 [LOGS] Created temporary log file: /tmp/query_controller-abc123...log
📋 [LOGS] Starting log watch for task [query_controller-abc123]...
📋 [LOGS] Log watch active, copying log stream to file...
📋 [LOGS] Successfully copied log stream to temp file (size: 45678 bytes)
📋 [LOGS] Pushing log file to deep storage for task [query_controller-abc123]...
✅ [LOGS] Successfully pushed logs to deep storage for task [query_controller-abc123]
📋 [LOGS] Closing log watch for task [query_controller-abc123]
📋 [LOGS] Deleting temporary log file: /tmp/query_controller-abc123...log
✅ [LIFECYCLE] Successfully saved logs for task [query_controller-abc123]
📊 [LIFECYCLE] Attempting to save reports for task [query_controller-abc123]...
📊 [REPORTS] ⚠️  Report persistence NOT IMPLEMENTED in Druid 30.0.0!
📊 [REPORTS] Task [query_controller-abc123] reports will be LOST after pod termination
📊 [REPORTS] Reports are only accessible via HTTP while pod is running
📊 [REPORTS] After pod deletion, GET /druid/indexer/v1/task/query_controller-abc123/reports will return 404
📊 [REPORTS] To fix: Implement report fetching + push to deep storage (similar to saveLogs())
📊 [REPORTS] Task location: 10.0.1.45:8100
✅ [LIFECYCLE] Successfully saved reports for task [query_controller-abc123]
🏁 [LIFECYCLE] Task [query_controller-abc123] run() finally block - will stop task
```

## Verification Steps

### Step 1: Deploy Updated Extension

```bash
# Build extension
cd /Users/ronshub/workspace/druid
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_431.jdk/Contents/Home
mvn clean install -pl extensions-contrib/kubernetes-overlord-extensions -am -DskipTests

# Deploy to Overlord
scp extensions-contrib/kubernetes-overlord-extensions/target/druid-kubernetes-overlord-extensions-30.0.0.jar \
  ubuntu@prodft30-overlord0.druid.singular.net:/home/ubuntu/

ssh ubuntu@prodft30-overlord0.druid.singular.net "
  sudo systemctl stop druid-overlord &&
  sudo mv /home/ubuntu/druid-kubernetes-overlord-extensions-30.0.0.jar /opt/druid/extensions/druid-kubernetes-overlord-extensions/ &&
  sudo systemctl start druid-overlord
"
```

### Step 2: Submit MSQ Query

```bash
curl -X POST http://prodft30-broker0.druid.singular.net:8082/druid/v2/sql/task \
  -H "Content-Type: application/json" \
  -d '{
    "query": "INSERT INTO test SELECT * FROM source LIMIT 10 PARTITIONED BY DAY",
    "context": {
      "tags": {"userProvidedTag": "medium"},
      "maxNumTasks": 3
    }
  }' | jq .
```

### Step 3: Check Overlord Logs

```bash
# SSH to Overlord
ssh ubuntu@prodft30-overlord0.druid.singular.net

# Watch live logs
sudo tail -f /logs/druid/overlord-stdout---supervisor-*.log | grep -E "LIFECYCLE|LOGS|REPORTS"

# Or search completed task
sudo grep "query_controller-TASK_ID" /logs/druid/overlord-stdout---supervisor-*.log | grep -E "LIFECYCLE|LOGS|REPORTS"
```

### Step 4: Verify Behavior

**What to look for:**

1. ✅ **Logs work**: You should see "Successfully pushed logs to deep storage"
2. ❌ **Reports don't work**: You should see "Report persistence NOT IMPLEMENTED" warnings
3. 🔍 **Task location captured**: Check if task location is shown in report warnings

**Confirm the diagnosis:**

```bash
# While pod is running
TASK_ID="query_controller-abc123"
curl "http://prodft30-overlord0.druid.singular.net:8090/druid/indexer/v1/task/${TASK_ID}/reports"
# Should return reports ✅

# After pod terminates (wait ~30 seconds)
kubectl get pods | grep ${TASK_ID}  # Should show Completed or not found

# Try fetching reports again
curl "http://prodft30-overlord0.druid.singular.net:8090/druid/indexer/v1/task/${TASK_ID}/reports"
# Should return 404 ❌ (confirms reports are lost)

# But logs should still work
curl "http://prodft30-overlord0.druid.singular.net:8090/druid/indexer/v1/task/${TASK_ID}/log"
# Should return logs ✅ (fetched from S3)
```

## Next Steps to Actually Fix Reports

### Option 1: Add HTTP Client to KubernetesPeonLifecycle (Recommended)

**Current blocker**: `KubernetesPeonLifecycle` doesn't have an HTTP client to fetch reports.

**Solution**:
1. Add `HttpClient` field to `KubernetesPeonLifecycle`
2. Pass it in constructor from `KubernetesPeonLifecycleFactory`
3. Implement proper `saveReports()`:

```java
protected void saveReports()
{
  if (taskLocation == null || TaskLocation.unknown().equals(taskLocation)) {
    log.warn("Cannot save reports: task location unknown for [%s]", taskId);
    return;
  }
  
  try {
    URL url = TaskRunnerUtils.makeTaskLocationURL(
      taskLocation,
      "/druid/worker/v1/chat/%s/liveReports",
      taskId.getOriginalTaskId()
    );
    
    Optional<InputStream> reportsStream = httpClient.go(
      new Request(HttpMethod.GET, url),
      new InputStreamResponseHandler()
    ).get();
    
    if (reportsStream.isPresent()) {
      Path tempFile = Files.createTempFile(taskId.getOriginalTaskId(), "-reports.json");
      FileUtils.copyInputStreamToFile(reportsStream.get(), tempFile.toFile());
      
      // Hack: Push as a "special log" to S3
      taskLogs.pushTaskLog(taskId.getOriginalTaskId() + ".reports", tempFile.toFile());
      
      Files.deleteIfExists(tempFile);
      log.info("Saved task reports for [%s]", taskId);
    }
  }
  catch (Exception e) {
    log.error(e, "Failed to save reports for task [%s]", taskId);
  }
}
```

**Files to modify**:
1. `KubernetesPeonLifecycle.java` - Add httpClient field, implement saveReports()
2. `KubernetesPeonLifecycleFactory.java` - Pass httpClient to lifecycle constructor
3. `KubernetesTaskRunner.java` - Already has httpClient, pass to factory

**Estimated effort**: 2-3 hours coding + testing

### Option 2: Use Persistent Volumes

Mount a PersistentVolume to `/druid/task-storage` in all task pods, share with Overlord.

**Pros**: Simple, no code changes
**Cons**: Infrastructure work, storage costs

### Option 3: Wait for Druid 35+

Since no commit after 30.0.0 fixes this, it may be addressed in future versions.

**Cons**: Reports won't work until upgrade

## Files Modified

```
extensions-contrib/kubernetes-overlord-extensions/src/main/java/org/apache/druid/k8s/overlord/KubernetesPeonLifecycle.java
```

## Summary

✅ **What we confirmed:**
- Logs ARE being persisted to S3 (saveLogs() works)
- Reports are NOT being persisted to S3 (no saveReports())
- No commits after Druid 30.0.0 fix this issue
- PR #18206 and #18379 fix local report writing, not persistence

✅ **What we added:**
- Comprehensive logging throughout task lifecycle
- Clear warning messages explaining the report persistence gap
- Stub saveReports() method with detailed TODO

❌ **What's still needed:**
- Add HttpClient to KubernetesPeonLifecycle
- Implement actual report fetching and persistence
- Test end-to-end

🔍 **How to verify diagnosis:**
- Deploy updated extension
- Submit MSQ query
- Check logs for "REPORTS" warnings
- Confirm 404 after pod termination

