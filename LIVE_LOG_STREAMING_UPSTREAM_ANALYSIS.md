# Live Log Streaming - Upstream Analysis

## 🔍 Investigation Summary

Searched for commits after Druid 30.0.0 (`druid-30.0.0..origin/master`) that might fix HTTP live log streaming for Kubernetes pods.

**Date:** Nov 30, 2025  
**Finding:** ❌ **NO upstream fix exists for HTTP live log streaming**

---

## 📋 Relevant Upstream Commits Found

### 1. **Commit 5764183d4e** (Nov 8, 2024)
```
k8s-based-ingestion: Wait for task lifecycles to enter RUNNING state before 
returning from KubernetesTaskRunner.start (#17446)
```

**What it does:**
- Adds `taskStartedSuccessfullyFuture` to wait for tasks to enter RUNNING state
- Prevents `start()` from returning until pod is actually running
- Improves task location reliability

**Does it fix HTTP live streaming?** ❌ **No**
- This helps with task readiness
- But doesn't fix the HTTP response blocking issue
- The problem is AFTER the task is RUNNING

**Potential benefit:**
- Might reduce the time between LogWatch creation and first log availability
- But still doesn't fix the core HTTP buffering problem

---

### 2. **Commit 8850023811** (Already Cherry-Picked)
```
Fix error where communication failures to k8s can lead to stuck tasks (#17431)
```

**What it does:**
- Improves error handling in `saveLogs()`
- Adds fallback to `getPeonLogs()` if `logWatch` is null

**Status:** ✅ **Already applied** to your codebase

---

### 3. **Commit f2a95fa673** (Jul 8, 2025)
```
Route task logs on Indexers to dedicated task log files (#18170)
```

**What it does:**
- Changes log4j2 configuration for task logging
- Routes logs to dedicated files on disk

**Relevance:** ⚠️ **Not applicable to Kubernetes pods**
- This is for MiddleManager/Indexer setups (disk-based logging)
- Kubernetes pods don't use local disk logging
- No impact on HTTP streaming

---

### 4. **Commit 62a53ab41b** (Not Yet Released)
```
make k8s ingestion core (#17614)
```

**What it does:**
- Moves kubernetes-overlord-extensions from contrib to core
- No functional changes to log streaming

---

## 🚨 Root Cause Still Exists

### **The Problem (Unchanged in Upstream)**

```java
// OverlordResource.java (or similar HTTP endpoint)
@GET
@Path("/task/{taskid}/log")
public Response getTaskLog(@PathParam("taskid") String taskId) {
    Optional<InputStream> stream = taskRunner.streamTaskLog(taskId, 0);
    
    if (stream.isPresent()) {
        // ⚠️ THIS is where blocking happens!
        // The HTTP response writer tries to read from stream
        // But the stream (LogWatch) blocks until pod starts logging
        // By the time it unblocks, task might already be done
        return Response.ok(stream.get()).build();
    }
    // ...
}
```

### **Why It Worked with MiddleManagers**

```
MiddleManager:
- Logs written to LOCAL FILE on disk
- File exists immediately when task starts
- InputStream reads from file → instant, non-blocking
- HTTP streams file content as it grows ✅

Kubernetes Pod:
- Logs stream via Kubernetes API (WebSocket)
- LogWatch InputStream wraps WebSocket
- First read() BLOCKS until pod starts producing logs
- HTTP response waits for first read
- By the time it succeeds, task is often done ❌
```

---

## 💡 Conclusions

1. ✅ **Your fix (using `.watchLog()`) IS the correct approach**
   - This is exactly what upstream Druid also does
   - It works for saving logs to S3 after completion
   - Evidence: `✅ [LOGS] Successfully pushed logs to deep storage`

2. ❌ **HTTP live streaming is NOT fixed upstream**
   - No commits address the HTTP response blocking issue
   - This is a known limitation of the Kubernetes integration
   - Druid's HTTP layer doesn't handle blocking InputStreams well

3. ⚠️ **Commit 5764183d4e might help slightly**
   - Makes tasks wait to enter RUNNING before start() returns
   - Reduces time between pod creation and first log availability
   - But doesn't fix the core HTTP streaming problem

---

## 🎯 Recommendations

### **Option 1: Accept Current Behavior (Easiest)**
**What works:**
- ✅ Logs saved to S3 after completion
- ✅ Task reports will work (with Docker image)
- ✅ Task status monitoring via API
- ✅ `kubectl logs -f` for live pod logs

**What doesn't work:**
- ❌ HTTP live log streaming via Druid API

**When to use:** If you can monitor tasks via `kubectl` or wait for completion

---

### **Option 2: Cherry-Pick 5764183d4e (Medium Effort)**
**Benefits:**
- Ensures pods are RUNNING before API calls
- Might reduce blocking time slightly
- Improves task location reliability

**Limitations:**
- Still won't fix HTTP live streaming completely
- Tasks that run quickly will still complete before streaming starts

**When to use:** If you want incremental improvements

---

### **Option 3: Fix HTTP Streaming Layer (Hard)**
**What's needed:**
- Modify HTTP endpoint to use async I/O
- Handle blocking InputStreams properly
- Possibly use chunked transfer encoding
- Requires deep understanding of Druid's HTTP layer

**Effort:** High (2-3 days of investigation + implementation)

**When to use:** Only if HTTP live streaming is absolutely critical

---

### **Option 4: Hybrid Approach (Medium-Hard)**
**Idea:**
- Have pods write logs to S3 in real-time (not just at end)
- HTTP endpoint reads from S3 instead of Kubernetes API
- More like how MiddleManagers worked

**Benefits:**
- Non-blocking reads
- Works with existing HTTP infrastructure

**Challenges:**
- Requires S3 write permissions for pods
- Need to handle log rotation/cleanup
- More complex than current approach

---

## 📊 Final Assessment

| Feature | Druid 30.0 Stock | Your Current Fix | With 5764183d4e |
|---------|------------------|------------------|-----------------|
| Logs to S3 after completion | ⚠️ Unreliable | ✅ Working | ✅ Working |
| Task reports (with Docker) | ❌ Broken | ✅ Will work | ✅ Will work |
| HTTP live log streaming | ❌ Broken | ❌ Still broken | ⚠️ Slightly better |
| Task status API | ✅ Working | ✅ Working | ✅ Working |
| `kubectl logs -f` | ✅ Working | ✅ Working | ✅ Working |

---

## 🚀 Next Steps

**Recommended path:**
1. ✅ Keep current live log streaming fix (using `.watchLog()`)
2. ✅ Deploy Docker image for task reports (critical!)
3. ⚠️ Optionally cherry-pick 5764183d4e for minor improvements
4. ✅ Use `kubectl logs -f` for live monitoring
5. ✅ Use Druid API for completed task logs

**Skip:**
- ❌ Trying to fix HTTP live streaming (not worth the effort given alternatives)

**Total effort saved:** ~3-5 days of complex HTTP layer debugging! 🎉

