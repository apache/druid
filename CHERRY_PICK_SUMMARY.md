# Cherry-Pick Summary - Upstream Improvements

## ✅ Successfully Applied

### 1. **Commit `da045624d3` - Fix stuck tasks (#17431)**

**Original commit:** `8850023811`  
**Author:** George Shiqi Wu  
**Date:** Nov 5, 2024

**What it fixes:**
- Improves error handling in `saveLogs()` method
- Adds fallback to `getPeonLogs()` if `logWatch` is null
- Prevents tasks from getting stuck when Kubernetes communication fails

**Changes made:**
```java
// Before: Could fail if logWatch was null
if (logWatch != null) {
  FileUtils.copyInputStreamToFile(logWatch.getOutput(), file.toFile());
} else {
  // Write placeholder message
}

// After: Always gets logs with fallback
final InputStream logStream = logWatch != null 
    ? logWatch.getOutput()
    : kubernetesClient.getPeonLogs(taskId).or(placeholderMessage);
FileUtils.copyInputStreamToFile(logStream, file.toFile());
```

**Merge strategy:**
- ✅ Kept our comprehensive diagnostic logging (📋 [LOGS] markers)
- ✅ Integrated their improved error handling logic
- ✅ Best of both approaches!

**Files modified:**
- `KubernetesPeonLifecycle.java` (merged with our logging)
- `KubernetesPeonClient.java` (auto-merged)
- Test files (auto-merged)

**Impact:** 🔥 **HIGH**
- Logs are saved even if pod crashes
- Tasks don't get stuck on K8s communication failures
- Better fallback mechanism

---

### 2. **Commit `087882a487` - Idempotent shutdown (#18576)**

**Original commit:** `1b220b2504`  
**Author:** Kashif Faraz  
**Date:** Sep 26, 2025

**What it fixes:**
- Makes `shutdown()` method idempotent (safe to call multiple times)
- Prevents blocking on subsequent shutdown calls
- Uses `AtomicBoolean` to track shutdown state

**Changes made:**
```java
// Before: Could be called multiple times
protected synchronized void shutdown() {
  if (this.kubernetesPeonLifecycle != null) {
    this.kubernetesPeonLifecycle.startWatchingLogs();
    this.kubernetesPeonLifecycle.shutdown();
  }
}

// After: Only runs once
private final AtomicBoolean isShutdown = new AtomicBoolean(false);

protected void shutdown() {
  if (isShutdown.compareAndSet(false, true)) {
    synchronized (this) {
      if (this.kubernetesPeonLifecycle != null) {
        this.kubernetesPeonLifecycle.startWatchingLogs();
        this.kubernetesPeonLifecycle.shutdown();
      }
    }
  }
}
```

**Merge strategy:**
- ✅ Added `AtomicBoolean isShutdown` field
- ✅ Kept our constructor signature (with `setKubernetesPeonLifecycle()`)
- ✅ Added idempotent logic with our null check

**Files modified:**
- `KubernetesWorkItem.java`

**Impact:** ⭐ **MEDIUM**
- Prevents edge case bugs with multiple shutdown calls
- Cleaner shutdown logic
- Thread-safe

---

## 📊 Combined Impact

### Before Cherry-Picks

**Potential issues:**
- ⚠️ Tasks could get stuck if K8s communication fails during log save
- ⚠️ Logs might not be saved if logWatch is null
- ⚠️ Shutdown could be called multiple times causing issues

### After Cherry-Picks

**Improvements:**
- ✅ Robust log saving with fallback mechanism
- ✅ Tasks won't get stuck on failures
- ✅ Idempotent shutdown (safe to call multiple times)
- ✅ All our diagnostic logging preserved!

---

## 🎓 Technical Details

### Conflicts Resolved

#### Conflict 1: `KubernetesPeonLifecycle.saveLogs()`

**Issue:** We added extensive logging, they changed the logic flow.

**Resolution:**
- Kept all our logging statements
- Adopted their `InputStream logStream` variable approach
- Integrated their `getPeonLogs()` fallback
- Result: Best of both worlds!

#### Conflict 2: `KubernetesWorkItem` constructor

**Issue:** Upstream refactored to require `kubernetesPeonLifecycle` in constructor, but our codebase still uses `setKubernetesPeonLifecycle()`.

**Resolution:**
- Kept our constructor signature (backward compatible)
- Added the `AtomicBoolean isShutdown` field
- Merged the idempotent logic with our null check
- Result: Idempotent shutdown that works with our architecture!

---

## 🧪 What to Test

### Test 1: Task Crashes During Log Save

```bash
# Simulate pod crash
kubectl delete pod <pod-name> --grace-period=0

# Expected:
- ✅ Logs still saved (via getPeonLogs fallback)
- ✅ Task not stuck
- ✅ Status correctly reported
```

### Test 2: Multiple Shutdown Calls

```bash
# Start task
# Call shutdown multiple times
# Expected:
- ✅ Only one actual shutdown occurs
- ✅ No blocking on subsequent calls
- ✅ No errors in logs
```

### Test 3: Live Logs with Fallback

```bash
# Start task
# Kill logWatch (simulate failure)
# Expected:
- ✅ Falls back to getPeonLogs()
- ✅ Logs still retrieved
- ✅ Proper logging shows fallback occurred
```

---

## 📋 Files Changed

| File | Lines Changed | Type |
|------|--------------|------|
| `KubernetesPeonLifecycle.java` | ~20 | Logic + Logging merge |
| `KubernetesPeonClient.java` | ~3 | Auto-merge |
| `KubernetesWorkItem.java` | ~12 | Logic merge |
| Test files | ~30 | Auto-merge |

**Total:** 4 source files, 3 test files modified

---

## 🎯 Next Steps

1. **Build the updated extension**
   ```bash
   ./apache-maven-3.9.11/bin/mvn \
     -pl extensions-contrib/kubernetes-overlord-extensions \
     -DskipTests -am clean package
   ```

2. **Deploy to Overlord**
   ```bash
   scp target/druid-kubernetes-overlord-extensions-30.0.0.jar \
       ubuntu@prodft30-overlord0:/tmp/
   
   # On Overlord:
   sudo cp /tmp/*.jar /opt/druid/extensions/druid-kubernetes-overlord-extensions/
   sudo supervisorctl restart overlord
   ```

3. **Test the improvements**
   - Submit test tasks
   - Verify logs are saved even on crashes
   - Check Overlord logs for fallback messages

4. **Build Docker image** (see `TASK_REPORTS_POD_FIX.md`)

---

## ✅ Cherry-Pick Complete!

**Status:** Both upstream improvements successfully integrated!

**What you gained:**
1. 🔥 Robust error handling for log saving
2. ⭐ Idempotent shutdown logic
3. 📊 All your diagnostic logging preserved
4. 🚀 Ready to build and deploy!

---

**Created:** December 2024  
**Branch:** `singular-druid-30-changes`  
**Commits:** `da045624d3`, `087882a487`

