# Deployment and Improvements Summary

## 🎯 **What to Deploy Where**

### **Live Log Streaming Fix**

| Component | Need to Deploy? | Reason |
|-----------|----------------|---------|
| **Overlord** | ✅ YES | `streamLogs()` runs in Overlord process |
| **Docker Image (Pods)** | ❌ NO | Overlord fetches logs from Kubernetes API |

**Deployment:**
```bash
# Build
./apache-maven-3.9.11/bin/mvn \
  -pl extensions-contrib/kubernetes-overlord-extensions \
  -DskipTests -am clean package

# Deploy to Overlord only
scp extensions-contrib/kubernetes-overlord-extensions/target/*.jar \
    ubuntu@prodft30-overlord0:/tmp/
sudo cp /tmp/*.jar /opt/druid/extensions/druid-kubernetes-overlord-extensions/
sudo supervisorctl restart overlord
```

---

### **Task Reports Fix (PR #18379)**

| Component | Need to Deploy? | Reason |
|-----------|----------------|---------|
| **Overlord** | ✅ YES | Needs updated JARs for consistency |
| **Docker Image (Pods)** | ✅ **CRITICAL** | `AbstractTask.cleanup()` runs inside pods |

**Deployment:**
```bash
# 1. Deploy JARs to Overlord (already done)

# 2. Build Docker image with cherry-picked JARs
# See TASK_REPORTS_POD_FIX.md for complete instructions

# 3. Update pod templates to use new image
image: singular/druid:30.0.0-reports-fix
```

---

## 🐛 **OOM Error Visibility**

### **Current Problem**

When a task OOMs, you don't see the actual error because:

1. **Kubernetes kills the pod immediately** (no graceful shutdown)
2. **Logs might not be flushed** to stdout before termination
3. **saveLogs() might run too late** (pod already terminated)

### **Will Live Log Streaming Help?**

**Partially:**
- ✅ You can stream logs BEFORE the OOM happens
- ✅ You'll see memory pressure warnings if app logs them
- ❌ You won't see the OOM kill message (kernel kills too fast)

### **Better Solution: Check Kubernetes Events**

When a task fails unexpectedly:

```bash
# 1. Get the pod name
TASK_ID="query-abc123"
POD_NAME=$(kubectl get pods -l "druid.k8s.taskid=${TASK_ID}" -o name | head -1)

# 2. Check pod events (this shows OOM kills!)
kubectl describe ${POD_NAME}

# Look for:
# Events:
#   Type     Reason     Message
#   Warning  OOMKilled  Container main was OOMKilled
```

**Kubernetes preserves this info even after pod deletion!**

---

## 📊 **Relevant Upstream Commits**

I analyzed all commits since Druid 30.0.0. Here's what's relevant:

### **✅ Worth Cherry-Picking**

#### 1. **Commit `8850023811` - Fix stuck tasks (#17431)**

**What it fixes:**
- Better error handling in `saveLogs()` 
- Falls back to `getPeonLogs()` if `logWatch` is null
- Prevents stuck tasks when K8s communication fails

**Changes:**
```java
// Before: Could fail if logWatch is null
startWatchingLogs();
if (logWatch != null) {
  copyLogs...
}

// After: Always gets logs (with fallback)
final InputStream logStream = logWatch != null 
    ? logWatch.getOutput()
    : kubernetesClient.getPeonLogs(taskId).or(placeholder);
```

**Impact:** ⭐⭐⭐ **HIGH** - Improves error handling and log reliability

**Conflicts:** ⚠️ Your logs already have custom logging, might need merge

---

#### 2. **Commit `1b220b2504` - Idempotent shutdown (#18576)**

**What it fixes:**
- Makes `shutdown()` safe to call multiple times
- Prevents blocking on subsequent shutdown calls

**Changes:**
```java
// Uses AtomicBoolean to track shutdown state
private final AtomicBoolean isShutdown = new AtomicBoolean(false);

protected void shutdown() {
  if (isShutdown.compareAndSet(false, true)) {
    // Only run once
  }
}
```

**Impact:** ⭐⭐ **MEDIUM** - Prevents edge case bugs

**Conflicts:** ✅ Should merge cleanly

---

### **⚠️ Nice to Have (Lower Priority)**

#### 3. **Commit `6cf6838eb9` - Fix getTaskLocation (#16711)**

**What it fixes:**
- Adds exception handling to `getTaskLocation()`
- Prevents crashes when getting task location

**Impact:** ⭐ **LOW** - Minor stability improvement

---

### **❌ Not Needed**

#### 4. **Commit `5764183d4e` - Wait for RUNNING state (#17446)**
- **What:** Changed state management in `KubernetesTaskRunner.start()`
- **Why skip:** Major refactor, not critical for your use case

#### 5. **Commit `62a53ab41b` - Move k8s to core (#17614)**
- **What:** Moved extension from `contrib` to `core`
- **Why skip:** Just directory reorganization

---

## 🎯 **Recommended Cherry-Pick Plan**

### **Priority 1: Stuck Tasks Fix (#17431)**

```bash
cd /Users/ronshub/workspace/druid

# Cherry-pick the stuck tasks fix
git cherry-pick 8850023811

# If conflicts in saveLogs():
# 1. Keep your custom logging (📋 [LOGS] markers)
# 2. Take the improved error handling logic
# 3. Merge both approaches
```

**Expected conflicts:**
- `saveLogs()` method (you added logging, they changed logic)

**How to merge:**
- Keep your diagnostic logs
- Use their improved fallback logic
- Best of both worlds!

---

### **Priority 2: Idempotent Shutdown (#18576)**

```bash
# Cherry-pick idempotent shutdown
git cherry-pick 1b220b2504
```

**Expected conflicts:**
- Minimal - just adding `AtomicBoolean` field

---

## 🧪 **Testing After Cherry-Pick**

### **Test Scenario 1: OOM Task**

```bash
# Create a task that will OOM
curl -X POST http://broker:8082/druid/v2/sql/task \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "SELECT * FROM huge_table",
    "context": {
      "maxNumTasks": 1,
      "tags": {"userProvidedTag": "small"}  # Force small pod
    }
  }'

# Wait for OOM, then check:
# 1. Task status
curl "http://overlord:8090/druid/indexer/v1/task/${TASK_ID}/status"

# 2. Kubernetes events (shows OOM!)
kubectl describe pod <pod-name>

# 3. Logs (should be saved even if pod OOMed)
curl "http://overlord:8090/druid/indexer/v1/task/${TASK_ID}/log"
```

**Expected with cherry-picks:**
- ✅ Logs saved (even if pod crashed)
- ✅ Task not stuck
- ✅ Clean shutdown

---

### **Test Scenario 2: Network Failure During Log Save**

```bash
# Simulate K8s API failure:
# (Temporarily block Overlord → K8s API network)

# Expected with cherry-picks:
- ✅ saveLogs() falls back to getPeonLogs()
- ✅ Task completes (not stuck)
- ✅ Placeholder log saved if both fail
```

---

## 📋 **Complete Deployment Checklist**

### **Step 1: Cherry-Pick Improvements**

- [ ] Cherry-pick `8850023811` (stuck tasks fix)
- [ ] Resolve conflicts in `saveLogs()` (merge your logs + their logic)
- [ ] Cherry-pick `1b220b2504` (idempotent shutdown)
- [ ] Test builds successfully

### **Step 2: Build Updated Extension**

```bash
./apache-maven-3.9.11/bin/mvn \
  -pl extensions-contrib/kubernetes-overlord-extensions \
  -DskipTests -am clean package
```

### **Step 3: Deploy to Overlord**

```bash
# Copy JAR
scp extensions-contrib/kubernetes-overlord-extensions/target/druid-kubernetes-overlord-extensions-30.0.0.jar \
    ubuntu@prodft30-overlord0:/tmp/

# Install
sudo cp /tmp/druid-kubernetes-overlord-extensions-30.0.0.jar \
     /opt/druid/extensions/druid-kubernetes-overlord-extensions/
sudo supervisorctl restart overlord
```

### **Step 4: Build Docker Image (for Task Reports)**

```bash
# See TASK_REPORTS_POD_FIX.md for complete guide
docker build \
  --build-arg AWS_ACCESS_KEY_ID=... \
  --build-arg AWS_SECRET_ACCESS_KEY=... \
  -t singular/druid:30.0.0-complete .
```

### **Step 5: Update Pod Templates**

```yaml
# Update all pod templates
image: singular/druid:30.0.0-complete  # New image with all fixes
```

### **Step 6: Test Everything**

- [ ] Live log streaming works
- [ ] Task reports saved to S3
- [ ] OOM tasks don't get stuck
- [ ] Logs saved even on crash
- [ ] No errors in Overlord logs

---

## 🎓 **Understanding OOM Detection**

### **Where to Look for OOM Errors**

| Source | What You See | How to Access |
|--------|--------------|---------------|
| **Druid Task Logs** | Logs before OOM | `curl .../task/${ID}/log` |
| **Kubernetes Events** | `OOMKilled` message | `kubectl describe pod` |
| **Pod Status** | Exit code 137 | `kubectl get pod -o yaml` |
| **S3 Logs** | Complete logs before crash | S3 bucket |

### **Exit Code Reference**

```bash
kubectl get pod <pod-name> -o jsonpath='{.status.containerStatuses[0].lastState.terminated.exitCode}'
```

**Common exit codes:**
- `0` = Success
- `137` = OOM killed (128 + 9)
- `143` = SIGTERM (graceful shutdown)
- `1` = General error

---

## ✅ **Summary**

### **What You Need:**

1. **Deploy to Overlord:**
   - ✅ Live log streaming fix (already committed)
   - ✅ Comprehensive logging (already committed)
   - ⭐ Cherry-pick stuck tasks fix (#17431)
   - ⭐ Cherry-pick idempotent shutdown (#18576)

2. **Build Docker Image:**
   - ✅ Task reports fix (PR #18379)
   - ✅ All cherry-picked improvements

3. **For OOM Debugging:**
   - ✅ Use `kubectl describe pod` for OOM detection
   - ✅ Live log streaming shows logs before crash
   - ✅ S3 logs show complete history

---

**Created:** December 2024  
**Purpose:** Complete deployment and improvement guide  
**Status:** Ready for implementation

