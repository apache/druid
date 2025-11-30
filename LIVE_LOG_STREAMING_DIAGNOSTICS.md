# Live Log Streaming Diagnostics Guide

## 🔍 Overview

This guide explains the comprehensive logging added to diagnose the live log streaming flow for Kubernetes tasks.

---

## 📊 Complete Log Flow

When a client requests live logs via `GET /druid/indexer/v1/task/{taskId}/log`, the request flows through these layers:

```
1. API Entry (Overlord HTTP)
   ↓
2. KubernetesTaskRunner.streamTaskLog()
   ↓
3. KubernetesWorkItem.streamTaskLogs()
   ↓
4. KubernetesPeonLifecycle.streamLogs()
   ↓
5. KubernetesPeonClient.getPeonLogWatcher()
   ↓
6. Kubernetes API (.watchLog())
```

Each layer now has detailed logging with unique markers.

---

## 🏷️ Log Markers Reference

| Marker | Component | What It Logs |
|--------|-----------|--------------|
| `📺 [STREAM]` | KubernetesTaskRunner | API entry point, work item lookup |
| `📺 [WORKITEM]` | KubernetesWorkItem | Lifecycle delegation |
| `📺 [LIFECYCLE]` | KubernetesPeonLifecycle | State checks, stream creation |
| `📺 [K8S-CLIENT]` | KubernetesPeonClient | Kubernetes API calls |
| `📊 [LIFECYCLE]` | KubernetesPeonLifecycle | State transitions |
| `✅` | All | Success messages |
| `⚠️` | All | Warnings (non-fatal issues) |
| `❌` | All | Errors |

---

## 🎯 Successful Log Flow Example

**When everything works correctly:**

```
# 1. API Entry
📺 [STREAM] API request to stream logs for task [query-abc123] (offset=0)
📺 [STREAM] Found work item for task [query-abc123], delegating to streamTaskLogs()

# 2. Work Item Layer
📺 [WORKITEM] streamTaskLogs() called for task [query-abc123]
📺 [WORKITEM] Peon lifecycle exists for task [query-abc123], delegating to streamLogs()

# 3. Lifecycle Layer
📺 [LIFECYCLE] streamLogs() called for task [query-abc123]
📺 [LIFECYCLE] Current task state: RUNNING
📺 [LIFECYCLE] Task [query-abc123] is RUNNING, requesting LogWatch from Kubernetes client

# 4. K8s Client Layer
📺 [K8S-CLIENT] getPeonLogWatcher() called for task [query-abc123] (K8s job: query-abc123)
📺 [K8S-CLIENT] Namespace: default, Container: main
📺 [K8S-CLIENT] Calling Kubernetes API .watchLog() for job [query-abc123]
✅ [K8S-CLIENT] Successfully created LogWatch for job [query-abc123]

# 5. Success Confirmation
✅ [LIFECYCLE] Successfully obtained LogWatch for task [query-abc123], returning output stream
✅ [WORKITEM] Peon lifecycle returned log stream for task [query-abc123]
✅ [STREAM] Successfully obtained log stream for task [query-abc123]
```

**Result:** Client receives live streaming logs! ✅

---

## ⚠️ Failure Scenarios & Diagnostics

### Scenario 1: Task Not Found

**Logs:**
```
📺 [STREAM] API request to stream logs for task [query-unknown] (offset=0)
⚠️  [STREAM] No work item found for task [query-unknown] - task may not exist
```

**Cause:** Task ID is invalid or task hasn't been submitted to this runner.

**Fix:** Verify task ID is correct and task was submitted to this Overlord.

---

### Scenario 2: Task Not Started Yet

**Logs:**
```
📺 [STREAM] API request to stream logs for task [query-abc123] (offset=0)
📺 [STREAM] Found work item for task [query-abc123], delegating to streamTaskLogs()
📺 [WORKITEM] streamTaskLogs() called for task [query-abc123]
⚠️  [WORKITEM] No peon lifecycle available for task [query-abc123] - task may not have started yet
```

**Cause:** Task is queued but hasn't started running yet.

**Fix:** Wait a few seconds and try again. Check task status with:
```bash
curl "http://overlord:8090/druid/indexer/v1/task/${TASK_ID}/status"
```

---

### Scenario 3: Task Not in RUNNING State

**Logs:**
```
📺 [LIFECYCLE] streamLogs() called for task [query-abc123]
📺 [LIFECYCLE] Current task state: PENDING
⚠️  [LIFECYCLE] Task [query-abc123] is not in RUNNING state (state=PENDING), cannot stream logs
```

**Cause:** Task hasn't transitioned to RUNNING state yet.

**What to check:**
1. Is the pod starting up?
   ```bash
   kubectl get pods -l job-name=query-abc123
   ```

2. Look for state transition logs:
   ```bash
   grep "Transitioning task.*RUNNING" /logs/druid/overlord-*.log
   ```

**Expected state transition log:**
```
🔄 [LIFECYCLE] Transitioning task [query-abc123] to RUNNING state
✅ [LIFECYCLE] Task [query-abc123] now in RUNNING state - logs should be streamable
```

---

### Scenario 4: Kubernetes API Failure

**Logs:**
```
📺 [K8S-CLIENT] getPeonLogWatcher() called for task [query-abc123]
📺 [K8S-CLIENT] Namespace: default, Container: main
📺 [K8S-CLIENT] Calling Kubernetes API .watchLog() for job [query-abc123]
❌ [K8S-CLIENT] Error watching logs from task [query-abc123] (job: query-abc123): Pod not found
```

**Possible causes:**
1. Pod was terminated too quickly
2. Kubernetes API server is unreachable
3. Namespace or job name is incorrect
4. Container name is wrong

**Debugging:**
```bash
# Check if pod exists
kubectl get pods -l job-name=query-abc123 -n default

# Check job status
kubectl get job query-abc123 -n default -o yaml

# Check container name (should be "main")
kubectl get pod <pod-name> -o jsonpath='{.spec.containers[*].name}'
```

---

### Scenario 5: LogWatch is Null

**Logs:**
```
📺 [K8S-CLIENT] Calling Kubernetes API .watchLog() for job [query-abc123]
⚠️  [K8S-CLIENT] Kubernetes API returned null LogWatch for job [query-abc123]
```

**Cause:** Kubernetes client library returned null (unusual but possible).

**What to check:**
1. Kubernetes version compatibility
2. fabric8 client library version
3. Network issues between Overlord and K8s API server

---

## 🧪 Testing Script

Use this script to test the complete flow:

```bash
#!/bin/bash
# test_live_logs_with_diagnostics.sh

echo "🚀 Submitting test task..."
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
echo "✅ Task submitted: $TASK_ID"
echo ""

echo "⏳ Waiting 5 seconds for task to start..."
sleep 5
echo ""

echo "📊 Checking task status..."
curl -s "http://prodft30-overlord0.druid.singular.net:8090/druid/indexer/v1/task/${TASK_ID}/status" | jq .status
echo ""

echo "🔍 Checking diagnostic logs in Overlord..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
ssh ubuntu@prodft30-overlord0 "sudo grep '${TASK_ID}' /logs/druid/overlord-stdout---supervisor-*.log | grep -E '📺|📊|✅|⚠️|❌' | tail -30"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

echo "📺 Attempting to stream logs..."
curl -s "http://prodft30-overlord0.druid.singular.net:8090/druid/indexer/v1/task/${TASK_ID}/log" | head -20
echo ""

echo "🔍 Final diagnostic check..."
ssh ubuntu@prodft30-overlord0 "sudo grep '${TASK_ID}' /logs/druid/overlord-stdout---supervisor-*.log | grep -E '📺|📊|✅|⚠️|❌' | tail -10"
```

---

## 📋 Diagnostic Checklist

When logs don't stream, check in this order:

### ✅ Step 1: Verify Task Exists
```bash
# Look for this log
grep "📺 \[STREAM\] API request to stream logs" /logs/druid/overlord-*.log | grep ${TASK_ID}
```

**Expected:** Should see the API request logged.

**If missing:** Task might not exist or request didn't reach Overlord.

---

### ✅ Step 2: Verify Work Item Exists
```bash
# Look for this log
grep "📺 \[STREAM\] Found work item" /logs/druid/overlord-*.log | grep ${TASK_ID}
```

**Expected:** Should see "Found work item for task [...]"

**If see "No work item found":** Task hasn't been submitted to this runner.

---

### ✅ Step 3: Verify Peon Lifecycle Exists
```bash
# Look for this log
grep "📺 \[WORKITEM\] Peon lifecycle exists" /logs/druid/overlord-*.log | grep ${TASK_ID}
```

**Expected:** Should see "Peon lifecycle exists for task [...]"

**If see "No peon lifecycle available":** Task hasn't started yet. Wait and retry.

---

### ✅ Step 4: Verify Task is RUNNING
```bash
# Look for state transition
grep "✅ \[LIFECYCLE\] Task.*now in RUNNING state" /logs/druid/overlord-*.log | grep ${TASK_ID}
```

**Expected:** Should see state transition to RUNNING.

**If task is PENDING:** Wait for pod to start.

**If task is STOPPED:** Task already finished.

---

### ✅ Step 5: Verify Kubernetes API Call
```bash
# Look for K8s client logs
grep "📺 \[K8S-CLIENT\] getPeonLogWatcher" /logs/druid/overlord-*.log | grep ${TASK_ID}
```

**Expected:** Should see Kubernetes API call.

**Check for:**
- ✅ "Successfully created LogWatch" = Working!
- ⚠️ "returned null LogWatch" = K8s issue
- ❌ "Error watching logs" = Check exception details

---

## 🎓 Understanding State Flow

### Task Lifecycle States

```
NOT_STARTED → PENDING → RUNNING → STOPPED
     ↓           ↓         ↓         ↓
  Can't stream Can't stream ✅ CAN STREAM Can't stream
```

**State transitions logged:**
```
📊 [LIFECYCLE] Current state before join: PENDING
🔄 [LIFECYCLE] Transitioning task [query-abc123] to RUNNING state
✅ [LIFECYCLE] Task [query-abc123] now in RUNNING state - logs should be streamable
```

**Key insight:** Logs can ONLY be streamed when state is RUNNING.

---

## 🔧 Quick Debugging Commands

### Check All Streaming Attempts
```bash
sudo grep "📺 \[STREAM\] API request" /logs/druid/overlord-*.log | tail -20
```

### Check Recent Successes
```bash
sudo grep "✅ \[STREAM\] Successfully obtained log stream" /logs/druid/overlord-*.log | tail -10
```

### Check Recent Failures
```bash
sudo grep "⚠️  \[STREAM\]\|❌ \[K8S-CLIENT\]" /logs/druid/overlord-*.log | tail -20
```

### Full Trace for a Specific Task
```bash
TASK_ID="query-abc123"
sudo grep "${TASK_ID}" /logs/druid/overlord-*.log | grep -E "📺|📊|✅|⚠️|❌"
```

---

## 📈 Performance Monitoring

### Count Streaming Requests
```bash
# Total requests
sudo grep "📺 \[STREAM\] API request" /logs/druid/overlord-*.log | wc -l

# Successful streams
sudo grep "✅ \[STREAM\] Successfully obtained" /logs/druid/overlord-*.log | wc -l

# Failed streams
sudo grep "⚠️  \[STREAM\] No log stream available" /logs/druid/overlord-*.log | wc -l
```

### Calculate Success Rate
```bash
TOTAL=$(sudo grep "📺 \[STREAM\] API request" /logs/druid/overlord-*.log | wc -l)
SUCCESS=$(sudo grep "✅ \[STREAM\] Successfully obtained" /logs/druid/overlord-*.log | wc -l)
echo "Success rate: $((SUCCESS * 100 / TOTAL))%"
```

---

## 🎯 Expected Behavior After Fix

### Before Fix (Buggy)
- ⚠️ No logs during execution
- ✅ Logs only after completion

### After Fix (Working)
- ✅ Logs stream live during execution
- ✅ Logs continue after completion (from S3)

---

## 📝 Summary

**Log Flow Markers:**
1. `📺 [STREAM]` - Entry point
2. `📺 [WORKITEM]` - Work item layer
3. `📺 [LIFECYCLE]` - Lifecycle state checks
4. `📺 [K8S-CLIENT]` - Kubernetes API
5. `✅` - Success at each layer
6. `⚠️` - Warnings (expected failures)
7. `❌` - Errors (unexpected failures)

**Key Success Indicator:**
```
✅ [STREAM] Successfully obtained log stream for task [...]
```

**If you see this, live streaming is working!** 🎉

---

**Created:** December 2024  
**Purpose:** Diagnose live log streaming for Kubernetes tasks  
**Status:** Comprehensive logging added to all layers

