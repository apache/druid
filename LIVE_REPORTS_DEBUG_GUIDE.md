# Live Task Reports Debugging Guide 📊

## Problem
Getting **404 Not Found** when requesting live task reports during execution:
```
GET /druid/indexer/v1/task/{taskId}/reports
```

## Flow of Live Reports

```
┌─────────────────────────────────────────────────────────────┐
│ 1. HTTP GET Request                                          │
│    /druid/indexer/v1/task/{taskId}/reports                  │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ↓
┌─────────────────────────────────────────────────────────────┐
│ 2. OverlordResource.doGetReports()                          │
│    - Calls taskLogStreamer.streamTaskReports(taskid)        │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ↓
┌─────────────────────────────────────────────────────────────┐
│ 3. SwitchingTaskLogStreamer.streamTaskReports()            │
│    - Tries TaskRunner first (for live reports)              │
│    - Falls back to S3 (for completed tasks)                 │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ↓
┌─────────────────────────────────────────────────────────────┐
│ 4. KubernetesTaskRunner.streamTaskReports()                │
│    - Gets work item for task                                 │
│    - Gets task location (pod IP + port)                      │
│    - Constructs URL: http://<podIP>:8100/druid/worker/v1/   │
│      chat/{taskId}/liveReports                               │
│    - Sends HTTP GET to pod                                   │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ↓
┌─────────────────────────────────────────────────────────────┐
│ 5. Pod HTTP Server (ControllerChatHandler)                  │
│    @GET @Path("/liveReports")                                │
│    - Returns live reports from MSQ controller                │
└─────────────────────────────────────────────────────────────┘
```

## Diagnostic Logging Added

I've added comprehensive logging to `KubernetesTaskRunner.streamTaskReports()`:

```
📊 [REPORTS] API request to stream live reports for task [...]
📊 [REPORTS] Work item found for task [...], retrieving task location
📊 [REPORTS] Task location for [...]: host=X.X.X.X, port=8100, tlsPort=-1
📊 [REPORTS] Constructed URL for task [...]: http://X.X.X.X:8100/druid/worker/v1/chat/.../liveReports
📊 [REPORTS] Sending HTTP GET request to pod for task [...]...
✅ [REPORTS] Successfully retrieved live reports from pod for task [...]
```

## Possible Causes of 404

### 1. **Work Item Not Found**
```
⚠️  [REPORTS] No work item found for task [...] - task may not exist or has not been registered
```
**Cause:** Task has not been started by the Overlord yet.  
**Fix:** Wait for task to start running.

### 2. **Task Location Unknown**
```
⚠️  [REPORTS] Task location unknown for task [...] - pod may not be running yet
```
**Cause:** Pod hasn't been assigned an IP yet (still in `Pending` or `ContainerCreating` state).  
**Fix:** Wait for pod to reach `Running` state.

### 3. **HTTP Request Failed**
```
❌ [REPORTS] HTTP request failed for task [...] - URL: http://X.X.X.X:8100/...
```
**Causes:**
- **Pod is not listening on port 8100** - Check if the pod's HTTP server started
- **Network isolation** - Overlord can't reach pod IP (firewall/network policy)
- **Pod crashed** - Pod terminated before Overlord could fetch reports

## Build and Deploy Diagnostics

### Step 1: Build Extension with Logging

```bash
cd /Users/ronshub/workspace/druid

# Build the extension
./apache-maven-3.9.11/bin/mvn \
  -pl extensions-contrib/kubernetes-overlord-extensions \
  -am clean package \
  -DskipTests \
  -Dcheckstyle.skip=true \
  -Dforbiddenapis.skip=true

# Check build success
ls -lh extensions-contrib/kubernetes-overlord-extensions/target/*.jar
```

### Step 2: Deploy to Overlord

```bash
# Copy JAR to Overlord
scp extensions-contrib/kubernetes-overlord-extensions/target/druid-kubernetes-overlord-extensions-30.0.0.jar \
    ubuntu@prodft30-overlord0:/tmp/

# SSH to Overlord and install
ssh ubuntu@prodft30-overlord0

# Backup existing JAR
sudo cp /opt/druid/extensions/druid-kubernetes-overlord-extensions/druid-kubernetes-overlord-extensions-30.0.0.jar \
       /opt/druid/extensions/druid-kubernetes-overlord-extensions/druid-kubernetes-overlord-extensions-30.0.0.jar.bak

# Install new JAR
sudo cp /tmp/druid-kubernetes-overlord-extensions-30.0.0.jar \
       /opt/druid/extensions/druid-kubernetes-overlord-extensions/

# Restart Overlord
sudo supervisorctl restart overlord

# Tail logs to see the new diagnostic output
tail -f /logs/druid/overlord.log | grep -E "REPORTS|📊|✅|⚠️|❌"
```

### Step 3: Test Live Reports

```bash
# Get a running MSQ task ID
TASK_ID="query-manual_MSQ_medium_historical_historical_backfill_buffer_tracker_stats_353011_2025-01-01_2025-12-01T08:55:19"

# Request live reports
curl -s "http://prod-router.druid.singular.net:8080/druid/indexer/v1/task/${TASK_ID}/reports" | jq .

# Check Overlord logs for diagnostic output
ssh ubuntu@prodft30-overlord0
tail -100 /logs/druid/overlord.log | grep -E "REPORTS.*${TASK_ID}"
```

## Debugging Steps

### 1. Check if Pod is Running

```bash
ssh ubuntu@prodft30-overlord0

# Find the pod for your task
kubectl get pods | grep "query-manual"

# Check pod status
POD_NAME="<pod-name-from-above>"
kubectl describe pod $POD_NAME

# Look for:
# - Status: Running
# - PodIP: Should be set (e.g., 10.0.x.x)
# - Containers Ready: 1/1
```

### 2. Check if Pod HTTP Server is Listening

```bash
# Test if pod is responding on port 8100
kubectl exec $POD_NAME -- netstat -tuln | grep 8100

# Should show:
# tcp        0      0 0.0.0.0:8100            0.0.0.0:*               LISTEN

# Test HTTP endpoint directly from within cluster
kubectl exec $POD_NAME -- curl -s http://localhost:8100/status/health
```

### 3. Check Overlord Logs

```bash
# On the Overlord
tail -f /logs/druid/overlord.log | grep -E "📊|REPORTS"

# Look for the diagnostic output:
# - Did it find the work item?
# - What is the task location (pod IP)?
# - What URL was constructed?
# - Did the HTTP request succeed or fail?
```

### 4. Test HTTP Connectivity from Overlord to Pod

```bash
# Get pod IP from Overlord logs or:
POD_IP=$(kubectl get pod $POD_NAME -o jsonpath='{.status.podIP}')
echo "Pod IP: $POD_IP"

# From Overlord host, test connectivity
curl -v "http://${POD_IP}:8100/status/health"

# Test the liveReports endpoint
TASK_ID="<your-task-id>"
curl -v "http://${POD_IP}:8100/druid/worker/v1/chat/${TASK_ID}/liveReports"
```

### 5. Check Pod Logs for HTTP Errors

```bash
# Check if pod received the request
kubectl logs $POD_NAME | grep -E "liveReports|ControllerChatHandler"
```

## Common Issues and Fixes

### Issue 1: Pod Not Exposing HTTP Server
**Symptom:** `netstat` shows port 8100 is not listening  
**Cause:** Pod configuration missing or incorrect  
**Fix:** Check `runtime.properties` in the pod has:
```properties
druid.port=8100
druid.service=druid/prodft30/peon
```

### Issue 2: Network Isolation
**Symptom:** Overlord can't reach pod IP  
**Cause:** Kubernetes NetworkPolicy or firewall blocking traffic  
**Fix:** Verify network policies allow Overlord → Pod communication on port 8100

### Issue 3: Pod Not Ready Yet
**Symptom:** Task location is "unknown"  
**Cause:** Pod still starting up  
**Fix:** Wait a few seconds and retry

### Issue 4: Task Has Already Completed
**Symptom:** Work item not found or pod terminated  
**Cause:** Task finished and pod was cleaned up  
**Fix:** For completed tasks, reports are in S3. Use:
```bash
aws s3 cp s3://singular-druid-indexing-logs/druid/prodft30/indexing_logs/${TASK_ID}/report.json -
```

## Expected Output (Success)

When live reports work correctly, you should see:

**Overlord Logs:**
```
📊 [REPORTS] API request to stream live reports for task [query-abc123]
📊 [REPORTS] Work item found for task [query-abc123], retrieving task location
📊 [REPORTS] Task location for [query-abc123]: host=10.0.5.42, port=8100, tlsPort=-1
📊 [REPORTS] Constructed URL for task [query-abc123]: http://10.0.5.42:8100/druid/worker/v1/chat/query-abc123/liveReports
📊 [REPORTS] Sending HTTP GET request to pod for task [query-abc123]...
✅ [REPORTS] Successfully retrieved live reports from pod for task [query-abc123]
```

**HTTP Response:**
```json
{
  "multiStageQuery": {
    "type": "multiStageQuery",
    "taskId": "query-abc123",
    "payload": {
      "status": {
        "status": "RUNNING",
        "startTime": "2025-12-01T10:00:00.000Z",
        "durationMs": 5000
      },
      "stages": [
        {
          "stageNumber": 0,
          "phase": "READING_INPUT",
          "workerCount": 2
        }
      ]
    }
  }
}
```

---

## Next Steps

1. ✅ Build extension with diagnostic logging
2. ✅ Deploy to Overlord
3. 🔍 Test with a running MSQ task
4. 📋 Check logs to identify exact failure point
5. 🛠️ Fix identified issue

**Created:** 2025-12-01  
**Status:** Diagnostics Added - Ready to Deploy  
**Priority:** High - Blocking live task monitoring

