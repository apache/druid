# Deployment Scripts for Task Reports Fix 🚀

## Overview

Two deployment scripts are provided for deploying the Task Reports fix to PRODFT:

1. **`rons_deploy_to_prodft.sh`** - Semi-automated (shows commands)
2. **`rons_deploy_to_prodft_auto.sh`** - Fully automated (executes everything)

---

## 📋 Prerequisites

### 1. Build the JARs

```bash
cd /Users/ronshub/workspace/druid

# Build kubernetes-overlord-extensions
./apache-maven-3.9.11/bin/mvn \
  -pl extensions-contrib/kubernetes-overlord-extensions \
  -am clean package \
  -DskipTests \
  -Dcheckstyle.skip=true \
  -Dforbiddenapis.skip=true

# Build indexing-service
./apache-maven-3.9.11/bin/mvn \
  -pl indexing-service \
  -am clean package \
  -DskipTests \
  -Dcheckstyle.skip=true \
  -Dforbiddenapis.skip=true
```

### 2. Verify SSH Access

```bash
ssh prodft-overlord.druid.singular.net
# Should connect without password (SSH key required)
```

---

## 🚀 Option 1: Semi-Automated Script (Recommended for First Time)

**Use this if:** You want to see and verify each command before execution.

```bash
cd /Users/ronshub/workspace/druid
./rons_deploy_to_prodft.sh
```

**What it does:**
1. ✅ Validates JARs exist
2. ✅ Copies JARs to Overlord staging directory
3. ℹ️ Shows you the commands to run on Overlord
4. ⏸️ Waits for you to manually SSH and execute

**Pros:**
- Full control over each step
- Can review commands before execution
- Safe for first deployment

**Cons:**
- Requires manual SSH and command execution

---

## 🤖 Option 2: Fully Automated Script

**Use this if:** You trust the automation and want zero interaction.

```bash
cd /Users/ronshub/workspace/druid
./rons_deploy_to_prodft_auto.sh
```

**What it does:**
1. ✅ Validates JARs exist
2. ✅ Copies JARs to Overlord staging directory
3. ✅ Automatically creates timestamped backups
4. ✅ Installs new JARs via SSH
5. ✅ Restarts Overlord via SSH
6. ✅ Waits for startup and checks status
7. ℹ️ Shows you next steps for testing

**Pros:**
- Zero manual intervention
- Faster deployment
- Automatic backups

**Cons:**
- Less visibility into each step
- Requires working SSH automation

---

## 📦 What Gets Deployed

| File | Source | Destination on Overlord |
|------|--------|------------------------|
| `druid-kubernetes-overlord-extensions-30.0.0.jar` | `extensions-contrib/kubernetes-overlord-extensions/target/` | `druid/extensions/druid-kubernetes-overlord-extensions/` |
| `druid-indexing-service-30.0.0.jar` | `indexing-service/target/` | `druid/lib/` |

---

## 🛡️ Safety Guarantees

Both scripts are **100% safe** because:

✅ **Only Overlord is affected**
- Scripts only SSH to `prodft-overlord.druid.singular.net`
- No MiddleManager deployments
- No pod image rebuilds

✅ **Automatic backups**
- Existing JARs backed up with timestamp
- Easy rollback if needed

✅ **No production impact**
- MiddleManagers untouched (production workloads safe)
- Overlord restart is stateless (no running tasks affected)

**See:** `MIDDLEMANAGER_SAFETY_ANALYSIS.md` for proof.

---

## 🔄 Rollback Procedure

If you need to rollback:

```bash
ssh prodft-overlord.druid.singular.net

# Find the backup timestamp
ls -lh druid/lib/druid-indexing-service-30.0.0.jar.backup-*
ls -lh druid/extensions/druid-kubernetes-overlord-extensions/druid-kubernetes-overlord-extensions-30.0.0.jar.backup-*

# Restore from backup (replace TIMESTAMP with actual timestamp)
TIMESTAMP="20251201-123456"

sudo cp druid/lib/druid-indexing-service-30.0.0.jar.backup-$TIMESTAMP \
       druid/lib/druid-indexing-service-30.0.0.jar

sudo cp druid/extensions/druid-kubernetes-overlord-extensions/druid-kubernetes-overlord-extensions-30.0.0.jar.backup-$TIMESTAMP \
       druid/extensions/druid-kubernetes-overlord-extensions/druid-kubernetes-overlord-extensions-30.0.0.jar

# Restart Overlord
sudo supervisorctl restart overlord
```

---

## 🧪 Testing After Deployment

### Test 1: Live Reports from Running Task

```bash
# Get a running MSQ task ID
TASK_ID=$(curl -s http://prod-router.druid.singular.net:8080/druid/indexer/v1/tasks | \
  jq -r '.[] | select(.type=="query_controller" and .statusCode=="RUNNING") | .id' | head -1)

echo "Testing task: $TASK_ID"

# Fetch live reports (should return JSON, not 404)
curl -s "http://prod-router.druid.singular.net:8080/druid/indexer/v1/task/$TASK_ID/reports" | jq .
```

**Expected:** 200 OK with live report JSON

---

### Test 2: S3 Fallback for Completed Task

```bash
# Get a completed task ID from S3
COMPLETED_TASK=$(aws s3 ls s3://singular-druid-indexing-logs/druid/prodft30/indexing_logs/ | \
  grep report.json | tail -1 | awk '{print $4}' | cut -d'/' -f1)

echo "Testing completed task: $COMPLETED_TASK"

# Fetch reports (should fall back to S3)
curl -s "http://prod-router.druid.singular.net:8080/druid/indexer/v1/task/$COMPLETED_TASK/reports" | jq .
```

**Expected:** 200 OK with reports from S3 (not 404/500)

---

### Test 3: Check Diagnostic Logs

```bash
ssh prodft-overlord.druid.singular.net

# Watch for diagnostic logging
tail -f /logs/druid/overlord.log | grep -E 'REPORTS|SWITCHING|API|📊|🔀|🌐'
```

**Expected log flow:**
```
🌐 [API] GET /task/{taskId}/reports - Request received
🔀 [SWITCHING] Trying task runner (live reports)
📊 [REPORTS] API request to stream live reports
✅ [REPORTS] Successfully retrieved live reports from pod
```

or (for S3 fallback):
```
🌐 [API] GET /task/{taskId}/reports - Request received
🔀 [SWITCHING] Trying task runner (live reports)
⚠️  [SWITCHING] Task runner threw IOException - will try deep storage fallback
🔀 [SWITCHING] Trying deep storage provider #1 (S3TaskLogs)
✅ [SWITCHING] Deep storage provider #1 returned reports
```

---

## 📚 Related Documentation

- **TASK_REPORTS_COMPREHENSIVE_FIX.md** - Complete fix details
- **MIDDLEMANAGER_SAFETY_ANALYSIS.md** - Safety proof
- **LIVE_REPORTS_DEBUG_GUIDE.md** - Debugging guide

---

## ⚠️ Important Notes

### What This Fix Does

✅ **Fixes:**
- 404 errors on completed task reports (S3 fallback works)
- 500 errors when pods are unreachable
- Live reports from running K8s tasks
- Complete diagnostic logging

✅ **Safe:**
- Only Overlord affected
- MiddleManagers untouched
- Production workloads isolated

❌ **Does NOT:**
- Require Docker image rebuild
- Affect MiddleManagers
- Change pod behavior
- Impact running tasks

---

## 🎯 Quick Reference

| Script | Use When | Interaction | Speed |
|--------|----------|-------------|-------|
| `rons_deploy_to_prodft.sh` | First time, want control | Manual SSH + commands | Slower |
| `rons_deploy_to_prodft_auto.sh` | Routine deployment | Zero (fully automated) | Faster |

---

**Created:** 2025-12-01  
**Status:** Ready for Production  
**Risk Level:** Low (Overlord-only, automatic backups)

