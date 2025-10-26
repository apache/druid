# Singular Druid 30.0.0 Custom Build - Complete Summary

## Overview

This document summarizes all custom changes made to Apache Druid 30.0.0 for Singular's production deployment.

## Changes Made

### 1. Task Reports Fix (PR #18379)

**Cherry-picked commit**: `c5127ba92f` from Apache Druid PR #18379  
**Purpose**: Fix task reports storage in Kubernetes deployments  
**Modules affected**:
- `processing` (interface change)
- `indexing-service` (implementation)
- `kubernetes-overlord-extensions` (cleaned up, no workaround code)

**Key changes**:
- Added `getReportsFile()` method to `TaskReportFileWriter` interface
- Modified `AbstractTask` to get reports file from writer instead of hardcoding path
- Updated `NoopTestTaskReportFileWriter` to implement new interface method
- **Deployment**: Requires custom Docker image with these JARs (see `TASK_REPORTS_POD_FIX.md`)

### 2. MSQ Task Context Support (Backport from PR #17140)

**Based on commit**: `878adff9aa` from Apache Druid PR #17140  
**Purpose**: Enable `context.tags` based pod template selection for MSQ tasks  
**Modules affected**:
- `extensions-core/multi-stage-query` (new implementation)

**Key changes**:
- Added task context creation in `MSQTaskQueryMaker.java`
- Extract tags from SQL query context and pass to task context
- Enable dynamic resource allocation for MSQ workloads in Kubernetes

### 3. Enhanced Logging for K8s Dynamic Config

**Purpose**: Debug and verify pod template selection  
**Modules affected**:
- `kubernetes-overlord-extensions`

**Files modified**:
- `PodTemplateTaskAdapter.java` - Added logging for template selection
- `Selector.java` - Added detailed logging for tag matching
- `SelectorBasedPodTemplateSelectStrategy.java` - Added strategy execution logging

**Test fixes**:
- `SelectorTest.java` - Replaced `NoopTask.TYPE` with `"noop"` string
- `SelectorBasedPodTemplateSelectStrategyTest.java` - Same fix

## Architecture

### Data Flow with Tags Support

```
SQL Query (Broker)
    ↓
context: {"tags": {"userProvidedTag": "medium"}}
    ↓
MSQTaskQueryMaker.runQuery()
    ↓
taskContext: {"tags": {"userProvidedTag": "medium"}}
    ↓
MSQControllerTask (with task context!)
    ↓
Overlord receives task
    ↓
PodTemplateTaskAdapter.fromTask()
    ↓
SelectorBasedPodTemplateSelectStrategy
    ↓
Selector evaluates tags
    ↓
Selected: prodft30-medium-peon-pod ✅
```

## Deployment Requirements

### Components That Need Updates

| Component | Extensions | Reason |
|-----------|------------|--------|
| **Broker** | `druid-multi-stage-query` | Creates MSQ tasks with task context |
| **Overlord** | `druid-multi-stage-query` | Manages MSQ tasks |
| **Overlord** | `druid-kubernetes-overlord-extensions` | Selects pod templates based on tags |
| **K8s Pods** | **Custom Docker Image** | Contains cherry-picked JARs for task reports |

### Docker Image Requirement

**CRITICAL**: Task reports will only work if Kubernetes pods use a custom Docker image containing:
- `druid-processing-30.0.0.jar` (with cherry-pick)
- `druid-indexing-service-30.0.0.jar` (with cherry-pick)
- `druid-kubernetes-overlord-extensions-30.0.0.jar` (with logging)

See **`TASK_REPORTS_POD_FIX.md`** for Docker image build and deployment instructions.

### Build Command

```bash
./apache-maven-3.9.11/bin/mvn \
  -pl extensions-contrib/kubernetes-overlord-extensions \
  -pl extensions-core/multi-stage-query \
  -DskipTests \
  -am \
  clean package
```

### Deployment Script

See `build_and_deploy_with_msq.sh` for automated deployment to Overlord.

**Manual steps required for Broker** (see `DEPLOYMENT_CHECKLIST.md`)

## Testing

### Test Query

```bash
curl -X POST http://prodft30-broker0.druid.singular.net:8082/druid/v2/sql/task \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "INSERT INTO test SELECT * FROM source WHERE __time >= TIMESTAMP '\''2025-08-01'\'' LIMIT 100 PARTITIONED BY DAY",
    "context": {
      "tags": {"userProvidedTag": "medium"},
      "maxNumTasks": 2
    }
  }'
```

### Expected Logs (Overlord)

```
📝 [ADAPTER] Creating Job from Task [query-xxxxx] (type=query_controller)
📝 [ADAPTER] Dynamic config present: true
📝 [ADAPTER] Using dynamic config strategy: SelectorBasedPodTemplateSelectStrategy
📝 [ADAPTER] Available templates in adapter: [base, prodft30-small-peon-pod, prodft30-medium-peon-pod]
🎯 [STRATEGY] SelectorBasedPodTemplateSelectStrategy starting for task [query-xxxxx]
🔍 [SELECTOR] Evaluating selector [prodft30-medium-peon-pod] for task [query-xxxxx]
🔍 [SELECTOR] Checking context.tags conditions
🔍 [SELECTOR] Task context.tags (key='tags'): {userProvidedTag=medium}
✅ [SELECTOR] Selector [prodft30-medium-peon-pod] MATCHED
📝 [ADAPTER] Template selected by strategy: prodft30-medium-peon-pod
```

## Files Modified

### Core Changes
1. `processing/src/main/java/org/apache/druid/indexer/report/TaskReportFileWriter.java`
2. `indexing-service/src/main/java/org/apache/druid/indexing/common/task/AbstractTask.java`
3. `indexing-service/src/test/java/org/apache/druid/indexing/common/task/NoopTestTaskReportFileWriter.java`
4. `extensions-core/multi-stage-query/src/main/java/org/apache/druid/msq/sql/MSQTaskQueryMaker.java` ⭐ NEW

### K8s Extension Changes
5. `extensions-contrib/kubernetes-overlord-extensions/src/main/java/org/apache/druid/k8s/overlord/taskadapter/PodTemplateTaskAdapter.java`
6. `extensions-contrib/kubernetes-overlord-extensions/src/main/java/org/apache/druid/k8s/overlord/execution/Selector.java`
7. `extensions-contrib/kubernetes-overlord-extensions/src/main/java/org/apache/druid/k8s/overlord/execution/SelectorBasedPodTemplateSelectStrategy.java`

### Test Fixes
8. `extensions-contrib/kubernetes-overlord-extensions/src/test/java/org/apache/druid/k8s/overlord/execution/SelectorTest.java`
9. `extensions-contrib/kubernetes-overlord-extensions/src/test/java/org/apache/druid/k8s/overlord/execution/SelectorBasedPodTemplateSelectStrategyTest.java`

## Documentation Created

### Production-Ready Documentation

1. **TASK_REPORTS_POD_FIX.md** ⭐ - Docker image build & deployment guide (REQUIRED)
2. **CUSTOM_DRUID_30_BUILD_SUMMARY.md** - This document (complete overview)
3. **MSQ_TAGS_SUPPORT_IMPLEMENTATION.md** - MSQ tags support documentation
4. **CHERRY_PICK_EXTENSION_GUIDE.md** - Original task reports fix guide
5. **DEPLOYMENT_CHECKLIST.md** - Step-by-step deployment instructions
6. **DEBUGGING_LOGS_GUIDE.md** - Guide to interpreting log output
7. **build_and_deploy_with_msq.sh** - Automated build & deploy script
8. **CLEANUP_COMPLETED.md** - Cleanup process documentation
9. **CLEANUP_SUCCESS_SUMMARY.md** - Quick cleanup summary

### Archived Documentation

Historical investigation docs moved to `docs/archive/`:
- TASK_REPORTS_INVESTIGATION_SUMMARY.md
- INVESTIGATION_COMPLETE.md

## Git History

### Branches
- `druid-30.0.0` - Base Druid 30.0.0 release tag
- `druid-30.0.0-task-reports-fix` - With task reports cherry-pick
- `druid-30.0.0-k8s-dynamic-config` - With K8s logging added
- Current working branch - With all changes including MSQ tags support

### Commits Applied
1. Cherry-pick `c5127ba92f` - Task reports fix
2. Manual changes - K8s dynamic config logging
3. Manual changes - MSQ task context support
4. Test fixes - NoopTask.TYPE compatibility

## Usage in Production

### Dynamic Config Applied

```json
{
  "type": "default",
  "podTemplateSelectStrategy": {
    "type": "selectorBased",
    "selectors": [
      {
        "selectionKey": "prodft30-medium-peon-pod",
        "context.tags": {
          "userProvidedTag": ["medium"]
        }
      },
      {
        "selectionKey": "prodft30-small-peon-pod",
        "context.tags": {
          "userProvidedTag": ["small"]
        }
      }
    ]
  }
}
```

### Pod Templates Configured

1. **base** - Default fallback (small.yaml)
2. **prodft30-small-peon-pod** - Small workloads (small.yaml)
3. **prodft30-medium-peon-pod** - Medium workloads (medium.yaml)

### Query Context Usage

✅ **Correct**:
```json
{
  "query": "...",
  "context": {
    "tags": {"userProvidedTag": "medium"}
  }
}
```

❌ **Wrong**:
```json
{
  "query": "...",
  "tags": {"userProvidedTag": "medium"}
}
```

## Upstream Compatibility

### Task Reports Fix
- ✅ **Fully compatible** with upstream Druid 31+
- Changes already merged in Apache Druid main branch
- No conflicts expected on upgrade

### MSQ Tags Support
- ⚠️ **Minimal backport** - Not full Dart profile implementation
- Based on PR #17140 but only extracted task context support
- On upgrade to Druid 31+:
  - Remove our backport
  - Use upstream implementation
  - Additional features available (lookup loading specs, etc.)

## Rollback Plan

If issues occur, rollback procedure:

1. SSH to affected server (Broker or Overlord)
2. Restore backup JARs from `~/druid-lib-backup/`
3. Restart service: `sudo supervisorctl restart {broker|overlord}`
4. Verify logs for successful startup

See `DEPLOYMENT_CHECKLIST.md` for detailed rollback commands.

## Success Criteria

✅ **Deployment successful if**:
1. Broker starts without errors
2. Overlord starts without errors
3. Extensions load successfully
4. Query with tags returns task ID
5. Overlord logs show tag propagation
6. Correct pod template is selected
7. K8s pods created with expected template

## Troubleshooting

### Tags Show as Null
- **Cause**: Broker not updated with new MSQ extension
- **Fix**: Deploy multi-stage-query to Broker

### Base Template Always Selected
- **Cause**: Tag values don't match selector conditions
- **Fix**: Verify dynamic config and query context match exactly

### Broker Won't Start
- **Cause**: JAR corruption or dependency issue
- **Fix**: Check logs, verify JAR integrity, restore from backup

See `DEPLOYMENT_CHECKLIST.md` for detailed troubleshooting.

## References

- **Task Reports Fix**: https://github.com/apache/druid/pull/18379
- **MSQ Dart Profile**: https://github.com/apache/druid/pull/17140
- **K8s Dynamic Config Docs**: https://druid.apache.org/docs/latest/development/extensions-core/k8s-jobs/#dynamic-config
- **MSQ SQL API Docs**: https://druid.apache.org/docs/latest/api-reference/sql-ingestion-api/

## Maintainer Notes

This custom build is specific to Singular's Druid 30.0.0 deployment. When upgrading to Druid 31+:

1. Review upstream changes in affected modules
2. Remove our MSQ task context backport
3. Keep K8s logging if still useful
4. Test dynamic config with upstream implementation
5. Verify tags propagation still works

---

**Last Updated**: October 26, 2025 (after cleanup)  
**Druid Version**: 30.0.0 (with custom patches)  
**Deployment Target**: prodft30 cluster  
**Status**: ✅ Production-Ready (requires Docker image deployment)

