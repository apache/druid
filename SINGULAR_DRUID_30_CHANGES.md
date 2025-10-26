# Singular Druid 30.0.0 Custom Changes

**Branch:** `druid-30-singular-changes`  
**Base Version:** Apache Druid 30.0.0 (tag: `druid-30.0.0-rc3`)  
**Last Updated:** October 26, 2025

---

## Overview

This branch contains custom patches and features added to Apache Druid 30.0.0 for Singular's production deployment (`prodft30` cluster). All changes are production-tested and ready for deployment.

---

## What Changes Were Made

### 1. Task Reports Fix (Cherry-Pick from PR #18379)

**Problem:** In Kubernetes deployments, task reports were not being saved to deep storage because `AbstractTask` used a hardcoded path that didn't match where the `TaskReportFileWriter` actually wrote the reports file.

**Solution:** Cherry-picked Apache Druid PR #18379 which fixes this by:
- Adding `getReportsFile()` method to `TaskReportFileWriter` interface
- Modifying `AbstractTask` to dynamically get the reports file path from the writer
- Ensuring reports are correctly pushed to S3/deep storage

**Why This Matters:** Without this fix, task reports (containing query statistics, error details, and performance metrics) are lost when Kubernetes pods terminate. This breaks observability and debugging capabilities.

**Files Modified:**
- `processing/src/main/java/org/apache/druid/indexer/report/TaskReportFileWriter.java`
- `indexing-service/src/main/java/org/apache/druid/indexing/common/task/AbstractTask.java`
- `indexing-service/src/test/java/org/apache/druid/indexing/common/task/NoopTestTaskReportFileWriter.java`

**Upstream Status:** ✅ Merged in Apache Druid 31+ (fully compatible with future upgrades)

**Documentation:** See `CHERRY_PICK_EXTENSION_GUIDE.md`

---

### 2. MSQ Context Tags Support (Backport from PR #17140)

**Problem:** MSQ (Multi-Stage Query) tasks didn't propagate `context.tags` from the SQL query context to the task context. This prevented dynamic pod template selection based on workload characteristics (e.g., resource requirements).

**Solution:** Backported the task context propagation logic from Apache Druid PR #17140:
- Modified `MSQTaskQueryMaker` to extract tags from SQL query context
- Pass tags as task context when creating `MSQControllerTask`
- Enable `context.tags` based pod template selection for MSQ workloads

**Why This Matters:** Enables dynamic resource allocation for MSQ queries. Users can specify workload size in their query context (e.g., `"tags": {"userProvidedTag": "medium"}`), and the system automatically selects the appropriate Kubernetes pod template (small/medium/large resources).

**Example:**
```sql
-- Query with medium resources
INSERT INTO target 
SELECT * FROM source 
WHERE __time >= TIMESTAMP '2025-01-01'
CONTEXT tags => '{"userProvidedTag": "medium"}'
```

**Files Modified:**
- `extensions-core/multi-stage-query/src/main/java/org/apache/druid/msq/sql/MSQTaskQueryMaker.java`
- `extensions-core/multi-stage-query/src/test/java/org/apache/druid/msq/indexing/WorkerChatHandlerTest.java`
- `extensions-core/multi-stage-query/src/test/java/org/apache/druid/msq/test/MSQTestWorkerContext.java`

**Upstream Status:** ⚠️ Based on PR #17140 but minimal backport (not full DART profile). On upgrade to Druid 31+, replace with upstream implementation.

**Documentation:** See `MSQ_TAGS_SUPPORT_IMPLEMENTATION.md`

---

### 3. Dynamic Pod Template Selection Logging

**Problem:** The Kubernetes dynamic config feature (cherry-picked from upstream) had limited logging, making it difficult to debug why a particular pod template was selected or why tags weren't matching.

**Solution:** Added comprehensive logging to the pod template selection flow:
- Log dynamic config status and strategy being used
- Log all available templates
- Log detailed tag matching logic (expected vs. actual values)
- Log final template selection

**Why This Matters:** Essential for debugging and verifying that dynamic pod template selection is working correctly. Helps operators understand why a particular template was chosen and troubleshoot configuration issues.

**Example Logs:**
```
[ADAPTER] Creating Job from Task [query-abc123] (type=query_controller, dataSource=my_data)
[ADAPTER] Dynamic config present: true
[ADAPTER] Using dynamic config strategy: SelectorBasedPodTemplateSelectStrategy
[ADAPTER] Available templates: [base, small, medium]
[SELECTOR] Checking context.tags: expected={userProvidedTag=[medium]}, actual={userProvidedTag=medium}
[SELECTOR] Selector [medium] MATCHED ✅
[ADAPTER] Template selected: medium
```

**Files Modified:**
- `extensions-contrib/kubernetes-overlord-extensions/.../PodTemplateTaskAdapter.java`
- `extensions-contrib/kubernetes-overlord-extensions/.../Selector.java`
- `extensions-contrib/kubernetes-overlord-extensions/.../SelectorBasedPodTemplateSelectStrategy.java`

**Upstream Status:** ⚠️ Custom logging, not in upstream. Maintain on upgrades if still useful.

**Documentation:** See `DEBUGGING_LOGS_GUIDE.md`

---

### 4. Cleanup: Removed HTTP Fetching Workaround

**Problem:** During development, we added code to the Overlord to fetch task reports via HTTP from running pods. This was a workaround because we didn't initially have the cherry-pick fix deployed in pod images.

**Solution:** Removed the workaround code (~200 lines) including:
- `KubernetesPeonLifecycle.saveReports()` method
- `HttpClient` field and constructor parameters
- Unused imports and test mocks

**Why This Matters:** 
- Cleaner, simpler code that aligns with Druid's design
- No race conditions between pod termination and HTTP fetching
- Pods handle their own report persistence (standard pattern)

**Files Modified:**
- `extensions-contrib/kubernetes-overlord-extensions/.../KubernetesPeonLifecycle.java`
- `extensions-contrib/kubernetes-overlord-extensions/.../KubernetesPeonLifecycleFactory.java`
- `extensions-contrib/kubernetes-overlord-extensions/.../KubernetesTaskRunnerFactory.java`
- Test files: `KubernetesPeonLifecycleTest.java`, `KubernetesWorkItemTest.java`

**Upstream Status:** ✅ No upstream conflict (workaround was never in upstream)

---

### 5. Test Compatibility Fixes

**Problem:** Some test code used `NoopTask.TYPE` constant which doesn't exist in Druid 30.0.0.

**Solution:** Replaced references with the literal string `"noop"`.

**Files Modified:**
- `extensions-contrib/kubernetes-overlord-extensions/.../SelectorTest.java`
- `extensions-contrib/kubernetes-overlord-extensions/.../SelectorBasedPodTemplateSelectStrategyTest.java`

**Upstream Status:** ✅ No conflict (test-only changes)

---

## Why These Changes Were Made

### Business Context

Singular's `prodft30` cluster runs large-scale MSQ ingestion queries that have varying resource requirements:
- **Small queries:** Historical data corrections, small batch imports
- **Medium queries:** Daily ingestion jobs, standard ETL
- **Large queries:** Backfills, major data migrations

**Before these changes:**
1. All MSQ tasks used the same pod template (small resources)
2. Large queries failed with OOM or took very long
3. Small queries wasted resources by using large pods
4. Task reports were lost, breaking observability

**After these changes:**
1. ✅ Queries specify resource needs via `context.tags`
2. ✅ System auto-selects appropriate pod template
3. ✅ Resource utilization optimized
4. ✅ Task reports saved to S3 for debugging
5. ✅ Full observability with detailed logs

---

## Architecture

### Data Flow: Query → Pod Template Selection

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. User submits SQL query via Broker                            │
│    POST /druid/v2/sql/task                                      │
│    {                                                             │
│      "query": "INSERT INTO...",                                 │
│      "context": {"tags": {"userProvidedTag": "medium"}}         │
│    }                                                             │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│ 2. Broker: MSQTaskQueryMaker.runQuery()                         │
│    - Extracts tags from query context                           │
│    - Creates taskContext with tags                              │
│    - Creates MSQControllerTask(taskContext)                     │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│ 3. Overlord: Receives MSQControllerTask                         │
│    - Task has context.tags = {"userProvidedTag": "medium"}      │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│ 4. K8s Extension: PodTemplateTaskAdapter.fromTask()             │
│    - Loads dynamic config                                       │
│    - Gets SelectorBasedPodTemplateSelectStrategy                │
│    - Evaluates selectors against task.context.tags              │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│ 5. Selector.evaluate()                                          │
│    - Checks if task.context.tags matches selector config        │
│    - Selector "medium": context.tags.userProvidedTag = [medium] │
│    - Task tags: {userProvidedTag: "medium"} ✅ MATCH!           │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│ 6. Creates Kubernetes Job                                       │
│    - Uses selected pod template (medium.yaml)                   │
│    - Pod has 8GB memory, 4 CPU (medium resources)               │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│ 7. Pod Runs Task                                                │
│    - Custom Docker image (with cherry-pick fix)                 │
│    - Task completes and writes reports                          │
│    - AbstractTask.cleanup() calls pushTaskReports()             │
│    - Reports pushed to S3 ✅                                    │
└─────────────────────────────────────────────────────────────────┘
```

---

## Deployment Requirements

### Critical: Custom Docker Image Required

**⚠️ Task reports will NOT work without a custom Docker image!**

The cherry-picked fix (PR #18379) modifies code that runs **inside the Kubernetes pods**, not on the Overlord. Stock `apache/druid:30.0.0` doesn't have this fix.

**What's needed:**
1. Build custom Docker image containing:
   - `druid-processing-30.0.0.jar` (with cherry-pick)
   - `druid-indexing-service-30.0.0.jar` (with cherry-pick)
   - `druid-kubernetes-overlord-extensions-30.0.0.jar` (with logging)
   - Correct S3 configuration in `_common/common.runtime.properties`

2. Update pod templates to use custom image:
   ```yaml
   # Change from:
   image: apache/druid:30.0.0
   
   # To:
   image: singular/druid:30.0.0-reports-fix
   ```

**See:** `TASK_REPORTS_POD_FIX.md` for complete Docker build instructions.

### Component Updates

| Component | Module | Required? | Why |
|-----------|--------|-----------|-----|
| **Broker** | `druid-multi-stage-query` | ✅ Yes | Extracts tags from query context |
| **Overlord** | `druid-multi-stage-query` | ✅ Yes | Manages MSQ tasks with context |
| **Overlord** | `druid-kubernetes-overlord-extensions` | ✅ Yes | Selects pod templates |
| **K8s Pods** | **Custom Docker Image** | ✅ **CRITICAL** | Contains cherry-pick fix |

---

## How to Deploy

### Quick Reference

1. **Build Custom JARs**
   ```bash
   ./build_and_deploy_with_msq.sh
   ```

2. **Build Docker Image**
   ```bash
   # Follow TASK_REPORTS_POD_FIX.md
   docker build -t singular/druid:30.0.0-reports-fix .
   ```

3. **Deploy**
   - Broker: Deploy `multi-stage-query` JAR
   - Overlord: Deploy both JARs
   - K8s: Update pod templates with new image

4. **Verify**
   - Submit test query with tags
   - Check Overlord logs for template selection
   - Verify reports in S3

**See:** `DEPLOYMENT_CHECKLIST.md` for detailed step-by-step instructions.

---

## Configuration

### Dynamic Config (Overlord)

Applied via: `POST /druid/indexer/v1/worker/k8s`

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

### Runtime Properties (Overlord)

```properties
# Pod Templates
druid.indexer.runner.k8s.podTemplate.base=/path/to/small.yaml
druid.indexer.runner.k8s.podTemplate.prodft30-small-peon-pod=/path/to/small.yaml
druid.indexer.runner.k8s.podTemplate.prodft30-medium-peon-pod=/path/to/medium.yaml

# Adapter
druid.indexer.runner.k8s.adapter.type=customTemplateAdapter
```

### Query Context (Users)

```json
{
  "query": "INSERT INTO target SELECT * FROM source",
  "context": {
    "tags": {"userProvidedTag": "medium"},
    "maxNumTasks": 4
  }
}
```

---

## Testing & Verification

### Test Query

```bash
curl -X POST http://broker:8082/druid/v2/sql/task \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "INSERT INTO test SELECT * FROM source LIMIT 1000",
    "context": {
      "tags": {"userProvidedTag": "medium"}
    }
  }'
```

### Expected Results

1. **Overlord Logs:**
   ```
   [ADAPTER] Template selected: prodft30-medium-peon-pod ✅
   ```

2. **Kubernetes:**
   ```bash
   kubectl get pod <pod-name> -o yaml | grep "image:"
   # Should show: singular/druid:30.0.0-reports-fix
   ```

3. **S3:**
   ```bash
   aws s3 ls s3://bucket/indexing_logs/<task-id>/
   # Should show: log, status.json, report.json ✅
   ```

4. **Reports API:**
   ```bash
   curl http://overlord:8090/druid/indexer/v1/task/<task-id>/reports
   # Should return JSON report ✅
   ```

---

## Rollback Plan

If issues occur:

1. **Broker/Overlord JARs:**
   ```bash
   # Restore from backup
   cp ~/druid-lib-backup/*.jar /opt/druid/extensions/
   sudo supervisorctl restart {broker|overlord}
   ```

2. **Docker Image:**
   ```bash
   # Revert pod templates to stock image
   image: apache/druid:30.0.0
   
   # Restart Overlord to reload templates
   sudo systemctl restart druid-overlord
   ```

**Note:** Reverting pods to stock image will break task reports again.

---

## Upstream Compatibility

### Upgrading to Druid 31+

When upgrading from 30.0.0 to 31+:

1. **Task Reports Fix (PR #18379)**
   - ✅ Already merged in Druid 31+
   - ✅ No action needed (fully compatible)

2. **MSQ Tags Support (PR #17140)**
   - ⚠️ Our backport is minimal (only task context)
   - ⚠️ Upstream has full DART profile implementation
   - **Action:** Remove our `MSQTaskQueryMaker` changes, use upstream

3. **Dynamic Config Logging**
   - ⚠️ Custom enhancement, not in upstream
   - **Action:** Review if still useful, keep or remove

4. **Test Fixes**
   - ⚠️ May not be needed in 31+ if NoopTask.TYPE exists
   - **Action:** Run tests, update if needed

---

## Documentation

### Essential Documentation (In This Branch)

| Document | Purpose | Audience |
|----------|---------|----------|
| **`SINGULAR_DRUID_30_CHANGES.md`** | This file - complete overview | Everyone |
| **`CUSTOM_DRUID_30_BUILD_SUMMARY.md`** | Technical summary, architecture | Developers |
| **`TASK_REPORTS_POD_FIX.md`** | Docker image build guide | DevOps |
| **`DEPLOYMENT_CHECKLIST.md`** | Step-by-step deployment | DevOps |
| **`MSQ_TAGS_SUPPORT_IMPLEMENTATION.md`** | MSQ tags feature details | Developers |
| **`CHERRY_PICK_EXTENSION_GUIDE.md`** | Cherry-pick process | Developers |
| **`DEBUGGING_LOGS_GUIDE.md`** | Log interpretation | Ops/Support |
| **`build_and_deploy_with_msq.sh`** | Build automation | DevOps |

### Archived Documentation

Historical investigation docs in `docs/archive/`:
- Investigation process documentation
- Development notes

---

## Troubleshooting

### Issue: Tags Show as Null

**Symptom:** Logs show `context.tags: null`

**Cause:** Broker not updated with MSQ changes

**Fix:** Deploy `druid-multi-stage-query-30.0.0.jar` to Broker

---

### Issue: Base Template Always Selected

**Symptom:** Logs show `Template selected: base` even with tags

**Cause:** Tag values don't match selector conditions

**Fix:** 
1. Check dynamic config: `GET /druid/indexer/v1/worker/k8s`
2. Verify query context format: `{"tags": {"key": "value"}}`
3. Check selector matching logic in logs

---

### Issue: No report.json in S3

**Symptom:** Only `log` and `status.json`, no `report.json`

**Cause:** Pods using stock Docker image (no cherry-pick fix)

**Fix:** 
1. Verify pod image: `kubectl describe pod <pod>`
2. Build custom Docker image (see `TASK_REPORTS_POD_FIX.md`)
3. Update pod templates to use custom image

---

### Issue: Broker Won't Start

**Symptom:** Broker fails on startup after JAR deployment

**Cause:** JAR corruption or dependency conflict

**Fix:**
1. Check broker logs: `tail -f /logs/druid/broker.log`
2. Verify JAR integrity: `md5sum *.jar`
3. Restore from backup if needed

---

## Maintenance

### Regular Tasks

1. **Monitor Logs**
   - Watch for template selection patterns
   - Verify reports are being saved
   - Check for any errors

2. **Review Configurations**
   - Periodically review dynamic config
   - Ensure pod templates are up to date
   - Verify S3 credentials are valid

3. **Test Deployments**
   - Test queries with different tags
   - Verify each pod template works
   - Check resource utilization

### Before Upgrades

1. Review upstream changes in affected modules
2. Check if our patches are included in new version
3. Test in staging environment
4. Plan rollback strategy

---

## Support & References

### Internal Documentation
- Build script: `build_and_deploy_with_msq.sh`
- Docker guide: `TASK_REPORTS_POD_FIX.md`
- Deployment checklist: `DEPLOYMENT_CHECKLIST.md`

### Upstream References
- **Task Reports Fix:** https://github.com/apache/druid/pull/18379
- **MSQ Dart Profile:** https://github.com/apache/druid/pull/17140
- **K8s Dynamic Config:** https://druid.apache.org/docs/30.0.0/development/extensions-core/k8s-jobs/#dynamic-config
- **MSQ SQL API:** https://druid.apache.org/docs/30.0.0/api-reference/sql-ingestion-api/

### Contact
For questions or issues:
1. Review relevant documentation above
2. Check `docs/archive/` for historical context
3. Contact platform team

---

## Summary

This branch adds critical features to Druid 30.0.0:
1. ✅ **Task reports fix** - Ensures observability
2. ✅ **MSQ tags support** - Enables dynamic resource allocation
3. ✅ **Enhanced logging** - Improves debuggability
4. ✅ **Clean codebase** - No workarounds, production-ready

**Status:** Production-ready, requires Docker image deployment

**Deployment Time:** ~2-3 hours (including Docker build and testing)

**Risk Level:** Low (changes are isolated, well-tested, with rollback plan)

---

**Last Updated:** October 26, 2025  
**Maintained By:** Singular Platform Team  
**Druid Version:** 30.0.0 (with custom patches)  
**Production Cluster:** prodft30

