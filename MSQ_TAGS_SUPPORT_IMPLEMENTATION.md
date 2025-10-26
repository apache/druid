# MSQ Tags Support Implementation for Druid 30.0.0

## Overview

This implementation backports the task context support from Druid 31+ (PR #17140) to enable `context.tags` 
based pod template selection for MSQ (Multi-Stage Query) tasks in Kubernetes deployments.

## Problem Statement

In Druid 30.0.0, MSQ tasks are created with `null` task context, which prevents the Kubernetes dynamic 
pod template selection feature from working with `context.tags` selectors. While the K8s extension 
supports tag-based selection, MSQ tasks couldn't use it because:

1. `MSQControllerTask` was created with `context = null`
2. Tags from SQL query context were not propagated to task context
3. `ControllerImpl` looks for tags in task context (which was always null)

## Solution

### Changes Made

**File: `extensions-core/multi-stage-query/src/main/java/org/apache/druid/msq/sql/MSQTaskQueryMaker.java`**

1. **Added import for DruidMetrics** (line 48):
   ```java
   import org.apache.druid.query.DruidMetrics;
   ```

2. **Created task context HashMap** (lines 118-127):
   ```java
   // Create task context map for passing task-level metadata (like tags) to the MSQ controller task.
   // This enables features like dynamic pod template selection based on context.tags in Kubernetes deployments.
   final Map<String, Object> taskContext = new HashMap<>();
   
   // Extract tags from query context and add to task context if present.
   // Tags are used for metrics reporting and can be used for resource selection (e.g., pod templates in K8s).
   final Map<String, Object> queryContextTags = 
       (Map<String, Object>) plannerContext.queryContext().get(DruidMetrics.TAGS);
   if (queryContextTags != null) {
     taskContext.put(DruidMetrics.TAGS, queryContextTags);
   }
   ```

3. **Pass taskContext to MSQControllerTask** (line 306):
   Changed from `null` to `taskContext`

### How It Works

```
SQL API Request (with tags in context)
         ↓
    SqlQuery object
         ↓
  PlannerContext (query context contains "tags")
         ↓
MSQTaskQueryMaker extracts tags from query context
         ↓
  Creates taskContext HashMap with tags
         ↓
MSQControllerTask receives taskContext (not null!)
         ↓
ControllerImpl reads tags from task context
         ↓
Tags propagated to worker tasks
         ↓
K8s pod template selector sees tags in task context
         ↓
   Correct pod template selected!
```

## Usage

### 1. Configure Dynamic Pod Template Selection

Apply the dynamic config to your Overlord:

```bash
curl -X POST http://localhost:8090/druid/indexer/v1/k8s/taskrunner/executionconfig \
  -H 'Content-Type: application/json' \
  -d '{
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
  }'
```

### 2. Submit SQL Queries with Tags

**IMPORTANT**: Tags must be passed in the query `context`, NOT at the top level of the request.

```bash
curl -X POST http://prodft30-broker0.druid.singular.net:8082/druid/v2/sql/task \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "INSERT INTO my_datasource SELECT * FROM source_table WHERE __time >= TIMESTAMP '\''2025-08-01'\'' PARTITIONED BY DAY",
    "context": {
      "tags": {
        "userProvidedTag": "medium"
      },
      "maxNumTasks": 3
    }
  }'
```

**Note**: The `tags` object is placed INSIDE the `context` object, not at the top level.

### 3. Verify It Works

Check the Overlord logs after submitting a query:

```bash
sudo tail -f /logs/druid/overlord-stdout---supervisor-*.log | grep -E "📝|🎯|🔍"
```

You should see logs like:

```
📝 [ADAPTER] Creating Job from Task [query-xxxxx] (type=query_controller)
🔍 [SELECTOR] Task context.tags (key='tags'): {userProvidedTag=medium}
✅ [SELECTOR] Selector [prodft30-medium-peon-pod] MATCHED
```

## Architecture Details

### Key Components

1. **MSQTaskQueryMaker**: Extracts tags from SQL query context and creates task context
2. **MSQControllerTask**: Receives and stores task context (including tags)
3. **ControllerImpl**: Reads tags from task context and propagates to workers
4. **PodTemplateTaskAdapter**: Uses tags from task context for template selection
5. **Selector**: Evaluates tags condition to match appropriate pod template

### Data Flow

```
User submits SQL:
{
  "query": "...",
  "context": {
    "tags": {"userProvidedTag": "medium"}
  }
}
         ↓
SqlQuery → PlannerContext
query context: {"tags": {"userProvidedTag": "medium"}}
         ↓
MSQTaskQueryMaker.runQuery()
taskContext: {"tags": {"userProvidedTag": "medium"}}
         ↓
MSQControllerTask (task context populated)
         ↓
ControllerImpl.initializeQueryDefAndState()
Propagates tags to worker tasks
         ↓
MSQWorkerTask (task context with tags)
         ↓
PodTemplateTaskAdapter.fromTask()
Selects template based on tags
```

## Comparison with Upstream

### Druid 30.0.0 (Before This Change)
```java
final MSQControllerTask controllerTask = new MSQControllerTask(
    taskId,
    querySpec,
    sqlQuery,
    sqlQueryContext,
    sqlResultsContext,
    sqlTypeNames,
    nativeTypeNames,
    null  // ❌ Task context is NULL
);
```

### Druid 31+ (PR #17140)
```java
final Map<String, Object> taskContext = new HashMap<>();
taskContext.put(LookupLoadingSpec.CTX_LOOKUP_LOADING_MODE, ...);
// ... other task context items

final MSQControllerTask controllerTask = new MSQControllerTask(
    taskId,
    querySpec,
    sqlQuery,
    sqlQueryContext,
    sqlResultsContext,
    sqlTypeNames,
    nativeTypeNames,
    taskContext  // ✅ Task context populated
);
```

### This Implementation (Druid 30.0.0 Backport)
```java
// Minimal backport - only tags support, not full lookup specs
final Map<String, Object> taskContext = new HashMap<>();
final Map<String, Object> queryContextTags = 
    (Map<String, Object>) plannerContext.queryContext().get(DruidMetrics.TAGS);
if (queryContextTags != null) {
  taskContext.put(DruidMetrics.TAGS, queryContextTags);
}

final MSQControllerTask controllerTask = new MSQControllerTask(
    taskId,
    querySpec,
    sqlQuery,
    sqlQueryContext,
    sqlResultsContext,
    sqlTypeNames,
    nativeTypeNames,
    taskContext  // ✅ Task context with tags
);
```

## Testing

### Test Scenario 1: Medium Pod Selection

```bash
# Submit query with medium tag
curl -X POST http://broker:8082/druid/v2/sql/task \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "INSERT INTO test_medium SELECT * FROM source LIMIT 100 PARTITIONED BY DAY",
    "context": {"tags": {"userProvidedTag": "medium"}}
  }'

# Expected: query_controller → prodft30-medium-peon-pod
# Expected: query_worker → prodft30-medium-peon-pod (if selector matches workers too)
```

### Test Scenario 2: Small Pod Selection

```bash
# Submit query with small tag
curl -X POST http://broker:8082/druid/v2/sql/task \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "INSERT INTO test_small SELECT * FROM source LIMIT 10 PARTITIONED BY DAY",
    "context": {"tags": {"userProvidedTag": "small"}}
  }'

# Expected: query_controller → prodft30-small-peon-pod
```

### Test Scenario 3: No Tags (Fallback to Base)

```bash
# Submit query without tags
curl -X POST http://broker:8082/druid/v2/sql/task \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "INSERT INTO test_base SELECT * FROM source LIMIT 10 PARTITIONED BY DAY",
    "context": {"maxNumTasks": 2}
  }'

# Expected: Falls back to base template
```

## Limitations

1. **Minimal Backport**: This implementation only adds tags support, not the full task context
   infrastructure from PR #17140 (which includes lookup loading specs and other features)

2. **Druid 30.0.0 Only**: This is a backport specifically for Druid 30.0.0. If you upgrade to
   Druid 31+, you'll get the full implementation from upstream.

3. **Tags Location**: Tags MUST be in `context.tags`, not at the top level of the request

## Migration Path

### If Using Druid 30.0.0
- Apply this implementation
- Use `context.tags` in your SQL queries
- Dynamic pod template selection will work!

### If Upgrading to Druid 31+
- Remove this backport (it will conflict)
- Use upstream implementation
- Additional features like lookup loading specs will be available

## Troubleshooting

### Tags Not Appearing in Logs

**Symptom**: Logs show `Task context.tags (key='tags'): null`

**Possible Causes**:
1. Tags not passed in request `context` object
2. Tags passed at wrong level (top level instead of inside `context`)
3. MSQTaskQueryMaker changes not applied/compiled

**Solution**:
```bash
# ✅ CORRECT
{
  "query": "...",
  "context": {
    "tags": {"userProvidedTag": "value"}
  }
}

# ❌ WRONG
{
  "query": "...",
  "tags": {"userProvidedTag": "value"}
}
```

### Base Template Always Selected

**Symptom**: Logs show template selection, but always falls back to "base"

**Possible Causes**:
1. Dynamic config not applied to Overlord
2. Selector conditions don't match the tags provided
3. Template names in config don't match runtime.properties

**Solution**:
```bash
# Verify dynamic config
curl http://localhost:8090/druid/indexer/v1/k8s/taskrunner/executionconfig

# Check available templates
cat druid_conf/overlord/runtime.properties | grep podTemplate
```

## Related PRs and Commits

- **Upstream PR #17140**: "MSQ profile for Brokers and Historicals" (Oct 2024)
  - Commit: `878adff9aa`
  - Full implementation with Dart profile, lookup specs, etc.

- **This Backport**: Minimal implementation for Druid 30.0.0
  - Only task context and tags support
  - No Dart profile or lookup specs
  - Focused on enabling K8s dynamic pod template selection

## References

- K8s Dynamic Config Docs: https://druid.apache.org/docs/latest/development/extensions-core/k8s-jobs/#dynamic-config
- MSQ Ingestion API: https://druid.apache.org/docs/latest/api-reference/sql-ingestion-api/
- Original Discussion: PR #18379 (Task reports fix)

