# Deployment Checklist for MSQ Tags Support

## What Changed

### Code Changes
1. ✅ `MSQTaskQueryMaker.java` - Added task context support with tags
2. ✅ `PodTemplateTaskAdapter.java` - Already had logging (from previous work)
3. ✅ `Selector.java` - Already had logging (from previous work)

### JARs That Need to Be Rebuilt and Deployed

| Extension | Location | Deploy To | Why |
|-----------|----------|-----------|-----|
| **multi-stage-query** | `extensions-core/` | **BROKER + OVERLORD** | Contains MSQTaskQueryMaker changes |
| **kubernetes-overlord-extensions** | `extensions-contrib/` | **OVERLORD only** | Contains logging we added earlier |

## ⚠️ CRITICAL: You Need to Deploy to BOTH Broker AND Overlord!

### Why Both?

1. **Broker**: Runs the MSQ SQL engine (`MSQTaskQueryMaker`)
   - Receives SQL queries from users
   - Creates MSQControllerTask with task context
   - **MUST have the updated multi-stage-query JAR**

2. **Overlord**: Manages MSQ tasks in Kubernetes
   - Receives tasks from Broker
   - Selects pod templates based on task context
   - **MUST have both updated JARs**

## Deployment Steps

### Step 1: Build Extensions

```bash
cd /Users/ronshub/workspace/druid
./build_and_deploy_with_msq.sh
```

This will:
- Build kubernetes-overlord-extensions
- Build multi-stage-query
- Copy both JARs to Overlord
- Install and restart Overlord

### Step 2: Deploy to Broker (ADDITIONAL STEP!)

**You also need to update the Broker with the multi-stage-query JAR:**

```bash
# Copy multi-stage-query JAR to Broker
for f in $(find . -name "*druid-multi-stage-query-30.0.0.jar" ! -name "*tests*"); do
  scp -P 41100 "$f" ubuntu@prodft30-broker0.druid.singular.net:~/druid-lib-build/
done

# SSH to Broker and install
ssh -p 41100 ubuntu@prodft30-broker0.druid.singular.net

# Backup old JAR
mkdir -p ~/druid-lib-backup
cp druid/extensions/druid-multi-stage-query/druid-multi-stage-query-30.0.0.jar \
   ~/druid-lib-backup/druid-multi-stage-query-30.0.0.jar.$(date +%Y%m%d_%H%M%S)

# Install new JAR
cp druid-lib-build/druid-multi-stage-query-30.0.0.jar druid/lib/
mv druid/lib/druid-multi-stage-query-30.0.0.jar \
   druid/extensions/druid-multi-stage-query/

# Restart Broker
sudo supervisorctl restart broker

# Check status
sudo supervisorctl status broker
sudo tail -f /logs/druid/broker-stdout---supervisor-*.log
```

## Verification

### 1. Check Broker Startup

```bash
ssh prodft30-broker0.druid.singular.net
sudo tail -f /logs/druid/broker-stdout---supervisor-*.log | grep -i "multi-stage-query"
```

Look for:
```
INFO [main] - Loading extension [druid-multi-stage-query]
```

### 2. Check Overlord Startup

```bash
ssh prodft30-overlord0.druid.singular.net
sudo tail -f /logs/druid/overlord-stdout---supervisor-*.log | grep -i "multi-stage-query\|kubernetes-overlord"
```

Look for:
```
INFO [main] - Loading extension [druid-multi-stage-query]
INFO [main] - Loading extension [druid-kubernetes-overlord-extensions]
```

### 3. Test Query with Tags

```bash
# Submit test query
curl -X POST http://prodft30-broker0.druid.singular.net:8082/druid/v2/sql/task \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "INSERT INTO test_msq_tags SELECT __time, country FROM buffer_tracker_stats_1728 WHERE __time >= TIMESTAMP '\''2025-08-01'\'' LIMIT 100 PARTITIONED BY DAY",
    "context": {
      "tags": {"userProvidedTag": "medium"},
      "maxNumTasks": 2
    }
  }'
```

Expected response:
```json
[["query-xxxxx-xxxxx-xxxxx"]]
```

### 4. Watch Overlord Logs

```bash
ssh prodft30-overlord0.druid.singular.net
sudo tail -f /logs/druid/overlord-stdout---supervisor-*.log | grep -E "📝|🎯|🔍"
```

Expected logs:
```
📝 [ADAPTER] Creating Job from Task [query-xxxxx] (type=query_controller)
🔍 [SELECTOR] Task context.tags (key='tags'): {userProvidedTag=medium}
✅ [SELECTOR] Selector [prodft30-medium-peon-pod] MATCHED
📝 [ADAPTER] Template selected by strategy: prodft30-medium-peon-pod
```

## Rollback Plan (If Needed)

### Overlord Rollback

```bash
ssh prodft30-overlord0.druid.singular.net

# Find backup JARs
ls -lt ~/druid-lib-backup/

# Restore old JARs
cp ~/druid-lib-backup/druid-kubernetes-overlord-extensions-30.0.0.jar.YYYYMMDD_HHMMSS \
   druid/extensions/druid-kubernetes-overlord-extensions/druid-kubernetes-overlord-extensions-30.0.0.jar
cp ~/druid-lib-backup/druid-multi-stage-query-30.0.0.jar.YYYYMMDD_HHMMSS \
   druid/extensions/druid-multi-stage-query/druid-multi-stage-query-30.0.0.jar

# Restart
sudo supervisorctl restart overlord
```

### Broker Rollback

```bash
ssh prodft30-broker0.druid.singular.net

# Find backup JAR
ls -lt ~/druid-lib-backup/

# Restore old JAR
cp ~/druid-lib-backup/druid-multi-stage-query-30.0.0.jar.YYYYMMDD_HHMMSS \
   druid/extensions/druid-multi-stage-query/druid-multi-stage-query-30.0.0.jar

# Restart
sudo supervisorctl restart broker
```

## Common Issues

### Issue 1: Tags Still Show as Null

**Symptom**: `Task context.tags (key='tags'): null`

**Cause**: Broker not updated with new multi-stage-query JAR

**Solution**: Deploy multi-stage-query to Broker (see Step 2 above)

### Issue 2: Broker Won't Start

**Symptom**: Broker service fails to start after JAR replacement

**Cause**: JAR file corruption or permission issues

**Solution**:
```bash
# Check JAR integrity
ls -lh druid/extensions/druid-multi-stage-query/druid-multi-stage-query-30.0.0.jar

# Fix permissions
chmod 644 druid/extensions/druid-multi-stage-query/druid-multi-stage-query-30.0.0.jar

# Check logs for actual error
sudo tail -100 /logs/druid/broker-stdout---supervisor-*.log
```

### Issue 3: Template Selection Still Uses Base

**Symptom**: Logs show tags are present but base template is selected

**Cause**: Dynamic config selector conditions don't match

**Solution**:
```bash
# Check dynamic config
curl http://localhost:8090/druid/indexer/v1/k8s/taskrunner/executionconfig

# Verify tag values match exactly
# The selector expects: {"userProvidedTag": ["medium"]}
# Your query should have: {"userProvidedTag": "medium"}
# Note: The selector uses an array, but your query value is a string
```

## Summary

✅ **Build**: Both extensions (kubernetes-overlord-extensions + multi-stage-query)

✅ **Deploy to Overlord**: Both JARs

✅ **Deploy to Broker**: multi-stage-query JAR

✅ **Test**: Submit query with tags in context

✅ **Verify**: Check logs for tag propagation and template selection

