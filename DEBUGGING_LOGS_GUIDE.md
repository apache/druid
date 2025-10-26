# Debugging Logs Guide - Pod Template Selection

## What Was Added

Added comprehensive logging to diagnose why `context.tags` selector isn't matching tasks.

## Files Modified

1. **`Selector.java`**
   - Added detailed logging for each selector evaluation
   - Shows task context keys, tags, and matching results

2. **`SelectorBasedPodTemplateSelectStrategy.java`**
   - Added logging for strategy execution
   - Shows which selectors were evaluated and which one matched

3. **`PodTemplateTaskAdapter.java`**  
   - Added logging for template selection entry point
   - Shows dynamic config status and final template selection

## Log Markers

All logs use emoji markers for easy searching:

- **`📝 [ADAPTER]`** - PodTemplateTaskAdapter (entry point)
- **`🎯 [STRATEGY]`** - SelectorBasedPodTemplateSelectStrategy (strategy execution)
- **`🔍 [SELECTOR]`** - Selector (individual selector evaluation)

## What to Look For in Logs

After deploying and running a task, search for these patterns:

### 1. Entry Point - Adapter
```
📝 [ADAPTER] Creating Job from Task [task-id] (type=query_controller, dataSource=...)
📝 [ADAPTER] Dynamic config present: true
📝 [ADAPTER] Using dynamic config strategy: SelectorBasedPodTemplateSelectStrategy
📝 [ADAPTER] Available templates in adapter: [base, prodft30-small-peon-pod, prodft30-medium-peon-pod]
```

### 2. Strategy Execution
```
🎯 [STRATEGY] SelectorBasedPodTemplateSelectStrategy starting for task [task-id]
🎯 [STRATEGY] Available templates: [base, prodft30-small-peon-pod, prodft30-medium-peon-pod]
🎯 [STRATEGY] Number of selectors to evaluate: 2
```

### 3. Selector Evaluation - THE KEY PART
```
🔍 [SELECTOR] Evaluating selector [prodft30-small-peon-pod] for task [task-id] (type=query_controller, dataSource=tracker_stats_364960)
🔍 [SELECTOR] Checking context.tags conditions for selector [prodft30-small-peon-pod]: expected={userProvidedTag=[small]}
🔍 [SELECTOR] Full task context keys: [queryId, maxParseExceptions, ...]
🔍 [SELECTOR] Task context.tags (key='tags'): {userProvidedTag=medium}
🔍 [SELECTOR] Checking tag [userProvidedTag]: expected=[small], actual=medium, matches=false
❌ [SELECTOR] Selector [prodft30-small-peon-pod] FAILED: context.tags did not match
```

### 4. Final Result
```
🎯 [STRATEGY] Selector [prodft30-medium-peon-pod] evaluation result: MATCHED ✅
🎯 [STRATEGY] Selected template key: prodft30-medium-peon-pod
✅ [STRATEGY] Final template selected: prodft30-medium-peon-pod for task [task-id]
📝 [ADAPTER] Template selected by strategy: prodft30-medium-peon-pod
```

## Key Questions These Logs Answer

1. **Is dynamic config loaded?**
   - Look for: `📝 [ADAPTER] Dynamic config present: true`
   - If false, dynamic config API isn't being used

2. **What strategy is being used?**
   - Look for: `📝 [ADAPTER] Using dynamic config strategy: SelectorBasedPodTemplateSelectStrategy`
   - Should NOT be `TaskTypePodTemplateSelectStrategy`

3. **Are templates loaded?**
   - Look for: `📝 [ADAPTER] Available templates in adapter:`
   - Should see: `[base, prodft30-small-peon-pod, prodft30-medium-peon-pod]`

4. **Does the task have context.tags?** ⭐ MOST IMPORTANT
   - Look for: `🔍 [SELECTOR] Task context.tags (key='tags'): ...`
   - If `null` → Task doesn't have tags!
   - If `{userProvidedTag=medium}` → Task HAS tags ✅

5. **Why isn't a selector matching?**
   - Look for: `🔍 [SELECTOR] Checking tag [userProvidedTag]: expected=[small], actual=medium, matches=false`
   - This shows exactly which value was expected vs actual

6. **Which template was ultimately selected?**
   - Look for: `📝 [ADAPTER] Template selected by strategy: prodft30-medium-peon-pod`
   - If it's always `base`, no selectors are matching

## How to Search Logs

On the Overlord:

```bash
# Search for all template selection logs for a specific task
sudo grep -i "task-id-here" /logs/druid/overlord-stdout---supervisor-*.log | grep -E "📝|🎯|🔍"

# Search for all selector evaluations
sudo grep "🔍 \[SELECTOR\]" /logs/druid/overlord-stdout---supervisor-*.log | tail -50

# Search for failures
sudo grep "❌ \[SELECTOR\]" /logs/druid/overlord-stdout---supervisor-*.log | tail -20

# Search for successful matches
sudo grep "✅" /logs/druid/overlord-stdout---supervisor-*.log | tail -20
```

## Expected Flow (If Working)

```
📝 [ADAPTER] Creating Job...
📝 [ADAPTER] Dynamic config present: true
📝 [ADAPTER] Using dynamic config strategy: SelectorBasedPodTemplateSelectStrategy
🎯 [STRATEGY] SelectorBasedPodTemplateSelectStrategy starting...
🔍 [SELECTOR] Evaluating selector [prodft30-small-peon-pod]...
🔍 [SELECTOR] Task context.tags (key='tags'): {userProvidedTag=medium}
🔍 [SELECTOR] Checking tag [userProvidedTag]: expected=[small], actual=medium, matches=false
❌ [SELECTOR] Selector [prodft30-small-peon-pod] FAILED
🎯 [STRATEGY] Selector [prodft30-small-peon-pod] evaluation result: NOT MATCHED ❌
🔍 [SELECTOR] Evaluating selector [prodft30-medium-peon-pod]...
🔍 [SELECTOR] Task context.tags (key='tags'): {userProvidedTag=medium}
🔍 [SELECTOR] Checking tag [userProvidedTag]: expected=[medium], actual=medium, matches=true
✅ [SELECTOR] Selector [prodft30-medium-peon-pod] MATCHED
🎯 [STRATEGY] Selector [prodft30-medium-peon-pod] evaluation result: MATCHED ✅
✅ [STRATEGY] Final template selected: prodft30-medium-peon-pod
📝 [ADAPTER] Template selected by strategy: prodft30-medium-peon-pod
```

## Most Likely Issues

Based on this logging, you'll quickly see:

### Issue 1: Task has no context.tags
```
🔍 [SELECTOR] Task context.tags (key='tags'): null
❌ [SELECTOR] Selector [...] FAILED: Task has no context.tags or tags are empty
```
**Solution:** Task needs to include `context.tags` with `userProvidedTag`

### Issue 2: Wrong tag key
```
🔍 [SELECTOR] Full task context keys: [queryId, maxParseExceptions, somethingElse]
🔍 [SELECTOR] Task context.tags (key='tags'): null
```
**Solution:** Context doesn't have a "tags" field - might be named differently

### Issue 3: Wrong tag value
```
🔍 [SELECTOR] Checking tag [userProvidedTag]: expected=[medium], actual=small, matches=false
```
**Solution:** Task has tags, but wrong value

### Issue 4: Dynamic config not loaded
```
📝 [ADAPTER] Dynamic config present: false
📝 [ADAPTER] Using DEFAULT strategy (TaskTypePodTemplateSelectStrategy)
```
**Solution:** Dynamic config API isn't being read by the adapter
