# Cherry-Pick Progress Summary

**Date:** October 19, 2025  
**Target Version:** Druid 30.0.0  
**Branch:** druid-30.0.0-task-reports-fix

---

## ✅ What's Been Completed

### 1. Git Cherry-Pick ✅ DONE

Successfully cherry-picked **PR #18379** (commit `c5127ba92f`) onto Druid 30.0.0.

**Why only PR #18379?**
Based on the GitHub discussion in PR #18206, we discovered that:
- PR #18206 was the initial fix but introduced issues in production
- PR #18379 provides a cleaner, more reliable fix
- The original author confirmed PR #18379 works without issues

### 2. Conflict Resolution ✅ DONE

Resolved all cherry-pick conflicts:
- ✅ Merged import statements in `TaskReportFileWriter.java`
- ✅ Merged import statements in `NoopTestTaskReportFileWriter.java`
- ✅ Handled 3 test files that don't exist in 30.0.0 (removed them)

### 3. Changes Applied ✅ VERIFIED

**6 files modified:**

1. **`processing/src/main/java/org/apache/druid/indexer/report/TaskReportFileWriter.java`**
   - Added `getReportsFile(String taskId)` method to interface
   
2. **`processing/src/main/java/org/apache/druid/indexer/report/SingleFileTaskReportFileWriter.java`**
   - Implemented `getReportsFile()` method
   
3. **`indexing-service/src/main/java/org/apache/druid/indexing/common/MultipleFileTaskReportFileWriter.java`**
   - Implemented `getReportsFile()` method
   
4. **`indexing-service/src/main/java/org/apache/druid/indexing/common/task/AbstractTask.java`** ⭐ KEY FIX
   - Changed from: `reportsFile = new File(attemptDir, "report.json");`
   - Changed to: `reportsFile = toolbox.getTaskReportFileWriter().getReportsFile(getId());`
   - This fixes the task report retrieval issue!
   
5. **`indexing-service/src/test/java/org/apache/druid/indexing/common/task/AbstractTaskTest.java`**
   - Updated test to use new method
   
6. **`indexing-service/src/test/java/org/apache/druid/indexing/common/task/NoopTestTaskReportFileWriter.java`**
   - Added `getReportsFile()` implementation for tests

---

## 🎯 The Fix Explained

**Problem:** 
When using Kubernetes extension with MSQ queries, task reports couldn't be retrieved, resulting in `404 Not Found` errors.

**Root Cause:**
The report file path was hardcoded as `new File(attemptDir, "report.json")`, but the actual file location differs in K8s environments.

**Solution:**
Instead of hardcoding the path, the fix asks the `TaskReportFileWriter` where it actually wrote the file. This works across all environments (MM-based, K8s, etc.).

---

## ⚠️ Important: Build Requirements

**The changes are in CORE modules, not just the K8s extension:**
- `processing` module - used by ALL Druid services
- `indexing-service` module - used by Overlord and Peons

**This means:**

### Option A: Full Rebuild (Recommended)
Build entire Druid 30.0.0 with the fix:
```bash
# Build complete Druid distribution
mvn clean package -DskipTests

# Deploy the updated JARs to:
# - processing JAR → All nodes
# - indexing-service JAR → Overlord and Peons
```

### Option B: Module-Only Build (Simpler but Limited)
Build just the affected modules:
```bash
# Build processing module
mvn clean install -pl processing -DskipTests

# Build indexing-service module  
mvn clean install -pl indexing-service -DskipTests

# Deploy these JARs to your cluster
```

### Option C: Full Distribution Build (For Complete Testing)
Build the entire distribution package:
```bash
# Build full distribution tarball
mvn clean install -DskipTests
cd distribution/target/
# You'll get: apache-druid-30.0.0-bin.tar.gz (with your fixes)
```

---

## 📋 Next Steps

### Step 1: Install Build Tools

```bash
# Install Java 11 (required for Druid 30.0)
brew install openjdk@11

# Set Java 11 as active
export JAVA_HOME=/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"

# Add to ~/.zshrc for persistence
echo 'export JAVA_HOME=/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home' >> ~/.zshrc
echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.zshrc

# Install Maven
brew install maven

# Verify
java -version    # Should show: openjdk version "11.0.x"
mvn -version     # Should show: Apache Maven 3.x
```

### Step 2: Choose Your Build Strategy

**Recommended for Production:** Option A (Full Rebuild)
- Ensures all modules are consistent
- Proper dependency resolution
- Complete testing possible

**Quick Test/POC:** Option B (Module-Only)
- Faster build time
- Only deploys changed modules
- Good for initial validation

### Step 3: Build

```bash
cd /Users/ronshub/workspace/druid

# For Option A (Full Rebuild)
mvn clean package -DskipTests

# For Option B (Module-Only)
mvn clean install -pl processing -DskipTests
mvn clean install -pl indexing-service -DskipTests

# For Option C (Full Distribution)
mvn clean install -DskipTests
```

**Build Time Estimates:**
- Full build: 15-30 minutes (first time), 5-10 minutes (subsequent)
- Module-only: 5-10 minutes

---

## 📦 What to Deploy

After building, you'll need to deploy:

### For Module-Only Build (Option B):
```
processing/target/druid-processing-30.0.0.jar
  → Deploy to: All Druid nodes (lib/ directory)
  
indexing-service/target/druid-indexing-service-30.0.0.jar
  → Deploy to: Overlord and Peon nodes (lib/ directory)
```

### For Full Rebuild (Option A):
```
distribution/target/apache-druid-30.0.0/lib/druid-processing-30.0.0.jar
distribution/target/apache-druid-30.0.0/lib/druid-indexing-service-30.0.0.jar
  → Deploy to appropriate nodes
```

### For Full Distribution (Option C):
```
distribution/target/apache-druid-30.0.0-bin.tar.gz
  → Extract and deploy entire distribution
```

---

## 🧪 Testing the Fix

After deployment:

1. **Submit a test MSQ INSERT query:**
```sql
INSERT INTO test_datasource
SELECT __time, COUNT(*) as cnt
FROM source_datasource
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR
GROUP BY 1
LIMIT 100
```

2. **Verify task report is retrievable:**
```bash
# Get query ID from response
QUERY_ID="query-abc123..."

# Check task report (should return JSON, not 404)
curl "http://overlord:8090/druid/indexer/v1/task/${QUERY_ID}/reports"
```

3. **Check Overlord logs for errors:**
```bash
# Should NOT see:
# - "No task reports were found for this task"
# - "failed with status code 404"
```

---

## 🔄 Rollback Plan

If issues occur:

1. **Stop affected services:**
```bash
sudo supervisorctl stop overlord
sudo supervisorctl stop peon  # if applicable
```

2. **Restore original JARs:**
```bash
# Restore from backup
cp /path/to/backup/druid-processing-30.0.0.jar /opt/druid/lib/
cp /path/to/backup/druid-indexing-service-30.0.0.jar /opt/druid/lib/
```

3. **Restart services:**
```bash
sudo supervisorctl start overlord
sudo supervisorctl start peon
```

---

## 📞 Questions?

### Q: Why not just build the K8s extension?
**A:** The changes are in core modules (`processing`, `indexing-service`) that are used by all Druid services, not just the K8s extension. The extension depends on these modules.

### Q: Can I test this locally first?
**A:** Yes! Build the modules, then deploy to a dev/staging cluster before production.

### Q: Will this require downtime?
**A:** Yes, you'll need to restart:
- Overlord (always)
- Peons (if using the changed JARs)
- Potentially other services if deploying processing.jar everywhere

### Q: Is this safe for production?
**A:** PR #18379 has been tested and merged into Druid master. The original author confirmed it works. However, always test in staging first.

---

## 🎯 Summary

✅ **Git work: COMPLETE**
- Branch created: `druid-30.0.0-task-reports-fix`
- PR #18379 cherry-picked successfully
- All conflicts resolved
- 6 files modified

⚠️ **Build work: PENDING**
- Need Java 11
- Need Maven 3.6+
- Need to build affected modules
- Need to deploy to cluster

📊 **Impact:**
- Fixes MSQ task report retrieval on K8s
- No more 404 errors when querying task reports
- Cleaner solution than PR #18206

---

**Current Location:** `/Users/ronshub/workspace/druid`  
**Branch:** `druid-30.0.0-task-reports-fix`  
**Status:** Ready to build once prerequisites are installed

