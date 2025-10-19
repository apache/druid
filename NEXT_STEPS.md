# 🚀 Next Steps - Quick Reference

## Current Status

✅ **DONE:** Git cherry-pick of PR #18379 onto Druid 30.0.0  
⏳ **TODO:** Install build tools and build the modules

---

## Step 1: Install Prerequisites (15 minutes)

Copy and paste these commands into your terminal:

```bash
# Install Java 11
brew install openjdk@11

# Configure Java 11
export JAVA_HOME=/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"

# Make it permanent
echo 'export JAVA_HOME=/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home' >> ~/.zshrc
echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.zshrc

# Install Maven
brew install maven

# Verify
java -version
mvn -version
```

---

## Step 2: Build (Choose ONE option)

### 🎯 Option A: Quick Module Build (Recommended for Testing)

**Time:** ~5-10 minutes  
**Use Case:** Quick validation, POC, staging environment

```bash
cd /Users/ronshub/workspace/druid

# Build only the changed modules
mvn clean install -pl processing -DskipTests
mvn clean install -pl indexing-service -DskipTests
```

**Output JARs:**
- `processing/target/druid-processing-30.0.0.jar`
- `indexing-service/target/druid-indexing-service-30.0.0.jar`

---

### 🏭 Option B: Full Distribution Build (Recommended for Production)

**Time:** ~15-30 minutes (first time)  
**Use Case:** Production deployment, complete testing

```bash
cd /Users/ronshub/workspace/druid

# Build complete distribution
mvn clean install -DskipTests
```

**Output:**
- Complete distribution: `distribution/target/apache-druid-30.0.0-bin.tar.gz`
- All JARs in: `distribution/target/apache-druid-30.0.0/lib/`

---

## Step 3: Test Build Success

```bash
# Check if JARs were created
ls -lh processing/target/druid-processing-30.0.0.jar
ls -lh indexing-service/target/druid-indexing-service-30.0.0.jar

# Or for full build:
ls -lh distribution/target/apache-druid-30.0.0-bin.tar.gz
```

---

## Step 4: Deploy to Your Cluster

### For Module Build (Option A):

```bash
# Backup originals first!
ssh your-overlord-host "sudo cp /opt/druid/lib/druid-processing-30.0.0.jar /opt/druid/lib/druid-processing-30.0.0.jar.backup"
ssh your-overlord-host "sudo cp /opt/druid/lib/druid-indexing-service-30.0.0.jar /opt/druid/lib/druid-indexing-service-30.0.0.jar.backup"

# Copy new JARs
scp processing/target/druid-processing-30.0.0.jar your-overlord-host:/tmp/
scp indexing-service/target/druid-indexing-service-30.0.0.jar your-overlord-host:/tmp/

# Install them
ssh your-overlord-host "sudo cp /tmp/druid-processing-30.0.0.jar /opt/druid/lib/"
ssh your-overlord-host "sudo cp /tmp/druid-indexing-service-30.0.0.jar /opt/druid/lib/"

# Restart Overlord
ssh your-overlord-host "sudo supervisorctl restart overlord"
```

### For Full Build (Option B):

```bash
# Copy entire distribution
scp distribution/target/apache-druid-30.0.0-bin.tar.gz your-overlord-host:/tmp/

# Extract and deploy per your standard procedure
```

---

## Step 5: Verify Fix Works

```bash
# Submit a test MSQ query
curl -X POST http://your-broker:8888/druid/v2/sql/statements \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "INSERT INTO test_table SELECT * FROM source_table LIMIT 10"
  }'

# Record the queryId from response

# Check task report (should return JSON, not 404!)
curl "http://your-overlord:8090/druid/indexer/v1/task/QUERY_ID/reports"
```

**Success = JSON response with task details**  
**Failure = 404 or "No task reports were found"**

---

## Common Issues

### Issue: "java: command not found"
**Solution:** Make sure you ran the export commands AND restarted your terminal

```bash
source ~/.zshrc
java -version
```

### Issue: "JAVA_HOME not set correctly"
**Solution:** Find your Java 11 installation

```bash
/usr/libexec/java_home -V
export JAVA_HOME=$(/usr/libexec/java_home -v 11)
```

### Issue: Maven build fails with "package does not exist"
**Solution:** Build dependencies first

```bash
# Build parent POM
mvn clean install -N -DskipTests

# Then build modules
mvn clean install -pl processing -DskipTests
mvn clean install -pl indexing-service -DskipTests
```

---

## Quick Commands Cheat Sheet

```bash
# Check current branch (should show druid-30.0.0-task-reports-fix)
git branch

# See what changed
git diff druid-30.0.0..HEAD --stat

# See the key fix
git show HEAD

# Start build
mvn clean install -pl processing -DskipTests

# Check Maven is working
mvn --version

# Check Java is correct version
java -version | grep "11.0"
```

---

## 📋 Pre-Build Checklist

Before running Maven:

- [ ] Java 11 installed (`java -version` shows 11.0.x)
- [ ] Maven installed (`mvn -version` works)
- [ ] JAVA_HOME set correctly (`echo $JAVA_HOME` shows path)
- [ ] In correct directory (`pwd` shows /Users/ronshub/workspace/druid)
- [ ] On correct branch (`git branch` shows * druid-30.0.0-task-reports-fix)

---

## 🎯 Expected Timeline

| Task | Time | Status |
|------|------|--------|
| Git cherry-pick | 5 min | ✅ DONE |
| Conflict resolution | 10 min | ✅ DONE |
| Install Java + Maven | 15 min | ⏳ TODO |
| Build modules | 5-10 min | ⏳ TODO |
| Build full dist | 15-30 min | ⏳ TODO |
| Deploy to cluster | 10 min | ⏳ TODO |
| Test verification | 5 min | ⏳ TODO |

**Total Time:** 1-1.5 hours

---

## 📞 Need Help?

1. Check `PROGRESS_SUMMARY.md` for detailed explanations
2. Check `CHERRY_PICK_EXTENSION_GUIDE.md` for original plan
3. Review the fix: `git show HEAD` to see what changed
4. Check logs: `tail -f /var/log/druid/overlord.log` after deployment

---

**Last Updated:** October 19, 2025  
**Your Location:** `/Users/ronshub/workspace/druid`  
**Your Branch:** `druid-30.0.0-task-reports-fix`  
**Next:** Install Java 11 and Maven, then build

