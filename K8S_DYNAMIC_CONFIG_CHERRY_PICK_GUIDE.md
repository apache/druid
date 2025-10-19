# Cherry-Pick K8s Dynamic Config Feature onto Druid 30.0.0

## 🎯 **Objective**

Cherry-pick the Kubernetes Task Runner Dynamic Config feature onto Druid 30.0.0, enabling you to dynamically tune pod template selection and K8s execution settings without restarting the Overlord.

---

## 📋 **What is K8s Dynamic Config?**

The K8s Dynamic Config feature allows Druid operators to:
- **Dynamically select pod templates** based on task properties (tags, task type, data source)
- **Update execution configurations** without Overlord restarts
- **Use selector-based matching** for fine-grained pod template control
- **Track configuration history** with audit information

### **Key APIs Provided:**
- `GET /druid/indexer/v1/k8s/taskrunner/executionconfig` - Retrieve current config
- `POST /druid/indexer/v1/k8s/taskrunner/executionconfig` - Update config dynamically
- `GET /druid/indexer/v1/k8s/taskrunner/executionconfig/history` - View config history

### **Use Cases:**
- Route heavy ingestion tasks to high-resource pod templates
- Route light queries to small pod templates  
- Apply different pod configurations based on data source
- Change pod templates on-the-fly during maintenance windows

---

## 📊 **What We're Doing**

```
Druid 30.0.0 (June 8, 2024)
    ↓
+ Cherry-pick 6 PRs (June-November 2024)
    ↓
= Druid 30.0.0 with K8s Dynamic Config
    ↓
Build kubernetes-overlord-extensions module
    ↓
Deploy to your Druid 30.0.0 cluster
```

### **PRs to Cherry-Pick (in order):**

| PR # | Date | Author | Description | Lines Changed |
|------|------|--------|-------------|---------------|
| [#16510](https://github.com/apache/druid/pull/16510) | June 12, 2024 | YongGang | Initial implementation | +1,485 / -40 |
| [#16600](https://github.com/apache/druid/pull/16600) | June 21, 2024 | Suneet Saldanha | Documentation enhancement | +384 / -84 |
| [#16720](https://github.com/apache/druid/pull/16720) | July 10, 2024 | YongGang | Fix dynamic config URLs | +9 / -9 |
| [#16772](https://github.com/apache/druid/pull/16772) | July 23, 2024 | George Shiqi Wu | Add pod template annotations | +291 / -14 |
| [#17400](https://github.com/apache/druid/pull/17400) | Oct 30, 2024 | Kiran Gadhave | Fix empty collection handling | +62 / -3 |
| [#17464](https://github.com/apache/druid/pull/17464) | Nov 12, 2024 | Kiran Gadhave | Document empty collection behavior | +34 / 0 |

**Total Changes:** 6 PRs, ~2,265 lines added

---

## 📋 **Prerequisites**

### **Required Tools**
- Git ✅ (Installed)
- Maven 3.6+ ⚠️ (Need to install)
- Java JDK 11 (same as Druid 30.0) ⚠️ (Need to install)
- SSH access to Overlord server
- kubectl (for K8s access) ⚠️

### **Install Prerequisites**

```bash
# Install Homebrew if not already installed
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install Java 11 (required for Druid 30.0)
brew install openjdk@11

# Add Java 11 to PATH (add to ~/.zshrc for persistence)
export JAVA_HOME=/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"

# Install Maven
brew install maven

# Verify installations
java -version    # Should show: openjdk version "11.0.x"
mvn -version     # Should show: Apache Maven 3.6.x or higher
```

---

## ⚠️ **Important Considerations**

### **Why Cherry-Pick Instead of Upgrading?**

Upgrading from 30.0.0 to 31.0.0+ requires:
- Testing across all components (Brokers, Historicals, Coordinators)
- Potential breaking changes in queries or APIs
- Cluster-wide downtime for rolling upgrades
- Extensive regression testing

Cherry-picking this feature:
- ✅ Only affects the Overlord
- ✅ Minimal risk (isolated to K8s extension)
- ✅ No breaking changes to query layer
- ✅ ~5 minute Overlord restart (no full cluster downtime)

### **Risk Assessment**

| Risk Level | Component | Mitigation |
|------------|-----------|------------|
| **Low** | Overlord | Backup extension, quick rollback available |
| **None** | Brokers | Not affected |
| **None** | Historicals | Not affected |
| **None** | Coordinators | Not affected |
| **Low** | Running tasks | Tasks decouple from Overlord, continue running |

---

## 🚀 **Step-by-Step Instructions**

### **STEP 1: Clone Druid Repository**

```bash
# Create working directory
mkdir -p ~/druid-k8s-dynamic-config
cd ~/druid-k8s-dynamic-config

# Clone Druid repository
git clone https://github.com/apache/druid.git
cd druid

# Verify clone succeeded
git log --oneline -5

# Verify 30.0.0 tag exists
git tag | grep "druid-30.0.0"
# Should output: druid-30.0.0
```

**✅ Checkpoint**: Repository cloned successfully

---

### **STEP 2: Create Custom Branch from 30.0.0**

```bash
# Checkout Druid 30.0.0
git checkout druid-30.0.0

# Verify you're on the right version
git describe --tags
# Should output: druid-30.0.0

# Create custom branch for dynamic config feature
git checkout -b druid-30.0.0-k8s-dynamic-config

# Verify branch created
git branch
# Should show: * druid-30.0.0-k8s-dynamic-config
```

**✅ Checkpoint**: On custom branch based on 30.0.0

---

### **STEP 3: Verify Current State (No Dynamic Config)**

```bash
# Check if dynamic config files exist in 30.0.0
echo "=== Checking for dynamic config files ==="
git ls-tree -r HEAD --name-only | grep -i "KubernetesTaskExecutionConfigResource\|PodTemplateSelectStrategy"

# Should return empty (these files don't exist yet)

# Verify extension exists
ls extensions-contrib/kubernetes-overlord-extensions/src/main/java/org/apache/druid/k8s/overlord/

# Should show existing K8s extension files but no execution/ directory
```

**✅ Checkpoint**: Confirmed dynamic config not present in 30.0.0

---

### **STEP 4: Identify Commit Hashes**

```bash
# Find all PR commit hashes
echo "=== Finding commit hashes ==="

echo "PR #16510:"
git log --all --grep="16510" --oneline --format="%H %s" | head -1

echo "PR #16600:"
git log --all --grep="16600" --oneline --format="%H %s" | head -1

echo "PR #16720:"
git log --all --grep="16720" --oneline --format="%H %s" | head -1

echo "PR #16772:"
git log --all --grep="16772" --oneline --format="%H %s" | head -1

echo "PR #17400:"
git log --all --grep="17400" --oneline --format="%H %s" | head -1

echo "PR #17464:"
git log --all --grep="17464" --oneline --format="%H %s" | head -1
```

**Expected Output:**
```
PR #16510:
46dbc7405390200eda87a04cb6eb70f8be7d3d18 Support Dynamic Peon Pod Template Selection in K8s extension (#16510)

PR #16600:
4e0ea7823b128cc214df8dc774d370b306b91461 Update docs for K8s TaskRunner Dynamic Config (#16600)

PR #16720:
4b293fc2a937da64cee8c1cd257a3ce85e333884 Docs: Fix k8s dynamic config URL (#16720)

PR #16772:
a64e9a17462d34c246a8793b3efebcb4ad4a3736 Add annotation for pod template (#16772)

PR #17400:
5fcf4205e4b1f17edccc4a37cf338ed5e3047ae1 Handle empty values for task and datasource conditions in pod template selector (#17400)

PR #17464:
1dbd005df6a7821ddbdd1599de132403abde1fb2 updated docs with behavior for empty collections in pod template selector config (#17464)
```

**📝 Record these commit hashes - you'll need them!**

**✅ Checkpoint**: Commit hashes identified

---

### **STEP 5: Set Commit Variables**

```bash
# Set all commit hashes as variables for easy reference
COMMIT_16510="46dbc7405390200eda87a04cb6eb70f8be7d3d18"
COMMIT_16600="4e0ea7823b128cc214df8dc774d370b306b91461"
COMMIT_16720="4b293fc2a937da64cee8c1cd257a3ce85e333884"
COMMIT_16772="a64e9a17462d34c246a8793b3efebcb4ad4a3736"
COMMIT_17400="5fcf4205e4b1f17edccc4a37cf338ed5e3047ae1"
COMMIT_17464="1dbd005df6a7821ddbdd1599de132403abde1fb2"

# Verify all variables set
echo "=== Commit variables set ==="
echo "16510: $COMMIT_16510"
echo "16600: $COMMIT_16600"
echo "16720: $COMMIT_16720"
echo "16772: $COMMIT_16772"
echo "17400: $COMMIT_17400"
echo "17464: $COMMIT_17464"
```

**✅ Checkpoint**: Variables configured

---

### **STEP 6: Examine What Will Change**

```bash
# See what files each PR will modify
echo "=== PR #16510 - Initial Implementation ==="
git show $COMMIT_16510 --stat | head -25

echo "=== PR #16600 - Documentation Enhancement ==="
git show $COMMIT_16600 --stat

echo "=== PR #16720 - URL Fixes ==="
git show $COMMIT_16720 --stat

echo "=== PR #16772 - Annotation Support ==="
git show $COMMIT_16772 --stat

echo "=== PR #17400 - Empty Collection Bug Fix ==="
git show $COMMIT_17400 --stat

echo "=== PR #17464 - Documentation Update ==="
git show $COMMIT_17464 --stat
```

**✅ Checkpoint**: Understood changes

---

### **STEP 7: Cherry-Pick PR #16510 (Initial Implementation)**

This is the **most important** PR - it adds the core dynamic config functionality.

```bash
echo "=== Cherry-picking PR #16510 - Initial Implementation ==="
git cherry-pick $COMMIT_16510

# Check result
if [ $? -eq 0 ]; then
    echo "✅ PR #16510 cherry-pick succeeded!"
    git log --oneline -1
    echo ""
    echo "Files added:"
    git show --name-status --format="" HEAD
else
    echo "⚠️ Conflicts detected!"
    git status
    echo ""
    echo "See CONFLICT RESOLUTION section below"
fi
```

#### **⚠️ CONFLICT RESOLUTION (if needed)**

```bash
# Check conflicts
git status

# Common conflicts:
# 1. KubernetesTaskRunnerFactory.java - dependency injection changes
# 2. pom.xml - dependency version differences

# For each conflicted file:
# 1. Open in editor: vim <file>
# 2. Look for conflict markers: <<<<<<<, =======, >>>>>>>
# 3. Choose the correct code (usually take NEW code from the cherry-pick)
# 4. Remove conflict markers
# 5. Save file

# After resolving ALL conflicts:
git add .
git status  # Should show "all conflicts fixed"
git cherry-pick --continue

# If you need to abort:
# git cherry-pick --abort
```

**Key Files Added in This PR:**
- `KubernetesTaskExecutionConfigResource.java` - REST API endpoints
- `KubernetesTaskRunnerDynamicConfig.java` - Config interface
- `DefaultKubernetesTaskRunnerDynamicConfig.java` - Default config
- `PodTemplateSelectStrategy.java` - Strategy interface
- `SelectorBasedPodTemplateSelectStrategy.java` - Selector matching
- `TaskTypePodTemplateSelectStrategy.java` - Task type strategy
- `Selector.java` - Matching logic
- `PodTemplateWithName.java` - Pod template wrapper

**✅ Checkpoint**: Core dynamic config implemented

---

### **STEP 8: Cherry-Pick PR #16600 (Documentation Enhancement)**

```bash
echo "=== Cherry-picking PR #16600 - Documentation ==="
git cherry-pick $COMMIT_16600

if [ $? -eq 0 ]; then
    echo "✅ PR #16600 cherry-pick succeeded!"
    git log --oneline -1
else
    echo "⚠️ Conflicts detected - resolve and continue"
    git status
fi
```

**What This Adds:**
- Comprehensive API documentation with examples
- cURL and HTTP request samples
- Detailed strategy explanations

**✅ Checkpoint**: Documentation enhanced

---

### **STEP 9: Cherry-Pick PR #16720 (URL Fixes)**

```bash
echo "=== Cherry-picking PR #16720 - URL Fixes ==="
git cherry-pick $COMMIT_16720

if [ $? -eq 0 ]; then
    echo "✅ PR #16720 cherry-pick succeeded!"
    git log --oneline -1
else
    echo "⚠️ Conflicts detected - resolve and continue"
    git status
fi
```

**What This Fixes:**
- Corrects API endpoint URLs in documentation

**✅ Checkpoint**: URLs corrected

---

### **STEP 10: Cherry-Pick PR #16772 (Annotation Support)**

```bash
echo "=== Cherry-picking PR #16772 - Annotations ==="
git cherry-pick $COMMIT_16772

if [ $? -eq 0 ]; then
    echo "✅ PR #16772 cherry-pick succeeded!"
    git log --oneline -1
else
    echo "⚠️ Conflicts detected - resolve and continue"
    git status
fi
```

**What This Adds:**
- Support for pod template annotations
- Enhanced metadata tracking

**✅ Checkpoint**: Annotation support added

---

### **STEP 11: Cherry-Pick PR #17400 (Bug Fix)**

```bash
echo "=== Cherry-picking PR #17400 - Bug Fix ==="
git cherry-pick $COMMIT_17400

if [ $? -eq 0 ]; then
    echo "✅ PR #17400 cherry-pick succeeded!"
    git log --oneline -1
else
    echo "⚠️ Conflicts detected - resolve and continue"
    git status
fi
```

**What This Fixes:**
- Handles empty collections in selectors correctly
- Empty arrays now mean "match all" for that dimension

**✅ Checkpoint**: Critical bug fixed

---

### **STEP 12: Cherry-Pick PR #17464 (Documentation)**

```bash
echo "=== Cherry-picking PR #17464 - Documentation Update ==="
git cherry-pick $COMMIT_17464

if [ $? -eq 0 ]; then
    echo "✅ PR #17464 cherry-pick succeeded!"
    git log --oneline -1
else
    echo "⚠️ Conflicts detected - resolve and continue"
    git status
fi
```

**What This Adds:**
- Documents empty collection behavior

**✅ Checkpoint**: All PRs applied!

---

### **STEP 13: Verify All Changes Applied**

```bash
echo "=== Verifying all cherry-picks ==="

# Show commit log
git log druid-30.0.0..HEAD --oneline

# Should show 6 commits (one for each PR)

# Verify new files exist
echo "=== Checking for new files ==="
ls -la extensions-contrib/kubernetes-overlord-extensions/src/main/java/org/apache/druid/k8s/overlord/execution/

# Should show:
# - KubernetesTaskExecutionConfigResource.java
# - PodTemplateSelectStrategy.java
# - Selector.java
# - SelectorBasedPodTemplateSelectStrategy.java
# - TaskTypePodTemplateSelectStrategy.java
# - DefaultKubernetesTaskRunnerDynamicConfig.java
# - KubernetesTaskRunnerDynamicConfig.java

# Check documentation
grep -i "Dynamic config" docs/development/extensions-contrib/k8s-jobs.md
# Should show section about dynamic config

echo "✅ All changes verified!"
```

**✅ Checkpoint**: All changes applied and verified

---

### **STEP 14: Build the Extension**

```bash
# Clean any previous builds
mvn clean

# Build ONLY the kubernetes-overlord-extensions module
echo "=== Building kubernetes-overlord-extensions with dynamic config ==="
mvn package -pl extensions-contrib/kubernetes-overlord-extensions -am -DskipTests

# Check build result
if [ $? -eq 0 ]; then
    echo "✅ Build succeeded!"
    echo ""
    echo "Built JAR location:"
    find extensions-contrib/kubernetes-overlord-extensions/target/ \
        -name "druid-kubernetes-overlord-extensions-30.0.0.jar" \
        -type f \
        ! -name "*sources*" \
        ! -name "*tests*"
else
    echo "❌ Build failed!"
    echo "Check errors above"
    echo ""
    echo "Common issues:"
    echo "  - Compilation errors from conflict resolution"
    echo "  - Java version mismatch (need Java 11)"
    echo "  - Maven version too old (need 3.6+)"
    exit 1
fi
```

**Build Time**: Approximately 5-15 minutes (depends on Maven cache)

**✅ Checkpoint**: Extension built successfully

---

### **STEP 15: Locate and Verify Built JAR**

```bash
# Find the built JAR
echo "=== Locating built JAR ==="
EXTENSION_JAR=$(find extensions-contrib/kubernetes-overlord-extensions/target/ \
    -name "druid-kubernetes-overlord-extensions-30.0.0.jar" \
    -type f \
    ! -name "*sources*" \
    ! -name "*tests*")

if [ -z "$EXTENSION_JAR" ]; then
    echo "❌ JAR not found!"
    echo "Searching for any JAR files:"
    find extensions-contrib/kubernetes-overlord-extensions/target/ -name "*.jar" -type f
    exit 1
fi

echo "✅ Found JAR: $EXTENSION_JAR"

# Show JAR details
ls -lh "$EXTENSION_JAR"

# Verify dynamic config classes are in JAR
echo "=== Verifying dynamic config classes in JAR ==="
jar tf "$EXTENSION_JAR" | grep -i "execution/KubernetesTaskExecutionConfigResource"
jar tf "$EXTENSION_JAR" | grep -i "execution/PodTemplateSelectStrategy"
jar tf "$EXTENSION_JAR" | grep -i "execution/Selector"

# Should show .class files for these

# Save JAR path
echo "$EXTENSION_JAR" > /tmp/druid-k8s-dynamic-config-jar.txt
echo "✅ JAR path saved to /tmp/druid-k8s-dynamic-config-jar.txt"
```

**✅ Checkpoint**: JAR verified with dynamic config classes

---

### **STEP 16: Create Backup of Original Extension**

**⚠️ CRITICAL**: Always backup before replacing!

```bash
# SSH to Overlord server
ssh ubuntu@<overlord-host>

# Set variables
DRUID_HOME="/opt/apache-druid-30.0"
EXTENSION_DIR="$DRUID_HOME/extensions/druid-kubernetes-overlord-extensions"
BACKUP_DIR="/opt/druid-backups/k8s-extension-$(date +%Y%m%d_%H%M%S)"

# Create backup directory
sudo mkdir -p "$BACKUP_DIR"

# Backup entire extension directory
sudo cp -r "$EXTENSION_DIR" "$BACKUP_DIR/"

# Verify backup
ls -lh "$BACKUP_DIR/druid-kubernetes-overlord-extensions/"

# Record backup location
echo "$BACKUP_DIR" | sudo tee /opt/druid-backups/LATEST_K8S_EXTENSION_BACKUP.txt

echo "✅ Backup created at: $BACKUP_DIR"
```

**✅ Checkpoint**: Original extension backed up

---

### **STEP 17: Deploy Custom Extension to Overlord**

```bash
# From your build machine
EXTENSION_JAR=$(cat /tmp/druid-k8s-dynamic-config-jar.txt)
OVERLORD_HOST="<overlord-host>"  # Replace with actual hostname

# Copy to Overlord
echo "=== Copying JAR to Overlord ==="
scp "$EXTENSION_JAR" ubuntu@$OVERLORD_HOST:/tmp/druid-k8s-extension-dynamic-config.jar

# Verify copy
ssh ubuntu@$OVERLORD_HOST "ls -lh /tmp/druid-k8s-extension-dynamic-config.jar"

echo "✅ JAR copied to Overlord"
```

**✅ Checkpoint**: JAR on Overlord server

---

### **STEP 18: Install Extension on Overlord**

```bash
# SSH to Overlord
ssh ubuntu@<overlord-host>

# Set variables
DRUID_HOME="/opt/apache-druid-30.0"
EXTENSION_DIR="$DRUID_HOME/extensions/druid-kubernetes-overlord-extensions"

# Stop Overlord
echo "=== Stopping Overlord ==="
sudo supervisorctl stop overlord

# Wait for shutdown
sleep 10

# Verify stopped
sudo supervisorctl status overlord
# Should show: overlord STOPPED

# Remove old JARs
echo "=== Removing old extension ==="
sudo rm -f "$EXTENSION_DIR"/*.jar

# Install new JAR
echo "=== Installing custom extension with dynamic config ==="
sudo cp /tmp/druid-k8s-extension-dynamic-config.jar "$EXTENSION_DIR/"

# Set ownership
sudo chown -R ubuntu:ubuntu "$EXTENSION_DIR"

# Verify
ls -lh "$EXTENSION_DIR"/*.jar

echo "✅ Custom extension installed"
```

**✅ Checkpoint**: Extension deployed

---

### **STEP 19: Start Overlord and Monitor**

```bash
# Start Overlord
echo "=== Starting Overlord ==="
sudo supervisorctl start overlord

# Wait for startup
sleep 10

# Check status
sudo supervisorctl status overlord
# Should show: overlord RUNNING

# Monitor logs for dynamic config loading
echo "=== Monitoring startup (Ctrl+C to stop) ==="
sudo tail -f /var/log/druid/overlord.log | grep -i "execution\|dynamic\|k8s"
```

**🔍 SUCCESS Indicators:**
```
INFO [main] org.apache.druid.initialization.Initialization - Loading extension [druid-kubernetes-overlord-extensions]
INFO [main] org.apache.druid.k8s.overlord.KubernetesTaskRunnerFactory - Initializing K8s task runner
INFO [main] org.apache.druid.k8s.overlord.execution.* - Loading dynamic config
INFO [main] org.apache.druid.server.initialization.jetty.JettyServerModule - Started Jetty server on port [8090]
```

**❌ FAILURE Indicators:**
```
ERROR [main] - Could not load extension
java.lang.NoSuchMethodError: ...
java.lang.ClassNotFoundException: ...
```

**If errors occur:** Stop Overlord and restore backup (see Step 23)

**✅ Checkpoint**: Overlord running with new extension

---

### **STEP 20: Verify Dynamic Config API Available**

```bash
# Check extension loaded
curl -s http://localhost:8090/status/properties | jq -r '.["druid.extensions.loadList"]'
# Should include: druid-kubernetes-overlord-extensions

# Test dynamic config GET endpoint
echo "=== Testing dynamic config API ==="
curl -s http://localhost:8090/druid/indexer/v1/k8s/taskrunner/executionconfig | jq .

# Expected response (default config):
# {
#   "type": "default",
#   "podTemplateSelectStrategy": {
#     "type": "taskType"
#   }
# }

# If you get this response, the dynamic config feature is working! ✅

# If you get 404, the feature isn't loaded ❌
```

**✅ SUCCESS**: API returns config JSON  
**❌ FAILURE**: 404 or connection refused

**✅ Checkpoint**: Dynamic config API verified

---

### **STEP 21: Configure Dynamic Pod Template Selection**

Now test the dynamic config feature!

#### **Option A: Task Type Strategy (Simple)**

```bash
# Update config to use task type strategy
curl -X POST http://localhost:8090/druid/indexer/v1/k8s/taskrunner/executionconfig \
  -H 'Content-Type: application/json' \
  -H 'X-Druid-Author: admin' \
  -H 'X-Druid-Comment: Initial config with task type strategy' \
  -d '{
    "type": "default",
    "podTemplateSelectStrategy": {
      "type": "taskType"
    }
  }'

# Verify update
curl -s http://localhost:8090/druid/indexer/v1/k8s/taskrunner/executionconfig | jq .
```

#### **Option B: Selector-Based Strategy (Advanced)**

```bash
# Configure selector-based strategy with routing rules
curl -X POST http://localhost:8090/druid/indexer/v1/k8s/taskrunner/executionconfig \
  -H 'Content-Type: application/json' \
  -H 'X-Druid-Author: admin' \
  -H 'X-Druid-Comment: Setup selector-based routing' \
  -d '{
    "type": "default",
    "podTemplateSelectStrategy": {
      "type": "selectorBased",
      "selectors": [
        {
          "selectionKey": "highResourcePod",
          "context.tags": {
            "size": ["large", "xlarge"]
          }
        },
        {
          "selectionKey": "smallResourcePod",
          "context.tags": {
            "size": ["small"]
          }
        },
        {
          "selectionKey": "kafkaIngestPod",
          "type": ["index_kafka"]
        }
      ]
    }
  }'

# Verify configuration
curl -s http://localhost:8090/druid/indexer/v1/k8s/taskrunner/executionconfig | jq .
```

**✅ Checkpoint**: Dynamic config configured

---

### **STEP 22: Test Dynamic Pod Template Selection**

#### **Prerequisites:**
1. Create custom pod templates (if using selector-based strategy)
2. Ensure pod templates exist as ConfigMaps or files

#### **Test 1: Submit Task with Tag**

```bash
# Submit MSQ query with size=large tag
curl -X POST http://localhost:8888/druid/v2/sql/statements \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "SELECT COUNT(*) FROM your_datasource WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '\''1'\'' HOUR",
    "context": {
      "tags": {
        "size": "large"
      }
    }
  }' | jq .

# Record task ID
TASK_ID="<task-id-from-response>"
```

#### **Test 2: Verify Pod Template Selection**

```bash
# Check which pod template was used
kubectl get pods -l druid.task=${TASK_ID} -o yaml | grep -A 5 "annotations:"

# Should show annotations indicating the selected pod template
# Look for: druid.k8s.peon.podTemplate: highResourcePod
```

#### **Test 3: View Configuration History**

```bash
# Check config history
curl -s http://localhost:8090/druid/indexer/v1/k8s/taskrunner/executionconfig/history | jq .

# Should show audit trail:
# [
#   {
#     "key": "k8s.taskrunner.config",
#     "type": "k8s.taskrunner.config",
#     "auditInfo": {
#       "author": "admin",
#       "comment": "Setup selector-based routing",
#       "ip": "127.0.0.1"
#     },
#     "payload": "{...}",
#     "auditTime": "2025-01-XX..."
#   }
# ]
```

#### **Test 4: Update Config Without Restart**

```bash
# Change config on-the-fly
curl -X POST http://localhost:8090/druid/indexer/v1/k8s/taskrunner/executionconfig \
  -H 'Content-Type: application/json' \
  -H 'X-Druid-Author: admin' \
  -H 'X-Druid-Comment: Update selector rules' \
  -d '{
    "type": "default",
    "podTemplateSelectStrategy": {
      "type": "selectorBased",
      "selectors": [
        {
          "selectionKey": "highResourcePod",
          "context.tags": {
            "size": ["large", "xlarge", "xxlarge"]
          }
        }
      ]
    }
  }'

# Verify change took effect IMMEDIATELY (no restart needed!)
curl -s http://localhost:8090/druid/indexer/v1/k8s/taskrunner/executionconfig | jq .

# Submit another task - should use new rules
```

**✅ SUCCESS CRITERIA:**
- ✅ Configuration updates without Overlord restart
- ✅ Tasks routed to correct pod templates based on selectors
- ✅ Configuration history tracks all changes
- ✅ No errors in Overlord logs

**✅ Checkpoint**: Dynamic config feature fully working!

---

### **STEP 23: Rollback Procedure (If Needed)**

If issues occur:

```bash
# SSH to Overlord
ssh ubuntu@<overlord-host>

# Stop Overlord
sudo supervisorctl stop overlord

# Find backup
BACKUP_DIR=$(sudo cat /opt/druid-backups/LATEST_K8S_EXTENSION_BACKUP.txt)

# Restore original extension
sudo rm -rf /opt/apache-druid-30.0/extensions/druid-kubernetes-overlord-extensions
sudo cp -r "$BACKUP_DIR/druid-kubernetes-overlord-extensions" \
    /opt/apache-druid-30.0/extensions/

# Set ownership
sudo chown -R ubuntu:ubuntu /opt/apache-druid-30.0/extensions/

# Start Overlord
sudo supervisorctl start overlord

# Monitor logs
sudo tail -f /var/log/druid/overlord.log

echo "✅ Rollback complete"
```

**✅ Checkpoint**: Rollback completed (if needed)

---

## 🔍 **Troubleshooting**

### **Issue 1: Cherry-Pick Conflicts**

**Symptoms:** Git conflicts during cherry-pick

**Common Conflicts:**
1. `KubernetesTaskRunnerFactory.java` - dependency injection changes
2. `pom.xml` - dependency versions
3. `KubernetesOverlordModule.java` - module bindings

**Resolution:**
```bash
# For each conflict:
vim <conflicted-file>

# Look for:
<<<<<<< HEAD
(your 30.0.0 code)
=======
(new dynamic config code)
>>>>>>> commit-hash

# Generally: KEEP the new code (between ======= and >>>>>>>)
# Remove conflict markers
# Save file

git add <file>
git cherry-pick --continue
```

---

### **Issue 2: Build Failures**

**Symptoms:** Maven compilation errors

**Common Causes:**
- Syntax errors from conflict resolution
- Java version mismatch
- Missing dependencies

**Solution:**
```bash
# Check Java version
java -version  # Must be 11

# Clean and rebuild
mvn clean
mvn compile -pl extensions-contrib/kubernetes-overlord-extensions

# Review error messages carefully
# Fix syntax errors in reported files
```

---

### **Issue 3: API Returns 404**

**Symptoms:** `GET /druid/indexer/v1/k8s/taskrunner/executionconfig` returns 404

**Possible Causes:**
1. Extension didn't load correctly
2. Resource class not registered
3. Build didn't include new classes

**Debug:**
```bash
# Check extension loaded
curl -s http://localhost:8090/status/properties | jq -r '.["druid.extensions.loadList"]'

# Check JAR contents
jar tf <extension-jar> | grep KubernetesTaskExecutionConfigResource

# Check Overlord logs
sudo grep -i "KubernetesTaskExecutionConfigResource" /var/log/druid/overlord.log

# Check for errors
sudo grep -i "error\|exception" /var/log/druid/overlord.log | grep -i "k8s\|execution"
```

---

### **Issue 4: NoSuchMethodError on Startup**

**Symptoms:** `java.lang.NoSuchMethodError` in logs

**Cause:** API mismatch between cherry-picked code and Druid 30.0.0 core

**Solution:**
This indicates the cherry-pick introduced code that depends on APIs not in 30.0.0. Options:
1. Review conflict resolution - may have missed required changes
2. Try building from newer base (31.0.0)
3. Manually adjust code to use 30.0.0 APIs

---

### **Issue 5: Pod Template Selection Not Working**

**Symptoms:** Tasks don't use expected pod templates

**Debug Steps:**
```bash
# Check current config
curl -s http://localhost:8090/druid/indexer/v1/k8s/taskrunner/executionconfig | jq .

# Check task context
kubectl get pods -l druid.task=<task-id> -o json | jq '.items[0].metadata.annotations'

# Check Overlord logs for selection logic
sudo grep -i "podTemplate\|selector" /var/log/druid/overlord.log

# Verify pod templates exist
kubectl get configmaps | grep -i druid
```

---

## 📝 **Success Checklist**

Before considering complete:

- [ ] All 6 PRs cherry-picked successfully
- [ ] Build completed with no errors
- [ ] Custom extension deployed to Overlord
- [ ] Overlord started with no errors
- [ ] Extension shows in loaded extensions list
- [ ] Dynamic config API returns 200 (not 404)
- [ ] Configuration can be updated via POST
- [ ] Configuration history API works
- [ ] Test tasks submitted successfully
- [ ] Pod template selection working (if configured)
- [ ] No errors in logs after 30 minutes

---

## 📊 **Summary**

### **What Was Accomplished:**
1. ✅ Cherry-picked 6 PRs adding dynamic config feature
2. ✅ Built custom K8s extension for Druid 30.0.0
3. ✅ Deployed to Overlord (only component affected)
4. ✅ Verified dynamic config APIs working
5. ✅ Tested pod template selection

### **New Capabilities Added:**
- **Dynamic pod template selection** based on task properties
- **Runtime configuration updates** without restarts
- **Selector-based routing** for fine-grained control
- **Configuration history** with audit trail
- **Annotation support** for pod templates

### **Components Modified:**
- **Changed:** kubernetes-overlord-extensions JAR
- **Unchanged:** All other Druid components

### **Downtime:**
- **Overlord:** ~5 minutes
- **Running tasks:** 0 minutes (tasks continue independently)
- **Queries:** 0 minutes (Brokers unaffected)

---

## 🎯 **Expected Outcomes**

### **✅ Success:**
- Dynamic config API accessible at `/druid/indexer/v1/k8s/taskrunner/executionconfig`
- Configuration updates take effect immediately
- Tasks routed to appropriate pod templates
- Configuration history tracks all changes
- No Overlord restarts needed for config updates

### **❌ If It Doesn't Work:**
- Rollback to original extension (< 5 minutes)
- Review conflict resolution
- Check compatibility issues
- Consider upgrading to 31.0.0 or newer

---

## 📚 **References**

### **Pull Requests:**
- [PR #16510](https://github.com/apache/druid/pull/16510) - Initial implementation
- [PR #16600](https://github.com/apache/druid/pull/16600) - Documentation
- [PR #16720](https://github.com/apache/druid/pull/16720) - URL fixes
- [PR #16772](https://github.com/apache/druid/pull/16772) - Annotations
- [PR #17400](https://github.com/apache/druid/pull/17400) - Bug fix
- [PR #17464](https://github.com/apache/druid/pull/17464) - Docs update

### **Documentation:**
- [K8s Extension Docs](https://druid.apache.org/docs/latest/development/extensions-core/k8s-jobs#dynamic-config)
- [K8s Dynamic Config History](./K8S_DYNAMIC_CONFIG_HISTORY.md)

### **Related Files:**
- Extension guide: `CHERRY_PICK_EXTENSION_GUIDE.md`
- Feature history: `K8S_DYNAMIC_CONFIG_HISTORY.md`

---

## 📞 **Support**

If issues occur:
1. Check Troubleshooting section above
2. Review Overlord logs: `/var/log/druid/overlord.log`
3. Verify JAR contents: `jar tf <extension-jar> | grep execution`
4. Check kubectl pod logs: `kubectl logs <peon-pod>`
5. Test API manually: `curl http://localhost:8090/druid/indexer/v1/k8s/taskrunner/executionconfig`

---

**Created:** January 2025  
**Target Version:** Druid 30.0.0  
**Feature:** K8s Dynamic Config (6 PRs, ~2,265 lines)  
**Impact:** Overlord only (minimal risk)  
**Restart Required:** Overlord only (~5 minutes)

---

## 🚀 **Quick Start Commands**

```bash
# Clone and setup
git clone https://github.com/apache/druid.git && cd druid
git checkout druid-30.0.0
git checkout -b druid-30.0.0-k8s-dynamic-config

# Cherry-pick all PRs
git cherry-pick 46dbc74053
git cherry-pick 4e0ea7823b
git cherry-pick 4b293fc2a9
git cherry-pick a64e9a1746
git cherry-pick 5fcf4205e4
git cherry-pick 1dbd005df6

# Build
mvn package -pl extensions-contrib/kubernetes-overlord-extensions -am -DskipTests

# Find JAR
find extensions-contrib/kubernetes-overlord-extensions/target/ -name "*30.0.0.jar" ! -name "*sources*" ! -name "*tests*"

# Deploy (on Overlord server)
sudo supervisorctl stop overlord
sudo cp /tmp/druid-k8s-extension-dynamic-config.jar /opt/apache-druid-30.0/extensions/druid-kubernetes-overlord-extensions/
sudo supervisorctl start overlord

# Verify
curl http://localhost:8090/druid/indexer/v1/k8s/taskrunner/executionconfig
```

