#!/bin/bash
set -e

# Task Reports Fix - FULLY AUTOMATED Deployment Script
# Deploys diagnostic logging and S3 fallback fix to PRODFT Overlord
# 
# SAFE: Only touches Overlord, NOT MiddleManagers
# See: MIDDLEMANAGER_SAFETY_ANALYSIS.md

echo "=========================================="
echo "Task Reports Fix - PRODFT Deployment"
echo "=========================================="
echo ""

# Configuration
OVERLORD_HOST="prodft-overlord.druid.singular.net"
BUILD_DIR="$HOME/workspace/druid"
REMOTE_STAGING_DIR="~/druid-lib-build"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${YELLOW}📋 Pre-flight checks...${NC}"
echo ""

# Check we're in the right directory
if [ ! -f "pom.xml" ] || [ ! -d "extensions-contrib/kubernetes-overlord-extensions" ]; then
    echo -e "${RED}❌ Error: Must run from Druid workspace root${NC}"
    echo "Expected: $BUILD_DIR"
    echo "Current:  $(pwd)"
    exit 1
fi

echo -e "${GREEN}✅ Working directory verified${NC}"
echo ""

# Check JARs exist
K8S_JAR="extensions-contrib/kubernetes-overlord-extensions/target/druid-kubernetes-overlord-extensions-30.0.0.jar"
INDEXING_JAR="indexing-service/target/druid-indexing-service-30.0.0.jar"

if [ ! -f "$K8S_JAR" ]; then
    echo -e "${RED}❌ Error: K8s extension JAR not found: $K8S_JAR${NC}"
    echo "Run: ./apache-maven-3.9.11/bin/mvn -pl extensions-contrib/kubernetes-overlord-extensions -am clean package -DskipTests -Dcheckstyle.skip=true -Dforbiddenapis.skip=true"
    exit 1
fi

if [ ! -f "$INDEXING_JAR" ]; then
    echo -e "${RED}❌ Error: Indexing service JAR not found: $INDEXING_JAR${NC}"
    echo "Run: ./apache-maven-3.9.11/bin/mvn -pl indexing-service -am clean package -DskipTests -Dcheckstyle.skip=true -Dforbiddenapis.skip=true"
    exit 1
fi

echo -e "${GREEN}✅ JARs verified:${NC}"
ls -lh "$K8S_JAR"
ls -lh "$INDEXING_JAR"
echo ""

echo -e "${YELLOW}📦 Step 1/4: Copy JARs to Overlord...${NC}"
echo ""

# Copy JARs to Overlord staging directory
scp "$K8S_JAR" "$OVERLORD_HOST:$REMOTE_STAGING_DIR/"
scp "$INDEXING_JAR" "$OVERLORD_HOST:$REMOTE_STAGING_DIR/"

echo -e "${GREEN}✅ JARs copied to Overlord${NC}"
echo ""

echo -e "${YELLOW}🔧 Step 2/4: Backup existing JARs on Overlord...${NC}"
echo ""

BACKUP_TIMESTAMP=$(date +%Y%m%d-%H%M%S)

ssh "$OVERLORD_HOST" bash << EOF
set -e

echo "Creating backups with timestamp: $BACKUP_TIMESTAMP"

# Backup indexing-service JAR
sudo cp druid/lib/druid-indexing-service-30.0.0.jar \
       druid/lib/druid-indexing-service-30.0.0.jar.backup-$BACKUP_TIMESTAMP

# Backup k8s extension JAR  
sudo cp druid/extensions/druid-kubernetes-overlord-extensions/druid-kubernetes-overlord-extensions-30.0.0.jar \
       druid/extensions/druid-kubernetes-overlord-extensions/druid-kubernetes-overlord-extensions-30.0.0.jar.backup-$BACKUP_TIMESTAMP

echo "✅ Backups created"
ls -lh druid/lib/druid-indexing-service-30.0.0.jar.backup-$BACKUP_TIMESTAMP
ls -lh druid/extensions/druid-kubernetes-overlord-extensions/druid-kubernetes-overlord-extensions-30.0.0.jar.backup-$BACKUP_TIMESTAMP
EOF

echo -e "${GREEN}✅ Backups created${NC}"
echo ""

echo -e "${YELLOW}📥 Step 3/4: Install new JARs on Overlord...${NC}"
echo ""

ssh "$OVERLORD_HOST" bash << 'EOF'
set -e

# Install new JARs
sudo cp ~/druid-lib-build/druid-indexing-service-30.0.0.jar druid/lib/
sudo cp ~/druid-lib-build/druid-kubernetes-overlord-extensions-30.0.0.jar \
       druid/extensions/druid-kubernetes-overlord-extensions/

echo "✅ New JARs installed"
ls -lh druid/lib/druid-indexing-service-30.0.0.jar
ls -lh druid/extensions/druid-kubernetes-overlord-extensions/druid-kubernetes-overlord-extensions-30.0.0.jar
EOF

echo -e "${GREEN}✅ JARs installed${NC}"
echo ""

echo -e "${YELLOW}🔄 Step 4/4: Restart Overlord...${NC}"
echo ""

ssh "$OVERLORD_HOST" "sudo supervisorctl restart overlord"

echo -e "${GREEN}✅ Overlord restarted${NC}"
echo ""

echo -e "${YELLOW}⏱️  Waiting 10 seconds for Overlord to start...${NC}"
sleep 10
echo ""

echo -e "${YELLOW}🔍 Checking Overlord status...${NC}"
echo ""

ssh "$OVERLORD_HOST" "sudo supervisorctl status overlord"
echo ""

echo -e "${GREEN}=========================================="
echo "✅ DEPLOYMENT COMPLETE!"
echo "==========================================${NC}"
echo ""

echo -e "${YELLOW}📊 Next Steps:${NC}"
echo ""
echo "1. Monitor Overlord logs for diagnostics:"
echo "   ssh $OVERLORD_HOST"
echo "   tail -f /logs/druid/overlord.log | grep -E 'REPORTS|SWITCHING|API|📊|🔀|🌐'"
echo ""
echo "2. Test task reports endpoint:"
echo "   # Get a running task ID:"
echo "   curl -s http://prod-router.druid.singular.net:8080/druid/indexer/v1/tasks | jq '.[] | select(.type==\"query_controller\") | .id' | head -1"
echo ""
echo "   # Fetch live reports:"
echo "   curl -s http://prod-router.druid.singular.net:8080/druid/indexer/v1/task/{TASK_ID}/reports | jq ."
echo ""
echo "3. Test S3 fallback (completed tasks):"
echo "   # Use a completed task ID from S3:"
echo "   aws s3 ls s3://singular-druid-indexing-logs/druid/prodft30/indexing_logs/ | grep report.json | tail -1"
echo ""

echo -e "${GREEN}📚 Documentation:${NC}"
echo "- TASK_REPORTS_COMPREHENSIVE_FIX.md"
echo "- MIDDLEMANAGER_SAFETY_ANALYSIS.md"
echo "- LIVE_REPORTS_DEBUG_GUIDE.md"
echo ""

echo -e "${YELLOW}⚠️  IMPORTANT:${NC}"
echo "- ✅ Only Overlord was updated (MiddleManagers untouched)"
echo "- ✅ Production workloads safe"
echo "- ✅ Backups created with timestamp: $BACKUP_TIMESTAMP"
echo ""

echo -e "${GREEN}🎉 Deployment successful!${NC}"

