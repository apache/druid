#!/bin/bash
set -e

# Task Reports Fix Deployment Script
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

echo -e "${YELLOW}📦 Step 1: Copy JARs to Overlord...${NC}"
echo ""

# Copy JARs to Overlord staging directory
scp "$K8S_JAR" "$OVERLORD_HOST:$REMOTE_STAGING_DIR/"
scp "$INDEXING_JAR" "$OVERLORD_HOST:$REMOTE_STAGING_DIR/"

echo -e "${GREEN}✅ JARs copied to Overlord${NC}"
echo ""

echo -e "${YELLOW}🔧 Step 2: Install JARs on Overlord (requires manual SSH)...${NC}"
echo ""
echo "Please run the following commands on the Overlord:"
echo ""
echo "---------------------------------------------------"
echo "ssh $OVERLORD_HOST"
echo ""
echo "# Backup existing JARs"
echo "sudo cp druid/lib/druid-indexing-service-30.0.0.jar \\"
echo "       druid/lib/druid-indexing-service-30.0.0.jar.backup-\$(date +%Y%m%d-%H%M%S)"
echo ""
echo "sudo cp druid/extensions/druid-kubernetes-overlord-extensions/druid-kubernetes-overlord-extensions-30.0.0.jar \\"
echo "       druid/extensions/druid-kubernetes-overlord-extensions/druid-kubernetes-overlord-extensions-30.0.0.jar.backup-\$(date +%Y%m%d-%H%M%S)"
echo ""
echo "# Install new JARs"
echo "sudo cp $REMOTE_STAGING_DIR/druid-indexing-service-30.0.0.jar druid/lib/"
echo "sudo cp $REMOTE_STAGING_DIR/druid-kubernetes-overlord-extensions-30.0.0.jar \\"
echo "       druid/extensions/druid-kubernetes-overlord-extensions/"
echo ""
echo "# Restart Overlord"
echo "sudo supervisorctl restart overlord"
echo ""
echo "# Monitor logs for diagnostics"
echo "tail -f /logs/druid/overlord.log | grep -E 'REPORTS|SWITCHING|API|📊|🔀|🌐|✅|⚠️|❌'"
echo "---------------------------------------------------"
echo ""

echo -e "${YELLOW}⚠️  IMPORTANT NOTES:${NC}"
echo ""
echo "1. ✅ SAFE: Only Overlord is affected - MiddleManagers untouched"
echo "2. ❌ DO NOT deploy to MiddleManagers (not needed, see MIDDLEMANAGER_SAFETY_ANALYSIS.md)"
echo "3. ❌ DO NOT rebuild Docker images for pods (not needed, pods already correct)"
echo "4. ✅ After restart, test with:"
echo "   curl http://prod-router.druid.singular.net:8080/druid/indexer/v1/task/{taskId}/reports"
echo ""

echo -e "${GREEN}📚 Documentation:${NC}"
echo "- Complete fix details: TASK_REPORTS_COMPREHENSIVE_FIX.md"
echo "- Safety analysis: MIDDLEMANAGER_SAFETY_ANALYSIS.md"
echo "- Debugging guide: LIVE_REPORTS_DEBUG_GUIDE.md"
echo ""

echo -e "${GREEN}✅ Script complete - JARs staged on Overlord${NC}"
echo -e "${YELLOW}➡️  Next: SSH to Overlord and run the commands above${NC}"

