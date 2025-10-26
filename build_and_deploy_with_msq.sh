#!/bin/bash
# Build and Deploy Script for Kubernetes Overlord Extensions + MSQ with Tags Support
# This script builds both extensions and deploys them to the prodft30 overlord

set -e  # Exit on any error

echo "════════════════════════════════════════════════════════════════════════════════"
echo "  Building Druid Extensions with MSQ Tags Support"
echo "════════════════════════════════════════════════════════════════════════════════"

# Build both the kubernetes-overlord-extensions AND multi-stage-query
echo "📦 Building extensions (this may take a few minutes)..."
./apache-maven-3.9.11/bin/mvn \
  -pl extensions-contrib/kubernetes-overlord-extensions \
  -pl extensions-core/multi-stage-query \
  -DskipTests \
  -am \
  clean package

echo ""
echo "════════════════════════════════════════════════════════════════════════════════"
echo "  Deploying JARs to prodft30-overlord0"
echo "════════════════════════════════════════════════════════════════════════════════"

# Copy K8s Overlord Extension
echo "📤 Copying kubernetes-overlord-extensions JAR..."
for f in $(find . -name "*druid-kubernetes-overlord-extensions-30.0.0.jar" ! -name "*tests*"); do
  echo "   Found: $f"
  scp -P 41100 "$f" ubuntu@prodft30-overlord0.druid.singular.net:~/druid-lib-build/
done

# Copy Multi-Stage Query Extension
echo "📤 Copying multi-stage-query JAR..."
for f in $(find . -name "*druid-multi-stage-query-30.0.0.jar" ! -name "*tests*"); do
  echo "   Found: $f"
  scp -P 41100 "$f" ubuntu@prodft30-overlord0.druid.singular.net:~/druid-lib-build/
done

echo ""
echo "════════════════════════════════════════════════════════════════════════════════"
echo "  Installing JARs on Remote Server"
echo "════════════════════════════════════════════════════════════════════════════════"

ssh -p 41100 ubuntu@prodft30-overlord0.druid.singular.net << 'REMOTE_COMMANDS'
set -e

echo "📋 Backing up old JARs..."
# Backup old jars (in case we need to rollback)
mkdir -p ~/druid-lib-backup
cp druid/extensions/druid-kubernetes-overlord-extensions/druid-kubernetes-overlord-extensions-30.0.0.jar \
   ~/druid-lib-backup/druid-kubernetes-overlord-extensions-30.0.0.jar.$(date +%Y%m%d_%H%M%S) 2>/dev/null || true
cp druid/extensions/druid-multi-stage-query/druid-multi-stage-query-30.0.0.jar \
   ~/druid-lib-backup/druid-multi-stage-query-30.0.0.jar.$(date +%Y%m%d_%H%M%S) 2>/dev/null || true

echo "📦 Installing kubernetes-overlord-extensions..."
# Install K8s Overlord Extension
cp druid-lib-build/druid-kubernetes-overlord-extensions-30.0.0.jar druid/lib/
mv druid/lib/druid-kubernetes-overlord-extensions-30.0.0.jar \
   druid/extensions/druid-kubernetes-overlord-extensions/

echo "📦 Installing multi-stage-query extension..."
# Install Multi-Stage Query Extension
cp druid-lib-build/druid-multi-stage-query-30.0.0.jar druid/lib/
mv druid/lib/druid-multi-stage-query-30.0.0.jar \
   druid/extensions/druid-multi-stage-query/

echo "✅ JARs installed successfully!"
echo ""
echo "📊 Verifying installation..."
ls -lh druid/extensions/druid-kubernetes-overlord-extensions/druid-kubernetes-overlord-extensions-30.0.0.jar
ls -lh druid/extensions/druid-multi-stage-query/druid-multi-stage-query-30.0.0.jar

echo ""
echo "🔄 Restarting overlord service..."
sudo supervisorctl restart overlord

echo ""
echo "⏳ Waiting for overlord to start..."
sleep 5

echo ""
echo "📊 Checking overlord status..."
sudo supervisorctl status overlord

REMOTE_COMMANDS

echo ""
echo "════════════════════════════════════════════════════════════════════════════════"
echo "  ✅ Deployment Complete!"
echo "════════════════════════════════════════════════════════════════════════════════"
echo ""
echo "📝 To check logs:"
echo "   ssh -p 41100 ubuntu@prodft30-overlord0.druid.singular.net"
echo "   sudo tail -f /logs/druid/overlord-stdout---supervisor-3cweefvk.log"
echo ""
echo "🧪 To test with tags:"
echo "   curl -X POST http://prodft30-broker0.druid.singular.net:8082/druid/v2/sql/task \\"
echo "     -H 'Content-Type: application/json' \\"
echo "     -d '{"
echo "       \"query\": \"INSERT INTO test_tags SELECT * FROM source LIMIT 100 PARTITIONED BY DAY\","
echo "       \"context\": {"
echo "         \"tags\": {\"userProvidedTag\": \"medium\"},"
echo "         \"maxNumTasks\": 2"
echo "       }"
echo "     }'"
echo ""
echo "🔍 Watch for these log lines:"
echo "   🔍 [SELECTOR] Task context.tags (key='tags'): {userProvidedTag=medium}"
echo "   ✅ [SELECTOR] Selector [prodft30-medium-peon-pod] MATCHED"
echo ""

