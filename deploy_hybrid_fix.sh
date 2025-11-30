#!/bin/bash
# Deploy Hybrid Log Streaming Fix to Overlord

set -e

echo "🔨 Building kubernetes-overlord-extensions with hybrid log streaming fix..."
cd /Users/ronshub/workspace/druid

mvn clean package -pl extensions-contrib/kubernetes-overlord-extensions \
  -am -DskipTests -Dforbiddenapis.skip=true

echo ""
echo "✅ Build complete!"
echo ""
echo "📦 JAR location:"
echo "   extensions-contrib/kubernetes-overlord-extensions/target/druid-kubernetes-overlord-extensions-30.0.0.jar"
echo ""
echo "📋 Next steps (run these commands):"
echo ""
echo "# 1. Copy to Overlord:"
echo "scp extensions-contrib/kubernetes-overlord-extensions/target/druid-kubernetes-overlord-extensions-30.0.0.jar \\"
echo "  ubuntu@prodft30-overlord0:~/druid-kubernetes-overlord-extensions-30.0.0-HYBRID.jar"
echo ""
echo "# 2. SSH to Overlord and deploy:"
echo "ssh ubuntu@prodft30-overlord0"
echo "sudo mv ~/druid-kubernetes-overlord-extensions-30.0.0-HYBRID.jar \\"
echo "  /opt/druid/extensions/druid-kubernetes-overlord-extensions/druid-kubernetes-overlord-extensions-30.0.0.jar"
echo "sudo systemctl restart druid-overlord"
echo ""
echo "# 3. Verify deployment:"
echo "sudo systemctl status druid-overlord"
echo "sudo tail -f /logs/druid/overlord-stdout---supervisor-*.log | grep '📺'"
echo ""
echo "# 4. Test with a running task:"
echo "TASK_ID=\"query-abc123...\""
echo "time curl -s http://prodft30-overlord0.druid.singular.net:8090/druid/indexer/v1/task/\${TASK_ID}/log | wc -l"
echo "# Should complete in 1-2 seconds! ✅"
echo ""

