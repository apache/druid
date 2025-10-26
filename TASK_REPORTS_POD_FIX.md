# Task Reports Fix for Kubernetes Pods 🐳

## 📋 Problem Summary

Task reports are not being persisted to S3 because:

1. **Kubernetes pods** use the stock `apache/druid:30.0.0` Docker image
2. This image does **NOT** have the cherry-picked fix from PR #18379
3. The image has incorrect config: `druid.indexer.logs.type=file` (should be `s3`)
4. When tasks complete, pods terminate immediately and reports are lost

**What we've already fixed:**
- ✅ Overlord: Custom JARs with fix deployed to `/opt/druid/extensions/`
- ✅ Overlord: HTTP-based report fetching implemented in `KubernetesPeonLifecycle`
- ✅ ConfigMaps: Updated to have `type=s3` (but Docker image overrides this)

**What still needs fixing:**
- ❌ Pods: Need custom Docker image with our JARs
- ❌ Pods: Need correct S3 configuration

---

## 🚀 SOLUTION OPTIONS

We have **two approaches** - pick the one that fits your infrastructure:

---

# Option A: Build Custom Docker Image (RECOMMENDED) 🏗️

**Pros:**
- Clean, permanent solution
- Pods will always have the correct JARs and config
- No complex volumeMount workarounds

**Cons:**
- Requires Docker build environment
- Need access to a Docker registry (or use local images)
- Pods need to be updated to use new image

---

## Option A - Step-by-Step Instructions

### Prerequisites

1. Docker installed on build machine (can be Overlord or your local machine)
2. Access to Kubernetes cluster to push/use images
3. Your custom-built JARs from the cherry-pick

### Step 1: Gather Custom JARs

On the **Overlord server**, verify you have the custom JARs:

```bash
# Check JARs exist
ls -lh /opt/druid/extensions/druid-indexing-service-30.0.0.jar
ls -lh /opt/druid/extensions/druid-processing-30.0.0.jar
ls -lh /opt/druid/extensions/druid-kubernetes-overlord-extensions/druid-kubernetes-overlord-extensions-30.0.0.jar

# Note the sizes - they should be:
# druid-indexing-service: ~3-4 MB
# druid-processing: ~15-20 MB
# druid-kubernetes-overlord-extensions: ~200-300 KB
```

### Step 2: Create Build Directory

```bash
# Create a temporary build directory
mkdir -p /tmp/druid-custom-build
cd /tmp/druid-custom-build

# Copy JARs
cp /opt/druid/extensions/druid-indexing-service-30.0.0.jar .
cp /opt/druid/extensions/druid-processing-30.0.0.jar .
cp /opt/druid/extensions/druid-kubernetes-overlord-extensions/druid-kubernetes-overlord-extensions-30.0.0.jar .

# Verify
ls -lh *.jar
```

### Step 3: Create Dockerfile

```bash
cat > Dockerfile <<'EOF'
FROM apache/druid:30.0.0

# Declare build arguments for AWS credentials (pass via --build-arg)
ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY

# Copy custom JARs with PR #18379 cherry-pick (task reports fix)
# These replace the stock Druid 30.0.0 JARs
COPY druid-indexing-service-30.0.0.jar /opt/druid/lib/
COPY druid-processing-30.0.0.jar /opt/druid/lib/
COPY druid-kubernetes-overlord-extensions-30.0.0.jar /opt/druid/extensions/druid-kubernetes-overlord-extensions/

# Override _common/common.runtime.properties with S3 config
RUN sed -i 's/druid.indexer.logs.type=file/druid.indexer.logs.type=s3/' \
    /opt/druid/conf/druid/cluster/_common/common.runtime.properties && \
    sed -i 's|druid.indexer.logs.directory=var/druid/indexing-logs|druid.indexer.logs.s3Bucket=singular-druid-indexing-logs\ndruid.indexer.logs.s3Prefix=druid/prodft30/indexing_logs|' \
    /opt/druid/conf/druid/cluster/_common/common.runtime.properties && \
    sed -i 's/#druid.storage.type=s3/druid.storage.type=s3/' \
    /opt/druid/conf/druid/cluster/_common/common.runtime.properties && \
    sed -i 's/#druid.s3.accessKey=.../druid.s3.accessKey=${AWS_ACCESS_KEY_ID}/' \
    /opt/druid/conf/druid/cluster/_common/common.runtime.properties && \
    sed -i 's|#druid.s3.secretKey=...|druid.s3.secretKey=${AWS_SECRET_ACCESS_KEY}|' \
    /opt/druid/conf/druid/cluster/_common/common.runtime.properties

USER druid

# Label for tracking
LABEL version="30.0.0-reports-fix" \
      description="Druid 30.0.0 with PR #18379 cherry-pick for task reports" \
      maintainer="Singular Platform Team"
EOF
```

### Step 4: Build Docker Image

**⚠️ IMPORTANT: AWS Credentials**

The Dockerfile uses environment variable placeholders for AWS credentials. You have two options:

**Option 1: Pass as build arguments (Recommended)**
```bash
docker build \
  --build-arg AWS_ACCESS_KEY_ID=your_access_key \
  --build-arg AWS_SECRET_ACCESS_KEY=your_secret_key \
  -t your-registry.io/singular/druid:30.0.0-reports-fix .
```

**Option 2: Use IAM roles (Best practice)**
- Remove the S3 credentials from the Dockerfile entirely
- Configure your K8s pods with IAM roles (IRSA on EKS or similar)
- See: https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html

---

**Choose ONE of these approaches:**

#### Option A1: Build and Push to Docker Registry (Recommended)

```bash
# Build the image (with credentials as build args)
docker build \
  --build-arg AWS_ACCESS_KEY_ID=your_access_key \
  --build-arg AWS_SECRET_ACCESS_KEY=your_secret_key \
  -t your-registry.io/singular/druid:30.0.0-reports-fix .

# Tag for versioning
docker tag your-registry.io/singular/druid:30.0.0-reports-fix \
           your-registry.io/singular/druid:30.0.0-reports-fix-$(date +%Y%m%d)

# Push to registry
docker push your-registry.io/singular/druid:30.0.0-reports-fix
docker push your-registry.io/singular/druid:30.0.0-reports-fix-$(date +%Y%m%d)
```

#### Option A2: Load Image Directly to Kubernetes Nodes (No Registry)

```bash
# Build the image (with credentials as build args)
docker build \
  --build-arg AWS_ACCESS_KEY_ID=your_access_key \
  --build-arg AWS_SECRET_ACCESS_KEY=your_secret_key \
  -t singular/druid:30.0.0-reports-fix .

# Save image to tar
docker save singular/druid:30.0.0-reports-fix > druid-custom.tar

# Copy to each Kubernetes node and load
for node in $(kubectl get nodes -o jsonpath='{.items[*].metadata.name}'); do
  echo "Loading image on node: $node"
  # SSH to node and load image (adjust SSH command as needed)
  ssh $node "docker load < /tmp/druid-custom.tar"
done
```

### Step 5: Update Pod Templates

Update your pod template YAML files to use the new image:

```bash
# Backup current templates
cp /home/ubuntu/druid_conf/overlord/msq_configs/podtemplates/small.yaml \
   /home/ubuntu/druid_conf/overlord/msq_configs/podtemplates/small.yaml.backup

cp /home/ubuntu/druid_conf/overlord/msq_configs/podtemplates/medium.yaml \
   /home/ubuntu/druid_conf/overlord/msq_configs/podtemplates/medium.yaml.backup

# Edit small template
nano /home/ubuntu/druid_conf/overlord/msq_configs/podtemplates/small.yaml
```

**Find this section:**
```yaml
spec:
  containers:
  - name: main
    image: apache/druid:30.0.0  # ← CHANGE THIS LINE
```

**Change to:**
```yaml
spec:
  containers:
  - name: main
    image: your-registry.io/singular/druid:30.0.0-reports-fix  # ← NEW IMAGE
    # OR if using local images without registry:
    # image: singular/druid:30.0.0-reports-fix
    imagePullPolicy: Always  # ← ADD THIS LINE
```

**Repeat for medium.yaml**

### Step 6: Restart Overlord

```bash
# Restart Overlord to pick up new pod templates
sudo systemctl restart druid-overlord

# Or if using different start method:
# sudo supervisorctl restart druid-overlord
```

### Step 7: Verify the Fix

```bash
# Submit a test MSQ query
curl -X POST -H 'Content-Type: application/json' \
  'http://prodft30-broker0.druid.singular.net:8082/druid/v2/sql/task' \
  -d '{
    "query": "SELECT COUNT(*) FROM your_datasource WHERE __time > CURRENT_TIMESTAMP - INTERVAL '\''1'\'' DAY",
    "context": {
      "tags": {"userProvidedTag": "medium"},
      "maxNumTasks": 2
    }
  }'

# Get the task ID from response, then wait ~60 seconds for completion

# Check a newly launched pod
kubectl get pods | grep query | grep Running | head -1

# Verify it's using the new image
POD_NAME="<pod-from-above>"
kubectl describe pod $POD_NAME | grep "Image:"
# Should show: singular/druid:30.0.0-reports-fix

# Check the pod's config
kubectl exec $POD_NAME -- cat /opt/druid/conf/druid/cluster/_common/common.runtime.properties | grep "indexer.logs"
# Should show: druid.indexer.logs.type=s3

# Wait for task to complete, then check S3
TASK_ID="<your-task-id>"
aws s3 ls s3://singular-druid-indexing-logs/druid/prodft30/indexing_logs/${TASK_ID}/ --recursive

# You should see:
# log (task logs)
# report.json (task reports) ← THIS IS NEW!
```

---

# Option B: ConfigMap VolumeMount Override (QUICK WORKAROUND) ⚡

**Pros:**
- No Docker build required
- Faster to implement
- No registry needed

**Cons:**
- More complex configuration
- Harder to maintain
- ConfigMaps have size limits (~1MB, our JARs are larger)
- **NOT VIABLE** for large JARs

**Status:** ❌ **NOT RECOMMENDED** - ConfigMaps can't hold JARs this large

---

## Option B - Alternative: Shared Volume Approach

If you **cannot** build Docker images, you could:

1. Mount a shared volume (NFS/EFS) to all K8s nodes
2. Copy custom JARs to this volume
3. Mount the volume into pods and override classpath

**This is complex and not recommended.** Use Option A instead.

---

# 🔍 Troubleshooting

## Issue: Pods Still Use Old Image

```bash
# Force delete running pods
kubectl delete pod -l druid.apache.org/component=worker

# Check image pull policy
kubectl describe pod <pod-name> | grep -A 5 "Image"

# Ensure imagePullPolicy: Always is set
```

## Issue: Image Not Found

```bash
# Check if image exists locally (if not using registry)
ssh <k8s-node>
docker images | grep druid

# Check registry authentication (if using registry)
kubectl get secret regcred
```

## Issue: Reports Still Not Appearing

```bash
# Check pod logs for S3 errors
kubectl logs <pod-name> | grep -i "s3\|report\|error"

# Check if pod has correct config
kubectl exec <pod-name> -- cat /opt/druid/conf/druid/cluster/_common/common.runtime.properties | grep indexer.logs

# Check AWS credentials work from pod
kubectl exec <pod-name> -- env | grep -i aws
```

## Issue: "Pushed task reports" Message But No File

```bash
# Check S3TaskLogs initialization
kubectl logs <pod-name> | grep "TaskLogs"

# Verify S3 extension is loaded
kubectl exec <pod-name> -- ls -la /opt/druid/extensions/ | grep s3

# Test S3 access manually
kubectl exec <pod-name> -- /bin/sh -c "echo 'test' > /tmp/test.txt && \
  aws s3 cp /tmp/test.txt s3://singular-druid-indexing-logs/test.txt"
```

---

# 📊 Verification Checklist

After implementing the fix, verify:

- [ ] New pods use custom Docker image: `kubectl describe pod <pod> | grep Image`
- [ ] Pod config has `type=s3`: `kubectl exec <pod> -- cat /opt/druid/conf/.../common.runtime.properties | grep indexer.logs`
- [ ] S3 extension is loaded: `kubectl exec <pod> -- ls /opt/druid/extensions/ | grep s3`
- [ ] Custom JARs are present: `kubectl exec <pod> -- ls -lh /opt/druid/lib/druid-indexing-service*`
- [ ] Task logs contain "Pushed task reports": Check S3 or pod logs
- [ ] Reports appear in S3: `aws s3 ls s3://singular-druid-indexing-logs/.../report.json`
- [ ] Druid API returns reports: `curl http://overlord:8090/druid/indexer/v1/task/<id>/reports`

---

# 🔄 Rollback Instructions

If something goes wrong:

## Rollback Pod Templates

```bash
# Restore original templates
cp /home/ubuntu/druid_conf/overlord/msq_configs/podtemplates/small.yaml.backup \
   /home/ubuntu/druid_conf/overlord/msq_configs/podtemplates/small.yaml

cp /home/ubuntu/druid_conf/overlord/msq_configs/podtemplates/medium.yaml.backup \
   /home/ubuntu/druid_conf/overlord/msq_configs/podtemplates/medium.yaml

# Restart Overlord
sudo systemctl restart druid-overlord
```

## Delete Custom Image (if needed)

```bash
# From registry
docker rmi your-registry.io/singular/druid:30.0.0-reports-fix

# From all K8s nodes
for node in $(kubectl get nodes -o jsonpath='{.items[*].metadata.name}'); do
  ssh $node "docker rmi singular/druid:30.0.0-reports-fix"
done
```

---

# 📚 Related Documentation

- **Original Fix:** See `CHERRY_PICK_EXTENSION_GUIDE.md` for the cherry-pick process
- **Deployment:** See `DEPLOYMENT_CHECKLIST.md` for Overlord deployment
- **Debugging:** See `DEBUGGING_LOGS_GUIDE.md` for log analysis
- **Investigation:** See `TASK_REPORTS_INVESTIGATION_SUMMARY.md` for full problem analysis

---

# 🎯 Summary

**Root Cause:**
- Kubernetes pods use stock `apache/druid:30.0.0` image
- This image lacks the PR #18379 fix for task report persistence
- Pod config has `type=file` instead of `type=s3`

**Solution:**
- Build custom Docker image with:
  - Cherry-picked JARs (with report fix)
  - Corrected S3 configuration
- Update pod templates to use custom image
- Verify reports are saved to S3

**Recommendation:** Use **Option A** (Custom Docker Image) for a clean, maintainable solution.

---

**Created:** $(date)
**Status:** Implementation Pending
**Priority:** High (Blocks MSQ task observability)

