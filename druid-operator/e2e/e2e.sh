#!/bin/bash
set -o errexit
set -x
# Get Kind
go install sigs.k8s.io/kind@v0.21.0
# minio statefulset name
MINIO_STS_NAME=myminio-minio
# druid namespace
NAMESPACE=druid
# fmt code
make fmt
# vet
make vet
# deploy kind
make kind
# build local docker druid operator image
make docker-build-local
# push to kind registry
make docker-push-local
# build local docker test image
make docker-build-local-test
# push to kind registry
make docker-push-local-test
# try to install the CRD with make
make install
# install druid-operator
make helm-install-druid-operator
# install minio-operator and tenant
make helm-minio-install
sleep 10
for m in $(kubectl get pod -n minio-operator -o name)
do
  kubectl wait -n minio-operator "$m" --for=condition=Ready --timeout=5m
done
sleep 10
kubectl wait pod -n ${NAMESPACE} -l app=minio --for=condition=Ready --timeout=5m
# wait for minio pods
kubectl rollout status sts $MINIO_STS_NAME -n ${NAMESPACE} --timeout=5m
# output pods
kubectl get pods -n ${NAMESPACE}
# apply druid cr
kubectl apply -f e2e/configs/druid-cr.yaml -n ${NAMESPACE}
sleep 10
for d in $(kubectl get pods -n ${NAMESPACE} -l app=druid -l druid_cr=tiny-cluster -o name)
do
  kubectl wait -n ${NAMESPACE} "$d" --for=condition=Ready --timeout=15m
done
# wait for druid pods
for s in $(kubectl get sts -n ${NAMESPACE} -l app=${NAMESPACE} -l druid_cr=tiny-cluster -o name)
do
  kubectl rollout status "$s" -n ${NAMESPACE}  --timeout=5m
done

# Running test job with an example dataset
make deploy-testjob

# Running a test DruidIngestion resource and wait for the task to be submitted
kubectl apply -f e2e/configs/druid-ingestion-cr.yaml -n ${NAMESPACE}
sleep 30 # wait for the manager to submit the ingestion task

# get the ingestion task ID and launch the monitoring job
taskId=`kubectl get druidingestion -n druid wikipedia-ingestion --template={{.status.taskId}}`
make deploy-testingestionjob TASK_ID=$taskId

# Running a test Kafka DruidIngestion resource and wait for the task to be submitted
kubectl apply -f e2e/configs/kafka-ingestion.yaml -n ${NAMESPACE}
sleep 30 # wait for the manager to submit the ingestion task

# Verify the supervisor task has been created
supervisorTaskId=`kubectl get druidingestion -n druid kafka-1 --template={{.status.taskId}}`
if [ -z "$supervisorTaskId" ]; then
  echo "Failed to get supervisor task ID"
  exit 1
else
  echo "Supervisor task ID: $supervisorTaskId"
fi

# Running a test Kafka DruidIngestion resource using nativeSpec and wait for the task to be submitted
kubectl apply -f e2e/configs/kafka-ingestion-native.yaml -n ${NAMESPACE}
sleep 30 # wait for the manager to submit the ingestion task

# Verify the supervisor task has been created
supervisorTaskId=`kubectl get druidingestion -n druid kafka-2 --template={{.status.taskId}}`
if [ -z "$supervisorTaskId" ]; then
  echo "Failed to get supervisor task ID"
  exit 1
else
  echo "Supervisor task ID: $supervisorTaskId"
fi

# Delete old druid
kubectl delete -f e2e/configs/druid-cr.yaml -n ${NAMESPACE}
for d in $(kubectl get pods -n ${NAMESPACE} -l app=druid -l druid_cr=tiny-cluster -o name)
do
  kubectl wait -n ${NAMESPACE} "$d" --for=delete --timeout=5m
done

# Start testing use-cases
bash e2e/test-extra-common-config.sh
kind delete cluster

