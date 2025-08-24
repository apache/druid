echo "Test: ExtraCommonConfig"
sed -e "s/CM_NAMESPACE/${NAMESPACE}/g" e2e/configs/extra-common-config.yaml | kubectl apply -n "${NAMESPACE}" -f -
sleep 10
# Wait for Druid
for d in $(kubectl get pods -n "${NAMESPACE}" -l app=druid -l druid_cr=tiny-cluster -o name)
do
  kubectl wait -n "${NAMESPACE}" "$d" --for=condition=Ready --timeout=5m
done
# wait for druid pods
for s in $(kubectl get sts -n "${NAMESPACE}" -l app="${NAMESPACE}" -l druid_cr=tiny-cluster -o name)
do
  kubectl rollout status "$s" -n "${NAMESPACE}"  --timeout=5m
done

extraDataTXT=$(kubectl get configmap -n "${NAMESPACE}" tiny-cluster-druid-common-config -o 'jsonpath={.data.test\.txt}')
if [[ "${extraDataTXT}" != "This Is Test" ]]
then
  echo "Bad value for key: test.txt"
  echo "Test: ExtraCommonConfig => FAILED\!"
fi

extraDataYAML=$(kubectl get configmap -n "${NAMESPACE}" tiny-cluster-druid-common-config -o 'jsonpath={.data.test\.yaml}')
if [[ "${extraDataYAML}" != "YAML" ]]
then
  echo "Bad value for key: test.yaml"
  echo "Test: ExtraCommonConfig => FAILED\!"
fi

kubectl delete -f e2e/configs/extra-common-config.yaml -n "${NAMESPACE}"
for d in $(kubectl get pods -n "${NAMESPACE}" -l app=druid -l druid_cr=tiny-cluster -o name)
do
  kubectl wait -n "${NAMESPACE}" "$d" --for=delete --timeout=5m
done

echo "Test: ExtraCommonConfig => SUCCESS\!"