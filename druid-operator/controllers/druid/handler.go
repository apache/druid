package druid

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"time"

	autoscalev2 "k8s.io/api/autoscaling/v2"
	networkingv1 "k8s.io/api/networking/v1"

	"github.com/datainfrahq/druid-operator/apis/druid/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	druidOpResourceHash          = "druidOpResourceHash"
	defaultCommonConfigMountPath = "/druid/conf/druid/_common"
	toBeDeletedLabel             = "toBeDeleted"
	deletionTSLabel              = "deletionTS"
)

var logger = logf.Log.WithName("druid_operator_handler")

func deployDruidCluster(ctx context.Context, sdk client.Client, m *v1alpha1.Druid, emitEvents EventEmitter) error {

	if err := verifyDruidSpec(m); err != nil {
		e := fmt.Errorf("invalid DruidSpec[%s:%s] due to [%s]", m.Kind, m.Name, err.Error())
		emitEvents.EmitEventGeneric(m, "DruidOperatorInvalidSpec", "", e)
		return nil
	}

	allNodeSpecs := getNodeSpecsByOrder(m)

	statefulSetNames := make(map[string]bool)
	deploymentNames := make(map[string]bool)
	serviceNames := make(map[string]bool)
	configMapNames := make(map[string]bool)
	podDisruptionBudgetNames := make(map[string]bool)
	hpaNames := make(map[string]bool)
	ingressNames := make(map[string]bool)
	pvcNames := make(map[string]bool)

	ls := makeLabelsForDruid(m)

	commonConfig, err := makeCommonConfigMap(ctx, sdk, m, ls)
	if err != nil {
		return err
	}
	commonConfigSHA, err := getObjectHash(commonConfig)
	if err != nil {
		return err
	}

	if _, err := sdkCreateOrUpdateAsNeeded(ctx, sdk,
		func() (object, error) { return makeCommonConfigMap(ctx, sdk, m, ls) },
		func() object { return &v1.ConfigMap{} },
		alwaysTrueIsEqualsFn, noopUpdaterFn, m, configMapNames, emitEvents); err != nil {
		return err
	}

	if m.GetDeletionTimestamp() != nil {
		return executeFinalizers(ctx, sdk, m, emitEvents)
	}

	if err := updateFinalizers(ctx, sdk, m, emitEvents); err != nil {
		return err
	}

	for _, elem := range allNodeSpecs {
		key := elem.key
		nodeSpec := elem.spec

		//Name in k8s must pass regex '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*'
		//So this unique string must follow same.
		nodeSpecUniqueStr := makeNodeSpecificUniqueString(m, key)

		lm := makeLabelsForNodeSpec(&nodeSpec, m, m.Name, nodeSpecUniqueStr)

		// create configmap first
		nodeConfig, err := makeConfigMapForNodeSpec(&nodeSpec, m, lm, nodeSpecUniqueStr)
		if err != nil {
			return err
		}

		nodeConfigSHA, err := getObjectHash(nodeConfig)
		if err != nil {
			return err
		}

		if _, err := sdkCreateOrUpdateAsNeeded(ctx, sdk,
			func() (object, error) { return nodeConfig, nil },
			func() object { return &v1.ConfigMap{} },
			alwaysTrueIsEqualsFn, noopUpdaterFn, m, configMapNames, emitEvents); err != nil {
			return err
		}

		//create services before creating statefulset
		firstServiceName := ""
		services := firstNonNilValue(nodeSpec.Services, m.Spec.Services).([]v1.Service)
		for _, svc := range services {
			if _, err := sdkCreateOrUpdateAsNeeded(ctx, sdk,
				func() (object, error) { return makeService(&svc, &nodeSpec, m, lm, nodeSpecUniqueStr) },
				func() object { return &v1.Service{} }, alwaysTrueIsEqualsFn,
				func(prev, curr object) { (curr.(*v1.Service)).Spec.ClusterIP = (prev.(*v1.Service)).Spec.ClusterIP },
				m, serviceNames, emitEvents); err != nil {
				return err
			}
			if firstServiceName == "" {
				firstServiceName = svc.ObjectMeta.Name
			}
		}

		nodeSpec.Ports = append(nodeSpec.Ports, v1.ContainerPort{ContainerPort: nodeSpec.DruidPort, Name: "druid-port"})

		if nodeSpec.Kind == "Deployment" {
			if deployCreateUpdateStatus, err := sdkCreateOrUpdateAsNeeded(ctx, sdk,
				func() (object, error) {
					return makeDeployment(&nodeSpec, m, lm, nodeSpecUniqueStr, fmt.Sprintf("%s-%s", commonConfigSHA, nodeConfigSHA), firstServiceName)
				},
				func() object { return &appsv1.Deployment{} },
				deploymentIsEquals, noopUpdaterFn, m, deploymentNames, emitEvents); err != nil {
				return err
			} else if m.Spec.RollingDeploy {

				if deployCreateUpdateStatus == resourceUpdated {
					return nil
				}

				// Ignore isObjFullyDeployed() for the first iteration ie cluster creation
				// will force cluster creation in parallel, post first iteration rolling updates
				// will be sequential.
				if m.Generation > 1 {
					// Check Deployment rolling update status, if in-progress then stop here
					done, err := isObjFullyDeployed(ctx, sdk, nodeSpec, nodeSpecUniqueStr, m, func() object { return &appsv1.Deployment{} }, emitEvents)
					if !done {
						return err
					}
				}
			}
		} else {

			//	scalePVCForSTS to be called only if volumeExpansion is supported by the storage class.
			//  Ignore for the first iteration ie cluster creation, else get sts shall unnecessary log errors.

			if m.Generation > 1 && m.Spec.ScalePvcSts {
				if err := expandStatefulSetVolumes(ctx, sdk, m, &nodeSpec, emitEvents, nodeSpecUniqueStr); err != nil {
					return err
				}
			}

			// Create/Update StatefulSet
			if stsCreateUpdateStatus, err := sdkCreateOrUpdateAsNeeded(ctx, sdk,
				func() (object, error) {
					return makeStatefulSet(&nodeSpec, m, lm, nodeSpecUniqueStr, fmt.Sprintf("%s-%s", commonConfigSHA, nodeConfigSHA), firstServiceName)
				},
				func() object { return &appsv1.StatefulSet{} },
				statefulSetIsEquals, noopUpdaterFn, m, statefulSetNames, emitEvents); err != nil {
				return err
			} else if m.Spec.RollingDeploy {

				if stsCreateUpdateStatus == resourceUpdated {
					// we just updated, give sts controller some time to update status of replicas after update
					return nil
				}

				// Default is set to true
				execCheckCrashStatus(ctx, sdk, &nodeSpec, m, nodeSpecUniqueStr, emitEvents)

				// Ignore isObjFullyDeployed() for the first iteration ie cluster creation
				// will force cluster creation in parallel, post first iteration rolling updates
				// will be sequential.
				if m.Generation > 1 {
					//Check StatefulSet rolling update status, if in-progress then stop here
					done, err := isObjFullyDeployed(ctx, sdk, nodeSpec, nodeSpecUniqueStr, m, func() object { return &appsv1.StatefulSet{} }, emitEvents)
					if !done {
						return err
					}
				}
			}

			// Default is set to true
			execCheckCrashStatus(ctx, sdk, &nodeSpec, m, nodeSpecUniqueStr, emitEvents)
		}

		// Create Ingress Spec
		if nodeSpec.Ingress != nil {
			if _, err := sdkCreateOrUpdateAsNeeded(ctx, sdk,
				func() (object, error) {
					return makeIngress(&nodeSpec, m, ls, nodeSpecUniqueStr)
				},
				func() object { return &networkingv1.Ingress{} },
				alwaysTrueIsEqualsFn, noopUpdaterFn, m, ingressNames, emitEvents); err != nil {
				return err
			}
		}

		// Create PodDisruptionBudget
		if nodeSpec.PodDisruptionBudgetSpec != nil {
			if _, err := sdkCreateOrUpdateAsNeeded(ctx, sdk,
				func() (object, error) { return makePodDisruptionBudget(&nodeSpec, m, lm, nodeSpecUniqueStr) },
				func() object { return &policyv1.PodDisruptionBudget{} },
				alwaysTrueIsEqualsFn, noopUpdaterFn, m, podDisruptionBudgetNames, emitEvents); err != nil {
				return err
			}
		}

		// Create HPA Spec
		if nodeSpec.HPAutoScaler != nil {
			if _, err := sdkCreateOrUpdateAsNeeded(ctx, sdk,
				func() (object, error) {
					return makeHorizontalPodAutoscaler(&nodeSpec, m, ls, nodeSpecUniqueStr)
				},
				func() object { return &autoscalev2.HorizontalPodAutoscaler{} },
				alwaysTrueIsEqualsFn, noopUpdaterFn, m, hpaNames, emitEvents); err != nil {
				return err
			}
		}

		if nodeSpec.PersistentVolumeClaim != nil {
			for _, pvc := range nodeSpec.PersistentVolumeClaim {
				if _, err := sdkCreateOrUpdateAsNeeded(ctx, sdk,
					func() (object, error) { return makePersistentVolumeClaim(&pvc, &nodeSpec, m, lm, nodeSpecUniqueStr) },
					func() object { return &v1.PersistentVolumeClaim{} }, alwaysTrueIsEqualsFn,
					noopUpdaterFn,
					m, pvcNames, emitEvents); err != nil {
					return err
				}
			}
		}
	}

	// Ignore on cluster creation
	if m.Generation > 1 && m.Spec.DeleteOrphanPvc {
		if err := deleteOrphanPVC(ctx, sdk, m, emitEvents); err != nil {
			return err
		}
	}

	//update status and delete unwanted resources
	updatedStatus := v1alpha1.DruidClusterStatus{}

	updatedStatus.StatefulSets = deleteUnusedResources(ctx, sdk, m, statefulSetNames, ls,
		func() objectList { return &appsv1.StatefulSetList{} },
		func(listObj runtime.Object) []object {
			items := listObj.(*appsv1.StatefulSetList).Items
			result := make([]object, len(items))
			for i := 0; i < len(items); i++ {
				result[i] = &items[i]
			}
			return result
		}, emitEvents)
	sort.Strings(updatedStatus.StatefulSets)

	updatedStatus.Deployments = deleteUnusedResources(ctx, sdk, m, deploymentNames, ls,
		func() objectList { return &appsv1.DeploymentList{} },
		func(listObj runtime.Object) []object {
			items := listObj.(*appsv1.DeploymentList).Items
			result := make([]object, len(items))
			for i := 0; i < len(items); i++ {
				result[i] = &items[i]
			}
			return result
		}, emitEvents)
	sort.Strings(updatedStatus.Deployments)

	updatedStatus.HPAutoScalers = deleteUnusedResources(ctx, sdk, m, hpaNames, ls,
		func() objectList { return &autoscalev2.HorizontalPodAutoscalerList{} },
		func(listObj runtime.Object) []object {
			items := listObj.(*autoscalev2.HorizontalPodAutoscalerList).Items
			result := make([]object, len(items))
			for i := 0; i < len(items); i++ {
				result[i] = &items[i]
			}
			return result
		}, emitEvents)
	sort.Strings(updatedStatus.HPAutoScalers)

	updatedStatus.Ingress = deleteUnusedResources(ctx, sdk, m, ingressNames, ls,
		func() objectList { return &networkingv1.IngressList{} },
		func(listObj runtime.Object) []object {
			items := listObj.(*networkingv1.IngressList).Items
			result := make([]object, len(items))
			for i := 0; i < len(items); i++ {
				result[i] = &items[i]
			}
			return result
		}, emitEvents)
	sort.Strings(updatedStatus.Ingress)

	updatedStatus.PodDisruptionBudgets = deleteUnusedResources(ctx, sdk, m, podDisruptionBudgetNames, ls,
		func() objectList { return &policyv1.PodDisruptionBudgetList{} },
		func(listObj runtime.Object) []object {
			items := listObj.(*policyv1.PodDisruptionBudgetList).Items
			result := make([]object, len(items))
			for i := 0; i < len(items); i++ {
				result[i] = &items[i]
			}
			return result
		}, emitEvents)
	sort.Strings(updatedStatus.PodDisruptionBudgets)

	updatedStatus.Services = deleteUnusedResources(ctx, sdk, m, serviceNames, ls,
		func() objectList { return &v1.ServiceList{} },
		func(listObj runtime.Object) []object {
			items := listObj.(*v1.ServiceList).Items
			result := make([]object, len(items))
			for i := 0; i < len(items); i++ {
				result[i] = &items[i]
			}
			return result
		}, emitEvents)
	sort.Strings(updatedStatus.Services)

	updatedStatus.ConfigMaps = deleteUnusedResources(ctx, sdk, m, configMapNames, ls,
		func() objectList { return &v1.ConfigMapList{} },
		func(listObj runtime.Object) []object {
			items := listObj.(*v1.ConfigMapList).Items
			result := make([]object, len(items))
			for i := 0; i < len(items); i++ {
				result[i] = &items[i]
			}
			return result
		}, emitEvents)
	sort.Strings(updatedStatus.ConfigMaps)

	podList, _ := readers.List(ctx, sdk, m, makeLabelsForDruid(m), emitEvents, func() objectList { return &v1.PodList{} }, func(listObj runtime.Object) []object {
		items := listObj.(*v1.PodList).Items
		result := make([]object, len(items))
		for i := 0; i < len(items); i++ {
			result[i] = &items[i]
		}
		return result
	})

	updatedStatus.Pods = getPodNames(podList)
	sort.Strings(updatedStatus.Pods)

	// All druid nodes are in Ready state.
	// In case any druid node goes into a bad state, it shall be handled in above rollingDeploy block
	updatedStatus.DruidNodeStatus = *newDruidNodeTypeStatus(v1.ConditionTrue, v1alpha1.DruidClusterReady, "", nil)

	// In case of rolling Deploy not present OR any error not catched in the above block, check the pod ready
	// state and condition and patch the status with the CR
	for _, po := range podList {
		for _, c := range po.(*v1.Pod).Status.Conditions {
			if c.Type == v1.PodReady && c.Status == v1.ConditionFalse {
				updatedStatus.DruidNodeStatus = *newDruidNodeTypeStatus(v1.ConditionTrue, v1alpha1.DruidNodeErrorState, po.GetName(), errors.New(c.Reason))
			}
		}
	}

	err = druidClusterStatusPatcher(ctx, sdk, updatedStatus, m, emitEvents)
	if err != nil {
		return err
	}

	return nil
}

func deleteSTSAndPVC(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid, stsList, pvcList []object, emitEvents EventEmitter) error {

	for _, sts := range stsList {
		err := writers.Delete(ctx, sdk, drd, sts, emitEvents, &client.DeleteAllOfOptions{})
		if err != nil {
			return err
		}
	}

	for i := range pvcList {
		err := writers.Delete(ctx, sdk, drd, pvcList[i], emitEvents, &client.DeleteAllOfOptions{})
		if err != nil {
			return err
		}
	}

	return nil
}

func checkIfCRExists(ctx context.Context, sdk client.Client, m *v1alpha1.Druid, emitEvents EventEmitter) bool {
	_, err := readers.Get(ctx, sdk, m.Name, m, func() object { return &v1alpha1.Druid{} }, emitEvents)
	if err != nil {
		return false
	} else {
		return true
	}
}

func deleteOrphanPVC(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid, emitEvents EventEmitter) error {

	podList, err := readers.List(ctx, sdk, drd, makeLabelsForDruid(drd), emitEvents, func() objectList { return &v1.PodList{} }, func(listObj runtime.Object) []object {
		items := listObj.(*v1.PodList).Items
		result := make([]object, len(items))
		for i := 0; i < len(items); i++ {
			result[i] = &items[i]
		}
		return result
	})
	if err != nil {
		return err
	}

	pvcLabels := map[string]string{
		"druid_cr": drd.Name,
	}

	pvcList, err := readers.List(ctx, sdk, drd, pvcLabels, emitEvents, func() objectList { return &v1.PersistentVolumeClaimList{} }, func(listObj runtime.Object) []object {
		items := listObj.(*v1.PersistentVolumeClaimList).Items
		result := make([]object, len(items))
		for i := 0; i < len(items); i++ {
			result[i] = &items[i]
		}
		return result
	})
	if err != nil {
		return err
	}

	// Fix: https://github.com/datainfrahq/druid-operator/issues/149
	for _, pod := range podList {
		if pod.(*v1.Pod).Status.Phase != v1.PodRunning {
			return nil
		}
		for _, status := range pod.(*v1.Pod).Status.Conditions {
			if status.Status != v1.ConditionTrue {
				return nil
			}
		}
	}

	mountedPVC := make([]string, len(podList))
	for _, pod := range podList {
		if pod.(*v1.Pod).Spec.Volumes != nil {
			for _, vol := range pod.(*v1.Pod).Spec.Volumes {
				if vol.PersistentVolumeClaim != nil && pod.(*v1.Pod).Status.Phase != v1.PodPending {
					if !ContainsString(mountedPVC, vol.PersistentVolumeClaim.ClaimName) {
						mountedPVC = append(mountedPVC, vol.PersistentVolumeClaim.ClaimName)
					}
				}
			}
		}

	}

	if mountedPVC != nil {
		for i, pvc := range pvcList {

			if !ContainsString(mountedPVC, pvc.GetName()) {

				if _, ok := pvc.GetLabels()[toBeDeletedLabel]; ok {
					err := checkPVCLabelsAndDelete(ctx, sdk, drd, emitEvents, pvcList[i])
					if err != nil {
						return err
					}
				} else {
					// set labels when pvc comes for deletion for the first time
					getPvcLabels := pvc.GetLabels()
					getPvcLabels[toBeDeletedLabel] = "yes"
					getPvcLabels[deletionTSLabel] = strconv.FormatInt(time.Now().Unix(), 10)

					err = setPVCLabels(ctx, sdk, drd, emitEvents, pvcList[i], getPvcLabels, true)
					if err != nil {
						return err
					}
				}
			} else {
				// do not delete pvc
				if _, ok := pvc.GetLabels()[toBeDeletedLabel]; ok {
					getPvcLabels := pvc.GetLabels()
					delete(getPvcLabels, toBeDeletedLabel)
					delete(getPvcLabels, deletionTSLabel)

					err = setPVCLabels(ctx, sdk, drd, emitEvents, pvcList[i], getPvcLabels, false)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func checkPVCLabelsAndDelete(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid, emitEvents EventEmitter, pvc object) error {
	deletionTS := pvc.GetLabels()[deletionTSLabel]

	parsedDeletionTS, err := strconv.ParseInt(deletionTS, 10, 64)

	if err != nil {
		msg := fmt.Sprintf("Unable to parse label %s [%s:%s]", deletionTSLabel, deletionTS, pvc.GetName())
		logger.Info(msg, "name", drd.Name, "namespace", drd.Namespace)
		return err
	}

	timeNow := time.Now().Unix()
	timeDiff := timeDifference(parsedDeletionTS, timeNow)

	if timeDiff >= int64(time.Second/time.Second)*60 {
		// delete pvc
		err = writers.Delete(ctx, sdk, drd, pvc, emitEvents, &client.DeleteAllOfOptions{})
		if err != nil {
			return err
		} else {
			msg := fmt.Sprintf("Deleted orphaned pvc [%s:%s] successfully", pvc.GetName(), drd.Namespace)
			logger.Info(msg, "name", drd.Name, "namespace", drd.Namespace)
		}
	} else {
		// wait for 60s
		msg := fmt.Sprintf("pvc [%s:%s] marked to be deleted after %ds", pvc.GetName(), drd.Namespace, 60-timeDiff)
		logger.Info(msg, "name", drd.Name, "namespace", drd.Namespace)
	}
	return nil
}

func setPVCLabels(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid, emitEvents EventEmitter, pvc object, labels map[string]string, isSetLabel bool) error {

	pvc.SetLabels(labels)
	_, err := writers.Update(ctx, sdk, drd, pvc, emitEvents)
	if err != nil {
		return err
	} else {
		if isSetLabel {
			msg := fmt.Sprintf("marked pvc for deletion , added labels %s and %s successfully [%s]", toBeDeletedLabel, deletionTSLabel, pvc.GetName())
			logger.Info(msg, "name", drd.Name, "namespace", drd.Namespace)
		} else {
			msg := fmt.Sprintf("unmarked pvc for deletion, removed labels %s and %s successfully in pvc [%s]", toBeDeletedLabel, deletionTSLabel, pvc.GetName())
			logger.Info(msg, "name", drd.Name, "namespace", drd.Namespace)
		}
	}
	return nil
}

func execCheckCrashStatus(ctx context.Context, sdk client.Client, nodeSpec *v1alpha1.DruidNodeSpec, m *v1alpha1.Druid, nodeSpecUniqueStr string, event EventEmitter) {
	if m.Spec.ForceDeleteStsPodOnError == false {
		return
	} else {
		if nodeSpec.PodManagementPolicy == "OrderedReady" {
			checkCrashStatus(ctx, sdk, nodeSpec, m, nodeSpecUniqueStr, event)
		}
	}
}

func checkCrashStatus(ctx context.Context, sdk client.Client, nodeSpec *v1alpha1.DruidNodeSpec, drd *v1alpha1.Druid, nodeSpecUniqueStr string, emitEvents EventEmitter) error {

	podList, err := readers.List(ctx, sdk, drd, makeLabelsForNodeSpec(nodeSpec, drd, drd.Name, nodeSpecUniqueStr), emitEvents, func() objectList { return &v1.PodList{} }, func(listObj runtime.Object) []object {
		items := listObj.(*v1.PodList).Items
		result := make([]object, len(items))
		for i := 0; i < len(items); i++ {
			result[i] = &items[i]
		}
		return result
	})
	if err != nil {
		return err
	}

	// the below condition evalutes if a pod is in
	// 1. failed state 2. unknown state
	// OR condtion.status is false which evalutes if neither of these conditions are met
	// 1. ContainersReady 2. PodInitialized 3. PodReady 4. PodScheduled
	for _, p := range podList {
		if p.(*v1.Pod).Status.Phase == v1.PodFailed || p.(*v1.Pod).Status.Phase == v1.PodUnknown {
			err := writers.Delete(ctx, sdk, drd, p, emitEvents, &client.DeleteOptions{})
			if err != nil {
				return err
			}
			msg := fmt.Sprintf("Deleted pod [%s] in namespace [%s], since it was in [%s] state.", p.GetName(), p.GetNamespace(), p.(*v1.Pod).Status.Phase)
			logger.Info(msg, "Object", stringifyForLogging(p, drd), "name", drd.Name, "namespace", drd.Namespace)
		} else {
			for _, condition := range p.(*v1.Pod).Status.Conditions {
				if condition.Type == v1.ContainersReady {
					if condition.Status == v1.ConditionFalse {
						for _, containerStatus := range p.(*v1.Pod).Status.ContainerStatuses {
							if containerStatus.RestartCount > 1 {
								err := writers.Delete(ctx, sdk, drd, p, emitEvents, &client.DeleteOptions{})
								if err != nil {
									return err
								}
								msg := fmt.Sprintf("Deleted pod [%s] in namespace [%s], since the container [%s] was crashlooping.", p.GetName(), p.GetNamespace(), containerStatus.Name)
								logger.Info(msg, "Object", stringifyForLogging(p, drd), "name", drd.Name, "namespace", drd.Namespace)
							}
						}
					}
				}
			}
		}
	}

	return nil
}

func deleteUnusedResources(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid,
	names map[string]bool, selectorLabels map[string]string, emptyListObjFn func() objectList, itemsExtractorFn func(obj runtime.Object) []object, emitEvents EventEmitter) []string {

	listOpts := []client.ListOption{
		client.InNamespace(drd.Namespace),
		client.MatchingLabels(selectorLabels),
	}

	survivorNames := make([]string, 0, len(names))

	listObj := emptyListObjFn()

	if err := sdk.List(ctx, listObj, listOpts...); err != nil {
		e := fmt.Errorf("failed to list [%s] due to [%s]", listObj.GetObjectKind().GroupVersionKind().Kind, err.Error())
		logger.Error(e, e.Error(), "name", drd.Name, "namespace", drd.Namespace)
	} else {
		for _, s := range itemsExtractorFn(listObj) {
			if names[s.GetName()] == false {
				err := writers.Delete(ctx, sdk, drd, s, emitEvents, &client.DeleteOptions{})
				if err != nil {
					survivorNames = append(survivorNames, s.GetName())
				}
			} else {
				survivorNames = append(survivorNames, s.GetName())
			}
		}
	}

	return survivorNames
}

func alwaysTrueIsEqualsFn(prev, curr object) bool {
	return true
}

func noopUpdaterFn(prev, curr object) {
	// do nothing
}

func sdkCreateOrUpdateAsNeeded(
	ctx context.Context,
	sdk client.Client,
	objFn func() (object, error),
	emptyObjFn func() object,
	isEqualFn func(prev, curr object) bool,
	updaterFn func(prev, curr object),
	drd *v1alpha1.Druid,
	names map[string]bool,
	emitEvent EventEmitter) (DruidNodeStatus, error) {

	if obj, err := objFn(); err != nil {
		return "", err
	} else {
		names[obj.GetName()] = true

		addOwnerRefToObject(obj, asOwner(drd))
		addHashToObject(obj)

		prevObj := emptyObjFn()
		if err := sdk.Get(ctx, *namespacedName(obj.GetName(), obj.GetNamespace()), prevObj); err != nil {
			if apierrors.IsNotFound(err) {
				// resource does not exist, create it.
				create, err := writers.Create(ctx, sdk, drd, obj, emitEvent)
				if err != nil {
					return "", err
				} else {
					return create, nil
				}
			} else {
				e := fmt.Errorf("Failed to get [%s:%s] due to [%s].", obj.GetObjectKind().GroupVersionKind().Kind, obj.GetName(), err.Error())
				logger.Error(e, e.Error(), "Prev object", stringifyForLogging(prevObj, drd), "name", drd.Name, "namespace", drd.Namespace)
				emitEvent.EmitEventGeneric(drd, string(druidOjectGetFail), "", err)
				return "", e
			}
		} else {
			// resource already exists, updated it if needed
			if obj.GetAnnotations()[druidOpResourceHash] != prevObj.GetAnnotations()[druidOpResourceHash] || !isEqualFn(prevObj, obj) {

				obj.SetResourceVersion(prevObj.GetResourceVersion())
				updaterFn(prevObj, obj)
				update, err := writers.Update(ctx, sdk, drd, obj, emitEvent)
				if err != nil {
					return "", err
				} else {
					return update, err
				}
			} else {
				return "", nil
			}
		}
	}
}

func isObjFullyDeployed(ctx context.Context, sdk client.Client, nodeSpec v1alpha1.DruidNodeSpec, nodeSpecUniqueStr string, drd *v1alpha1.Druid, emptyObjFn func() object, emitEvent EventEmitter) (bool, error) {

	// Get Object
	obj, err := readers.Get(ctx, sdk, nodeSpecUniqueStr, drd, emptyObjFn, emitEvent)
	if err != nil {
		return false, err
	}

	// In case obj is a statefulset or deployment, make sure the sts/deployment has successfully reconciled to desired state
	// TODO: @AdheipSingh once https://github.com/kubernetes/kubernetes/blob/master/pkg/apis/apps/types.go#L217 k8s conditions detect sts fail errors.
	if detectType(obj) == "*v1.StatefulSet" {
		if obj.(*appsv1.StatefulSet).Status.CurrentRevision != obj.(*appsv1.StatefulSet).Status.UpdateRevision {
			return false, nil
		} else if obj.(*appsv1.StatefulSet).Status.CurrentReplicas != obj.(*appsv1.StatefulSet).Status.ReadyReplicas {
			return false, nil
		} else {
			return obj.(*appsv1.StatefulSet).Status.CurrentRevision == obj.(*appsv1.StatefulSet).Status.UpdateRevision, nil
		}
	} else if detectType(obj) == "*v1.Deployment" {
		for _, condition := range obj.(*appsv1.Deployment).Status.Conditions {
			// This detects a failure condition, operator should send a rolling deployment failed event
			if condition.Type == appsv1.DeploymentReplicaFailure {
				return false, errors.New(condition.Reason)
			} else if condition.Type == appsv1.DeploymentProgressing && condition.Status != v1.ConditionTrue || obj.(*appsv1.Deployment).Status.ReadyReplicas != obj.(*appsv1.Deployment).Status.Replicas {
				return false, nil
			} else {
				return obj.(*appsv1.Deployment).Status.ReadyReplicas == obj.(*appsv1.Deployment).Status.Replicas, nil
			}
		}
	}
	return false, nil
}

// desVolumeClaimTemplateSize: the druid CR holds this value for a sts volumeclaimtemplate
// currVolumeClaimTemplateSize: the sts owned by druid CR holds this value in volumeclaimtemplate
// pvcSize: the pvc referenced by the sts holds this value
// type of vars is resource.Quantity. ref: https://godoc.org/k8s.io/apimachinery/pkg/api/resource
func getVolumeClaimTemplateSizes(sts object, nodeSpec *v1alpha1.DruidNodeSpec, pvc []object) (desVolumeClaimTemplateSize, currVolumeClaimTemplateSize, pvcSize []resource.Quantity) {

	for i := range nodeSpec.VolumeClaimTemplates {
		desVolumeClaimTemplateSize = append(desVolumeClaimTemplateSize, nodeSpec.VolumeClaimTemplates[i].Spec.Resources.Requests[v1.ResourceStorage])
	}

	for i := range sts.(*appsv1.StatefulSet).Spec.VolumeClaimTemplates {
		currVolumeClaimTemplateSize = append(currVolumeClaimTemplateSize, sts.(*appsv1.StatefulSet).Spec.VolumeClaimTemplates[i].Spec.Resources.Requests[v1.ResourceStorage])
	}

	for i := range pvc {
		pvcSize = append(pvcSize, pvc[i].(*v1.PersistentVolumeClaim).Spec.Resources.Requests[v1.ResourceStorage])
	}

	return desVolumeClaimTemplateSize, currVolumeClaimTemplateSize, pvcSize

}

func stringifyForLogging(obj object, drd *v1alpha1.Druid) string {
	if bytes, err := json.Marshal(obj); err != nil {
		logger.Error(err, err.Error(), fmt.Sprintf("Failed to serialize [%s:%s]", obj.GetObjectKind().GroupVersionKind().Kind, obj.GetName()), "name", drd.Name, "namespace", drd.Namespace)
		return fmt.Sprintf("%v", obj)
	} else {
		return string(bytes)
	}

}

func addHashToObject(obj object) error {
	if sha, err := getObjectHash(obj); err != nil {
		return err
	} else {
		annotations := obj.GetAnnotations()
		if annotations == nil {
			annotations = make(map[string]string)
			obj.SetAnnotations(annotations)
		}
		annotations[druidOpResourceHash] = sha
		return nil
	}
}

func getObjectHash(obj object) (string, error) {
	if bytes, err := json.Marshal(obj); err != nil {
		return "", err
	} else {
		sha1Bytes := sha1.Sum(bytes)
		return base64.StdEncoding.EncodeToString(sha1Bytes[:]), nil
	}
}

func makeNodeSpecificUniqueString(m *v1alpha1.Druid, key string) string {
	return fmt.Sprintf("druid-%s-%s", m.Name, key)
}

func makeService(svc *v1.Service, nodeSpec *v1alpha1.DruidNodeSpec, m *v1alpha1.Druid, ls map[string]string, nodeSpecUniqueStr string) (*v1.Service, error) {
	svc.TypeMeta = metav1.TypeMeta{
		APIVersion: "v1",
		Kind:       "Service",
	}

	svc.ObjectMeta.Name = getServiceName(svc.ObjectMeta.Name, nodeSpecUniqueStr)

	svc.ObjectMeta.Namespace = m.Namespace

	if svc.ObjectMeta.Labels == nil {
		svc.ObjectMeta.Labels = ls
	} else {
		for k, v := range ls {
			svc.ObjectMeta.Labels[k] = v
		}
	}

	if svc.Spec.Selector == nil {
		svc.Spec.Selector = ls
	} else {
		for k, v := range ls {
			svc.Spec.Selector[k] = v
		}
	}

	if svc.Spec.Ports == nil {
		svc.Spec.Ports = []v1.ServicePort{
			{
				Name:       "service-port",
				Port:       nodeSpec.DruidPort,
				TargetPort: intstr.FromInt(int(nodeSpec.DruidPort)),
			},
		}
	}

	return svc, nil
}

func getServiceName(nameTemplate, nodeSpecUniqueStr string) string {
	if nameTemplate == "" {
		return nodeSpecUniqueStr
	} else {
		return fmt.Sprintf(nameTemplate, nodeSpecUniqueStr)
	}
}

func getPersistentVolumeClaim(nodeSpec *v1alpha1.DruidNodeSpec, m *v1alpha1.Druid) []v1.PersistentVolumeClaim {
	pvc := []v1.PersistentVolumeClaim{}

	for _, val := range m.Spec.VolumeClaimTemplates {
		pvc = append(pvc, val)
	}

	for _, val := range nodeSpec.VolumeClaimTemplates {
		pvc = append(pvc, val)
	}

	return pvc

}

func getVolumeMounts(nodeSpec *v1alpha1.DruidNodeSpec, m *v1alpha1.Druid) []v1.VolumeMount {
	volumeMount := []v1.VolumeMount{
		{
			MountPath: firstNonEmptyStr(m.Spec.CommonConfigMountPath, defaultCommonConfigMountPath),
			Name:      "common-config-volume",
			ReadOnly:  true,
		},
		{
			MountPath: firstNonEmptyStr(nodeSpec.NodeConfigMountPath, getNodeConfigMountPath(nodeSpec)),
			Name:      "nodetype-config-volume",
			ReadOnly:  true,
		},
	}

	volumeMount = append(volumeMount, m.Spec.VolumeMounts...)
	volumeMount = append(volumeMount, nodeSpec.VolumeMounts...)
	return volumeMount
}

func getTolerations(nodeSpec *v1alpha1.DruidNodeSpec, m *v1alpha1.Druid) []v1.Toleration {
	tolerations := []v1.Toleration{}

	for _, val := range m.Spec.Tolerations {
		tolerations = append(tolerations, val)
	}
	for _, val := range nodeSpec.Tolerations {
		tolerations = append(tolerations, val)
	}

	return tolerations
}

func getTopologySpreadConstraints(nodeSpec *v1alpha1.DruidNodeSpec) []v1.TopologySpreadConstraint {
	var topologySpreadConstraint []v1.TopologySpreadConstraint

	for _, val := range nodeSpec.TopologySpreadConstraints {
		topologySpreadConstraint = append(topologySpreadConstraint, val)
	}

	return topologySpreadConstraint
}

func getVolume(nodeSpec *v1alpha1.DruidNodeSpec, m *v1alpha1.Druid, nodeSpecUniqueStr string) []v1.Volume {
	volumesHolder := []v1.Volume{
		{
			Name: "common-config-volume",
			VolumeSource: v1.VolumeSource{
				ConfigMap: &v1.ConfigMapVolumeSource{
					LocalObjectReference: v1.LocalObjectReference{
						Name: fmt.Sprintf("%s-druid-common-config", m.ObjectMeta.Name),
					},
				}},
		},
		{
			Name: "nodetype-config-volume",
			VolumeSource: v1.VolumeSource{
				ConfigMap: &v1.ConfigMapVolumeSource{
					LocalObjectReference: v1.LocalObjectReference{
						Name: fmt.Sprintf("%s-config", nodeSpecUniqueStr),
					},
				},
			},
		},
	}
	volumesHolder = append(volumesHolder, m.Spec.Volumes...)
	volumesHolder = append(volumesHolder, nodeSpec.Volumes...)
	return volumesHolder
}

func getEnv(nodeSpec *v1alpha1.DruidNodeSpec, m *v1alpha1.Druid, configMapSHA string) []v1.EnvVar {
	envHolder := firstNonNilValue(nodeSpec.Env, m.Spec.Env).([]v1.EnvVar)
	// enables to do the trick to force redeployment in case of configmap changes.
	envHolder = append(envHolder, v1.EnvVar{Name: "configMapSHA", Value: configMapSHA})

	return envHolder
}

func getAffinity(nodeSpec *v1alpha1.DruidNodeSpec, m *v1alpha1.Druid) *v1.Affinity {
	affinity := firstNonNilValue(m.Spec.Affinity, &v1.Affinity{}).(*v1.Affinity)
	affinity = firstNonNilValue(nodeSpec.Affinity, affinity).(*v1.Affinity)
	return affinity
}

func setLivenessProbe(nodeSpec *v1alpha1.DruidNodeSpec, m *v1alpha1.Druid) *v1.Probe {
	probeType := "liveness"
	livenessProbe := updateDefaultPortInProbe(
		firstNonNilValue(nodeSpec.LivenessProbe, m.Spec.LivenessProbe).(*v1.Probe),
		nodeSpec.DruidPort)
	if livenessProbe == nil && m.Spec.DefaultProbes {
		livenessProbe = setDefaultProbe(nodeSpec.DruidPort, nodeSpec.NodeType, probeType)
	}
	return livenessProbe
}

func setReadinessProbe(nodeSpec *v1alpha1.DruidNodeSpec, m *v1alpha1.Druid) *v1.Probe {
	probeType := "readiness"
	readinessProbe := updateDefaultPortInProbe(
		firstNonNilValue(nodeSpec.ReadinessProbe, m.Spec.ReadinessProbe).(*v1.Probe),
		nodeSpec.DruidPort)
	if readinessProbe == nil && m.Spec.DefaultProbes {
		readinessProbe = setDefaultProbe(nodeSpec.DruidPort, nodeSpec.NodeType, probeType)
	}
	return readinessProbe
}

func setStartUpProbe(nodeSpec *v1alpha1.DruidNodeSpec, m *v1alpha1.Druid) *v1.Probe {
	probeType := "startup"
	startUpProbe := updateDefaultPortInProbe(
		firstNonNilValue(nodeSpec.StartUpProbe, m.Spec.StartUpProbe).(*v1.Probe),
		nodeSpec.DruidPort)
	if startUpProbe == nil && m.Spec.DefaultProbes {
		startUpProbe = setDefaultProbe(nodeSpec.DruidPort, nodeSpec.NodeType, probeType)
	}
	return startUpProbe
}

func getEnvFrom(nodeSpec *v1alpha1.DruidNodeSpec, m *v1alpha1.Druid) []v1.EnvFromSource {
	envFromHolder := firstNonNilValue(nodeSpec.EnvFrom, m.Spec.EnvFrom).([]v1.EnvFromSource)
	return envFromHolder
}

func getRollingUpdateStrategy(nodeSpec *v1alpha1.DruidNodeSpec) *appsv1.RollingUpdateDeployment {
	var nil *int32 = nil
	if nodeSpec.MaxSurge != nil || nodeSpec.MaxUnavailable != nil {
		return &appsv1.RollingUpdateDeployment{
			MaxUnavailable: &intstr.IntOrString{
				IntVal: *nodeSpec.MaxUnavailable,
			},
			MaxSurge: &intstr.IntOrString{
				IntVal: *nodeSpec.MaxSurge,
			},
		}
	}
	return &appsv1.RollingUpdateDeployment{}

}

// makeStatefulSet shall create statefulset object.
func makeStatefulSet(nodeSpec *v1alpha1.DruidNodeSpec, m *v1alpha1.Druid, ls map[string]string, nodeSpecUniqueStr, configMapSHA, serviceName string) (*appsv1.StatefulSet, error) {

	return &appsv1.StatefulSet{
		TypeMeta: metav1.TypeMeta{
			Kind:       "StatefulSet",
			APIVersion: "apps/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        fmt.Sprintf("%s", nodeSpecUniqueStr),
			Annotations: makeAnnotationsForWorkload(nodeSpec, m),
			Namespace:   m.Namespace,
			Labels:      ls,
		},
		Spec: makeStatefulSetSpec(nodeSpec, m, ls, nodeSpecUniqueStr, configMapSHA, serviceName),
	}, nil
}

func statefulSetIsEquals(obj1, obj2 object) bool {

	// This used to match replica counts, but was reverted to fix https://github.com/datainfrahq/druid-operator/issues/160
	// because it is legitimate for HPA to change replica counts and operator shouldn't reset those.

	return true
}

// makeDeployment shall create deployment object.
func makeDeployment(nodeSpec *v1alpha1.DruidNodeSpec, m *v1alpha1.Druid, ls map[string]string, nodeSpecUniqueStr, configMapSHA, serviceName string) (*appsv1.Deployment, error) {
	return &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Deployment",
			APIVersion: "apps/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        fmt.Sprintf("%s", nodeSpecUniqueStr),
			Annotations: makeAnnotationsForWorkload(nodeSpec, m),
			Namespace:   m.Namespace,
			Labels:      ls,
		},
		Spec: makeDeploymentSpec(nodeSpec, m, ls, nodeSpecUniqueStr, configMapSHA, serviceName),
	}, nil
}

func deploymentIsEquals(obj1, obj2 object) bool {

	// This used to match replica counts, but was reverted to fix https://github.com/datainfrahq/druid-operator/issues/160
	// because it is legitimate for HPA to change replica counts and operator shouldn't reset those.

	return true
}

// makeStatefulSetSpec shall create statefulset spec for statefulsets.
func makeStatefulSetSpec(nodeSpec *v1alpha1.DruidNodeSpec, m *v1alpha1.Druid, ls map[string]string, nodeSpecificUniqueString, configMapSHA, serviceName string) appsv1.StatefulSetSpec {

	updateStrategy := firstNonNilValue(m.Spec.UpdateStrategy, &appsv1.StatefulSetUpdateStrategy{}).(*appsv1.StatefulSetUpdateStrategy)
	updateStrategy = firstNonNilValue(nodeSpec.UpdateStrategy, updateStrategy).(*appsv1.StatefulSetUpdateStrategy)

	stsSpec := appsv1.StatefulSetSpec{
		ServiceName: serviceName,
		Selector: &metav1.LabelSelector{
			MatchLabels: ls,
		},
		Replicas:             &nodeSpec.Replicas,
		PodManagementPolicy:  appsv1.PodManagementPolicyType(firstNonEmptyStr(firstNonEmptyStr(string(nodeSpec.PodManagementPolicy), string(m.Spec.PodManagementPolicy)), string(appsv1.ParallelPodManagement))),
		UpdateStrategy:       *updateStrategy,
		Template:             makePodTemplate(nodeSpec, m, ls, nodeSpecificUniqueString, configMapSHA),
		VolumeClaimTemplates: getPersistentVolumeClaim(nodeSpec, m),
	}

	return stsSpec

}

// makeDeploymentSpec shall create deployment Spec for deployments.
func makeDeploymentSpec(nodeSpec *v1alpha1.DruidNodeSpec, m *v1alpha1.Druid, ls map[string]string, nodeSpecificUniqueString, configMapSHA, serviceName string) appsv1.DeploymentSpec {
	deploySpec := appsv1.DeploymentSpec{
		Selector: &metav1.LabelSelector{
			MatchLabels: ls,
		},
		Replicas: &nodeSpec.Replicas,
		Template: makePodTemplate(nodeSpec, m, ls, nodeSpecificUniqueString, configMapSHA),
		Strategy: appsv1.DeploymentStrategy{
			Type:          "RollingUpdate",
			RollingUpdate: getRollingUpdateStrategy(nodeSpec),
		},
	}

	return deploySpec
}

// makePodTemplate shall create podTemplate common to both deployment and statefulset.
func makePodTemplate(nodeSpec *v1alpha1.DruidNodeSpec, m *v1alpha1.Druid, ls map[string]string, nodeSpecUniqueStr, configMapSHA string) v1.PodTemplateSpec {
	return v1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      ls,
			Annotations: firstNonNilValue(nodeSpec.PodAnnotations, m.Spec.PodAnnotations).(map[string]string),
		},
		Spec: makePodSpec(nodeSpec, m, nodeSpecUniqueStr, configMapSHA),
	}
}

// makePodSpec shall create podSpec common to both deployment and statefulset.
func makePodSpec(nodeSpec *v1alpha1.DruidNodeSpec, m *v1alpha1.Druid, nodeSpecUniqueStr, configMapSHA string) v1.PodSpec {

	mainContainer := v1.Container{
		Image:           firstNonEmptyStr(nodeSpec.Image, m.Spec.Image),
		Name:            fmt.Sprintf("%s", nodeSpecUniqueStr),
		Command:         []string{firstNonEmptyStr(m.Spec.StartScript, "bin/run-druid.sh"), nodeSpec.NodeType},
		ImagePullPolicy: v1.PullPolicy(firstNonEmptyStr(string(nodeSpec.ImagePullPolicy), string(m.Spec.ImagePullPolicy))),
		Ports:           nodeSpec.Ports,
		Resources:       nodeSpec.Resources,
		Env:             getEnv(nodeSpec, m, configMapSHA),
		EnvFrom:         getEnvFrom(nodeSpec, m),
		VolumeMounts:    getVolumeMounts(nodeSpec, m),
		LivenessProbe:   setLivenessProbe(nodeSpec, m),
		ReadinessProbe:  setReadinessProbe(nodeSpec, m),
		StartupProbe:    setStartUpProbe(nodeSpec, m),
		Lifecycle:       nodeSpec.Lifecycle,
		SecurityContext: firstNonNilValue(nodeSpec.ContainerSecurityContext, m.Spec.ContainerSecurityContext).(*v1.SecurityContext),
	}

	spec := v1.PodSpec{
		NodeSelector:                  firstNonNilValue(nodeSpec.NodeSelector, m.Spec.NodeSelector).(map[string]string),
		TopologySpreadConstraints:     getTopologySpreadConstraints(nodeSpec),
		Tolerations:                   getTolerations(nodeSpec, m),
		Affinity:                      getAffinity(nodeSpec, m),
		ImagePullSecrets:              firstNonNilValue(nodeSpec.ImagePullSecrets, m.Spec.ImagePullSecrets).([]v1.LocalObjectReference),
		Containers:                    []v1.Container{mainContainer},
		TerminationGracePeriodSeconds: nodeSpec.TerminationGracePeriodSeconds,
		Volumes:                       getVolume(nodeSpec, m, nodeSpecUniqueStr),
		SecurityContext:               firstNonNilValue(nodeSpec.PodSecurityContext, m.Spec.PodSecurityContext).(*v1.PodSecurityContext),
		ServiceAccountName:            firstNonEmptyStr(nodeSpec.ServiceAccountName, m.Spec.ServiceAccount),
		PriorityClassName:             firstNonEmptyStr(nodeSpec.PriorityClassName, m.Spec.PriorityClassName),
		DNSPolicy:                     v1.DNSPolicy(firstNonEmptyStr(string(nodeSpec.DNSPolicy), string(m.Spec.DNSPolicy))),
		DNSConfig:                     firstNonNilValue(nodeSpec.DNSConfig, m.Spec.DNSConfig).(*v1.PodDNSConfig),
	}

	addAdditionalContainers(m, nodeSpec, &spec)

	return spec
}

func setDefaultProbe(defaultPort int32, nodeType string, probeType string) *v1.Probe {
	probe := &v1.Probe{
		ProbeHandler: v1.ProbeHandler{
			HTTPGet: &v1.HTTPGetAction{
				Path: "/status/health",
				Port: intstr.IntOrString{
					IntVal: defaultPort,
				},
			},
		},
		InitialDelaySeconds: 5,
		TimeoutSeconds:      5,
		PeriodSeconds:       10,
		SuccessThreshold:    1,
		FailureThreshold:    10,
	}

	if nodeType == historical && probeType != "liveness" {
		probe.HTTPGet.Path = "/druid/historical/v1/readiness"
		probe.FailureThreshold = 20
	}
	if nodeType == broker && probeType != "liveness" {
		probe.HTTPGet.Path = "/druid/broker/v1/readiness"
		probe.FailureThreshold = 20
	}

	if nodeType == historical && probeType == "startup" {
		probe.InitialDelaySeconds = 180
		probe.PeriodSeconds = 30
		probe.TimeoutSeconds = 10
	}
	return probe
}

func updateDefaultPortInProbe(probe *v1.Probe, defaultPort int32) *v1.Probe {
	if probe != nil && probe.HTTPGet != nil && probe.HTTPGet.Port.IntVal == 0 && probe.HTTPGet.Port.StrVal == "" {
		probe.HTTPGet.Port.IntVal = defaultPort
	}
	return probe
}

func makePodDisruptionBudget(nodeSpec *v1alpha1.DruidNodeSpec, m *v1alpha1.Druid, ls map[string]string, nodeSpecUniqueStr string) (*policyv1.PodDisruptionBudget, error) {
	pdbSpec := *nodeSpec.PodDisruptionBudgetSpec
	pdbSpec.Selector = &metav1.LabelSelector{MatchLabels: ls}

	pdb := &policyv1.PodDisruptionBudget{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "policy/v1",
			Kind:       "PodDisruptionBudget",
		},

		ObjectMeta: metav1.ObjectMeta{
			Name:      nodeSpecUniqueStr,
			Namespace: m.Namespace,
			Labels:    ls,
		},

		Spec: pdbSpec,
	}

	return pdb, nil
}

func makeHorizontalPodAutoscaler(nodeSpec *v1alpha1.DruidNodeSpec, m *v1alpha1.Druid, ls map[string]string, nodeSpecUniqueStr string) (*autoscalev2.HorizontalPodAutoscaler, error) {
	nodeHSpec := *nodeSpec.HPAutoScaler

	hpa := &autoscalev2.HorizontalPodAutoscaler{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "autoscaling/v2",
			Kind:       "HorizontalPodAutoscaler",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      nodeSpecUniqueStr,
			Namespace: m.Namespace,
			Labels:    ls,
		},
		Spec: nodeHSpec,
	}

	return hpa, nil
}

func makeIngress(nodeSpec *v1alpha1.DruidNodeSpec, m *v1alpha1.Druid, ls map[string]string, nodeSpecUniqueStr string) (*networkingv1.Ingress, error) {
	nodeIngressSpec := *nodeSpec.Ingress

	ingress := &networkingv1.Ingress{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "networking.k8s.io/v1",
			Kind:       "Ingress",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        nodeSpecUniqueStr,
			Annotations: nodeSpec.IngressAnnotations,
			Namespace:   m.Namespace,
			Labels:      ls,
		},
		Spec: nodeIngressSpec,
	}

	return ingress, nil
}

func makePersistentVolumeClaim(pvc *v1.PersistentVolumeClaim, nodeSpec *v1alpha1.DruidNodeSpec, m *v1alpha1.Druid, ls map[string]string, nodeSpecUniqueStr string) (*v1.PersistentVolumeClaim, error) {

	pvc.TypeMeta = metav1.TypeMeta{
		APIVersion: "v1",
		Kind:       "PersistentVolumeClaim",
	}

	pvc.ObjectMeta.Namespace = m.Namespace

	if pvc.ObjectMeta.Labels == nil {
		pvc.ObjectMeta.Labels = ls
	} else {
		for k, v := range ls {
			pvc.ObjectMeta.Labels[k] = v
		}
	}

	if pvc.ObjectMeta.Name == "" {
		pvc.ObjectMeta.Name = nodeSpecUniqueStr
	} else {
		for _, p := range nodeSpec.PersistentVolumeClaim {
			pvc.ObjectMeta.Name = p.Name
			pvc.Spec = p.Spec
		}

	}

	return pvc, nil
}

// makeLabelsForDruid returns the labels for selecting the resources
// belonging to the given Druid object.
func makeLabelsForDruid(druid *v1alpha1.Druid) map[string]string {
	return map[string]string{"app": "druid", "druid_cr": druid.GetName()}
}

// makeLabelsForDruid returns the labels for selecting the resources
// belonging to the given druid CR name. adds labels from both node &
// cluster specs. node spec labels will take precedence over clusters labels
func makeLabelsForNodeSpec(nodeSpec *v1alpha1.DruidNodeSpec, m *v1alpha1.Druid, clusterName, nodeSpecUniqueStr string) map[string]string {
	var labels = map[string]string{}

	for k, v := range m.Spec.PodLabels {
		labels[k] = v
	}

	for k, v := range nodeSpec.PodLabels {
		labels[k] = v
	}

	labels["app"] = "druid"
	labels["druid_cr"] = clusterName
	labels["nodeSpecUniqueStr"] = nodeSpecUniqueStr
	labels["component"] = nodeSpec.NodeType
	return labels
}

// makeAnnotationsForWorkload returns the annotations for a Deployment or StatefulSet
// If a given key is set in both the DruidSpec and DruidNodeSpec, the node-scoped value will take precedence.
func makeAnnotationsForWorkload(nodeSpec *v1alpha1.DruidNodeSpec, m *v1alpha1.Druid) map[string]string {
	var annotations = map[string]string{}

	if m.Spec.WorkloadAnnotations != nil {
		annotations = m.Spec.WorkloadAnnotations
	}

	for k, v := range nodeSpec.WorkloadAnnotations {
		annotations[k] = v
	}

	return annotations
}

// addOwnerRefToObject appends the desired OwnerReference to the object
func addOwnerRefToObject(obj metav1.Object, ownerRef metav1.OwnerReference) {
	obj.SetOwnerReferences(append(obj.GetOwnerReferences(), ownerRef))
}

// asOwner returns an OwnerReference set as the druid CR
func asOwner(m *v1alpha1.Druid) metav1.OwnerReference {
	trueVar := true
	return metav1.OwnerReference{
		APIVersion: m.APIVersion,
		Kind:       m.Kind,
		Name:       m.Name,
		UID:        m.UID,
		Controller: &trueVar,
	}
}

// getPodNames returns the pod names of the array of pods passed in
func getPodNames(pods []object) []string {
	var podNames []string
	for _, pod := range pods {
		podNames = append(podNames, pod.(*v1.Pod).Name)
	}
	return podNames
}

func sendEvent(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid, eventtype, reason, message string) {

	ref := &v1.ObjectReference{
		Kind:            drd.Kind,
		APIVersion:      drd.APIVersion,
		Name:            drd.Name,
		Namespace:       drd.Namespace,
		UID:             drd.UID,
		ResourceVersion: drd.ResourceVersion,
	}

	t := metav1.Now()
	namespace := ref.Namespace
	if namespace == "" {
		namespace = metav1.NamespaceDefault
	}

	event := &v1.Event{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Event",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%v.%x", ref.Name, t.UnixNano()),
			Namespace: namespace,
		},
		InvolvedObject: *ref,
		Reason:         reason,
		Message:        message,
		FirstTimestamp: t,
		LastTimestamp:  t,
		Count:          1,
		Type:           eventtype,
		Source:         v1.EventSource{Component: "druid-operator"},
	}

	if err := sdk.Create(ctx, event); err != nil {
		logger.Error(err, fmt.Sprintf("Failed to push event [%v]", event))
	}
}

func verifyDruidSpec(drd *v1alpha1.Druid) error {
	keyValidationRegex, err := regexp.Compile("[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*")
	if err != nil {
		return err
	}

	if err = validateAdditionalContainersSpec(drd); err != nil {
		return err
	}

	if err = validateVolumeClaimTemplateSpec(drd); err != nil {
		return err
	}

	errorMsg := ""
	for key, node := range drd.Spec.Nodes {
		if drd.Spec.Image == "" && node.Image == "" {
			errorMsg = fmt.Sprintf("%sImage missing from Druid Cluster Spec\n", errorMsg)
		}

		if !keyValidationRegex.MatchString(key) {
			errorMsg = fmt.Sprintf("%sNode[%s] Key must match k8s resource name regex '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*'", errorMsg, key)
		}
	}

	if errorMsg == "" {
		return nil
	} else {
		return fmt.Errorf(errorMsg)
	}
}

func namespacedName(name, namespace string) *types.NamespacedName {
	return &types.NamespacedName{Name: name, Namespace: namespace}
}
