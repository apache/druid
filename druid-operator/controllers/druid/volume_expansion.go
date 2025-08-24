package druid

import (
	"context"
	"errors"
	"fmt"

	"github.com/datainfrahq/druid-operator/apis/druid/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func validateVolumeClaimTemplateSpec(drd *v1alpha1.Druid) error {
	for _, nodeSpec := range drd.Spec.Nodes {
		if nodeSpec.Kind == "StatefulSet" {
			if err := validateNodeVolumeClaimTemplateSpec(&nodeSpec); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateNodeVolumeClaimTemplateSpec(nodeSpec *v1alpha1.DruidNodeSpec) error {
	for _, vct := range nodeSpec.VolumeClaimTemplates {
		if vct.Spec.StorageClassName == nil || *vct.Spec.StorageClassName == "" {
			return fmt.Errorf("node group %s has volume claim template without storage class which is not allowed: %s",
				nodeSpec.NodeType, vct.Name)
		}
	}
	return nil
}

func expandStatefulSetVolumes(ctx context.Context, sdk client.Client, m *v1alpha1.Druid,
	nodeSpec *v1alpha1.DruidNodeSpec, emitEvent EventEmitter, nodeSpecUniqueStr string) error {

	isEnabled, err := isVolumeExpansionEnabled(ctx, sdk, m, nodeSpec, emitEvent)
	if err != nil {
		return err
	}

	if isEnabled {
		err := scalePVCForSts(ctx, sdk, nodeSpec, nodeSpecUniqueStr, m, emitEvent)
		if err != nil {
			return err
		}
	}

	return nil
}

func isVolumeExpansionEnabled(ctx context.Context, sdk client.Client, m *v1alpha1.Druid, nodeSpec *v1alpha1.DruidNodeSpec, emitEvent EventEmitter) (bool, error) {

	for _, nodeVCT := range nodeSpec.VolumeClaimTemplates {
		if nodeVCT.Spec.StorageClassName == nil {
			err := errors.New("StorageClassName does not exists")
			logger.WithValues("NodeType", nodeSpec.NodeType, "VolumeClaimTemplate", nodeVCT.Name).
				Error(err, "storageClassName does not exists in spec")
			return false, err
		}
		sc, err := readers.Get(ctx, sdk, *nodeVCT.Spec.StorageClassName, m, func() object { return &storage.StorageClass{} }, emitEvent)
		if err != nil {
			return false, err
		}

		if sc.(*storage.StorageClass).AllowVolumeExpansion != boolFalse() {
			return true, nil
		}
	}
	return false, nil
}

// scalePVCForSts shall expand the StatefulSet's VolumeClaimTemplates size as well as N no of pvc supported by the sts.
func scalePVCForSts(ctx context.Context, sdk client.Client, nodeSpec *v1alpha1.DruidNodeSpec, nodeSpecUniqueStr string, drd *v1alpha1.Druid, emitEvent EventEmitter) error {

	getSTSList, err := readers.List(ctx, sdk, drd, makeLabelsForDruid(drd), emitEvent, func() objectList { return &appsv1.StatefulSetList{} }, func(listObj runtime.Object) []object {
		items := listObj.(*appsv1.StatefulSetList).Items
		result := make([]object, len(items))
		for i := 0; i < len(items); i++ {
			result[i] = &items[i]
		}
		return result
	})
	if err != nil {
		return nil
	}

	// Dont proceed unless all statefulsets are up and running.
	//  This can cause the go routine to panic

	for _, sts := range getSTSList {
		if sts.(*appsv1.StatefulSet).Status.Replicas != sts.(*appsv1.StatefulSet).Status.ReadyReplicas {
			return nil
		}
	}

	// return nil, in case return err the program halts since sts would not be able
	// we would like the operator to create sts.
	sts, err := readers.Get(ctx, sdk, nodeSpecUniqueStr, drd, func() object { return &appsv1.StatefulSet{} }, emitEvent)
	if err != nil {
		return nil
	}

	pvcLabels := map[string]string{
		"nodeSpecUniqueStr": nodeSpecUniqueStr,
	}

	pvcList, err := readers.List(ctx, sdk, drd, pvcLabels, emitEvent, func() objectList { return &v1.PersistentVolumeClaimList{} }, func(listObj runtime.Object) []object {
		items := listObj.(*v1.PersistentVolumeClaimList).Items
		result := make([]object, len(items))
		for i := 0; i < len(items); i++ {
			result[i] = &items[i]
		}
		return result
	})
	if err != nil {
		return nil
	}

	desVolumeClaimTemplateSize, currVolumeClaimTemplateSize, pvcSize := getVolumeClaimTemplateSizes(sts, nodeSpec, pvcList)

	// current number of PVC can't be less than desired number of pvc
	if len(pvcSize) < len(desVolumeClaimTemplateSize) {
		return nil
	}

	// iterate over array for matching each index in desVolumeClaimTemplateSize, currVolumeClaimTemplateSize and pvcSize
	for i := range desVolumeClaimTemplateSize {

		// Validate Request, shrinking of pvc not supported
		// desired size cant be less than current size
		// in that case re-create sts/pvc which is a user execute manual step

		desiredSize, _ := desVolumeClaimTemplateSize[i].AsInt64()
		currentSize, _ := currVolumeClaimTemplateSize[i].AsInt64()

		if desiredSize < currentSize {
			e := fmt.Errorf("Request for Shrinking of sts pvc size [sts:%s] in [namespace:%s] is not Supported", sts.(*appsv1.StatefulSet).Name, sts.(*appsv1.StatefulSet).Namespace)
			logger.Error(e, e.Error(), "name", drd.Name, "namespace", drd.Namespace)
			emitEvent.EmitEventGeneric(drd, "DruidOperatorPvcReSizeFail", "", err)
			return e
		}

		// In case size dont match and dessize > currsize, delete the sts using casacde=false / propagation policy set to orphan
		// The operator on next reconcile shall create the sts with latest changes
		if desiredSize != currentSize {
			msg := fmt.Sprintf("Detected Change in VolumeClaimTemplate Sizes for Statefuleset [%s] in Namespace [%s], desVolumeClaimTemplateSize: [%s], currVolumeClaimTemplateSize: [%s]\n, deleteing STS [%s] with casacde=false]", sts.(*appsv1.StatefulSet).Name, sts.(*appsv1.StatefulSet).Namespace, desVolumeClaimTemplateSize[i].String(), currVolumeClaimTemplateSize[i].String(), sts.(*appsv1.StatefulSet).Name)
			logger.Info(msg)
			emitEvent.EmitEventGeneric(drd, "DruidOperatorPvcReSizeDetected", msg, nil)

			if err := writers.Delete(ctx, sdk, drd, sts, emitEvent, client.PropagationPolicy(metav1.DeletePropagationOrphan)); err != nil {
				return err
			} else {
				msg := fmt.Sprintf("[StatefuleSet:%s] successfully deleted with casacde=false", sts.(*appsv1.StatefulSet).Name)
				logger.Info(msg, "name", drd.Name, "namespace", drd.Namespace)
				emitEvent.EmitEventGeneric(drd, "DruidOperatorStsOrphaned", msg, nil)
			}

		}

		// In case size dont match, patch the pvc with the desiredsize from druid CR
		for p := range pvcSize {
			pSize, _ := pvcSize[p].AsInt64()
			if desiredSize != pSize {
				// use deepcopy
				patch := client.MergeFrom(pvcList[p].(*v1.PersistentVolumeClaim).DeepCopy())
				pvcList[p].(*v1.PersistentVolumeClaim).Spec.Resources.Requests[v1.ResourceStorage] = desVolumeClaimTemplateSize[i]
				if err := writers.Patch(ctx, sdk, drd, pvcList[p].(*v1.PersistentVolumeClaim), false, patch, emitEvent); err != nil {
					return err
				} else {
					msg := fmt.Sprintf("[PVC:%s] successfully Patched with [Size:%s]", pvcList[p].(*v1.PersistentVolumeClaim).Name, desVolumeClaimTemplateSize[i].String())
					logger.Info(msg, "name", drd.Name, "namespace", drd.Namespace)
				}
			}
		}

	}

	return nil
}
