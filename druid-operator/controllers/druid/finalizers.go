package druid

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/datainfrahq/druid-operator/apis/druid/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	deletePVCFinalizerName = "deletepvc.finalizers.druid.apache.org"
)

var (
	defaultFinalizers []string
)

func updateFinalizers(ctx context.Context, sdk client.Client, m *v1alpha1.Druid, emitEvents EventEmitter) error {
	desiredFinalizers := m.GetFinalizers()
	additionFinalizers := defaultFinalizers

	desiredFinalizers = RemoveString(desiredFinalizers, deletePVCFinalizerName)
	if !m.Spec.DisablePVCDeletionFinalizer {
		additionFinalizers = append(additionFinalizers, deletePVCFinalizerName)
	}

	for _, finalizer := range additionFinalizers {
		if !ContainsString(desiredFinalizers, finalizer) {
			desiredFinalizers = append(desiredFinalizers, finalizer)
		}
	}

	if !equality.Semantic.DeepEqual(m.GetFinalizers(), desiredFinalizers) {
		m.SetFinalizers(desiredFinalizers)

		finalizersBytes, err := json.Marshal(m.GetFinalizers())
		if err != nil {
			return fmt.Errorf("failed to serialize finalizers patch to bytes: %v", err)
		}

		patch := []byte(fmt.Sprintf(`[{"op": "replace", "path": "/metadata/finalizers", "value": %s}]`, finalizersBytes))

		err = sdk.Patch(ctx, m, client.RawPatch(types.JSONPatchType, patch))
		if err != nil {
			return err
		}

	}

	return nil
}

func executeFinalizers(ctx context.Context, sdk client.Client, m *v1alpha1.Druid, emitEvents EventEmitter) error {
	if m.Spec.DisablePVCDeletionFinalizer == false {
		if err := executePVCFinalizer(ctx, sdk, m, emitEvents); err != nil {
			return err
		}
	}
	return nil
}

/*
executePVCFinalizer will execute a PVC deletion of all Druid's PVCs.
Flow:
 1. Get sts List and PVC List
 2. Range and Delete sts first and then delete pvc. PVC must be deleted after sts termination has been executed
    else pvc finalizer shall block deletion since a pod/sts is referencing it.
 3. Once delete is executed we block program and return.
*/
func executePVCFinalizer(ctx context.Context, sdk client.Client, druid *v1alpha1.Druid, eventEmitter EventEmitter) error {
	if ContainsString(druid.ObjectMeta.Finalizers, deletePVCFinalizerName) {
		pvcLabels := map[string]string{
			"druid_cr": druid.Name,
		}

		pvcList, err := readers.List(ctx, sdk, druid, pvcLabels, eventEmitter, func() objectList { return &v1.PersistentVolumeClaimList{} }, func(listObj runtime.Object) []object {
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

		stsList, err := readers.List(ctx, sdk, druid, makeLabelsForDruid(druid), eventEmitter, func() objectList { return &appsv1.StatefulSetList{} }, func(listObj runtime.Object) []object {
			items := listObj.(*appsv1.StatefulSetList).Items
			result := make([]object, len(items))
			for i := 0; i < len(items); i++ {
				result[i] = &items[i]
			}
			return result
		})
		if err != nil {
			return err
		}

		eventEmitter.EmitEventGeneric(druid, string(druidFinalizerTriggered),
			fmt.Sprintf("Trigerring finalizer [%s] for CR [%s] in namespace [%s]", deletePVCFinalizerName, druid.Name, druid.Namespace), nil)

		if err = deleteSTSAndPVC(ctx, sdk, druid, stsList, pvcList, eventEmitter); err != nil {
			eventEmitter.EmitEventGeneric(druid, string(druidFinalizerFailed),
				fmt.Sprintf("Finalizer [%s] failed for CR [%s] in namespace [%s]", deletePVCFinalizerName, druid.Name, druid.Namespace), err)

			return err
		}

		eventEmitter.EmitEventGeneric(druid, string(druidFinalizerSuccess),
			fmt.Sprintf("Finalizer [%s] success for CR [%s] in namespace [%s]", deletePVCFinalizerName, druid.Name, druid.Namespace), nil)

		// remove our finalizer from the list and update it.
		druid.ObjectMeta.Finalizers = RemoveString(druid.ObjectMeta.Finalizers, deletePVCFinalizerName)

		_, err = writers.Update(ctx, sdk, druid, druid, eventEmitter)
		if err != nil {
			return err
		}

	}
	return nil
}
