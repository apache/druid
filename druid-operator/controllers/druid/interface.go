package druid

import (
	"context"
	"fmt"
	"reflect"

	"github.com/datainfrahq/druid-operator/apis/druid/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type DruidNodeStatus string

const (
	resourceCreated DruidNodeStatus = "CREATED"
	resourceUpdated DruidNodeStatus = "UPDATED"
)

type druidEventReason string

// Events emitted should be UpperCamelCaseFormat
// druidEventreason is the reason this event is generated. druidEventreason should be short and unique
const (
	rollingDeployWait      druidEventReason = "DruidNodeRollingDeployWait"
	druidOjectGetFail      druidEventReason = "DruidOperatorGetFail"
	druidNodeUpdateFail    druidEventReason = "DruidOperatorUpdateFail"
	druidNodeUpdateSuccess druidEventReason = "DruidOperatorUpdateSuccess"
	druidNodeDeleteFail    druidEventReason = "DruidOperatorDeleteFail"
	druidNodeDeleteSuccess druidEventReason = "DruidOperatorDeleteSuccess"
	druidNodeCreateSuccess druidEventReason = "DruidOperatorCreateSuccess"
	druidNodeCreateFail    druidEventReason = "DruidOperatorCreateFail"
	druidNodePatchFail     druidEventReason = "DruidOperatorPatchFail"
	druidNodePatchSucess   druidEventReason = "DruidOperatorPatchSuccess"
	druidObjectListFail    druidEventReason = "DruidOperatorListFail"

	druidFinalizerTriggered druidEventReason = "DruidOperatorFinalizerTriggered"
	druidFinalizerFailed    druidEventReason = "DruidFinalizerFailed"
	druidFinalizerSuccess   druidEventReason = "DruidFinalizerSuccess"

	druidGetRouterSvcUrlFailed     druidEventReason = "DruidAPIGetRouterSvcUrlFailed"
	druidGetAuthCredsFailed        druidEventReason = "DruidAPIGetAuthCredsFailed"
	druidFetchCurrentConfigsFailed druidEventReason = "DruidAPIFetchCurrentConfigsFailed"
	druidConfigComparisonFailed    druidEventReason = "DruidAPIConfigComparisonFailed"
	druidUpdateConfigsFailed       druidEventReason = "DruidAPIUpdateConfigsFailed"
	druidUpdateConfigsSuccess      druidEventReason = "DruidAPIUpdateConfigsSuccess"
)

// Reader Interface
type Reader interface {
	List(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid, selectorLabels map[string]string, emitEvent EventEmitter, emptyListObjFn func() objectList, ListObjFn func(obj runtime.Object) []object) ([]object, error)
	Get(ctx context.Context, sdk client.Client, nodeSpecUniqueStr string, drd *v1alpha1.Druid, emptyObjFn func() object, emitEvent EventEmitter) (object, error)
}

// Writer Interface
type Writer interface {
	Delete(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid, obj object, emitEvent EventEmitter, deleteOptions ...client.DeleteOption) error
	Create(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid, obj object, emitEvent EventEmitter) (DruidNodeStatus, error)
	Update(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid, obj object, emitEvent EventEmitter) (DruidNodeStatus, error)
	Patch(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid, obj object, status bool, patch client.Patch, emitEvent EventEmitter) error
}

// EventEmitter Interface is a wrapper interface for all the emitter interface druid operator shall support.
// EventEmitter interface is initalized in druid_controller.go, reconcile method. The interface is passed as an arg to deployDruid(), handler.go
type EventEmitter interface {
	K8sEventEmitter
	GenericEventEmitter
}

// GenericEventEmitter can be used for any case where the state change isn't handled by reader,writer or any custom event.
type GenericEventEmitter interface {
	EmitEventGeneric(obj object, eventReason, msg string, err error)
}

// Methods include an obj and k8s obj, obj is druid CR (runtime.Object) and k8s obj ( object interface )
// K8sEventEmitter will also be displayed in logs of the operator. (only on state change)
// All methods are tied to reader,writer interfaces. Custom errors msg's and msg's are constructed within the methods and are not expected to change.
type K8sEventEmitter interface {
	EmitEventRollingDeployWait(obj, k8sObj object, nodeSpecUniqueStr string)
	EmitEventOnGetError(obj, getObj object, err error)
	EmitEventOnUpdate(obj, updateObj object, err error)
	EmitEventOnDelete(obj, deleteObj object, err error)
	EmitEventOnCreate(obj, createObj object, err error)
	EmitEventOnPatch(obj, patchObj object, err error)
	EmitEventOnList(obj object, listObj objectList, err error)
}

// Object Interface : Wrapper interface includes metav1 object and runtime object interface.
type object interface {
	metav1.Object
	runtime.Object
}

// Object List Interface : Wrapper interface includes metav1 List and runtime object interface.
type objectList interface {
	metav1.ListInterface
	runtime.Object
}

// WriterFuncs struct
type WriterFuncs struct{}

// ReaderFuncs struct
type ReaderFuncs struct{}

// EmitEventFuncs struct
type EmitEventFuncs struct {
	record.EventRecorder
}

// Initalizie Reader
var readers Reader = ReaderFuncs{}

// Initalize Writer
var writers Writer = WriterFuncs{}

// return k8s object type
// Deployment : *v1.Deployment
// StatefulSet: *v1.StatefulSet
func detectType(obj object) string { return reflect.TypeOf(obj).String() }

// Patch method shall patch the status of Obj or the status.
// Pass status as true to patch the object status.
// NOTE: Not logging on patch success, it shall keep logging on each reconcile
func (f WriterFuncs) Patch(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid, obj object, status bool, patch client.Patch, emitEvent EventEmitter) error {

	if !status {
		if err := sdk.Patch(ctx, obj, patch); err != nil {
			emitEvent.EmitEventOnPatch(drd, obj, err)
			return err
		}
	} else {
		if err := sdk.Status().Patch(ctx, obj, patch); err != nil {
			emitEvent.EmitEventOnPatch(drd, obj, err)
			return err
		}
	}
	return nil
}

// Update Func shall update the Object
func (f WriterFuncs) Update(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid, obj object, emitEvent EventEmitter) (DruidNodeStatus, error) {

	if err := sdk.Update(ctx, obj); err != nil {
		emitEvent.EmitEventOnUpdate(drd, obj, err)
		return "", err
	} else {
		emitEvent.EmitEventOnUpdate(drd, obj, nil)
		return resourceUpdated, nil
	}

}

// Create methods shall create an object, and returns a string, error
func (f WriterFuncs) Create(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid, obj object, emitEvent EventEmitter) (DruidNodeStatus, error) {

	if err := sdk.Create(ctx, obj); err != nil {
		logger.Error(err, err.Error(), "object", stringifyForLogging(obj, drd), "name", drd.Name, "namespace", drd.Namespace, "errorType", apierrors.ReasonForError(err))
		emitEvent.EmitEventOnCreate(drd, obj, err)
		return "", err
	} else {
		emitEvent.EmitEventOnCreate(drd, obj, nil)
		return resourceCreated, nil
	}

}

// Delete methods shall delete the object, deleteOptions is a variadic parameter to support various delete options such as cascade deletion.
func (f WriterFuncs) Delete(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid, obj object, emitEvent EventEmitter, deleteOptions ...client.DeleteOption) error {

	if err := sdk.Delete(ctx, obj, deleteOptions...); err != nil {
		emitEvent.EmitEventOnDelete(drd, obj, err)
		return err
	} else {
		emitEvent.EmitEventOnDelete(drd, obj, err)
		return nil
	}
}

// Get methods shall the get the object.
func (f ReaderFuncs) Get(ctx context.Context, sdk client.Client, nodeSpecUniqueStr string, drd *v1alpha1.Druid, emptyObjFn func() object, emitEvent EventEmitter) (object, error) {
	obj := emptyObjFn()

	if err := sdk.Get(ctx, *namespacedName(nodeSpecUniqueStr, drd.Namespace), obj); err != nil {
		emitEvent.EmitEventOnGetError(drd, obj, err)
		return nil, err
	}
	return obj, nil
}

// List methods shall return the list of an object
func (f ReaderFuncs) List(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid, selectorLabels map[string]string, emitEvent EventEmitter, emptyListObjFn func() objectList, ListObjFn func(obj runtime.Object) []object) ([]object, error) {
	listOpts := []client.ListOption{
		client.InNamespace(drd.Namespace),
		client.MatchingLabels(selectorLabels),
	}
	listObj := emptyListObjFn()

	if err := sdk.List(ctx, listObj, listOpts...); err != nil {
		emitEvent.EmitEventOnList(drd, listObj, err)
		return nil, err
	}

	return ListObjFn(listObj), nil
}

// EmitEventRollingDeployWait shall emit an event when the current state of a druid node is rolling deploy
func (e EmitEventFuncs) EmitEventRollingDeployWait(obj, k8sObj object, nodeSpecUniqueStr string) {
	if detectType(k8sObj) == "*v1.StatefulSet" {
		msg := fmt.Sprintf("StatefulSet[%s] roll out is in progress CurrentRevision[%s] != UpdateRevision[%s]", nodeSpecUniqueStr, k8sObj.(*appsv1.StatefulSet).Status.CurrentRevision, k8sObj.(*appsv1.StatefulSet).Status.UpdateRevision)
		e.Event(obj, v1.EventTypeNormal, string(rollingDeployWait), msg)
	} else if detectType(k8sObj) == "*v1.Deployment" {
		msg := fmt.Sprintf("Deployment[%s] roll out is in progress in namespace [%s], ReadyReplicas [%d] != Current Replicas [%d]", k8sObj.(*appsv1.Deployment).Name, k8sObj.GetNamespace(), k8sObj.(*appsv1.Deployment).Status.ReadyReplicas, k8sObj.(*appsv1.Deployment).Status.Replicas)
		e.Event(obj, v1.EventTypeNormal, string(rollingDeployWait), msg)
	}
}

// EmitEventGeneric shall emit a generic event
func (e EmitEventFuncs) EmitEventGeneric(obj object, eventReason, msg string, err error) {
	if err != nil {
		e.Event(obj, v1.EventTypeWarning, eventReason, err.Error())
	} else if msg != "" {
		e.Event(obj, v1.EventTypeNormal, eventReason, msg)

	}
}

// EmitEventOnGetError shall emit event on GET err operation
func (e EmitEventFuncs) EmitEventOnGetError(obj, getObj object, err error) {
	getErr := fmt.Errorf("Failed to get [Object:%s] due to [%s]", getObj.GetName(), err.Error())
	e.Event(obj, v1.EventTypeWarning, string(druidOjectGetFail), getErr.Error())
}

// EmitEventOnList shall emit event on LIST err operation
func (e EmitEventFuncs) EmitEventOnList(obj object, listObj objectList, err error) {
	if err != nil {
		errMsg := fmt.Errorf("Error listing object [%s] in namespace [%s] due to [%s]", listObj.GetObjectKind().GroupVersionKind().Kind, obj.GetNamespace(), err.Error())
		e.Event(obj, v1.EventTypeWarning, string(druidObjectListFail), errMsg.Error())
	}
}

// EmitEventOnUpdate shall emit event on UPDATE operation
func (e EmitEventFuncs) EmitEventOnUpdate(obj, updateObj object, err error) {
	if err != nil {
		errMsg := fmt.Errorf("Failed to update [%s:%s] due to [%s].", updateObj.GetName(), detectType(updateObj), err.Error())
		e.Event(obj, v1.EventTypeWarning, string(druidNodeUpdateFail), errMsg.Error())
	} else {
		msg := fmt.Sprintf("Updated [%s:%s].", updateObj.GetName(), detectType(updateObj))
		e.Event(obj, v1.EventTypeNormal, string(druidNodeUpdateSuccess), msg)
	}
}

// EmitEventOnDelete shall emit event on DELETE operation
func (e EmitEventFuncs) EmitEventOnDelete(obj, deleteObj object, err error) {
	if err != nil {
		errMsg := fmt.Errorf("Error deleting object [%s:%s] in namespace [%s] due to [%s]", detectType(deleteObj), deleteObj.GetName(), deleteObj.GetNamespace(), err.Error())
		e.Event(obj, v1.EventTypeWarning, string(druidNodeDeleteFail), errMsg.Error())
	} else {
		msg := fmt.Sprintf("Successfully deleted object [%s:%s] in namespace [%s]", deleteObj.GetName(), detectType(deleteObj), deleteObj.GetNamespace())
		e.Event(obj, v1.EventTypeNormal, string(druidNodeDeleteSuccess), msg)
	}
}

// EmitEventOnCreate shall emit event on CREATE operation
func (e EmitEventFuncs) EmitEventOnCreate(obj, createObj object, err error) {
	if err != nil {
		errMsg := fmt.Errorf("Error creating object [%s] in namespace [%s:%s] due to [%s]", createObj.GetName(), detectType(createObj), createObj.GetNamespace(), err.Error())
		e.Event(obj, v1.EventTypeWarning, string(druidNodeCreateFail), errMsg.Error())
	} else {
		msg := fmt.Sprintf("Successfully created object [%s:%s] in namespace [%s]", createObj.GetName(), detectType(createObj), createObj.GetNamespace())
		e.Event(obj, v1.EventTypeNormal, string(druidNodeCreateSuccess), msg)
	}
}

// EmitEventOnPatch shall emit event on PATCH operation
func (e EmitEventFuncs) EmitEventOnPatch(obj, patchObj object, err error) {
	if err != nil {
		errMsg := fmt.Errorf("Error patching object [%s:%s] in namespace [%s] due to [%s]", patchObj.GetName(), detectType(patchObj), patchObj.GetNamespace(), err.Error())
		e.Event(obj, v1.EventTypeWarning, string(druidNodePatchFail), errMsg.Error())
	} else {
		msg := fmt.Sprintf("Successfully patched object [%s:%s] in namespace [%s]", patchObj.GetName(), detectType(patchObj), patchObj.GetNamespace())
		e.Event(obj, v1.EventTypeNormal, string(druidNodePatchSucess), msg)
	}
}
