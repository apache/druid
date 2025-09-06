/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package ingestion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"time"

	"github.com/datainfrahq/druid-operator/apis/druid/v1alpha1"
	"github.com/datainfrahq/druid-operator/controllers/druid"
	druidapi "github.com/datainfrahq/druid-operator/pkg/druidapi"
	internalhttp "github.com/datainfrahq/druid-operator/pkg/http"
	"github.com/datainfrahq/druid-operator/pkg/util"
	"github.com/datainfrahq/operator-runtime/builder"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	DruidIngestionControllerCreateSuccess      = "DruidIngestionControllerCreateSuccess"
	DruidIngestionControllerCreateFail         = "DruidIngestionControllerCreateFail"
	DruidIngestionControllerGetSuccess         = "DruidIngestionControllerGetSuccess"
	DruidIngestionControllerGetFail            = "DruidIngestionControllerGetFail"
	DruidIngestionControllerUpdateSuccess      = "DruidIngestionControllerUpdateSuccess"
	DruidIngestionControllerUpdateFail         = "DruidIngestionControllerUpdateFail"
	DruidIngestionControllerShutDownSuccess    = "DruidIngestionControllerShutDownSuccess"
	DruidIngestionControllerShutDownFail       = "DruidIngestionControllerShutDownFail"
	DruidIngestionControllerPatchStatusSuccess = "DruidIngestionControllerPatchStatusSuccess"
	DruidIngestionControllerPatchStatusFail    = "DruidIngestionControllerPatchStatusFail"
	DruidIngestionControllerFinalizer          = "druidingestion.datainfra.io/finalizer"
)

func (r *DruidIngestionReconciler) do(ctx context.Context, di *v1alpha1.DruidIngestion) error {
	basicAuth, err := druidapi.GetAuthCreds(
		ctx,
		r.Client,
		di.Spec.Auth,
	)
	if err != nil {
		return err
	}

	svcName, err := druidapi.GetRouterSvcUrl(di.Namespace, di.Spec.DruidClusterName, r.Client)
	if err != nil {
		return err
	}

	build := builder.NewBuilder(
		builder.ToNewBuilderRecorder(builder.BuilderRecorder{Recorder: r.Recorder, ControllerName: "DruidIngestionController"}),
	)

	_, err = r.CreateOrUpdate(di, svcName, *build, internalhttp.Auth{BasicAuth: basicAuth})
	if err != nil {
		return err
	}

	if di.ObjectMeta.DeletionTimestamp.IsZero() {
		// The object is not being deleted, so if it does not have our finalizer,
		// then lets add the finalizer and update the object. This is equivalent
		// registering our finalizer.
		if !controllerutil.ContainsFinalizer(di, DruidIngestionControllerFinalizer) {
			controllerutil.AddFinalizer(di, DruidIngestionControllerFinalizer)
			if err := r.Update(ctx, di.DeepCopyObject().(*v1alpha1.DruidIngestion)); err != nil {
				return nil
			}
		}
	} else {
		if controllerutil.ContainsFinalizer(di, DruidIngestionControllerFinalizer) {
			// our finalizer is present, so lets handle any external dependency
			svcName, err := druidapi.GetRouterSvcUrl(di.Namespace, di.Spec.DruidClusterName, r.Client)
			if err != nil {
				return err
			}

			posthttp := internalhttp.NewHTTPClient(
				&http.Client{},
				&internalhttp.Auth{BasicAuth: basicAuth},
			)

			respShutDownTask, err := posthttp.Do(
				http.MethodPost,
				getPath(di.Spec.Ingestion.Type, svcName, http.MethodPost, di.Status.TaskId, true),
				[]byte{},
			)
			if err != nil {
				return err
			}
			if respShutDownTask.StatusCode != 200 {
				build.Recorder.GenericEvent(
					di,
					v1.EventTypeWarning,
					fmt.Sprintf("Resp [%s], StatusCode [%d]", string(respShutDownTask.ResponseBody), respShutDownTask.StatusCode),
					DruidIngestionControllerShutDownFail,
				)
			} else {
				build.Recorder.GenericEvent(
					di,
					v1.EventTypeNormal,
					fmt.Sprintf("Resp [%s], StatusCode [%d]", string(respShutDownTask.ResponseBody), respShutDownTask.StatusCode),
					DruidIngestionControllerShutDownSuccess,
				)
			}

			// remove our finalizer from the list and update it.
			controllerutil.RemoveFinalizer(di, DruidIngestionControllerFinalizer)
			if err := r.Update(ctx, di.DeepCopyObject().(*v1alpha1.DruidIngestion)); err != nil {
				return nil
			}
		}
	}

	return nil
}

// getSpec extracts the current ingestion spec from the DruidIngestion object.
// It first attempts to extract the nativeSpec, and if that is not available, it falls back to the Spec.
func getSpec(di *v1alpha1.DruidIngestion) (map[string]interface{}, error) {
	if di.Spec.Ingestion.NativeSpec.Size() > 0 {
		var nativeSpecMap map[string]interface{}
		if err := json.Unmarshal(di.Spec.Ingestion.NativeSpec.Raw, &nativeSpecMap); err != nil {
			return nil, fmt.Errorf("error unmarshalling nativeSpec: %v", err)
		}
		return nativeSpecMap, nil
	} else if di.Spec.Ingestion.Spec != "" {
		// Attempt to unmarshal the JSON Spec if nativeSpec is not available
		var spec map[string]interface{}
		err := json.Unmarshal([]byte(di.Spec.Ingestion.Spec), &spec)
		if err != nil {
			return nil, fmt.Errorf("error unmarshalling Spec: %v", err)
		}
		return spec, nil
	} else {
		// Return an error if neither nativeSpec nor Spec is valid
		return nil, fmt.Errorf("no valid ingestion spec provided")
	}
}

// getSpecJson extracts the current ingestion spec from the DruidIngestion object and returns it as a string.
func getSpecJson(di *v1alpha1.DruidIngestion) (string, error) {
	specData, err := getSpec(di)
	if err != nil {
		return "", err
	}
	return util.ToJsonString(specData)
}

// getRules extracts the rules from the DruidIngestion object and returns them as a slice of maps.
// Each map represents a single rule.
func getRules(di *v1alpha1.DruidIngestion) ([]map[string]interface{}, error) {
	if len(di.Spec.Ingestion.Rules) == 0 {
		return nil, nil
	}

	rules := make([]map[string]interface{}, 0, len(di.Spec.Ingestion.Rules)) // Initialize with capacity
	for _, rule := range di.Spec.Ingestion.Rules {
		var ruleMap map[string]interface{}
		if err := json.Unmarshal(rule.Raw, &ruleMap); err != nil {
			return nil, fmt.Errorf("error unmarshalling rule: %v", err)
		}
		rules = append(rules, ruleMap)
	}
	return rules, nil
}

// getRulesJson extracts the rules from the DruidIngestion object and returns them as a JSON string.
func getRulesJson(di *v1alpha1.DruidIngestion) (string, error) {
	rules, err := getRules(di)
	if err != nil {
		return "", err
	}
	return util.ToJsonString(rules)
}

// extractDataSourceFromSpec extracts the dataSource from the spec map
func getDataSource(di *v1alpha1.DruidIngestion) (string, error) {
	// Get the current ingestion spec
	spec, err := getSpec(di)
	if err != nil {
		return "", err
	}

	// Navigate through the nested structure to find dataSource
	if specSection, ok := spec["spec"].(map[string]interface{}); ok {
		if dataSchema, ok := specSection["dataSchema"].(map[string]interface{}); ok {
			if dataSource, ok := dataSchema["dataSource"].(string); ok {
				return dataSource, nil
			}
		}
	}
	return "", fmt.Errorf("dataSource not found in spec")
}

func getCompaction(di *v1alpha1.DruidIngestion) (map[string]interface{}, error) {
	compaction := di.Spec.Ingestion.Compaction

	compactionMap := make(map[string]interface{})
	if len(compaction.Raw) == 0 {
		return compactionMap, nil
	}

	if err := json.Unmarshal(compaction.Raw, &compactionMap); err != nil {
		return nil, fmt.Errorf("error unmarshalling compaction: %v", err)
	}

	dataSource, err := getDataSource(di)
	if err != nil {
		return nil, fmt.Errorf("error getting dataSource: %v", err)
	}

	// Add dataSource to the map
	compactionMap["dataSource"] = dataSource

	return compactionMap, nil
}

func getCompactionJson(di *v1alpha1.DruidIngestion) (string, error) {
	compaction, err := getCompaction(di)
	if err != nil {
		return "", err
	}
	return util.ToJsonString(compaction)
}

// UpdateCompaction updates the compaction settings for a Druid data source.
func (r *DruidIngestionReconciler) UpdateCompaction(
	di *v1alpha1.DruidIngestion,
	svcName string,
	auth internalhttp.Auth,
) (bool, error) {
	// If there are no compaction settings, return false
	if di.Spec.Ingestion.Compaction.Size() == 0 {
		return false, nil
	}

	httpClient := internalhttp.NewHTTPClient(
		&http.Client{},
		&auth,
	)

	dataSource, err := getDataSource(di)
	if err != nil {
		return false, err
	}

	// Get current compaction settings
	currentResp, err := httpClient.Do(
		http.MethodGet,
		druidapi.MakePath(svcName, "coordinator", "config", "compaction", dataSource),
		nil,
	)
	if err != nil {
		return false, err
	}

	var currentCompactionJson string
	if currentResp.StatusCode == http.StatusOK {
		currentCompactionJson = string(currentResp.ResponseBody)
	} else if currentResp.StatusCode == http.StatusNotFound {
		// Assume no compaction settings are currently set
		currentCompactionJson = "{}"
	} else {
		return false, fmt.Errorf("failed to retrieve current compaction settings, status code: %d", currentResp.StatusCode)
	}

	desiredCompactionJson, err := getCompactionJson(di)
	if err != nil {
		return false, err
	}

	// Compare current and desired compaction settings
	if areEqual, err := util.IncludesJson(currentCompactionJson, desiredCompactionJson); err != nil {
		return false, err
	} else if areEqual {
		// Compaction settings are already up-to-date
		return false, nil
	}

	// Update compaction settings
	respUpdateCompaction, err := httpClient.Do(
		http.MethodPost,
		druidapi.MakePath(svcName, "coordinator", "config", "compaction"),
		[]byte(desiredCompactionJson),
	)
	if err != nil {
		return false, err
	}

	if respUpdateCompaction.StatusCode == 200 {
		return true, nil
	}

	return false, fmt.Errorf(
		"failed to update compaction, status code: %d, response body: %s",
		respUpdateCompaction.StatusCode, respUpdateCompaction.ResponseBody)
}

// UpdateRules updates the rules for a Druid data source.
func (r *DruidIngestionReconciler) UpdateRules(
	di *v1alpha1.DruidIngestion,
	svcName string,
	auth internalhttp.Auth,
) (bool, error) {
	// If there are no rules, return true
	if len(di.Spec.Ingestion.Rules) == 0 {
		return true, nil
	}

	rulesData, err := getRulesJson(di)
	if err != nil {
		return false, err
	}
	postHttp := internalhttp.NewHTTPClient(
		&http.Client{},
		&auth,
	)

	dataSource, err := getDataSource(di)
	if err != nil {
		return false, err
	}

	// Update rules
	respUpdateRules, err := postHttp.Do(
		http.MethodPost,
		druidapi.MakePath(svcName, "coordinator", "rules", dataSource),
		[]byte(rulesData),
	)

	if err != nil {
		return false, err
	}

	if respUpdateRules.StatusCode == 200 {
		return true, nil
	}

	return false, fmt.Errorf("failed to update rules, status code: %d, response body: %s", respUpdateRules.StatusCode, respUpdateRules.ResponseBody)
}

func (r *DruidIngestionReconciler) CreateOrUpdate(
	di *v1alpha1.DruidIngestion,
	svcName string,
	build builder.Builder,
	auth internalhttp.Auth,
) (controllerutil.OperationResult, error) {

	// Marshal the current spec to JSON
	specJson, err := getSpecJson(di)
	if err != nil {
		return controllerutil.OperationResultNone, err
	}

	// check if task id does not exist in status
	if di.Status.TaskId == "" && di.Status.CurrentIngestionSpec == "" {
		// if does not exist create task
		postHttp := internalhttp.NewHTTPClient(
			&http.Client{},
			&auth,
		)

		// Create ingestion task
		respCreateTask, err := postHttp.Do(
			http.MethodPost,
			getPath(di.Spec.Ingestion.Type, svcName, http.MethodPost, "", false),
			[]byte(specJson),
		)

		if err != nil {
			return controllerutil.OperationResultNone, err
		}

		compactionOk, err := r.UpdateCompaction(di, svcName, auth)
		if err != nil {
			return controllerutil.OperationResultNone, err
		}

		rulesOk, err := r.UpdateRules(di, svcName, auth)
		if err != nil {
			return controllerutil.OperationResultNone, err
		}

		// If the task creation was successful, patch the status with the new task ID.
		if respCreateTask.StatusCode == 200 && compactionOk && rulesOk {
			taskId, err := getTaskIdFromResponse(respCreateTask.ResponseBody)
			if err != nil {
				return controllerutil.OperationResultNone, err
			}
			result, err := r.makePatchDruidIngestionStatus(
				di,
				taskId,
				DruidIngestionControllerCreateSuccess,
				string(respCreateTask.ResponseBody),
				v1.ConditionTrue,
				DruidIngestionControllerCreateSuccess,
			)
			if err != nil {
				return controllerutil.OperationResultNone, err
			}
			build.Recorder.GenericEvent(
				di,
				v1.EventTypeNormal,
				fmt.Sprintf("Resp [%s]", string(respCreateTask.ResponseBody)),
				DruidIngestionControllerCreateSuccess,
			)
			build.Recorder.GenericEvent(
				di,
				v1.EventTypeNormal,
				fmt.Sprintf("Resp [%s], Result [%s]", string(respCreateTask.ResponseBody), result),
				DruidIngestionControllerPatchStatusSuccess)
			return controllerutil.OperationResultCreated, nil
		} else {
			// If task creation failed, patch the status to reflect the failure.
			taskId, err := getTaskIdFromResponse(respCreateTask.ResponseBody)
			if err != nil {
				return controllerutil.OperationResultNone, err
			}
			_, err = r.makePatchDruidIngestionStatus(
				di,
				taskId,
				DruidIngestionControllerCreateFail,
				string(respCreateTask.ResponseBody),
				v1.ConditionTrue,
				DruidIngestionControllerCreateFail,
			)
			if err != nil {
				return controllerutil.OperationResultNone, err
			}
			build.Recorder.GenericEvent(
				di,
				v1.EventTypeWarning,
				fmt.Sprintf("Resp [%s], Status", string(respCreateTask.ResponseBody)),
				DruidIngestionControllerCreateFail,
			)
			return controllerutil.OperationResultCreated, nil
		}
	} else {

		currentIngestionSpec, err := getSpecJson(di)
		if err != nil {
			return controllerutil.OperationResultNone, err
		}

		ok, err := druid.IsEqualJson(di.Status.CurrentIngestionSpec, currentIngestionSpec)
		if err != nil {
			return controllerutil.OperationResultNone, err
		}

		if !ok {
			postHttp := internalhttp.NewHTTPClient(
				&http.Client{},
				&auth,
			)

			respUpdateSpec, err := postHttp.Do(
				http.MethodPost,
				getPath(di.Spec.Ingestion.Type, svcName, http.MethodPost, "", false),
				[]byte(specJson),
			)
			if err != nil {
				return controllerutil.OperationResultNone, err
			}

			if respUpdateSpec.StatusCode == 200 {
				// patch status to store the current valid ingestion spec json
				taskId, err := getTaskIdFromResponse(respUpdateSpec.ResponseBody)
				if err != nil {
					return controllerutil.OperationResultNone, err
				}
				result, err := r.makePatchDruidIngestionStatus(
					di,
					taskId,
					DruidIngestionControllerUpdateSuccess,
					string(respUpdateSpec.ResponseBody),
					v1.ConditionTrue,
					DruidIngestionControllerUpdateSuccess,
				)
				if err != nil {
					return controllerutil.OperationResultNone, err
				}
				build.Recorder.GenericEvent(
					di,
					v1.EventTypeNormal,
					fmt.Sprintf("Resp [%s]", string(respUpdateSpec.ResponseBody)),
					DruidIngestionControllerUpdateSuccess,
				)
				build.Recorder.GenericEvent(
					di,
					v1.EventTypeNormal,
					fmt.Sprintf("Resp [%s], Result [%s]", string(respUpdateSpec.ResponseBody), result),
					DruidIngestionControllerPatchStatusSuccess)
			}

		}

		compactionOk, err := r.UpdateCompaction(di, svcName, auth)
		if err != nil {
			return controllerutil.OperationResultNone, err
		}

		if compactionOk {
			// patch status to store the current compaction json
			_, err := r.makePatchDruidIngestionStatus(
				di,
				di.Status.TaskId,
				DruidIngestionControllerUpdateSuccess,
				"compaction updated",
				v1.ConditionTrue,
				DruidIngestionControllerUpdateSuccess,
			)
			if err != nil {
				return controllerutil.OperationResultNone, err
			}
			build.Recorder.GenericEvent(
				di,
				v1.EventTypeNormal,
				"compaction updated",
				DruidIngestionControllerUpdateSuccess,
			)
		}

		// compare the rules state
		rulesEqual := reflect.DeepEqual(di.Status.CurrentRules, di.Spec.Ingestion.Rules)

		if !rulesEqual {
			rulesOk, err := r.UpdateRules(di, svcName, auth)
			if err != nil {
				return controllerutil.OperationResultNone, err
			}

			if rulesOk {
				// patch status to store the current valid rules json
				_, err := r.makePatchDruidIngestionStatus(
					di,
					di.Status.TaskId,
					DruidIngestionControllerUpdateSuccess,
					"rules updated",
					v1.ConditionTrue,
					DruidIngestionControllerUpdateSuccess,
				)
				if err != nil {
					return controllerutil.OperationResultNone, err
				}
				build.Recorder.GenericEvent(
					di,
					v1.EventTypeNormal,
					"rules updated",
					DruidIngestionControllerUpdateSuccess,
				)
			}
		}

		return controllerutil.OperationResultUpdated, nil

	}
}

func (r *DruidIngestionReconciler) makePatchDruidIngestionStatus(
	di *v1alpha1.DruidIngestion,
	taskId string,
	msg string,
	reason string,
	status v1.ConditionStatus,
	diConditionType string,

) (controllerutil.OperationResult, error) {

	// Get the current ingestion spec, stored in either nativeSpec or Spec
	ingestionSpec, specErr := getSpecJson(di)
	if specErr != nil {
		return controllerutil.OperationResultNone, specErr
	}

	if _, _, err := patchStatus(context.Background(), r.Client, di, func(obj client.Object) client.Object {

		in := obj.(*v1alpha1.DruidIngestion)
		in.Status.CurrentIngestionSpec = ingestionSpec
		in.Status.CurrentRules = di.Spec.Ingestion.Rules
		in.Status.TaskId = taskId
		in.Status.LastUpdateTime = metav1.Time{Time: time.Now()}
		in.Status.Message = msg
		in.Status.Reason = reason
		in.Status.Status = status
		in.Status.Type = diConditionType
		return in
	}); err != nil {
		return controllerutil.OperationResultNone, err
	}

	return controllerutil.OperationResultUpdatedStatusOnly, nil
}

func getPath(
	ingestionType v1alpha1.DruidIngestionMethod,
	svcName, httpMethod, taskId string,
	shutDownTask bool) string {

	switch ingestionType {
	case v1alpha1.NativeBatchIndexParallel:
		if httpMethod == http.MethodGet {
			// get task
			return druidapi.MakePath(svcName, "indexer", "task", taskId)
		} else if httpMethod == http.MethodPost && !shutDownTask {
			// create or update task
			return druidapi.MakePath(svcName, "indexer", "task")
		} else if shutDownTask {
			// shutdown task
			return druidapi.MakePath(svcName, "indexer", "task", taskId, "shutdown")
		}
	case v1alpha1.HadoopIndexHadoop:
	case v1alpha1.Kafka:
		if httpMethod == http.MethodGet {
			// get supervisor task
			return druidapi.MakePath(svcName, "indexer", "supervisor", taskId)
		} else if httpMethod == http.MethodPost && !shutDownTask {
			// create or update supervisor task
			return druidapi.MakePath(svcName, "indexer", "supervisor")
		} else if shutDownTask {
			// shut down supervisor
			return druidapi.MakePath(svcName, "indexer", "supervisor", taskId, "shutdown")
		}
	case v1alpha1.Kinesis:
	case v1alpha1.QueryControllerSQL:
	default:
		return ""
	}

	return ""
}

type taskHolder struct {
	Task string `json:"task"` // tasks
	ID   string `json:"id"`   // supervisor
}

func getTaskIdFromResponse(resp string) (string, error) {
	var task taskHolder
	if err := json.Unmarshal([]byte(resp), &task); err != nil {
		return "", err
	}

	// check both fields and return the appropriate value
	// tasks use different field names than supervisors
	if task.Task != "" {
		return task.Task, nil
	}
	if task.ID != "" {
		return task.ID, nil
	}

	return "", errors.New("task id not found")
}

type VerbType string

type (
	TransformStatusFunc func(obj client.Object) client.Object
)

const (
	VerbPatched   VerbType = "Patched"
	VerbUnchanged VerbType = "Unchanged"
)

func patchStatus(ctx context.Context, c client.Client, obj client.Object, transform TransformStatusFunc, opts ...client.SubResourcePatchOption) (client.Object, VerbType, error) {
	key := types.NamespacedName{
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}
	err := c.Get(ctx, key, obj)
	if err != nil {
		return nil, VerbUnchanged, err
	}

	// The body of the request was in an unknown format -
	// accepted media types include:
	//   - application/json-patch+json,
	//   - application/merge-patch+json,
	//   - application/apply-patch+yaml
	patch := client.MergeFrom(obj)
	obj = transform(obj.DeepCopyObject().(client.Object))
	err = c.Status().Patch(ctx, obj, patch, opts...)
	if err != nil {
		return nil, VerbUnchanged, err
	}
	return obj, VerbPatched, nil
}
