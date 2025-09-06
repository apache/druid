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

package druid

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/datainfrahq/druid-operator/apis/druid/v1alpha1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// constructor to DruidNodeTypeStatus status
// handles error
func newDruidNodeTypeStatus(
	nodeConditionStatus v1.ConditionStatus,
	nodeCondition v1alpha1.DruidNodeConditionType,
	nodeTierOrType string,
	err error) *v1alpha1.DruidNodeTypeStatus {

	var reason string

	if nodeCondition == v1alpha1.DruidClusterReady {
		nodeTierOrType = "All"
		reason = "All Druid Nodes are in Ready Condition"
	} else if nodeCondition == v1alpha1.DruidNodeRollingUpdate {
		reason = "Druid Node [" + nodeTierOrType + "] is Rolling Update"
	} else if err != nil {
		reason = err.Error()
		nodeCondition = v1alpha1.DruidNodeErrorState
	}

	return &v1alpha1.DruidNodeTypeStatus{
		DruidNode:                nodeTierOrType,
		DruidNodeConditionStatus: nodeConditionStatus,
		DruidNodeConditionType:   nodeCondition,
		Reason:                   reason,
	}

}

// wrapper to patch druid cluster status
func druidClusterStatusPatcher(ctx context.Context, sdk client.Client, updatedStatus v1alpha1.DruidClusterStatus, m *v1alpha1.Druid, emitEvent EventEmitter) error {

	if !reflect.DeepEqual(updatedStatus, m.Status) {
		patchBytes, err := json.Marshal(map[string]v1alpha1.DruidClusterStatus{"status": updatedStatus})
		if err != nil {
			return fmt.Errorf("failed to serialize status patch to bytes: %v", err)
		}
		_ = writers.Patch(ctx, sdk, m, m, true, client.RawPatch(types.MergePatchType, patchBytes), emitEvent)
	}
	return nil
}

// In case of state change, patch the status and emit event.
// emit events only on state change, to avoid event pollution.
func druidNodeConditionStatusPatch(ctx context.Context,
	updatedStatus v1alpha1.DruidClusterStatus,
	sdk client.Client,
	nodeSpecUniqueStr string,
	m *v1alpha1.Druid,
	emitEvent EventEmitter,
	emptyObjFn func() object) (err error) {

	if !reflect.DeepEqual(updatedStatus.DruidNodeStatus, m.Status.DruidNodeStatus) {

		err = druidClusterStatusPatcher(ctx, sdk, updatedStatus, m, emitEvent)
		if err != nil {
			return err
		}

		obj, err := readers.Get(ctx, sdk, nodeSpecUniqueStr, m, emptyObjFn, emitEvent)
		if err != nil {
			return err
		}

		emitEvent.EmitEventRollingDeployWait(m, obj, nodeSpecUniqueStr)

		return nil

	}
	return nil
}
