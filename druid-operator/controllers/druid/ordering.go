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
	"sort"

	"github.com/datainfrahq/druid-operator/apis/druid/v1alpha1"
)

var (
	druidServicesOrder = []string{historical, overlord, middleManager, indexer, broker, coordinator, router}
)

type ServiceGroup struct {
	key  string
	spec v1alpha1.DruidNodeSpec
}

// getNodeSpecsByOrder returns all NodeSpecs f a given Druid object.
// Recommended order is described at http://druid.io/docs/latest/operations/rolling-updates.html
func getNodeSpecsByOrder(m *v1alpha1.Druid) []*ServiceGroup {

	scaledServiceSpecsByNodeType := map[string][]*ServiceGroup{}
	for _, t := range druidServicesOrder {
		scaledServiceSpecsByNodeType[t] = []*ServiceGroup{}
	}

	for key, nodeSpec := range m.Spec.Nodes {
		scaledServiceSpec := scaledServiceSpecsByNodeType[nodeSpec.NodeType]
		scaledServiceSpecsByNodeType[nodeSpec.NodeType] = append(scaledServiceSpec, &ServiceGroup{key: key, spec: nodeSpec})
	}

	allScaledServiceSpecs := make([]*ServiceGroup, 0, len(m.Spec.Nodes))

	for _, t := range druidServicesOrder {
		specs := scaledServiceSpecsByNodeType[t]
		sort.Slice(specs, func(i, j int) bool { return specs[i].key < specs[j].key })
		allScaledServiceSpecs = append(allScaledServiceSpecs, specs...)
	}

	return allScaledServiceSpecs
}
