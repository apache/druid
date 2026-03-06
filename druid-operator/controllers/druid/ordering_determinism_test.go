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
	"testing"

	druidv1alpha1 "github.com/datainfrahq/druid-operator/apis/druid/v1alpha1"
)

// TestGetNodeSpecsByOrderDeterministic verifies that getNodeSpecsByOrder returns
// the same order on repeated calls when multiple node specs share the same NodeType.
// Without deterministic sorting by key, map iteration would make order vary across calls,
// leading to non-deterministic rollout order when rollingDeploy is enabled.
func TestGetNodeSpecsByOrderDeterministic(t *testing.T) {
	m := &druidv1alpha1.Druid{
		Spec: druidv1alpha1.DruidSpec{
			Nodes: map[string]druidv1alpha1.DruidNodeSpec{
				"historicals-hot":  {NodeType: "historical"},
				"historicals-cold": {NodeType: "historical"},
				"overlords":        {NodeType: "overlord"},
			},
		},
	}
	const numCalls = 100
	var firstKeys []string
	for i := 0; i < numCalls; i++ {
		ordered := getNodeSpecsByOrder(m)
		keys := make([]string, len(ordered))
		for j, sg := range ordered {
			keys[j] = sg.key
		}
		if i == 0 {
			firstKeys = keys
			continue
		}
		if len(keys) != len(firstKeys) {
			t.Fatalf("call %d: got %d keys, first call had %d", i+1, len(keys), len(firstKeys))
		}
		for j := range keys {
			if keys[j] != firstKeys[j] {
				t.Fatalf("call %d: order changed at index %d: got %q, first call had %q (full first: %v, full this: %v)",
					i+1, j, keys[j], firstKeys[j], firstKeys, keys)
			}
		}
	}
}

// TestGetNodeSpecsByOrderSortedWithinNodeType verifies that within the same NodeType,
// specs are ordered by key ascending (e.g. historicals-cold before historicals-hot).
func TestGetNodeSpecsByOrderSortedWithinNodeType(t *testing.T) {
	m := &druidv1alpha1.Druid{
		Spec: druidv1alpha1.DruidSpec{
			Nodes: map[string]druidv1alpha1.DruidNodeSpec{
				"historicals-hot":  {NodeType: "historical"},
				"historicals-cold": {NodeType: "historical"},
				"historicals-warm": {NodeType: "historical"},
			},
		},
	}
	ordered := getNodeSpecsByOrder(m)
	if len(ordered) != 3 {
		t.Fatalf("expected 3 specs, got %d", len(ordered))
	}
	want := []string{"historicals-cold", "historicals-hot", "historicals-warm"}
	for i, w := range want {
		if ordered[i].key != w {
			t.Errorf("index %d: got key %q, want %q", i, ordered[i].key, w)
		}
	}
}
