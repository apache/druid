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
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/datainfrahq/druid-operator/apis/druid/v1alpha1"
	"github.com/datainfrahq/druid-operator/controllers/druid/ext"
)

var deepStorageExtTypes = map[string]reflect.Type{}

func init() {
	deepStorageExtTypes["default"] = reflect.TypeOf(ext.DefaultDeepStorageManager{})
}

// We might have to add more methods to this interface to enable extensions that completely manage
// deploy, upgrade and termination of deep storage.
type deepStorageManager interface {
	Configuration() string
}

func createDeepStorageManager(spec *v1alpha1.DeepStorageSpec) (deepStorageManager, error) {
	if t, ok := deepStorageExtTypes[spec.Type]; ok {
		v := reflect.New(t).Interface()
		if err := json.Unmarshal(spec.Spec, v); err != nil {
			return nil, fmt.Errorf("Couldn't unmarshall deepStorage type[%s]. Error[%s].", spec.Type, err.Error())
		} else {
			return v.(deepStorageManager), nil
		}
	} else {
		return nil, fmt.Errorf("Can't find type[%s] for DeepStorage Mgmt.", spec.Type)
	}
}
