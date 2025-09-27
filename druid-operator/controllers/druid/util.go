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
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func firstNonEmptyStr(s1 string, s2 string) string {
	if len(s1) > 0 {
		return s1
	} else {
		return s2
	}
}

// Note that all the arguments passed to this function must have zero value of Nil.
func firstNonNilValue(v1, v2 interface{}) interface{} {
	if !reflect.ValueOf(v1).IsNil() {
		return v1
	} else {
		return v2
	}
}

// lookup DENY_LIST, default is nil
func getDenyListEnv(key string, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultVal
}

// pass slice of strings for namespaces
func getEnvAsSlice(name string, defaultVal []string, sep string) []string {
	valStr := getDenyListEnv(name, "")
	if valStr == "" {
		return defaultVal
	}
	// split on ","
	val := strings.Split(valStr, sep)
	return val
}

func ContainsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

func RemoveString(slice []string, s string) (result []string) {
	for _, item := range slice {
		if item == s {
			continue
		}
		result = append(result, item)
	}
	return
}

// returns pointer to bool
func boolFalse() *bool {
	bool := false
	return &bool
}

// to be used in max concurrent reconciles only
// defaulting to return 1
func Str2Int(s string) int {
	i, err := strconv.Atoi(s)
	if err != nil {
		return 1
	}
	return i
}

func IsEqualJson(s1, s2 string) (bool, error) {
	var o1 interface{}
	var o2 interface{}

	var err error
	err = json.Unmarshal([]byte(s1), &o1)
	if err != nil {
		return false, fmt.Errorf("error mashalling string 1 :: %s", err.Error())
	}
	err = json.Unmarshal([]byte(s2), &o2)
	if err != nil {
		return false, fmt.Errorf("error mashalling string 2 :: %s", err.Error())
	}

	return reflect.DeepEqual(o1, o2), nil
}

// to find the time difference between two epoch times
func timeDifference(epochTime1, epochTime2 int64) int64 {
	t1 := time.Unix(epochTime1, 0)
	t2 := time.Unix(epochTime2, 0)

	diff := time.Duration(t2.Sub(t1))
	return int64(diff.Seconds())
}

func containsString(all []string, string string) bool {
	for _, s := range all {
		if s == string {
			return true
		}
	}

	return false
}

func hasDuplicateString(slice []string) (bool, string) {
	seen := make(map[string]bool)
	for _, s := range slice {
		if seen[s] {
			return true, s
		}
		seen[s] = true
	}
	return false, ""
}
