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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/datainfrahq/druid-operator/apis/druid/v1alpha1"

	internalhttp "github.com/datainfrahq/druid-operator/pkg/http"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestGetRules(t *testing.T) {
	tests := []struct {
		name           string
		druidIngestion *v1alpha1.DruidIngestion
		expectedRules  []map[string]interface{}
		expectError    bool
	}{
		{
			name: "No rules",
			druidIngestion: &v1alpha1.DruidIngestion{
				Spec: v1alpha1.DruidIngestionSpec{
					Ingestion: v1alpha1.IngestionSpec{
						Rules: []runtime.RawExtension{},
					},
				},
			},
			expectedRules: nil,
			expectError:   false,
		},
		{
			name: "Valid rules",
			druidIngestion: &v1alpha1.DruidIngestion{
				Spec: v1alpha1.DruidIngestionSpec{
					Ingestion: v1alpha1.IngestionSpec{
						Rules: []runtime.RawExtension{
							{Raw: []byte(`{"type": "load", "tieredReplicants": {"hot": 2}}`)},
							{Raw: []byte(`{"type": "drop", "interval": "2012-01-01/2013-01-01"}`)},
						},
					},
				},
			},
			expectedRules: []map[string]interface{}{
				{"type": "load", "tieredReplicants": map[string]interface{}{"hot": float64(2)}},
				{"type": "drop", "interval": "2012-01-01/2013-01-01"},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rules, err := getRules(tt.druidIngestion)

			if tt.expectError {
				assert.Error(t, err, "expected an error but got none")
			} else {
				assert.NoError(t, err, "unexpected error")
				assert.Equal(t, tt.expectedRules, rules, "rules do not match expected")
			}
		})
	}
}

func TestGetRulesJson(t *testing.T) {
	tests := []struct {
		name           string
		druidIngestion *v1alpha1.DruidIngestion
		expectedJson   string
		expectError    bool
	}{
		{
			name: "No rules",
			druidIngestion: &v1alpha1.DruidIngestion{
				Spec: v1alpha1.DruidIngestionSpec{
					Ingestion: v1alpha1.IngestionSpec{
						Rules: []runtime.RawExtension{},
					},
				},
			},
			expectedJson: "null",
			expectError:  false,
		},
		{
			name: "Valid rules",
			druidIngestion: &v1alpha1.DruidIngestion{
				Spec: v1alpha1.DruidIngestionSpec{
					Ingestion: v1alpha1.IngestionSpec{
						Rules: []runtime.RawExtension{
							{Raw: []byte(`{"type": "load", "tieredReplicants": {"hot": 2}}`)},
							{Raw: []byte(`{"type": "drop", "interval": "2012-01-01/2013-01-01"}`)},
						},
					},
				},
			},
			expectedJson: `[{"type":"load","tieredReplicants":{"hot":2}},{"type":"drop","interval":"2012-01-01/2013-01-01"}]`,
			expectError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualJson, err := getRulesJson(tt.druidIngestion)

			if tt.expectError {
				assert.Error(t, err, "expected an error but got none")
			} else {
				assert.NoError(t, err, "unexpected error")
				assert.JSONEq(t, tt.expectedJson, actualJson, "JSON output does not match expected")
			}
		})
	}
}

func TestUpdateCompaction_Success(t *testing.T) {
	// Mock DruidIngestion data
	di := &v1alpha1.DruidIngestion{
		Spec: v1alpha1.DruidIngestionSpec{
			Ingestion: v1alpha1.IngestionSpec{
				Spec: `{
                    "spec": {
                        "dataSchema": {
                            "dataSource": "testDataSource"
                        }
                    }
                }`,
				Compaction: runtime.RawExtension{
					Raw: []byte(`{"metricsSpec": "testMetric"}`),
				},
			},
		},
	}

	// Mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			// Return current compaction settings
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"metricsSpec": "currentMetric"}`))
		} else if r.Method == http.MethodPost {
			// Simulate successful update
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	// Mock Auth
	auth := internalhttp.Auth{
		BasicAuth: internalhttp.BasicAuth{
			UserName: "user",
			Password: "pass",
		},
	}

	// Mock DruidIngestionReconciler
	r := &DruidIngestionReconciler{}

	// Call UpdateCompaction
	success, err := r.UpdateCompaction(di, server.URL, auth)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !success {
		t.Fatalf("expected success, got failure")
	}
}

func TestUpdateCompaction_Failure(t *testing.T) {
	// Mock DruidIngestion data
	di := &v1alpha1.DruidIngestion{
		Spec: v1alpha1.DruidIngestionSpec{
			Ingestion: v1alpha1.IngestionSpec{
				Spec: `{
					"spec": {
						"dataSchema": {
							"dataSource": "testDataSource"
						}
					}
				}`,
				Compaction: runtime.RawExtension{
					Raw: []byte(`{"metricsSpec": "testMetric"}`),
				},
			},
		},
	}

	// Mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	// Mock Auth
	auth := internalhttp.Auth{
		BasicAuth: internalhttp.BasicAuth{
			UserName: "user",
			Password: "pass",
		},
	}

	// Mock DruidIngestionReconciler
	r := &DruidIngestionReconciler{}

	// Call UpdateCompaction
	success, err := r.UpdateCompaction(di, server.URL, auth)

	if err == nil {
		t.Fatalf("expected error, got nil")
	}

	if success {
		t.Fatalf("expected failure, got success")
	}
}

func TestGetCompaction(t *testing.T) {
	compactionMap := make(map[string]interface{})
	tests := []struct {
		name          string
		compactionRaw string
		specRaw       string
		expectedMap   map[string]interface{}
		expectingErr  bool
	}{
		{
			name: "Valid compaction settings with dataSource",
			compactionRaw: `{
                "metricsSpec": "testMetric",
                "tuningConfig": {"maxRowsInMemory": 10000}
            }`,
			specRaw: `{
                "spec": {
                    "dataSchema": {
                        "dataSource": "testDataSource"
                    }
                }
            }`,
			expectedMap: map[string]interface{}{
				"metricsSpec": "testMetric",
				"tuningConfig": map[string]interface{}{
					"maxRowsInMemory": float64(10000),
				},
				"dataSource": "testDataSource",
			},
			expectingErr: false,
		},
		{
			name:          "No compaction settings",
			compactionRaw: ``,
			specRaw: `{
                "spec": {
                    "dataSchema": {
                        "dataSource": "testDataSource"
                    }
                }
            }`,
			expectedMap:  compactionMap,
			expectingErr: false,
		},
		{
			name: "Missing dataSource",
			compactionRaw: `{
                "metricsSpec": "testMetric",
                "tuningConfig": {"maxRowsInMemory": 10000}
            }`,
			specRaw: `{
                "spec": {
                    "dataSchema": {}
                }
            }`,
			expectedMap:  nil,
			expectingErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			di := &v1alpha1.DruidIngestion{
				Spec: v1alpha1.DruidIngestionSpec{
					Ingestion: v1alpha1.IngestionSpec{
						Compaction: runtime.RawExtension{
							Raw: []byte(tt.compactionRaw),
						},
						Spec: string(tt.specRaw),
					},
				},
			}

			compactionMap, err := getCompaction(di)
			if tt.expectingErr {
				assert.Error(t, err, "Expected an error but got none")
			} else {
				assert.NoError(t, err, "Unexpected error")
				assert.Equal(t, tt.expectedMap, compactionMap, "Compaction map does not match expected value")
			}
		})
	}
}

func TestGetCompactionJson(t *testing.T) {
	tests := []struct {
		name          string
		compactionRaw string
		specRaw       string
		expectedJson  string
		expectingErr  bool
	}{
		{
			name: "Valid compaction settings with dataSource",
			compactionRaw: `{
                "metricsSpec": "testMetric",
                "tuningConfig": {"maxRowsInMemory": 10000}
            }`,
			specRaw: `{
                "spec": {
                    "dataSchema": {
                        "dataSource": "testDataSource"
                    }
                }
            }`,
			expectedJson: `{"metricsSpec":"testMetric","tuningConfig":{"maxRowsInMemory":10000},"dataSource":"testDataSource"}`,
			expectingErr: false,
		},
		{
			name:          "No compaction settings",
			compactionRaw: ``,
			specRaw: `{
                "spec": {
                    "dataSchema": {
                        "dataSource": "testDataSource"
                    }
                }
            }`,
			expectedJson: "{}",
			expectingErr: false,
		},
		{
			name: "Missing dataSource",
			compactionRaw: `{
                "metricsSpec": "testMetric",
                "tuningConfig": {"maxRowsInMemory": 10000}
            }`,
			specRaw: `{
                "spec": {
                    "dataSchema": {}
                }
            }`,
			expectedJson: "",
			expectingErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			di := &v1alpha1.DruidIngestion{
				Spec: v1alpha1.DruidIngestionSpec{
					Ingestion: v1alpha1.IngestionSpec{
						Compaction: runtime.RawExtension{
							Raw: []byte(tt.compactionRaw),
						},
						Spec: string(tt.specRaw),
					},
				},
			}

			compactionJson, err := getCompactionJson(di)
			if tt.expectingErr {
				assert.Error(t, err, "Expected an error but got none")
			} else {
				assert.NoError(t, err, "Unexpected error")
				assert.JSONEq(t, tt.expectedJson, compactionJson, "Compaction JSON does not match expected value")
			}
		})
	}
}

func TestGetPath(t *testing.T) {
	tests := []struct {
		name          string
		ingestionType v1alpha1.DruidIngestionMethod
		svcName       string
		httpMethod    string
		taskId        string
		shutDownTask  bool
		expected      string
	}{
		{
			name:          "NativeBatchGetTask",
			ingestionType: v1alpha1.NativeBatchIndexParallel,
			svcName:       "http://example-druid-service",
			httpMethod:    http.MethodGet,
			taskId:        "task1",
			expected:      "http://example-druid-service/druid/indexer/v1/task/task1",
		},
		{
			name:          "NativeBatchCreateUpdateTask",
			ingestionType: v1alpha1.NativeBatchIndexParallel,
			svcName:       "http://example-druid-service",
			httpMethod:    http.MethodPost,
			shutDownTask:  false,
			expected:      "http://example-druid-service/druid/indexer/v1/task",
		},
		{
			name:          "NativeBatchShutdownTask",
			ingestionType: v1alpha1.NativeBatchIndexParallel,
			svcName:       "http://example-druid-service",
			httpMethod:    http.MethodPost,
			taskId:        "task1",
			shutDownTask:  true,
			expected:      "http://example-druid-service/druid/indexer/v1/task/task1/shutdown",
		},
		{
			name:          "KafkaGetSupervisorTask",
			ingestionType: v1alpha1.Kafka,
			svcName:       "http://example-druid-service",
			httpMethod:    http.MethodGet,
			taskId:        "supervisor1",
			expected:      "http://example-druid-service/druid/indexer/v1/supervisor/supervisor1",
		},
		{
			name:          "KafkaCreateUpdateSupervisorTask",
			ingestionType: v1alpha1.Kafka,
			svcName:       "http://example-druid-service",
			httpMethod:    http.MethodPost,
			shutDownTask:  false,
			expected:      "http://example-druid-service/druid/indexer/v1/supervisor",
		},
		{
			name:          "KafkaShutdownSupervisor",
			ingestionType: v1alpha1.Kafka,
			svcName:       "http://example-druid-service",
			httpMethod:    http.MethodPost,
			taskId:        "supervisor1",
			shutDownTask:  true,
			expected:      "http://example-druid-service/druid/indexer/v1/supervisor/supervisor1/shutdown",
		},
		{
			name:          "UnsupportedIngestionType",
			ingestionType: v1alpha1.Kinesis,
			svcName:       "http://example-druid-service",
			httpMethod:    http.MethodGet,
			expected:      "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := getPath(tt.ingestionType, tt.svcName, tt.httpMethod, tt.taskId, tt.shutDownTask)
			if actual != tt.expected {
				t.Errorf("getPath() = %v, expected %v", actual, tt.expected)
			}
		})
	}
}

func TestGetSpec(t *testing.T) {
	tests := []struct {
		name           string
		di             *v1alpha1.DruidIngestion
		expectedSpec   map[string]interface{}
		expectingError bool
	}{
		{
			name: "NativeSpec is used",
			di: &v1alpha1.DruidIngestion{
				Spec: v1alpha1.DruidIngestionSpec{
					Ingestion: v1alpha1.IngestionSpec{
						NativeSpec: runtime.RawExtension{
							Raw: []byte(`{"key": "value"}`),
						},
					},
				},
			},
			expectedSpec: map[string]interface{}{
				"key": "value",
			},
			expectingError: false,
		},
		{
			name: "Spec is used when NativeSpec is empty",
			di: &v1alpha1.DruidIngestion{
				Spec: v1alpha1.DruidIngestionSpec{
					Ingestion: v1alpha1.IngestionSpec{
						Spec: `{"key": "value"}`,
					},
				},
			},
			expectedSpec: map[string]interface{}{
				"key": "value",
			},
			expectingError: false,
		},
		{
			name: "Error when both NativeSpec and Spec are empty",
			di: &v1alpha1.DruidIngestion{
				Spec: v1alpha1.DruidIngestionSpec{
					Ingestion: v1alpha1.IngestionSpec{
						NativeSpec: runtime.RawExtension{},
						Spec:       "",
					},
				},
			},
			expectedSpec:   nil,
			expectingError: true,
		},
		{
			name: "Error when Spec is invalid JSON",
			di: &v1alpha1.DruidIngestion{
				Spec: v1alpha1.DruidIngestionSpec{
					Ingestion: v1alpha1.IngestionSpec{
						Spec: `{"key": "value"`, // Invalid JSON
					},
				},
			},
			expectedSpec:   nil,
			expectingError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec, err := getSpec(tt.di)
			if tt.expectingError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedSpec, spec)
			}
		})
	}
}

func TestGetDataSource(t *testing.T) {
	tests := []struct {
		name         string
		specJSON     string
		expected     string
		expectingErr bool
	}{
		{
			name: "Valid dataSource extraction",
			specJSON: `
            {
                "spec": {
                    "dataSchema": {
                        "dataSource": "wikipedia-2"
                    }
                }
            }`,
			expected:     "wikipedia-2",
			expectingErr: false,
		},
		{
			name: "Missing dataSource",
			specJSON: `
            {
                "spec": {
                    "dataSchema": {}
                }
            }`,
			expected:     "",
			expectingErr: true,
		},
		{
			name: "Incorrect dataSource type",
			specJSON: `
            {
                "spec": {
                    "dataSchema": {
                        "dataSource": 123
                    }
                }
            }`,
			expected:     "",
			expectingErr: true,
		},
		{
			name: "Missing spec section",
			specJSON: `
            {
                "otherField": {}
            }`,
			expected:     "",
			expectingErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Prepare the DruidIngestion object with the test JSON
			di := &v1alpha1.DruidIngestion{
				Spec: v1alpha1.DruidIngestionSpec{
					Ingestion: v1alpha1.IngestionSpec{
						Spec: (tt.specJSON),
					},
				},
			}

			dataSource, err := getDataSource(di)
			if tt.expectingErr {
				assert.Error(t, err, "Expected an error but got none")
			} else {
				assert.NoError(t, err, "Unexpected error")
				assert.Equal(t, tt.expected, dataSource, "DataSource does not match expected value")
			}
		})
	}
}
