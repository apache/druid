package util

import (
	"testing"
)

func TestIncludesJson(t *testing.T) {
	tests := []struct {
		name          string
		currentJson   string
		desiredJson   string
		expectedEqual bool
		expectError   bool
	}{
		{
			name: "Exact match",
			currentJson: `{
                "key1": "value1",
                "key2": "value2"
            }`,
			desiredJson: `{
                "key1": "value1",
                "key2": "value2"
            }`,
			expectedEqual: true,
			expectError:   false,
		},
		{
			name: "Real config not matching",
			currentJson: `{
				"type": "default",
				"selectStrategy": {
					"type": "fillCapacityWithCategorySpec",
					"workerCategorySpec": {
					"categoryMap": {},
					"strong": false
					}
				},
				"autoScaler": null
			}`,
			desiredJson: `{
				"type": "default",
				"selectStrategy": {
					"type": "fillCapacityWithCategorySpec",
					"workerCategorySpec": {
					"categoryMap": {},
					"strong": true
					}
				},
				"autoScaler": null
			}`,
			expectedEqual: false,
			expectError:   false,
		},
		{
			// This covers a case where a user may use the JSON body from the cURL example
			// in Druid documentation (https://druid.apache.org/docs/latest/api-reference/dynamic-configuration-api),
			// which includes extra fields that are not supported by Druid anymore.
			name:        "Incorrect coordinator dynamic config",
			currentJson: `{"millisToWaitBeforeDeleting":900000,"mergeBytesLimit":524288000,"mergeSegmentsLimit":100,"maxSegmentsToMove":5,"replicantLifetime":15,"replicationThrottleLimit":10,"balancerComputeThreads":1,"killDataSourceWhitelist":[],"killTaskSlotRatio":0.1,"maxKillTaskSlots":2147483647,"killPendingSegmentsSkipList":[],"maxSegmentsInNodeLoadingQueue":100,"decommissioningNodes":[],"pauseCoordination":false,"replicateAfterLoadTimeout":false,"useRoundRobinSegmentAssignment":true,"smartSegmentLoading":true,"debugDimensions":null}`,
			desiredJson: `{
			"millisToWaitBeforeDeleting": 900000,
			"mergeBytesLimit": 524288000,
			"mergeSegmentsLimit": 100,
			"maxSegmentsToMove": 5,
			"percentOfSegmentsToConsiderPerMove": 100,
			"useBatchedSegmentSampler": false,
			"replicantLifetime": 15,
			"replicationThrottleLimit": 10,
			"balancerComputeThreads": 1,
			"emitBalancingStats": false,
			"killDataSourceWhitelist": [],
			"killAllDataSources": true,
			"killPendingSegmentsSkipList": [],
			"maxSegmentsInNodeLoadingQueue": 100,
			"decommissioningNodes": [],
			"decommissioningMaxPercentOfMaxSegmentsToMove": 70,
			"pauseCoordination": false,
			"replicateAfterLoadTimeout": false,
			"maxNonPrimaryReplicantsToLoad": 2147483647
			}`,
			expectedEqual: false,
			expectError:   false,
		},
		{
			name: "Subset match with nested maps",
			currentJson: `{
                "key1": "value1",
                "key2": {
                    "nestedKey1": "nestedValue1",
                    "nestedKey2": "nestedValue2"
                }
            }`,
			desiredJson: `{
                "key2": {
                    "nestedKey1": "nestedValue1"
                }
            }`,
			expectedEqual: true,
			expectError:   false,
		},
		{
			name: "Mismatch with nested maps",
			currentJson: `{
                "key1": "value1",
                "key2": {
                    "nestedKey1": "nestedValue1"
                }
            }`,
			desiredJson: `{
                "key2": {
                    "nestedKey2": "nestedValue2"
                }
            }`,
			expectedEqual: false,
			expectError:   false,
		},
		{
			name: "Subset match with arrays",
			currentJson: `{
                "key1": ["value1", "value2", "value3"]
            }`,
			desiredJson: `{
                "key1": ["value1", "value2"]
            }`,
			expectedEqual: true,
			expectError:   false,
		},
		{
			name: "Mismatch with arrays",
			currentJson: `{
                "key1": ["value1", "value2"]
            }`,
			desiredJson: `{
                "key1": ["value3"]
            }`,
			expectedEqual: false,
			expectError:   false,
		},
		{
			name: "Invalid JSON",
			currentJson: `{
                "key1": "value1"
            `,
			desiredJson: `{
                "key1": "value1"
            }`,
			expectedEqual: false,
			expectError:   true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			equal, err := IncludesJson(test.currentJson, test.desiredJson)
			if (err != nil) != test.expectError {
				t.Errorf("IncludesJson() error = %v, expectError %v", err, test.expectError)
				return
			}
			if equal != test.expectedEqual {
				t.Errorf("IncludesJson() = %v, expectedEqual %v", equal, test.expectedEqual)
			}
		})
	}
}
