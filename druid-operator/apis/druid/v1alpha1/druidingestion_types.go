/*
DataInfra 2023 Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	druidapi "github.com/datainfrahq/druid-operator/pkg/druidapi"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

type DruidIngestionMethod string

const (
	Kafka                    DruidIngestionMethod = "kafka"
	Kinesis                  DruidIngestionMethod = "kinesis"
	NativeBatchIndexParallel DruidIngestionMethod = "native-batch"
	QueryControllerSQL       DruidIngestionMethod = "sql"
	HadoopIndexHadoop        DruidIngestionMethod = "index-hadoop"
)

type DruidIngestionSpec struct {
	// +optional
	Suspend bool `json:"suspend"`
	// +required
	DruidClusterName string `json:"druidCluster"`
	// +required
	Ingestion IngestionSpec `json:"ingestion"`
	// +optional
	Auth druidapi.Auth `json:"auth"`
}

type IngestionSpec struct {
	// +required
	Type DruidIngestionMethod `json:"type"`
	// +optional
	// Spec should be passed in as a JSON string.
	// Note: This field is planned for deprecation in favor of nativeSpec.
	Spec string `json:"spec,omitempty"`
	// +optional
	// nativeSpec allows the ingestion specification to be defined in a native Kubernetes format.
	// This is particularly useful for environment-specific configurations and will eventually
	// replace the JSON-based Spec field.
	// Note: Spec will be ignored if nativeSpec is provided.
	NativeSpec runtime.RawExtension `json:"nativeSpec,omitempty"`
	// +optional
	Compaction runtime.RawExtension `json:"compaction,omitempty"`
	// +optional
	Rules []runtime.RawExtension `json:"rules,omitempty"`
}

type DruidIngestionStatus struct {
	TaskId         string             `json:"taskId"`
	Type           string             `json:"type,omitempty"`
	Status         v1.ConditionStatus `json:"status,omitempty"`
	Reason         string             `json:"reason,omitempty"`
	Message        string             `json:"message,omitempty"`
	LastUpdateTime metav1.Time        `json:"lastUpdateTime,omitempty"`
	// CurrentIngestionSpec is a string instead of RawExtension to maintain compatibility with existing
	// IngestionSpecs that are stored as JSON strings.
	CurrentIngestionSpec string                 `json:"currentIngestionSpec.json"`
	CurrentRules         []runtime.RawExtension `json:"rules,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:printcolumn:name="Type",type="string",JSONPath=".spec.ingestionSpec.type"
// Ingestion is the Schema for the Ingestion API
type DruidIngestion struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DruidIngestionSpec   `json:"spec"`
	Status DruidIngestionStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
// IngestionList contains a list of Ingestion
type DruidIngestionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DruidIngestion `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DruidIngestion{}, &DruidIngestionList{})
}
