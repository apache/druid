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

package v1alpha1

import (
	"encoding/json"

	druidapi "github.com/datainfrahq/druid-operator/pkg/druidapi"
	appsv1 "k8s.io/api/apps/v1"
	autoscalev2 "k8s.io/api/autoscaling/v2"
	v1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// druid-operator deploys a druid cluster from given spec below, based on the spec it would create following
// k8s resources
// - one ConfigMap containing common.runtime.properties
// - for each item in the "nodes" field in spec
//   - one StatefulSet that manages one or more Druid pods with same config
//   - one ConfigMap containing runtime.properties, jvm.config, log4j.xml contents to be used by above Pods
//   - zero or more Headless/ClusterIP/LoadBalancer etc Service resources backed by above Pods
//   - optional PodDisruptionBudget resource for the StatefulSet
//

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// AdditionalContainer defines additional sidecar containers to be deployed with the `Druid` pods.
// (will be part of Kubernetes native in the future:
// https://github.com/kubernetes/enhancements/blob/master/keps/sig-node/753-sidecar-containers/README.md#summary).
type AdditionalContainer struct {
	// RunAsInit indicate whether this should be an init container.
	// +optional
	RunAsInit bool `json:"runAsInit"`

	// Image Image of the additional container.
	// +required
	Image string `json:"image"`

	// ContainerName name of the additional container.
	// +required
	ContainerName string `json:"containerName"`

	// Command command for the additional container.
	// +required
	Command []string `json:"command"`

	// ImagePullPolicy If not present, will be taken from top level spec.
	// +optional
	ImagePullPolicy v1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// Args Arguments to call the command.
	// +optional
	Args []string `json:"args,omitempty"`

	// ContainerSecurityContext If not present, will be taken from top level pod.
	// +optional
	ContainerSecurityContext *v1.SecurityContext `json:"securityContext,omitempty"`

	// Resources Kubernetes Native `resources` specification.
	// +optional
	Resources v1.ResourceRequirements `json:"resources,omitempty"`

	// VolumeMounts Kubernetes Native `VolumeMount` specification.
	// +optional
	VolumeMounts []v1.VolumeMount `json:"volumeMounts,omitempty"`

	// Env Environment variables for the additional container.
	// +optional
	Env []v1.EnvVar `json:"env,omitempty"`

	// EnvFrom Extra environment variables from remote source (ConfigMaps, Secrets...).
	// +optional
	EnvFrom []v1.EnvFromSource `json:"envFrom,omitempty"`
}

// DruidSpec defines the desired state of the Druid cluster.
type DruidSpec struct {

	// Ignored is now deprecated API. In order to avoid reconciliation of objects use the
	// `druid.apache.org/ignored: "true"` annotation.
	// +optional
	// +kubebuilder:default:=false
	Ignored bool `json:"ignored,omitempty"`

	// CommonRuntimeProperties Content fo the `common.runtime.properties` configuration file.
	// +required
	CommonRuntimeProperties string `json:"common.runtime.properties"`

	// ExtraCommonConfig References to ConfigMaps holding more configuration files to mount to the
	// common configuration path.
	// +optional
	ExtraCommonConfig []*v1.ObjectReference `json:"extraCommonConfig"`

	// ForceDeleteStsPodOnError Delete the StatefulSet's pods if the StatefulSet is set to ordered ready.
	// issue: https://github.com/kubernetes/kubernetes/issues/67250
	// doc: https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#forced-rollback
	// +optional
	// +kubebuilder:default:=true
	ForceDeleteStsPodOnError bool `json:"forceDeleteStsPodOnError,omitempty"`

	// ScalePvcSts When enabled, operator will allow volume expansion of StatefulSet's PVCs.
	// +optional
	// +kubebuilder:default:=false
	ScalePvcSts bool `json:"scalePvcSts,omitempty"`

	// CommonConfigMountPath In-container directory to mount the Druid common configuration
	// +optional
	// +kubebuilder:default:="/opt/druid/conf/druid/cluster/_common"
	CommonConfigMountPath string `json:"commonConfigMountPath"`

	// DisablePVCDeletionFinalizer Whether PVCs shall be deleted on the deletion of the Druid cluster.
	// +optional
	// +kubebuilder:default:=false
	DisablePVCDeletionFinalizer bool `json:"disablePVCDeletionFinalizer,omitempty"`

	// DeleteOrphanPvc Orphaned (unmounted PVCs) shall be cleaned up by the operator.
	// +optional
	// +kubebuilder:default:=true
	DeleteOrphanPvc bool `json:"deleteOrphanPvc"`

	// StartScript Path to Druid's start script to be run on start.
	// +optional
	// +kubebuilder:default:="/druid.sh"
	StartScript string `json:"startScript"`

	// Image Required here or at the NodeSpec level.
	// +optional
	Image string `json:"image,omitempty"`

	// ServiceAccount
	// +optional
	ServiceAccount string `json:"serviceAccount,omitempty"`

	// ImagePullSecrets
	// +optional
	ImagePullSecrets []v1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// ImagePullPolicy
	// +optional
	// +kubebuilder:default:="IfNotPresent"
	ImagePullPolicy v1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// Env Environment variables for druid containers.
	// +optional
	Env []v1.EnvVar `json:"env,omitempty"`

	// EnvFrom Extra environment variables from remote source (ConfigMaps, Secrets...).
	// +optional
	EnvFrom []v1.EnvFromSource `json:"envFrom,omitempty"`

	// JvmOptions Contents of the shared `jvm.options` configuration file for druid JVM processes.
	// +optional
	JvmOptions string `json:"jvm.options,omitempty"`

	// Log4jConfig contents `log4j.config` configuration file.
	// +optional
	Log4jConfig string `json:"log4j.config,omitempty"`

	// PodSecurityContext
	// +optional
	PodSecurityContext *v1.PodSecurityContext `json:"securityContext,omitempty"`

	// ContainerSecurityContext
	// +optional
	ContainerSecurityContext *v1.SecurityContext `json:"containerSecurityContext,omitempty"`

	// VolumeClaimTemplates Kubernetes Native `VolumeClaimTemplate` specification.
	// +optional
	VolumeClaimTemplates []v1.PersistentVolumeClaim `json:"volumeClaimTemplates,omitempty"`

	// VolumeMounts Kubernetes Native `VolumeMount` specification.
	// +optional
	VolumeMounts []v1.VolumeMount `json:"volumeMounts,omitempty"`

	// Volumes Kubernetes Native `Volumes` specification.
	// +optional
	Volumes []v1.Volume `json:"volumes,omitempty"`

	// PodAnnotations Custom annotations to be populated in `Druid` pods.
	// +optional
	PodAnnotations map[string]string `json:"podAnnotations,omitempty"`

	// WorkloadAnnotations annotations to be populated in StatefulSet or Deployment spec.
	// if the same key is specified at both the DruidNodeSpec level and DruidSpec level, the DruidNodeSpec WorkloadAnnotations will take precedence.
	// +optional
	WorkloadAnnotations map[string]string `json:"workloadAnnotations,omitempty"`

	// PodManagementPolicy
	// +optional
	// +kubebuilder:default:="Parallel"
	PodManagementPolicy appsv1.PodManagementPolicyType `json:"podManagementPolicy,omitempty"`

	// PodLabels Custom labels to be populated in `Druid` pods.
	// +optional
	PodLabels map[string]string `json:"podLabels,omitempty"`

	// PriorityClassName Kubernetes native `priorityClassName` specification.
	// +optional
	PriorityClassName string `json:"priorityClassName,omitempty"`

	// UpdateStrategy
	// +optional
	UpdateStrategy *appsv1.StatefulSetUpdateStrategy `json:"updateStrategy,omitempty"`

	// LivenessProbe
	// Port is set to `druid.port` if not specified with httpGet handler.
	// +optional
	LivenessProbe *v1.Probe `json:"livenessProbe,omitempty"`

	// ReadinessProbe
	// Port is set to `druid.port` if not specified with httpGet handler.
	// +optional
	ReadinessProbe *v1.Probe `json:"readinessProbe,omitempty"`

	// StartUpProbe
	// +optional
	StartUpProbe *v1.Probe `json:"startUpProbe,omitempty"`

	// Services Kubernetes services to be created for each workload.
	// +optional
	Services []v1.Service `json:"services,omitempty"`

	// NodeSelector Kubernetes native `nodeSelector` specification.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Tolerations Kubernetes native `tolerations` specification.
	// +optional
	Tolerations []v1.Toleration `json:"tolerations,omitempty"`

	// Affinity Kubernetes native `affinity` specification.
	// +optional
	Affinity *v1.Affinity `json:"affinity,omitempty"`

	// Nodes a list of `Druid` Node types and their configurations.
	// `DruidSpec` is used to create Kubernetes workload specs. Many of the fields above can be overridden at the specific
	// `NodeSpec` level.
	// +required
	Nodes map[string]DruidNodeSpec `json:"nodes"`

	// AdditionalContainer defines additional sidecar containers to be deployed with the `Druid` pods.
	// +optional
	AdditionalContainer []AdditionalContainer `json:"additionalContainer,omitempty"`

	// RollingDeploy Whether to deploy the components in a rolling update as described in the documentation:
	// https://druid.apache.org/docs/latest/operations/rolling-updates.html
	// If set to true then operator checks the rollout status of previous version workloads before updating the next.
	// This will be done only for update actions.
	// +optional
	// +kubebuilder:default:=true
	RollingDeploy bool `json:"rollingDeploy"`

	// DefaultProbes If set to true this will add default probes (liveness / readiness / startup) for all druid components
	// but it won't override existing probes
	// +optional
	// +kubebuilder:default:=true
	DefaultProbes bool `json:"defaultProbes"`

	// Zookeeper IGNORED (Future API): In order to make Druid dependency setup extensible from within Druid operator.
	// +optional
	Zookeeper *ZookeeperSpec `json:"zookeeper,omitempty"`

	// MetadataStore IGNORED (Future API): In order to make Druid dependency setup extensible from within Druid operator.
	// +optional
	MetadataStore *MetadataStoreSpec `json:"metadataStore,omitempty"`

	// DeepStorage IGNORED (Future API): In order to make Druid dependency setup extensible from within Druid operator.
	// +optional
	DeepStorage *DeepStorageSpec `json:"deepStorage,omitempty"`

	// DimensionsMapPath Custom Dimension Map Path for statsd emitter.
	// stastd documentation is described in the following documentation:
	// https://druid.apache.org/docs/latest/development/extensions-contrib/statsd.html
	// +optional
	DimensionsMapPath string `json:"metricDimensions.json,omitempty"`

	// HdfsSite Contents of `hdfs-site.xml`.
	// +optional
	HdfsSite string `json:"hdfs-site.xml,omitempty"`

	// CoreSite Contents of `core-site.xml`.
	// +optional
	CoreSite string `json:"core-site.xml,omitempty"`

	// Dynamic Configurations for Druid. Applied through the dynamic configuration API.
	// +optional
	DynamicConfig runtime.RawExtension `json:"dynamicConfig,omitempty"`

	// +optional
	Auth druidapi.Auth `json:"auth,omitempty"`

	// See v1.DNSPolicy for more details.
	// +optional
	DNSPolicy v1.DNSPolicy `json:"dnsPolicy,omitempty" protobuf:"bytes,6,opt,name=dnsPolicy,casttype=DNSPolicy"`

	// See v1.PodDNSConfig for more details.
	// +optional
	DNSConfig *v1.PodDNSConfig `json:"dnsConfig,omitempty" protobuf:"bytes,26,opt,name=dnsConfig"`
}

// DruidNodeSpec Specification of `Druid` Node type and its configurations.
// The key in following map can be arbitrary string that helps you identify resources for a specific nodeSpec.
// It is used in the Kubernetes resources' names, so it must be compliant with restrictions
// placed on Kubernetes resource names:
// https://kubernetes.io/docs/concepts/overview/working-with-objects/names/
type DruidNodeSpec struct {
	// NodeDruid `Druid` node type.
	// +required
	// +kubebuilder:validation:Enum:=historical;overlord;middleManager;indexer;broker;coordinator;router
	NodeType string `json:"nodeType"`

	// DruidPort Used by the `Druid` process.
	// +required
	DruidPort int32 `json:"druid.port"`

	// Kind Can be StatefulSet or Deployment.
	// Note: volumeClaimTemplates are ignored when kind=Deployment
	// +optional
	// +kubebuilder:default:="StatefulSet"
	Kind string `json:"kind,omitempty"`

	// Replicas replica of the workload
	// +optional
	// +kubebuilder:validation:Minimum=0
	Replicas int32 `json:"replicas"`

	// PodLabels Custom labels to be populated in the workload's pods.
	// +optional
	PodLabels map[string]string `json:"podLabels,omitempty"`

	// PodDisruptionBudgetSpec Kubernetes native `podDisruptionBudget` specification.
	// +optional
	PodDisruptionBudgetSpec *policyv1.PodDisruptionBudgetSpec `json:"podDisruptionBudgetSpec,omitempty"`

	// PriorityClassName Kubernetes native `priorityClassName` specification.
	// +optional
	PriorityClassName string `json:"priorityClassName,omitempty"`

	// RuntimeProperties Additional runtime configuration for the specific workload.
	// +required
	RuntimeProperties string `json:"runtime.properties"`

	// JvmOptions overrides `JvmOptions` at top level.
	// +optional
	JvmOptions string `json:"jvm.options,omitempty"`

	// ExtraJvmOptions Appends extra jvm options to the `JvmOptions` field.
	// +optional
	ExtraJvmOptions string `json:"extra.jvm.options,omitempty"`

	// Log4jConfig Overrides `Log4jConfig` at top level.
	// +optional
	Log4jConfig string `json:"log4j.config,omitempty"`

	// NodeConfigMountPath in-container directory to mount with runtime.properties, jvm.config, log4j2.xml files.
	// +required
	NodeConfigMountPath string `json:"nodeConfigMountPath"`

	// Services Overrides services at top level.
	// +optional
	Services []v1.Service `json:"services,omitempty"`

	// Tolerations Kubernetes native `tolerations` specification.
	// +optional
	Tolerations []v1.Toleration `json:"tolerations,omitempty"`

	// Affinity Kubernetes native `affinity` specification.
	// +optional
	Affinity *v1.Affinity `json:"affinity,omitempty"`

	// NodeSelector Kubernetes native `nodeSelector` specification.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// TerminationGracePeriodSeconds
	// +optional
	TerminationGracePeriodSeconds *int64 `json:"terminationGracePeriodSeconds,omitempty"`

	// Ports Extra ports to be added to pod spec.
	// +optional
	Ports []v1.ContainerPort `json:"ports,omitempty"`

	// Image Overrides image from top level, Required if no image specified at top level.
	// +optional
	Image string `json:"image,omitempty"`

	// ImagePullSecrets Overrides `imagePullSecrets` from top level.
	// +optional
	ImagePullSecrets []v1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// ImagePullPolicy Overrides `imagePullPolicy` from top level.
	// +optional
	ImagePullPolicy v1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// Env Environment variables for druid containers.
	// +optional
	Env []v1.EnvVar `json:"env,omitempty"`

	// EnvFrom Extra environment variables from remote source (ConfigMaps, Secrets...).
	// +optional
	EnvFrom []v1.EnvFromSource `json:"envFrom,omitempty"`

	// Resources Kubernetes Native `resources` specification.
	// +optional
	Resources v1.ResourceRequirements `json:"resources,omitempty"`

	// PodSecurityContext Overrides `securityContext` at top level.
	// +optional
	PodSecurityContext *v1.PodSecurityContext `json:"securityContext,omitempty"`

	// ContainerSecurityContext
	// +optional
	ContainerSecurityContext *v1.SecurityContext `json:"containerSecurityContext,omitempty"`

	// PodAnnotations Custom annotation to be populated in the workload's pods.
	// +optional
	PodAnnotations map[string]string `json:"podAnnotations,omitempty"`

	// PodManagementPolicy
	// +optional
	// +kubebuilder:default:="Parallel"
	PodManagementPolicy appsv1.PodManagementPolicyType `json:"podManagementPolicy,omitempty"`

	// MaxSurge For Deployment object only.
	// Set to 25% by default.
	// +optional
	MaxSurge *int32 `json:"maxSurge,omitempty"`

	// MaxUnavailable For deployment object only.
	// Set to 25% by default
	// +optional
	MaxUnavailable *int32 `json:"maxUnavailable,omitempty"`

	// UpdateStrategy
	// +optional
	UpdateStrategy *appsv1.StatefulSetUpdateStrategy `json:"updateStrategy,omitempty"`

	// LivenessProbe
	// Port is set to `druid.port` if not specified with httpGet handler.
	// +optional
	LivenessProbe *v1.Probe `json:"livenessProbe,omitempty"`

	// ReadinessProbe
	// Port is set to `druid.port` if not specified with httpGet handler.
	// +optional
	ReadinessProbe *v1.Probe `json:"readinessProbe,omitempty"`

	// StartUpProbe
	// +optional
	StartUpProbe *v1.Probe `json:"startUpProbe,omitempty"`

	// IngressAnnotations `Ingress` annotations to be populated in ingress spec.
	// +optional
	IngressAnnotations map[string]string `json:"ingressAnnotations,omitempty"`

	// WorkloadAnnotations annotations to be populated in StatefulSet or Deployment spec.
	// +optional
	WorkloadAnnotations map[string]string `json:"workloadAnnotations,omitempty"`

	// Ingress Kubernetes Native `Ingress` specification.
	// +optional
	Ingress *networkingv1.IngressSpec `json:"ingress,omitempty"`

	// VolumeClaimTemplates Kubernetes Native `VolumeClaimTemplate` specification.
	// +optional
	PersistentVolumeClaim []v1.PersistentVolumeClaim `json:"persistentVolumeClaim,omitempty"`

	// Lifecycle
	// +optional
	Lifecycle *v1.Lifecycle `json:"lifecycle,omitempty"`

	// HPAutoScaler Kubernetes Native `HorizontalPodAutoscaler` specification.
	// +optional
	HPAutoScaler *autoscalev2.HorizontalPodAutoscalerSpec `json:"hpAutoscaler,omitempty"`

	// TopologySpreadConstraints Kubernetes Native `topologySpreadConstraints` specification.
	// +optional
	TopologySpreadConstraints []v1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`

	// VolumeClaimTemplates Kubernetes Native `volumeClaimTemplates` specification.
	// +optional
	VolumeClaimTemplates []v1.PersistentVolumeClaim `json:"volumeClaimTemplates,omitempty"`

	// VolumeMounts Kubernetes Native `volumeMounts` specification.
	// +optional
	VolumeMounts []v1.VolumeMount `json:"volumeMounts,omitempty"`

	// Volumes Kubernetes Native `volumes` specification.
	// +optional
	Volumes []v1.Volume `json:"volumes,omitempty"`

	// Operator deploys the sidecar container based on these properties.
	// +optional
	AdditionalContainer []AdditionalContainer `json:"additionalContainer,omitempty"`

	// ServiceAccountName Kubernetes native `serviceAccountName` specification.
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty"`

	// Dynamic Configurations for Druid. Applied through the dynamic configuration API.
	// +optional
	DynamicConfig runtime.RawExtension `json:"dynamicConfig,omitempty"`

	// See v1.DNSPolicy for more details.
	// +optional
	DNSPolicy v1.DNSPolicy `json:"dnsPolicy,omitempty" protobuf:"bytes,6,opt,name=dnsPolicy,casttype=DNSPolicy"`

	// See v1.PodDNSConfig for more details.
	// +optional
	DNSConfig *v1.PodDNSConfig `json:"dnsConfig,omitempty" protobuf:"bytes,26,opt,name=dnsConfig"`
}

// ZookeeperSpec IGNORED (Future API): In order to make Druid dependency setup extensible from within Druid operator.
type ZookeeperSpec struct {
	Type string          `json:"type"`
	Spec json.RawMessage `json:"spec"`
}

// MetadataStoreSpec IGNORED (Future API): In order to make Druid dependency setup extensible from within Druid operator.
type MetadataStoreSpec struct {
	Type string          `json:"type"`
	Spec json.RawMessage `json:"spec"`
}

// DeepStorageSpec IGNORED (Future API): In order to make Druid dependency setup extensible from within Druid operator.
type DeepStorageSpec struct {
	Type string          `json:"type"`
	Spec json.RawMessage `json:"spec"`
}

// These are valid conditions of a druid Node.
const (
	// DruidClusterReady indicates the underlying druid objects is fully deployed.
	// Underlying pods are able to handle requests.
	DruidClusterReady DruidNodeConditionType = "DruidClusterReady"

	// DruidNodeRollingUpdate means that Druid Node is rolling update.
	DruidNodeRollingUpdate DruidNodeConditionType = "DruidNodeRollingUpdate"

	// DruidNodeErrorState indicates the DruidNode is in an error state.
	DruidNodeErrorState DruidNodeConditionType = "DruidNodeErrorState"
)

type DruidNodeConditionType string

type DruidNodeTypeStatus struct {
	DruidNode                string                 `json:"druidNode,omitempty"`
	DruidNodeConditionStatus v1.ConditionStatus     `json:"druidNodeConditionStatus,omitempty"`
	DruidNodeConditionType   DruidNodeConditionType `json:"druidNodeConditionType,omitempty"`
	Reason                   string                 `json:"reason,omitempty"`
}

// DruidClusterStatus Defines the observed state of Druid.
type DruidClusterStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	DruidNodeStatus        DruidNodeTypeStatus `json:"druidNodeStatus,omitempty"`
	StatefulSets           []string            `json:"statefulSets,omitempty"`
	Deployments            []string            `json:"deployments,omitempty"`
	Services               []string            `json:"services,omitempty"`
	ConfigMaps             []string            `json:"configMaps,omitempty"`
	PodDisruptionBudgets   []string            `json:"podDisruptionBudgets,omitempty"`
	Ingress                []string            `json:"ingress,omitempty"`
	HPAutoScalers          []string            `json:"hpAutoscalers,omitempty"`
	Pods                   []string            `json:"pods,omitempty"`
	PersistentVolumeClaims []string            `json:"persistentVolumeClaims,omitempty"`
}

// Druid is the Schema for the druids API.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type Druid struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DruidSpec          `json:"spec"`
	Status DruidClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DruidList contains a list of Druid
type DruidList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Druid `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Druid{}, &DruidList{})
}
