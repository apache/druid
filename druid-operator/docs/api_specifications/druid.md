<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

<h1>Druid API reference</h1>
<p>Packages:</p>
<ul class="simple">
<li>
<a href="#druid.apache.org%2fv1alpha1">druid.apache.org/v1alpha1</a>
</li>
</ul>
<h2 id="druid.apache.org/v1alpha1">druid.apache.org/v1alpha1</h2>
Resource Types:
<ul class="simple"></ul>
<h3 id="druid.apache.org/v1alpha1.AdditionalContainer">AdditionalContainer
</h3>
<p>
(<em>Appears on:</em>
<a href="#druid.apache.org/v1alpha1.DruidNodeSpec">DruidNodeSpec</a>, 
<a href="#druid.apache.org/v1alpha1.DruidSpec">DruidSpec</a>)
</p>
<p>AdditionalContainer defines additional sidecar containers to be deployed with the <code>Druid</code> pods.
(will be part of Kubernetes native in the future:
<a href="https://github.com/kubernetes/enhancements/blob/master/keps/sig-node/753-sidecar-containers/README.md#summary)">https://github.com/kubernetes/enhancements/blob/master/keps/sig-node/753-sidecar-containers/README.md#summary)</a>.</p>
<div class="md-typeset__scrollwrap">
<div class="md-typeset__table">
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>runAsInit</code><br>
<em>
bool
</em>
</td>
<td>
<em>(Optional)</em>
<p>RunAsInit indicate whether this should be an init container.</p>
</td>
</tr>
<tr>
<td>
<code>image</code><br>
<em>
string
</em>
</td>
<td>
<p>Image Image of the additional container.</p>
</td>
</tr>
<tr>
<td>
<code>containerName</code><br>
<em>
string
</em>
</td>
<td>
<p>ContainerName name of the additional container.</p>
</td>
</tr>
<tr>
<td>
<code>command</code><br>
<em>
[]string
</em>
</td>
<td>
<p>Command command for the additional container.</p>
</td>
</tr>
<tr>
<td>
<code>imagePullPolicy</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#pullpolicy-v1-core">
Kubernetes core/v1.PullPolicy
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>ImagePullPolicy If not present, will be taken from top level spec.</p>
</td>
</tr>
<tr>
<td>
<code>args</code><br>
<em>
[]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>Args Arguments to call the command.</p>
</td>
</tr>
<tr>
<td>
<code>securityContext</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#securitycontext-v1-core">
Kubernetes core/v1.SecurityContext
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>ContainerSecurityContext If not present, will be taken from top level pod.</p>
</td>
</tr>
<tr>
<td>
<code>resources</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#resourcerequirements-v1-core">
Kubernetes core/v1.ResourceRequirements
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Resources Kubernetes Native <code>resources</code> specification.</p>
</td>
</tr>
<tr>
<td>
<code>volumeMounts</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#volumemount-v1-core">
[]Kubernetes core/v1.VolumeMount
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>VolumeMounts Kubernetes Native <code>VolumeMount</code> specification.</p>
</td>
</tr>
<tr>
<td>
<code>env</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#envvar-v1-core">
[]Kubernetes core/v1.EnvVar
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Env Environment variables for the additional container.</p>
</td>
</tr>
<tr>
<td>
<code>envFrom</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#envfromsource-v1-core">
[]Kubernetes core/v1.EnvFromSource
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>EnvFrom Extra environment variables from remote source (ConfigMaps, Secrets&hellip;).</p>
</td>
</tr>
</tbody>
</table>
</div>
</div>
<h3 id="druid.apache.org/v1alpha1.DeepStorageSpec">DeepStorageSpec
</h3>
<p>
(<em>Appears on:</em>
<a href="#druid.apache.org/v1alpha1.DruidSpec">DruidSpec</a>)
</p>
<p>DeepStorageSpec IGNORED (Future API): In order to make Druid dependency setup extensible from within Druid operator.</p>
<div class="md-typeset__scrollwrap">
<div class="md-typeset__table">
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>type</code><br>
<em>
string
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>spec</code><br>
<em>
encoding/json.RawMessage
</em>
</td>
<td>
<br/>
<br/>
<table>
</table>
</td>
</tr>
</tbody>
</table>
</div>
</div>
<h3 id="druid.apache.org/v1alpha1.Druid">Druid
</h3>
<p>Druid is the Schema for the druids API.</p>
<div class="md-typeset__scrollwrap">
<div class="md-typeset__table">
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>metadata</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#objectmeta-v1-meta">
Kubernetes meta/v1.ObjectMeta
</a>
</em>
</td>
<td>
Refer to the Kubernetes API documentation for the fields of the
<code>metadata</code> field.
</td>
</tr>
<tr>
<td>
<code>spec</code><br>
<em>
<a href="#druid.apache.org/v1alpha1.DruidSpec">
DruidSpec
</a>
</em>
</td>
<td>
<br/>
<br/>
<table>
<tr>
<td>
<code>ignored</code><br>
<em>
bool
</em>
</td>
<td>
<em>(Optional)</em>
<p>Ignored is now deprecated API. In order to avoid reconciliation of objects use the
<code>druid.apache.org/ignored: &quot;true&quot;</code> annotation.</p>
</td>
</tr>
<tr>
<td>
<code>common.runtime.properties</code><br>
<em>
string
</em>
</td>
<td>
<p>CommonRuntimeProperties Content fo the <code>common.runtime.properties</code> configuration file.</p>
</td>
</tr>
<tr>
<td>
<code>extraCommonConfig</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#*k8s.io/api/core/v1.objectreference--">
[]*k8s.io/api/core/v1.ObjectReference
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>ExtraCommonConfig References to ConfigMaps holding more configuration files to mount to the
common configuration path.</p>
</td>
</tr>
<tr>
<td>
<code>forceDeleteStsPodOnError</code><br>
<em>
bool
</em>
</td>
<td>
<em>(Optional)</em>
<p>ForceDeleteStsPodOnError Delete the StatefulSet&rsquo;s pods if the StatefulSet is set to ordered ready.
issue: <a href="https://github.com/kubernetes/kubernetes/issues/67250">https://github.com/kubernetes/kubernetes/issues/67250</a>
doc: <a href="https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#forced-rollback">https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#forced-rollback</a></p>
</td>
</tr>
<tr>
<td>
<code>scalePvcSts</code><br>
<em>
bool
</em>
</td>
<td>
<em>(Optional)</em>
<p>ScalePvcSts When enabled, operator will allow volume expansion of StatefulSet&rsquo;s PVCs.</p>
</td>
</tr>
<tr>
<td>
<code>commonConfigMountPath</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>CommonConfigMountPath In-container directory to mount the Druid common configuration</p>
</td>
</tr>
<tr>
<td>
<code>disablePVCDeletionFinalizer</code><br>
<em>
bool
</em>
</td>
<td>
<em>(Optional)</em>
<p>DisablePVCDeletionFinalizer Whether PVCs shall be deleted on the deletion of the Druid cluster.</p>
</td>
</tr>
<tr>
<td>
<code>deleteOrphanPvc</code><br>
<em>
bool
</em>
</td>
<td>
<em>(Optional)</em>
<p>DeleteOrphanPvc Orphaned (unmounted PVCs) shall be cleaned up by the operator.</p>
</td>
</tr>
<tr>
<td>
<code>startScript</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>StartScript Path to Druid&rsquo;s start script to be run on start.</p>
</td>
</tr>
<tr>
<td>
<code>image</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>Image Required here or at the NodeSpec level.</p>
</td>
</tr>
<tr>
<td>
<code>serviceAccount</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>ServiceAccount</p>
</td>
</tr>
<tr>
<td>
<code>imagePullSecrets</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#localobjectreference-v1-core">
[]Kubernetes core/v1.LocalObjectReference
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>ImagePullSecrets</p>
</td>
</tr>
<tr>
<td>
<code>imagePullPolicy</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#pullpolicy-v1-core">
Kubernetes core/v1.PullPolicy
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>ImagePullPolicy</p>
</td>
</tr>
<tr>
<td>
<code>env</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#envvar-v1-core">
[]Kubernetes core/v1.EnvVar
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Env Environment variables for druid containers.</p>
</td>
</tr>
<tr>
<td>
<code>envFrom</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#envfromsource-v1-core">
[]Kubernetes core/v1.EnvFromSource
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>EnvFrom Extra environment variables from remote source (ConfigMaps, Secrets&hellip;).</p>
</td>
</tr>
<tr>
<td>
<code>jvm.options</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>JvmOptions Contents of the shared <code>jvm.options</code> configuration file for druid JVM processes.</p>
</td>
</tr>
<tr>
<td>
<code>log4j.config</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>Log4jConfig contents <code>log4j.config</code> configuration file.</p>
</td>
</tr>
<tr>
<td>
<code>securityContext</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#podsecuritycontext-v1-core">
Kubernetes core/v1.PodSecurityContext
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>PodSecurityContext</p>
</td>
</tr>
<tr>
<td>
<code>containerSecurityContext</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#securitycontext-v1-core">
Kubernetes core/v1.SecurityContext
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>ContainerSecurityContext</p>
</td>
</tr>
<tr>
<td>
<code>volumeClaimTemplates</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#persistentvolumeclaim-v1-core">
[]Kubernetes core/v1.PersistentVolumeClaim
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>VolumeClaimTemplates Kubernetes Native <code>VolumeClaimTemplate</code> specification.</p>
</td>
</tr>
<tr>
<td>
<code>volumeMounts</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#volumemount-v1-core">
[]Kubernetes core/v1.VolumeMount
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>VolumeMounts Kubernetes Native <code>VolumeMount</code> specification.</p>
</td>
</tr>
<tr>
<td>
<code>volumes</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#volume-v1-core">
[]Kubernetes core/v1.Volume
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Volumes Kubernetes Native <code>Volumes</code> specification.</p>
</td>
</tr>
<tr>
<td>
<code>podAnnotations</code><br>
<em>
map[string]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>PodAnnotations Custom annotations to be populated in <code>Druid</code> pods.</p>
</td>
</tr>
<tr>
<td>
<code>workloadAnnotations</code><br>
<em>
map[string]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>WorkloadAnnotations annotations to be populated in StatefulSet or Deployment spec.
if the same key is specified at both the DruidNodeSpec level and DruidSpec level, the DruidNodeSpec WorkloadAnnotations will take precedence.</p>
</td>
</tr>
<tr>
<td>
<code>podManagementPolicy</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#podmanagementpolicytype-v1-apps">
Kubernetes apps/v1.PodManagementPolicyType
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>PodManagementPolicy</p>
</td>
</tr>
<tr>
<td>
<code>podLabels</code><br>
<em>
map[string]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>PodLabels Custom labels to be populated in <code>Druid</code> pods.</p>
</td>
</tr>
<tr>
<td>
<code>priorityClassName</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>PriorityClassName Kubernetes native <code>priorityClassName</code> specification.</p>
</td>
</tr>
<tr>
<td>
<code>updateStrategy</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#statefulsetupdatestrategy-v1-apps">
Kubernetes apps/v1.StatefulSetUpdateStrategy
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>UpdateStrategy</p>
</td>
</tr>
<tr>
<td>
<code>livenessProbe</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#probe-v1-core">
Kubernetes core/v1.Probe
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>LivenessProbe
Port is set to <code>druid.port</code> if not specified with httpGet handler.</p>
</td>
</tr>
<tr>
<td>
<code>readinessProbe</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#probe-v1-core">
Kubernetes core/v1.Probe
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>ReadinessProbe
Port is set to <code>druid.port</code> if not specified with httpGet handler.</p>
</td>
</tr>
<tr>
<td>
<code>startUpProbe</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#probe-v1-core">
Kubernetes core/v1.Probe
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>StartUpProbe</p>
</td>
</tr>
<tr>
<td>
<code>services</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#service-v1-core">
[]Kubernetes core/v1.Service
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Services Kubernetes services to be created for each workload.</p>
</td>
</tr>
<tr>
<td>
<code>nodeSelector</code><br>
<em>
map[string]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>NodeSelector Kubernetes native <code>nodeSelector</code> specification.</p>
</td>
</tr>
<tr>
<td>
<code>tolerations</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#toleration-v1-core">
[]Kubernetes core/v1.Toleration
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Tolerations Kubernetes native <code>tolerations</code> specification.</p>
</td>
</tr>
<tr>
<td>
<code>affinity</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#affinity-v1-core">
Kubernetes core/v1.Affinity
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Affinity Kubernetes native <code>affinity</code> specification.</p>
</td>
</tr>
<tr>
<td>
<code>nodes</code><br>
<em>
<a href="#druid.apache.org/v1alpha1.DruidNodeSpec">
map[string]./apis/druid/v1alpha1.DruidNodeSpec
</a>
</em>
</td>
<td>
<p>Nodes a list of <code>Druid</code> Node types and their configurations.
<code>DruidSpec</code> is used to create Kubernetes workload specs. Many of the fields above can be overridden at the specific
<code>NodeSpec</code> level.</p>
</td>
</tr>
<tr>
<td>
<code>additionalContainer</code><br>
<em>
<a href="#druid.apache.org/v1alpha1.AdditionalContainer">
[]AdditionalContainer
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>AdditionalContainer defines additional sidecar containers to be deployed with the <code>Druid</code> pods.</p>
</td>
</tr>
<tr>
<td>
<code>rollingDeploy</code><br>
<em>
bool
</em>
</td>
<td>
<em>(Optional)</em>
<p>RollingDeploy Whether to deploy the components in a rolling update as described in the documentation:
<a href="https://druid.apache.org/docs/latest/operations/rolling-updates.html">https://druid.apache.org/docs/latest/operations/rolling-updates.html</a>
If set to true then operator checks the rollout status of previous version workloads before updating the next.
This will be done only for update actions.</p>
</td>
</tr>
<tr>
<td>
<code>defaultProbes</code><br>
<em>
bool
</em>
</td>
<td>
<em>(Optional)</em>
<p>DefaultProbes If set to true this will add default probes (liveness / readiness / startup) for all druid components
but it won&rsquo;t override existing probes</p>
</td>
</tr>
<tr>
<td>
<code>zookeeper</code><br>
<em>
<a href="#druid.apache.org/v1alpha1.ZookeeperSpec">
ZookeeperSpec
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Zookeeper IGNORED (Future API): In order to make Druid dependency setup extensible from within Druid operator.</p>
</td>
</tr>
<tr>
<td>
<code>metadataStore</code><br>
<em>
<a href="#druid.apache.org/v1alpha1.MetadataStoreSpec">
MetadataStoreSpec
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>MetadataStore IGNORED (Future API): In order to make Druid dependency setup extensible from within Druid operator.</p>
</td>
</tr>
<tr>
<td>
<code>deepStorage</code><br>
<em>
<a href="#druid.apache.org/v1alpha1.DeepStorageSpec">
DeepStorageSpec
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>DeepStorage IGNORED (Future API): In order to make Druid dependency setup extensible from within Druid operator.</p>
</td>
</tr>
<tr>
<td>
<code>metricDimensions.json</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>DimensionsMapPath Custom Dimension Map Path for statsd emitter.
stastd documentation is described in the following documentation:
<a href="https://druid.apache.org/docs/latest/development/extensions-contrib/statsd.html">https://druid.apache.org/docs/latest/development/extensions-contrib/statsd.html</a></p>
</td>
</tr>
<tr>
<td>
<code>hdfs-site.xml</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>HdfsSite Contents of <code>hdfs-site.xml</code>.</p>
</td>
</tr>
<tr>
<td>
<code>core-site.xml</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>CoreSite Contents of <code>core-site.xml</code>.</p>
</td>
</tr>
<tr>
<td>
<code>dynamicConfig</code><br>
<em>
k8s.io/apimachinery/pkg/runtime.RawExtension
</em>
</td>
<td>
<em>(Optional)</em>
<p>Dynamic Configurations for Druid. Applied through the dynamic configuration API.</p>
</td>
</tr>
<tr>
<td>
<code>auth</code><br>
<em>
github.com/datainfrahq/druid-operator/pkg/druidapi.Auth
</em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>dnsPolicy</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#dnspolicy-v1-core">
Kubernetes core/v1.DNSPolicy
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>See v1.DNSPolicy for more details.</p>
</td>
</tr>
<tr>
<td>
<code>dnsConfig</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#poddnsconfig-v1-core">
Kubernetes core/v1.PodDNSConfig
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>See v1.PodDNSConfig for more details.</p>
</td>
</tr>
</table>
</td>
</tr>
<tr>
<td>
<code>status</code><br>
<em>
<a href="#druid.apache.org/v1alpha1.DruidClusterStatus">
DruidClusterStatus
</a>
</em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
</div>
</div>
<h3 id="druid.apache.org/v1alpha1.DruidClusterStatus">DruidClusterStatus
</h3>
<p>
(<em>Appears on:</em>
<a href="#druid.apache.org/v1alpha1.Druid">Druid</a>)
</p>
<p>DruidClusterStatus Defines the observed state of Druid.</p>
<div class="md-typeset__scrollwrap">
<div class="md-typeset__table">
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>druidNodeStatus</code><br>
<em>
<a href="#druid.apache.org/v1alpha1.DruidNodeTypeStatus">
DruidNodeTypeStatus
</a>
</em>
</td>
<td>
<p>INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
Important: Run &ldquo;make&rdquo; to regenerate code after modifying this file</p>
</td>
</tr>
<tr>
<td>
<code>statefulSets</code><br>
<em>
[]string
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>deployments</code><br>
<em>
[]string
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>services</code><br>
<em>
[]string
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>configMaps</code><br>
<em>
[]string
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>podDisruptionBudgets</code><br>
<em>
[]string
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>ingress</code><br>
<em>
[]string
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>hpAutoscalers</code><br>
<em>
[]string
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>pods</code><br>
<em>
[]string
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>persistentVolumeClaims</code><br>
<em>
[]string
</em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
</div>
</div>
<h3 id="druid.apache.org/v1alpha1.DruidIngestion">DruidIngestion
</h3>
<p>Ingestion is the Schema for the Ingestion API</p>
<div class="md-typeset__scrollwrap">
<div class="md-typeset__table">
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>metadata</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#objectmeta-v1-meta">
Kubernetes meta/v1.ObjectMeta
</a>
</em>
</td>
<td>
Refer to the Kubernetes API documentation for the fields of the
<code>metadata</code> field.
</td>
</tr>
<tr>
<td>
<code>spec</code><br>
<em>
<a href="#druid.apache.org/v1alpha1.DruidIngestionSpec">
DruidIngestionSpec
</a>
</em>
</td>
<td>
<br/>
<br/>
<table>
<tr>
<td>
<code>suspend</code><br>
<em>
bool
</em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>druidCluster</code><br>
<em>
string
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>ingestion</code><br>
<em>
<a href="#druid.apache.org/v1alpha1.IngestionSpec">
IngestionSpec
</a>
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>auth</code><br>
<em>
github.com/datainfrahq/druid-operator/pkg/druidapi.Auth
</em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
</table>
</td>
</tr>
<tr>
<td>
<code>status</code><br>
<em>
<a href="#druid.apache.org/v1alpha1.DruidIngestionStatus">
DruidIngestionStatus
</a>
</em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
</div>
</div>
<h3 id="druid.apache.org/v1alpha1.DruidIngestionMethod">DruidIngestionMethod
(<code>string</code> alias)</h3>
<p>
(<em>Appears on:</em>
<a href="#druid.apache.org/v1alpha1.IngestionSpec">IngestionSpec</a>)
</p>
<h3 id="druid.apache.org/v1alpha1.DruidIngestionSpec">DruidIngestionSpec
</h3>
<p>
(<em>Appears on:</em>
<a href="#druid.apache.org/v1alpha1.DruidIngestion">DruidIngestion</a>)
</p>
<div class="md-typeset__scrollwrap">
<div class="md-typeset__table">
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>suspend</code><br>
<em>
bool
</em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>druidCluster</code><br>
<em>
string
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>ingestion</code><br>
<em>
<a href="#druid.apache.org/v1alpha1.IngestionSpec">
IngestionSpec
</a>
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>auth</code><br>
<em>
github.com/datainfrahq/druid-operator/pkg/druidapi.Auth
</em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
</tbody>
</table>
</div>
</div>
<h3 id="druid.apache.org/v1alpha1.DruidIngestionStatus">DruidIngestionStatus
</h3>
<p>
(<em>Appears on:</em>
<a href="#druid.apache.org/v1alpha1.DruidIngestion">DruidIngestion</a>)
</p>
<div class="md-typeset__scrollwrap">
<div class="md-typeset__table">
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>taskId</code><br>
<em>
string
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>type</code><br>
<em>
string
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>status</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#conditionstatus-v1-core">
Kubernetes core/v1.ConditionStatus
</a>
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>reason</code><br>
<em>
string
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>message</code><br>
<em>
string
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>lastUpdateTime</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#time-v1-meta">
Kubernetes meta/v1.Time
</a>
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>currentIngestionSpec.json</code><br>
<em>
string
</em>
</td>
<td>
<p>CurrentIngestionSpec is a string instead of RawExtension to maintain compatibility with existing
IngestionSpecs that are stored as JSON strings.</p>
</td>
</tr>
<tr>
<td>
<code>rules</code><br>
<em>
[]k8s.io/apimachinery/pkg/runtime.RawExtension
</em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
</div>
</div>
<h3 id="druid.apache.org/v1alpha1.DruidNodeConditionType">DruidNodeConditionType
(<code>string</code> alias)</h3>
<p>
(<em>Appears on:</em>
<a href="#druid.apache.org/v1alpha1.DruidNodeTypeStatus">DruidNodeTypeStatus</a>)
</p>
<h3 id="druid.apache.org/v1alpha1.DruidNodeSpec">DruidNodeSpec
</h3>
<p>
(<em>Appears on:</em>
<a href="#druid.apache.org/v1alpha1.DruidSpec">DruidSpec</a>)
</p>
<p>DruidNodeSpec Specification of <code>Druid</code> Node type and its configurations.
The key in following map can be arbitrary string that helps you identify resources for a specific nodeSpec.
It is used in the Kubernetes resources&rsquo; names, so it must be compliant with restrictions
placed on Kubernetes resource names:
<a href="https://kubernetes.io/docs/concepts/overview/working-with-objects/names/">https://kubernetes.io/docs/concepts/overview/working-with-objects/names/</a></p>
<div class="md-typeset__scrollwrap">
<div class="md-typeset__table">
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>nodeType</code><br>
<em>
string
</em>
</td>
<td>
<p>NodeDruid <code>Druid</code> node type.</p>
</td>
</tr>
<tr>
<td>
<code>druid.port</code><br>
<em>
int32
</em>
</td>
<td>
<p>DruidPort Used by the <code>Druid</code> process.</p>
</td>
</tr>
<tr>
<td>
<code>kind</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>Kind Can be StatefulSet or Deployment.
Note: volumeClaimTemplates are ignored when kind=Deployment</p>
</td>
</tr>
<tr>
<td>
<code>replicas</code><br>
<em>
int32
</em>
</td>
<td>
<em>(Optional)</em>
<p>Replicas replica of the workload</p>
</td>
</tr>
<tr>
<td>
<code>podLabels</code><br>
<em>
map[string]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>PodLabels Custom labels to be populated in the workload&rsquo;s pods.</p>
</td>
</tr>
<tr>
<td>
<code>podDisruptionBudgetSpec</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#poddisruptionbudgetspec-v1-policy">
Kubernetes policy/v1.PodDisruptionBudgetSpec
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>PodDisruptionBudgetSpec Kubernetes native <code>podDisruptionBudget</code> specification.</p>
</td>
</tr>
<tr>
<td>
<code>priorityClassName</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>PriorityClassName Kubernetes native <code>priorityClassName</code> specification.</p>
</td>
</tr>
<tr>
<td>
<code>runtime.properties</code><br>
<em>
string
</em>
</td>
<td>
<p>RuntimeProperties Additional runtime configuration for the specific workload.</p>
</td>
</tr>
<tr>
<td>
<code>jvm.options</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>JvmOptions overrides <code>JvmOptions</code> at top level.</p>
</td>
</tr>
<tr>
<td>
<code>extra.jvm.options</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>ExtraJvmOptions Appends extra jvm options to the <code>JvmOptions</code> field.</p>
</td>
</tr>
<tr>
<td>
<code>log4j.config</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>Log4jConfig Overrides <code>Log4jConfig</code> at top level.</p>
</td>
</tr>
<tr>
<td>
<code>nodeConfigMountPath</code><br>
<em>
string
</em>
</td>
<td>
<p>NodeConfigMountPath in-container directory to mount with runtime.properties, jvm.config, log4j2.xml files.</p>
</td>
</tr>
<tr>
<td>
<code>services</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#service-v1-core">
[]Kubernetes core/v1.Service
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Services Overrides services at top level.</p>
</td>
</tr>
<tr>
<td>
<code>tolerations</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#toleration-v1-core">
[]Kubernetes core/v1.Toleration
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Tolerations Kubernetes native <code>tolerations</code> specification.</p>
</td>
</tr>
<tr>
<td>
<code>affinity</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#affinity-v1-core">
Kubernetes core/v1.Affinity
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Affinity Kubernetes native <code>affinity</code> specification.</p>
</td>
</tr>
<tr>
<td>
<code>nodeSelector</code><br>
<em>
map[string]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>NodeSelector Kubernetes native <code>nodeSelector</code> specification.</p>
</td>
</tr>
<tr>
<td>
<code>terminationGracePeriodSeconds</code><br>
<em>
int64
</em>
</td>
<td>
<em>(Optional)</em>
<p>TerminationGracePeriodSeconds</p>
</td>
</tr>
<tr>
<td>
<code>ports</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#containerport-v1-core">
[]Kubernetes core/v1.ContainerPort
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Ports Extra ports to be added to pod spec.</p>
</td>
</tr>
<tr>
<td>
<code>image</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>Image Overrides image from top level, Required if no image specified at top level.</p>
</td>
</tr>
<tr>
<td>
<code>imagePullSecrets</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#localobjectreference-v1-core">
[]Kubernetes core/v1.LocalObjectReference
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>ImagePullSecrets Overrides <code>imagePullSecrets</code> from top level.</p>
</td>
</tr>
<tr>
<td>
<code>imagePullPolicy</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#pullpolicy-v1-core">
Kubernetes core/v1.PullPolicy
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>ImagePullPolicy Overrides <code>imagePullPolicy</code> from top level.</p>
</td>
</tr>
<tr>
<td>
<code>env</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#envvar-v1-core">
[]Kubernetes core/v1.EnvVar
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Env Environment variables for druid containers.</p>
</td>
</tr>
<tr>
<td>
<code>envFrom</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#envfromsource-v1-core">
[]Kubernetes core/v1.EnvFromSource
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>EnvFrom Extra environment variables from remote source (ConfigMaps, Secrets&hellip;).</p>
</td>
</tr>
<tr>
<td>
<code>resources</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#resourcerequirements-v1-core">
Kubernetes core/v1.ResourceRequirements
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Resources Kubernetes Native <code>resources</code> specification.</p>
</td>
</tr>
<tr>
<td>
<code>securityContext</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#podsecuritycontext-v1-core">
Kubernetes core/v1.PodSecurityContext
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>PodSecurityContext Overrides <code>securityContext</code> at top level.</p>
</td>
</tr>
<tr>
<td>
<code>containerSecurityContext</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#securitycontext-v1-core">
Kubernetes core/v1.SecurityContext
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>ContainerSecurityContext</p>
</td>
</tr>
<tr>
<td>
<code>podAnnotations</code><br>
<em>
map[string]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>PodAnnotations Custom annotation to be populated in the workload&rsquo;s pods.</p>
</td>
</tr>
<tr>
<td>
<code>podManagementPolicy</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#podmanagementpolicytype-v1-apps">
Kubernetes apps/v1.PodManagementPolicyType
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>PodManagementPolicy</p>
</td>
</tr>
<tr>
<td>
<code>maxSurge</code><br>
<em>
int32
</em>
</td>
<td>
<em>(Optional)</em>
<p>MaxSurge For Deployment object only.
Set to 25% by default.</p>
</td>
</tr>
<tr>
<td>
<code>maxUnavailable</code><br>
<em>
int32
</em>
</td>
<td>
<em>(Optional)</em>
<p>MaxUnavailable For deployment object only.
Set to 25% by default</p>
</td>
</tr>
<tr>
<td>
<code>updateStrategy</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#statefulsetupdatestrategy-v1-apps">
Kubernetes apps/v1.StatefulSetUpdateStrategy
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>UpdateStrategy</p>
</td>
</tr>
<tr>
<td>
<code>livenessProbe</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#probe-v1-core">
Kubernetes core/v1.Probe
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>LivenessProbe
Port is set to <code>druid.port</code> if not specified with httpGet handler.</p>
</td>
</tr>
<tr>
<td>
<code>readinessProbe</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#probe-v1-core">
Kubernetes core/v1.Probe
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>ReadinessProbe
Port is set to <code>druid.port</code> if not specified with httpGet handler.</p>
</td>
</tr>
<tr>
<td>
<code>startUpProbe</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#probe-v1-core">
Kubernetes core/v1.Probe
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>StartUpProbe</p>
</td>
</tr>
<tr>
<td>
<code>ingressAnnotations</code><br>
<em>
map[string]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>IngressAnnotations <code>Ingress</code> annotations to be populated in ingress spec.</p>
</td>
</tr>
<tr>
<td>
<code>workloadAnnotations</code><br>
<em>
map[string]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>WorkloadAnnotations annotations to be populated in StatefulSet or Deployment spec.</p>
</td>
</tr>
<tr>
<td>
<code>ingress</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#ingressspec-v1-networking">
Kubernetes networking/v1.IngressSpec
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Ingress Kubernetes Native <code>Ingress</code> specification.</p>
</td>
</tr>
<tr>
<td>
<code>persistentVolumeClaim</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#persistentvolumeclaim-v1-core">
[]Kubernetes core/v1.PersistentVolumeClaim
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>VolumeClaimTemplates Kubernetes Native <code>VolumeClaimTemplate</code> specification.</p>
</td>
</tr>
<tr>
<td>
<code>lifecycle</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#lifecycle-v1-core">
Kubernetes core/v1.Lifecycle
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Lifecycle</p>
</td>
</tr>
<tr>
<td>
<code>hpAutoscaler</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#horizontalpodautoscalerspec-v2-autoscaling">
Kubernetes autoscaling/v2.HorizontalPodAutoscalerSpec
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>HPAutoScaler Kubernetes Native <code>HorizontalPodAutoscaler</code> specification.</p>
</td>
</tr>
<tr>
<td>
<code>topologySpreadConstraints</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#topologyspreadconstraint-v1-core">
[]Kubernetes core/v1.TopologySpreadConstraint
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>TopologySpreadConstraints Kubernetes Native <code>topologySpreadConstraints</code> specification.</p>
</td>
</tr>
<tr>
<td>
<code>volumeClaimTemplates</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#persistentvolumeclaim-v1-core">
[]Kubernetes core/v1.PersistentVolumeClaim
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>VolumeClaimTemplates Kubernetes Native <code>volumeClaimTemplates</code> specification.</p>
</td>
</tr>
<tr>
<td>
<code>volumeMounts</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#volumemount-v1-core">
[]Kubernetes core/v1.VolumeMount
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>VolumeMounts Kubernetes Native <code>volumeMounts</code> specification.</p>
</td>
</tr>
<tr>
<td>
<code>volumes</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#volume-v1-core">
[]Kubernetes core/v1.Volume
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Volumes Kubernetes Native <code>volumes</code> specification.</p>
</td>
</tr>
<tr>
<td>
<code>additionalContainer</code><br>
<em>
<a href="#druid.apache.org/v1alpha1.AdditionalContainer">
[]AdditionalContainer
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Operator deploys the sidecar container based on these properties.</p>
</td>
</tr>
<tr>
<td>
<code>serviceAccountName</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>ServiceAccountName Kubernetes native <code>serviceAccountName</code> specification.</p>
</td>
</tr>
<tr>
<td>
<code>dynamicConfig</code><br>
<em>
k8s.io/apimachinery/pkg/runtime.RawExtension
</em>
</td>
<td>
<em>(Optional)</em>
<p>Dynamic Configurations for Druid. Applied through the dynamic configuration API.</p>
</td>
</tr>
<tr>
<td>
<code>dnsPolicy</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#dnspolicy-v1-core">
Kubernetes core/v1.DNSPolicy
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>See v1.DNSPolicy for more details.</p>
</td>
</tr>
<tr>
<td>
<code>dnsConfig</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#poddnsconfig-v1-core">
Kubernetes core/v1.PodDNSConfig
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>See v1.PodDNSConfig for more details.</p>
</td>
</tr>
</tbody>
</table>
</div>
</div>
<h3 id="druid.apache.org/v1alpha1.DruidNodeTypeStatus">DruidNodeTypeStatus
</h3>
<p>
(<em>Appears on:</em>
<a href="#druid.apache.org/v1alpha1.DruidClusterStatus">DruidClusterStatus</a>)
</p>
<div class="md-typeset__scrollwrap">
<div class="md-typeset__table">
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>druidNode</code><br>
<em>
string
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>druidNodeConditionStatus</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#conditionstatus-v1-core">
Kubernetes core/v1.ConditionStatus
</a>
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>druidNodeConditionType</code><br>
<em>
<a href="#druid.apache.org/v1alpha1.DruidNodeConditionType">
DruidNodeConditionType
</a>
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>reason</code><br>
<em>
string
</em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
</div>
</div>
<h3 id="druid.apache.org/v1alpha1.DruidSpec">DruidSpec
</h3>
<p>
(<em>Appears on:</em>
<a href="#druid.apache.org/v1alpha1.Druid">Druid</a>)
</p>
<p>DruidSpec defines the desired state of the Druid cluster.</p>
<div class="md-typeset__scrollwrap">
<div class="md-typeset__table">
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>ignored</code><br>
<em>
bool
</em>
</td>
<td>
<em>(Optional)</em>
<p>Ignored is now deprecated API. In order to avoid reconciliation of objects use the
<code>druid.apache.org/ignored: &quot;true&quot;</code> annotation.</p>
</td>
</tr>
<tr>
<td>
<code>common.runtime.properties</code><br>
<em>
string
</em>
</td>
<td>
<p>CommonRuntimeProperties Content fo the <code>common.runtime.properties</code> configuration file.</p>
</td>
</tr>
<tr>
<td>
<code>extraCommonConfig</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#*k8s.io/api/core/v1.objectreference--">
[]*k8s.io/api/core/v1.ObjectReference
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>ExtraCommonConfig References to ConfigMaps holding more configuration files to mount to the
common configuration path.</p>
</td>
</tr>
<tr>
<td>
<code>forceDeleteStsPodOnError</code><br>
<em>
bool
</em>
</td>
<td>
<em>(Optional)</em>
<p>ForceDeleteStsPodOnError Delete the StatefulSet&rsquo;s pods if the StatefulSet is set to ordered ready.
issue: <a href="https://github.com/kubernetes/kubernetes/issues/67250">https://github.com/kubernetes/kubernetes/issues/67250</a>
doc: <a href="https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#forced-rollback">https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#forced-rollback</a></p>
</td>
</tr>
<tr>
<td>
<code>scalePvcSts</code><br>
<em>
bool
</em>
</td>
<td>
<em>(Optional)</em>
<p>ScalePvcSts When enabled, operator will allow volume expansion of StatefulSet&rsquo;s PVCs.</p>
</td>
</tr>
<tr>
<td>
<code>commonConfigMountPath</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>CommonConfigMountPath In-container directory to mount the Druid common configuration</p>
</td>
</tr>
<tr>
<td>
<code>disablePVCDeletionFinalizer</code><br>
<em>
bool
</em>
</td>
<td>
<em>(Optional)</em>
<p>DisablePVCDeletionFinalizer Whether PVCs shall be deleted on the deletion of the Druid cluster.</p>
</td>
</tr>
<tr>
<td>
<code>deleteOrphanPvc</code><br>
<em>
bool
</em>
</td>
<td>
<em>(Optional)</em>
<p>DeleteOrphanPvc Orphaned (unmounted PVCs) shall be cleaned up by the operator.</p>
</td>
</tr>
<tr>
<td>
<code>startScript</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>StartScript Path to Druid&rsquo;s start script to be run on start.</p>
</td>
</tr>
<tr>
<td>
<code>image</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>Image Required here or at the NodeSpec level.</p>
</td>
</tr>
<tr>
<td>
<code>serviceAccount</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>ServiceAccount</p>
</td>
</tr>
<tr>
<td>
<code>imagePullSecrets</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#localobjectreference-v1-core">
[]Kubernetes core/v1.LocalObjectReference
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>ImagePullSecrets</p>
</td>
</tr>
<tr>
<td>
<code>imagePullPolicy</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#pullpolicy-v1-core">
Kubernetes core/v1.PullPolicy
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>ImagePullPolicy</p>
</td>
</tr>
<tr>
<td>
<code>env</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#envvar-v1-core">
[]Kubernetes core/v1.EnvVar
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Env Environment variables for druid containers.</p>
</td>
</tr>
<tr>
<td>
<code>envFrom</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#envfromsource-v1-core">
[]Kubernetes core/v1.EnvFromSource
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>EnvFrom Extra environment variables from remote source (ConfigMaps, Secrets&hellip;).</p>
</td>
</tr>
<tr>
<td>
<code>jvm.options</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>JvmOptions Contents of the shared <code>jvm.options</code> configuration file for druid JVM processes.</p>
</td>
</tr>
<tr>
<td>
<code>log4j.config</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>Log4jConfig contents <code>log4j.config</code> configuration file.</p>
</td>
</tr>
<tr>
<td>
<code>securityContext</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#podsecuritycontext-v1-core">
Kubernetes core/v1.PodSecurityContext
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>PodSecurityContext</p>
</td>
</tr>
<tr>
<td>
<code>containerSecurityContext</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#securitycontext-v1-core">
Kubernetes core/v1.SecurityContext
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>ContainerSecurityContext</p>
</td>
</tr>
<tr>
<td>
<code>volumeClaimTemplates</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#persistentvolumeclaim-v1-core">
[]Kubernetes core/v1.PersistentVolumeClaim
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>VolumeClaimTemplates Kubernetes Native <code>VolumeClaimTemplate</code> specification.</p>
</td>
</tr>
<tr>
<td>
<code>volumeMounts</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#volumemount-v1-core">
[]Kubernetes core/v1.VolumeMount
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>VolumeMounts Kubernetes Native <code>VolumeMount</code> specification.</p>
</td>
</tr>
<tr>
<td>
<code>volumes</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#volume-v1-core">
[]Kubernetes core/v1.Volume
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Volumes Kubernetes Native <code>Volumes</code> specification.</p>
</td>
</tr>
<tr>
<td>
<code>podAnnotations</code><br>
<em>
map[string]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>PodAnnotations Custom annotations to be populated in <code>Druid</code> pods.</p>
</td>
</tr>
<tr>
<td>
<code>workloadAnnotations</code><br>
<em>
map[string]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>WorkloadAnnotations annotations to be populated in StatefulSet or Deployment spec.
if the same key is specified at both the DruidNodeSpec level and DruidSpec level, the DruidNodeSpec WorkloadAnnotations will take precedence.</p>
</td>
</tr>
<tr>
<td>
<code>podManagementPolicy</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#podmanagementpolicytype-v1-apps">
Kubernetes apps/v1.PodManagementPolicyType
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>PodManagementPolicy</p>
</td>
</tr>
<tr>
<td>
<code>podLabels</code><br>
<em>
map[string]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>PodLabels Custom labels to be populated in <code>Druid</code> pods.</p>
</td>
</tr>
<tr>
<td>
<code>priorityClassName</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>PriorityClassName Kubernetes native <code>priorityClassName</code> specification.</p>
</td>
</tr>
<tr>
<td>
<code>updateStrategy</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#statefulsetupdatestrategy-v1-apps">
Kubernetes apps/v1.StatefulSetUpdateStrategy
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>UpdateStrategy</p>
</td>
</tr>
<tr>
<td>
<code>livenessProbe</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#probe-v1-core">
Kubernetes core/v1.Probe
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>LivenessProbe
Port is set to <code>druid.port</code> if not specified with httpGet handler.</p>
</td>
</tr>
<tr>
<td>
<code>readinessProbe</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#probe-v1-core">
Kubernetes core/v1.Probe
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>ReadinessProbe
Port is set to <code>druid.port</code> if not specified with httpGet handler.</p>
</td>
</tr>
<tr>
<td>
<code>startUpProbe</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#probe-v1-core">
Kubernetes core/v1.Probe
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>StartUpProbe</p>
</td>
</tr>
<tr>
<td>
<code>services</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#service-v1-core">
[]Kubernetes core/v1.Service
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Services Kubernetes services to be created for each workload.</p>
</td>
</tr>
<tr>
<td>
<code>nodeSelector</code><br>
<em>
map[string]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>NodeSelector Kubernetes native <code>nodeSelector</code> specification.</p>
</td>
</tr>
<tr>
<td>
<code>tolerations</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#toleration-v1-core">
[]Kubernetes core/v1.Toleration
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Tolerations Kubernetes native <code>tolerations</code> specification.</p>
</td>
</tr>
<tr>
<td>
<code>affinity</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#affinity-v1-core">
Kubernetes core/v1.Affinity
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Affinity Kubernetes native <code>affinity</code> specification.</p>
</td>
</tr>
<tr>
<td>
<code>nodes</code><br>
<em>
<a href="#druid.apache.org/v1alpha1.DruidNodeSpec">
map[string]./apis/druid/v1alpha1.DruidNodeSpec
</a>
</em>
</td>
<td>
<p>Nodes a list of <code>Druid</code> Node types and their configurations.
<code>DruidSpec</code> is used to create Kubernetes workload specs. Many of the fields above can be overridden at the specific
<code>NodeSpec</code> level.</p>
</td>
</tr>
<tr>
<td>
<code>additionalContainer</code><br>
<em>
<a href="#druid.apache.org/v1alpha1.AdditionalContainer">
[]AdditionalContainer
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>AdditionalContainer defines additional sidecar containers to be deployed with the <code>Druid</code> pods.</p>
</td>
</tr>
<tr>
<td>
<code>rollingDeploy</code><br>
<em>
bool
</em>
</td>
<td>
<em>(Optional)</em>
<p>RollingDeploy Whether to deploy the components in a rolling update as described in the documentation:
<a href="https://druid.apache.org/docs/latest/operations/rolling-updates.html">https://druid.apache.org/docs/latest/operations/rolling-updates.html</a>
If set to true then operator checks the rollout status of previous version workloads before updating the next.
This will be done only for update actions.</p>
</td>
</tr>
<tr>
<td>
<code>defaultProbes</code><br>
<em>
bool
</em>
</td>
<td>
<em>(Optional)</em>
<p>DefaultProbes If set to true this will add default probes (liveness / readiness / startup) for all druid components
but it won&rsquo;t override existing probes</p>
</td>
</tr>
<tr>
<td>
<code>zookeeper</code><br>
<em>
<a href="#druid.apache.org/v1alpha1.ZookeeperSpec">
ZookeeperSpec
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Zookeeper IGNORED (Future API): In order to make Druid dependency setup extensible from within Druid operator.</p>
</td>
</tr>
<tr>
<td>
<code>metadataStore</code><br>
<em>
<a href="#druid.apache.org/v1alpha1.MetadataStoreSpec">
MetadataStoreSpec
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>MetadataStore IGNORED (Future API): In order to make Druid dependency setup extensible from within Druid operator.</p>
</td>
</tr>
<tr>
<td>
<code>deepStorage</code><br>
<em>
<a href="#druid.apache.org/v1alpha1.DeepStorageSpec">
DeepStorageSpec
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>DeepStorage IGNORED (Future API): In order to make Druid dependency setup extensible from within Druid operator.</p>
</td>
</tr>
<tr>
<td>
<code>metricDimensions.json</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>DimensionsMapPath Custom Dimension Map Path for statsd emitter.
stastd documentation is described in the following documentation:
<a href="https://druid.apache.org/docs/latest/development/extensions-contrib/statsd.html">https://druid.apache.org/docs/latest/development/extensions-contrib/statsd.html</a></p>
</td>
</tr>
<tr>
<td>
<code>hdfs-site.xml</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>HdfsSite Contents of <code>hdfs-site.xml</code>.</p>
</td>
</tr>
<tr>
<td>
<code>core-site.xml</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>CoreSite Contents of <code>core-site.xml</code>.</p>
</td>
</tr>
<tr>
<td>
<code>dynamicConfig</code><br>
<em>
k8s.io/apimachinery/pkg/runtime.RawExtension
</em>
</td>
<td>
<em>(Optional)</em>
<p>Dynamic Configurations for Druid. Applied through the dynamic configuration API.</p>
</td>
</tr>
<tr>
<td>
<code>auth</code><br>
<em>
github.com/datainfrahq/druid-operator/pkg/druidapi.Auth
</em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>dnsPolicy</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#dnspolicy-v1-core">
Kubernetes core/v1.DNSPolicy
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>See v1.DNSPolicy for more details.</p>
</td>
</tr>
<tr>
<td>
<code>dnsConfig</code><br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#poddnsconfig-v1-core">
Kubernetes core/v1.PodDNSConfig
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>See v1.PodDNSConfig for more details.</p>
</td>
</tr>
</tbody>
</table>
</div>
</div>
<h3 id="druid.apache.org/v1alpha1.IngestionSpec">IngestionSpec
</h3>
<p>
(<em>Appears on:</em>
<a href="#druid.apache.org/v1alpha1.DruidIngestionSpec">DruidIngestionSpec</a>)
</p>
<div class="md-typeset__scrollwrap">
<div class="md-typeset__table">
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>type</code><br>
<em>
<a href="#druid.apache.org/v1alpha1.DruidIngestionMethod">
DruidIngestionMethod
</a>
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>spec</code><br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>Spec should be passed in as a JSON string.
Note: This field is planned for deprecation in favor of nativeSpec.</p>
<br/>
<br/>
<table>
</table>
</td>
</tr>
<tr>
<td>
<code>nativeSpec</code><br>
<em>
k8s.io/apimachinery/pkg/runtime.RawExtension
</em>
</td>
<td>
<em>(Optional)</em>
<p>nativeSpec allows the ingestion specification to be defined in a native Kubernetes format.
This is particularly useful for environment-specific configurations and will eventually
replace the JSON-based Spec field.
Note: Spec will be ignored if nativeSpec is provided.</p>
</td>
</tr>
<tr>
<td>
<code>compaction</code><br>
<em>
k8s.io/apimachinery/pkg/runtime.RawExtension
</em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>rules</code><br>
<em>
[]k8s.io/apimachinery/pkg/runtime.RawExtension
</em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
</tbody>
</table>
</div>
</div>
<h3 id="druid.apache.org/v1alpha1.MetadataStoreSpec">MetadataStoreSpec
</h3>
<p>
(<em>Appears on:</em>
<a href="#druid.apache.org/v1alpha1.DruidSpec">DruidSpec</a>)
</p>
<p>MetadataStoreSpec IGNORED (Future API): In order to make Druid dependency setup extensible from within Druid operator.</p>
<div class="md-typeset__scrollwrap">
<div class="md-typeset__table">
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>type</code><br>
<em>
string
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>spec</code><br>
<em>
encoding/json.RawMessage
</em>
</td>
<td>
<br/>
<br/>
<table>
</table>
</td>
</tr>
</tbody>
</table>
</div>
</div>
<h3 id="druid.apache.org/v1alpha1.ZookeeperSpec">ZookeeperSpec
</h3>
<p>
(<em>Appears on:</em>
<a href="#druid.apache.org/v1alpha1.DruidSpec">DruidSpec</a>)
</p>
<p>ZookeeperSpec IGNORED (Future API): In order to make Druid dependency setup extensible from within Druid operator.</p>
<div class="md-typeset__scrollwrap">
<div class="md-typeset__table">
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>type</code><br>
<em>
string
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>spec</code><br>
<em>
encoding/json.RawMessage
</em>
</td>
<td>
<br/>
<br/>
<table>
</table>
</td>
</tr>
</tbody>
</table>
</div>
</div>
<div class="admonition note">
<p class="last">This page was automatically generated with <code>gen-crd-api-reference-docs</code></p>
</div>
