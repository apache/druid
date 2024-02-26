---
id: k8s-jobs
title: "MM-less Druid in K8s"
---

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

Apache Druid Extension to enable using Kubernetes for launching and managing tasks instead of the Middle Managers.  This extension allows you to launch tasks as kubernetes jobs removing the need for your middle manager.  

Consider this an [EXPERIMENTAL](../experimental.md) feature mostly because it has not been tested yet on a wide variety of long-running Druid clusters.

## How it works

The K8s extension builds a pod spec for each task using the specified pod adapter. All jobs are natively restorable, they are decoupled from the Druid deployment, thus restarting pods or doing upgrades has no affect on tasks in flight.  They will continue to run and when the overlord comes back up it will start tracking them again.  


## Configuration

To use this extension please make sure to  [include](../../configuration/extensions.md#loading-extensions)`druid-kubernetes-overlord-extensions` in the extensions load list for your overlord process.

The extension uses `druid.indexer.runner.capacity` to limit the number of k8s jobs in flight. A good initial value for this would be the sum of the total task slots of all the middle managers you were running before switching to K8s based ingestion. The K8s task runner uses one thread per Job that is created, so setting this number too large can cause memory issues on the overlord. Additionally set the variable `druid.indexer.runner.namespace` to the namespace in which you are running druid.

Other configurations required are:
`druid.indexer.runner.type: k8s` and `druid.indexer.task.encapsulatedTask: true`

## Pod Adapters
The logic defining how the pod template is built for your Kubernetes Job depends on which pod adapter you have specified.

### Overlord Single Container Pod Adapter/Overlord Multi Container Pod Adapter
The overlord single container pod adapter takes the podSpec of your `Overlord` pod and creates a kubernetes job from this podSpec.  This is the default pod adapter implementation, to explicitly enable it you can specify the runtime property `druid.indexer.runner.k8s.adapter.type: overlordSingleContainer`

The overlord multi container pod adapter takes the podSpec of your `Overlord` pod and creates a kubernetes job from this podSpec.  It uses kubexit to manage dependency ordering between the main container that runs your druid peon and other sidecars defined in the `Overlord` pod spec. Thus if you have sidecars such as Splunk or Istio it will be able to handle them. To enable this pod adapter you can specify the runtime property `druid.indexer.runner.k8s.adapter.type: overlordMultiContainer` 

For the sidecar support to work for the multi container pod adapter, your entry point / command in docker must be explicitly defined your spec.

You can't have something like this:
Dockerfile:
``` ENTRYPOINT: ["foo.sh"] ```

and in your sidecar specs:
``` container:
        name: foo
        args: 
           - arg1
           - arg2 
```

That will not work, because we cannot decipher what your command is, the extension needs to know it explicitly.
**Even for sidecars like Istio which are dynamically created by the service mesh, this needs to happen.*

Instead do the following:
You can keep your Dockerfile the same but you must have a sidecar spec like so:
``` container:
        name: foo
        command: foo.sh
        args: 
           - arg1
           - arg2 
```

For both of these adapters, you can add optional labels to your K8s jobs / pods if you need them by using the following configuration:
`druid.indexer.runner.labels: '{"key":"value"}'`
Annotations are the same with:
`druid.indexer.runner.annotations: '{"key":"value"}'`

All other configurations you had for the middle manager tasks must be moved under the overlord with one caveat, you must specify javaOpts as an array:
`druid.indexer.runner.javaOptsArray`, `druid.indexer.runner.javaOpts` is no longer supported.

If you are running without a middle manager you need to also use `druid.processing.intermediaryData.storage.type=deepstore`

### Custom Template Pod Adapter
The custom template pod adapter allows you to specify a pod template file per task type for more flexibility on how to define your pods. This adapter expects a [Pod Template](https://kubernetes.io/docs/concepts/workloads/pods/#pod-templates) to be available on the overlord's file system. This pod template is used as the base of the pod spec for the Kubernetes Job. You can override things like labels, environment variables, resources, annotation, or even the base image with this template. To enable this pod adapter you can specify the runtime property `druid.indexer.runner.k8s.adapter.type: customTemplateAdapter`

The base pod template must be specified as the runtime property `druid.indexer.runner.k8s.podTemplate.base: /path/to/basePodSpec.yaml`

Task specific pod templates can be specified as the runtime property `druid.indexer.runner.k8s.podTemplate.{taskType}: /path/to/taskSpecificPodSpec.yaml` where {taskType} is the name of the task type i.e `index_parallel`.

If you are trying to use the default image's environment variable parsing feature to set runtime properties, you need to add a extra escape underscore when specifying pod templates.
e.g. set the environment variable `druid_indexer_runner_k8s_podTemplate_index__parallel` when setting `druid.indxer.runner.k8s.podTemplate.index_parallel`

The following is an example Pod Template that uses the regular druid docker image.
```
apiVersion: "v1"
kind: "PodTemplate"
template:
  metadata:
    annotations:
      sidecar.istio.io/proxyCPU: "512m" # to handle a injected istio sidecar
    labels:
      app.kubernetes.io/name: "druid-realtime-backend"
  spec:
    affinity: {}
    containers:
    - command:
        - sh
        - -c
        - |
          /peon.sh /druid/data 1
      env:
      - name: CUSTOM_ENV_VARIABLE
        value: "hello"
      image: apache/druid:{{DRUIDVERSION}}
      name: main
      ports:
      - containerPort: 8091
        name: druid-tls-port
        protocol: TCP
      - containerPort: 8100
        name: druid-port
        protocol: TCP
      resources:
        limits:
          cpu: "1"
          memory: 2400M
        requests:
          cpu: "1"
          memory: 2400M
      volumeMounts:
      - mountPath: /opt/druid/conf/druid/cluster/master/coordinator-overlord # runtime props are still mounted in this location because that's where peon.sh looks for configs
        name: nodetype-config-volume
        readOnly: true
      - mountPath: /druid/data
        name: data-volume
      - mountPath: /druid/deepstorage
        name: deepstorage-volume
    restartPolicy: "Never"
    securityContext:
      fsGroup: 1000
      runAsGroup: 1000
      runAsUser: 1000
    tolerations:
    - effect: NoExecute
      key: node.kubernetes.io/not-ready
      operator: Exists
      tolerationSeconds: 300
    - effect: NoExecute
      key: node.kubernetes.io/unreachable
      operator: Exists
      tolerationSeconds: 300
    volumes:
    - configMap:
        defaultMode: 420
        name: druid-tiny-cluster-peons-config
      name: nodetype-config-volume
    - emptyDir: {}
      name: data-volume
    - emptyDir: {}
      name: deepstorage-volume
```

The below runtime properties need to be passed to the Job's peon process.

```
druid.port=8100 (what port the peon should run on)
druid.peon.mode=remote
druid.service=druid/peon (for metrics reporting)
druid.indexer.task.baseTaskDir=/druid/data (this should match the argument to the ./peon.sh run command in the PodTemplate)
druid.indexer.runner.type=k8s
druid.indexer.task.encapsulatedTask=true
```

Any runtime property or JVM config used by the peon process can also be passed. E.G. below is a example of a ConfigMap that can be used to generate the `nodetype-config-volume` mount in the above template.
```
kind: ConfigMap
metadata:
    name: druid-tiny-cluster-peons-config
    namespace: default
apiVersion: v1
data:
    jvm.config: |-
        -server
        -XX:MaxDirectMemorySize=1000M
        -Duser.timezone=UTC
        -Dfile.encoding=UTF-8
        -Dlog4j.debug
        -Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager
        -Djava.io.tmpdir=/druid/data
        -Xmx1024M
        -Xms1024M
    log4j2.xml: |-
        <?xml version="1.0" encoding="UTF-8" ?>
        <Configuration status="WARN">
            <Appenders>
                <Console name="Console" target="SYSTEM_OUT">
                    <PatternLayout pattern="%d{ISO8601} %p [%t] %c - %m%n"/>
                </Console>
            </Appenders>
            <Loggers>
                <Root level="info">
                    <AppenderRef ref="Console"/>
                </Root>
            </Loggers>
        </Configuration>
    runtime.properties: |
        druid.port=8100
        druid.service=druid/peon
        druid.server.http.numThreads=5
        druid.indexer.task.baseTaskDir=/druid/data
        druid.indexer.runner.type=k8s
        druid.peon.mode=remote
        druid.indexer.task.encapsulatedTask=true
```

### Properties
|Property| Possible Values | Description                                                                                                                                                                                                                                      |Default|required|
|--------|-----------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------|--------|
|`druid.indexer.runner.debugJobs`| `boolean`       | Clean up K8s jobs after tasks complete.                                                                                                                                                                                                          |False|No|
|`druid.indexer.runner.sidecarSupport`| `boolean`       | Deprecated, specify adapter type as runtime property `druid.indexer.runner.k8s.adapter.type: overlordMultiContainer` instead. If your overlord pod has sidecars, this will attempt to start the task with the same sidecars as the overlord pod. |False|No|
|`druid.indexer.runner.primaryContainerName`| `String`        | If running with sidecars, the `primaryContainerName` should be that of your druid container like `druid-overlord`.                                                                                                                               |First container in `podSpec` list|No|
|`druid.indexer.runner.kubexitImage`| `String`        | Used kubexit project to help shutdown sidecars when the main pod completes.  Otherwise jobs with sidecars never terminate.                                                                                                                       |karlkfi/kubexit:latest|No|
|`druid.indexer.runner.disableClientProxy`| `boolean`       | Use this if you have a global http(s) proxy and you wish to bypass it.                                                                                                                                                                           |false|No|
|`druid.indexer.runner.maxTaskDuration`| `Duration`      | Max time a task is allowed to run for before getting killed                                                                                                                                                                                      |`PT4H`|No|
|`druid.indexer.runner.taskCleanupDelay`| `Duration`      | How long do jobs stay around before getting reaped from K8s                                                                                                                                                                                      |`P2D`|No|
|`druid.indexer.runner.taskCleanupInterval`| `Duration`      | How often to check for jobs to be reaped                                                                                                                                                                                                         |`PT10M`|No|
|`druid.indexer.runner.K8sjobLaunchTimeout`| `Duration`      | How long to wait to launch a K8s task before marking it as failed, on a resource constrained cluster it may take some time.                                                                                                                      |`PT1H`|No|
|`druid.indexer.runner.javaOptsArray`| `JsonArray`     | java opts for the task.                                                                                                                                                                                                                          |`-Xmx1g`|No|
|`druid.indexer.runner.labels`| `JsonObject`    | Additional labels you want to add to peon pod                                                                                                                                                                                                    |`{}`|No|
|`druid.indexer.runner.annotations`| `JsonObject`    | Additional annotations you want to add to peon pod                                                                                                                                                                                               |`{}`|No|
|`druid.indexer.runner.peonMonitors`| `JsonArray`     | Overrides `druid.monitoring.monitors`. Use this property if you don't want to inherit monitors from the Overlord.                                                                                                                                |`[]`|No|
|`druid.indexer.runner.graceTerminationPeriodSeconds`| `Long`          | Number of seconds you want to wait after a sigterm for container lifecycle hooks to complete.  Keep at a smaller value if you want tasks to hold locks for shorter periods.                                                                      |`PT30S` (K8s default)|No|
|`druid.indexer.runner.capacity`| `Integer`       | Number of concurrent jobs that can be sent to Kubernetes.                                                                                                                                                                                        |`2147483647`|No|

### Metrics added

|Metric|Description|Dimensions|Normal value|
|------|-----------|----------|------------|
| `k8s/peon/startup/time` | Metric indicating the milliseconds for peon pod to startup. | `dataSource`, `taskId`, `taskType`, `groupId`, `taskStatus`, `tags` |Varies|

### Gotchas

- All Druid Pods belonging to one Druid cluster must be inside the same Kubernetes namespace.

- You must have a role binding for the overlord's service account that provides the needed permissions for interacting with Kubernetes. An example spec could be:
```
kind: Role
apiVersion: rbac.authorization.k8s.io/v1
metadata:
  namespace: <druid-namespace>
  name: druid-k8s-task-scheduler
rules:
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["get", "watch", "list", "delete", "create"]
  - apiGroups: [""]
    resources: ["pods", "pods/log"]
    verbs: ["get", "watch", "list", "delete", "create"]
---
kind: RoleBinding
apiVersion: rbac.authorization.k8s.io/v1
metadata:
  name: druid-k8s-binding
  namespace: <druid-namespace>
subjects:
  - kind: ServiceAccount
    name: <druid-overlord-k8s-service-account>
    namespace: <druid-namespace>
roleRef:
  kind: Role
  name: druid-k8s-task-scheduler
  apiGroup: rbac.authorization.k8s.io
```

## Migration/Kubernetes and Worker Task Runner
If you are running a cluster with tasks running on middle managers or indexers and want to do a zero downtime migration to mm-less ingestion, the mm-less ingestion system is capable of running in migration mode by reading tasks from middle managers/indexers and Kubernetes and writing tasks to either middle managers or to Kubernetes.

To do this, set the following property.
`druid.indexer.runner.type: k8sAndWorker` (instead of `druid.indexer.runner.type: k8s`)

### Additional Configurations

|Property| Possible Values |Description|Default|required|
|--------|-----------------|-----------|-------|--------|
|`druid.indexer.runner.k8sAndWorker.runnerStrategy.type`| `String` (e.g., `k8s`, `worker`, `taskType`)| Defines the strategy for task runner selection. |`k8s`|No|
|`druid.indexer.runner.k8sAndWorker.runnerStrategy.workerType`| `String` (e.g., `httpRemote`, `remote`)| Specifies the variant of the worker task runner to be utilized.|`httpRemote`|No|
| **For `taskType` runner strategy:**|||||
|`druid.indexer.runner.k8sAndWorker.runnerStrategy.taskType.default`| `String` (e.g., `k8s`, `worker`) | Specifies the default runner to use if no overrides apply. This setting ensures there is always a fallback runner available.|None|No|
|`druid.indexer.runner.k8sAndWorker.runnerStrategy.taskType.overrides`| `JsonObject`(e.g., `{"index_kafka": "worker"}`)| Defines task-specific overrides for runner types. Each entry sets a task type to a specific runner, allowing fine control. |`{}`|No|

