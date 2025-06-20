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

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Apache Druid Extension to enable using Kubernetes for launching and managing tasks instead of the Middle Managers.  This extension allows you to launch tasks as kubernetes jobs removing the need for your middle manager.  

Consider this an [EXPERIMENTAL](../experimental.md) feature mostly because it has not been tested yet on a wide variety of long-running Druid clusters.

## How it works

The K8s extension builds a pod spec for each task using the specified pod adapter. All jobs are natively restorable, they are decoupled from the Druid deployment, thus restarting pods or doing upgrades has no effect on tasks in flight.  They will continue to run and when the overlord comes back up it will start tracking them again.  


## Configuration

To use this extension please make sure to [include](../../configuration/extensions.md#loading-extensions) `druid-kubernetes-overlord-extensions` in the extensions load list for your overlord process.

The extension uses `druid.indexer.runner.capacity` to limit the number of k8s jobs in flight. A good initial value for this would be the sum of the total task slots of all the middle managers you were running before switching to K8s based ingestion. The K8s task runner uses one thread per Job that is created, so setting this number too large can cause memory issues on the overlord. Additionally set the variable `druid.indexer.runner.namespace` to the namespace in which you are running druid.

Other configurations required are:
`druid.indexer.runner.type: k8s` and `druid.indexer.task.encapsulatedTask: true`

### Dynamic config

Druid operators can dynamically tune certain features within this extension. You don't need to restart the Overlord
service for these changes to take effect.

Druid can dynamically tune [pod template selection](#pod-template-selection), which allows you to configure the pod 
template based on the task to be run. To enable dynamic pod template selection, first configure the 
[custom template pod adapter](#custom-template-pod-adapter).

Use the following APIs to view and update the dynamic configuration for the Kubernetes task runner.

To use these APIs, ensure you have read and write permissions for the CONFIG resource type with the resource name
"CONFIG". For more information on permissions, see 
[User authentication and authorization](../../operations/security-user-auth.md#config).

#### Get dynamic configuration

Retrieves the current dynamic execution config for the Kubernetes task runner. 
Returns a JSON object with the dynamic configuration properties.

##### URL

`GET` `/druid/indexer/v1/k8s/taskrunner/executionconfig`

##### Responses

<Tabs>

<TabItem value="1" label="200 SUCCESS">


*Successfully retrieved dynamic configuration*

</TabItem>
</Tabs>

---

##### Sample request

<Tabs>

<TabItem value="2" label="cURL">

```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/k8s/taskrunner/executionconfig"
```
</TabItem>

<TabItem value="3" label="HTTP">

```HTTP
GET /druid/indexer/v1/k8s/taskrunner/executionconfig HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>
</Tabs>

##### Sample response

<details>
<summary>View the response</summary>

```json
{
  "type": "default",
  "podTemplateSelectStrategy":
  {
    "type": "selectorBased",
    "selectors": [
      {
        "selectionKey": "podSpec1",
        "context.tags": {
          "userProvidedTag": ["tag1", "tag2"]
        },
        "dataSource": ["wikipedia"]
      },
      {
        "selectionKey": "podSpec2",
        "type": ["index_kafka"]
      }
    ]
  }
}
```
</details>

#### Update dynamic configuration

Updates the dynamic configuration for the Kubernetes Task Runner

##### URL

`POST` `/druid/indexer/v1/k8s/taskrunner/executionconfig`

##### Header parameters

The endpoint supports the following optional header parameters to populate the `author` and `comment` fields in the configuration history.

* `X-Druid-Author`
  * Type: String
  * Author of the configuration change.
* `X-Druid-Comment`
  * Type: String
  * Description for the update.

##### Responses

<Tabs>

<TabItem value="4" label="200 SUCCESS">


*Successfully updated dynamic configuration*

</TabItem>
</Tabs>

---

##### Sample request

<Tabs>

<TabItem value="5" label="cURL">


```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/k8s/taskrunner/executionconfig" \
--header 'Content-Type: application/json' \
--data '{
  "type": "default",
  "podTemplateSelectStrategy":
  {
    "type": "selectorBased",
    "selectors": [
      {
        "selectionKey": "podSpec1",
        "context.tags":
        {
          "userProvidedTag": ["tag1", "tag2"]
        },
        "dataSource": ["wikipedia"]
      },
      {
        "selectionKey": "podSpec2",
        "type": ["index_kafka"]
      }
    ]
  }
}'
```

</TabItem>
<TabItem value="6" label="HTTP">


```HTTP
POST /druid/indexer/v1/k8s/taskrunner/executionconfig HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
Content-Type: application/json

{
  "type": "default",
  "podTemplateSelectStrategy":
  {
    "type": "selectorBased",
    "selectors": [
      {
        "selectionKey": "podSpec1",
        "context.tags":
        {
          "userProvidedTag": ["tag1", "tag2"]
        },
        "dataSource": ["wikipedia"]
      },
      {
        "selectionKey": "podSpec2",
        "type": ["index_kafka"]
      }
    ]
  }
}
```

</TabItem>
</Tabs>

##### Sample response

A successful request returns an HTTP `200 OK` message code and an empty response body.

#### Get dynamic configuration history

Retrieves the history of changes to Kubernetes task runner's dynamic execution config over an interval of time. Returns 
an empty array if there are no history records available.

##### URL

`GET` `/druid/indexer/v1/k8s/taskrunner/executionconfig/history`

##### Query parameters

The endpoint supports the following optional query parameters to filter results.

* `interval`
  * Type: String
  * Limit the results to the specified time interval in ISO 8601 format delimited with `/`. For example, `2023-07-13/2023-07-19`. The default interval is one week. You can change this period by setting `druid.audit.manager.auditHistoryMillis` in the `runtime.properties` file for the Coordinator.

* `count`
  * Type: Integer
  * Limit the number of results to the last `n` entries.

##### Responses

<Tabs>

<TabItem value="1" label="200 SUCCESS">


*Successfully retrieved dynamic configuration*

</TabItem>
</Tabs>

---

##### Sample request

<Tabs>

<TabItem value="2" label="cURL">


```shell
curl "http://ROUTER_IP:ROUTER_PORT/druid/indexer/v1/k8s/taskrunner/executionconfig/history"
```

</TabItem>
<TabItem value="3" label="HTTP">


```HTTP
GET /druid/indexer/v1/k8s/taskrunner/executionconfig/history HTTP/1.1
Host: http://ROUTER_IP:ROUTER_PORT
```

</TabItem>
</Tabs>

##### Sample response

<details>
<summary>View the response</summary>

```json
[
  {
    "key": "k8s.taskrunner.config",
    "type": "k8s.taskrunner.config",
    "auditInfo": {
      "author": "",
      "comment": "",
      "ip": "127.0.0.1"
    },
    "payload": "{\"type\": \"default\",\"podTemplateSelectStrategy\":{\"type\": \"taskType\"}",
    "auditTime": "2024-06-13T20:59:51.622Z"
  }
]
```
</details>

## Pod adapters
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

Instead, do the following:
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

The below runtime properties need to be passed to the Job's peon process.

```
druid.port=8100 (what port the peon should run on)
druid.peon.mode=remote
druid.service=druid/peon (for metrics reporting)
druid.indexer.task.baseTaskDir=/druid/data (this should match the argument to the ./peon.sh run command in the PodTemplate)
druid.indexer.runner.type=k8s
druid.indexer.task.encapsulatedTask=true
```

#### Example 1: Using a Pod Template that retrieves values from a ConfigMap 

<details>
<summary>Example Pod Template that uses the regular druid docker image</summary>

```yaml
apiVersion: "v1"
kind: "PodTemplate"
template:
  metadata:
    annotations:
      sidecar.istio.io/proxyCPU: "512m" # to handle an injected istio sidecar
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
</details>

Any runtime property or JVM config used by the peon process can also be passed. E.G. below is an example of a ConfigMap that can be used to generate the `nodetype-config-volume` mount in the above template.

<details>
<summary>Example ConfigMap</summary>

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
    name: druid-tiny-cluster-peons-config
    namespace: default
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
</details>

#### Example 2: Using a ConfigMap to upload the Pod Template file

Alternatively, we can mount the ConfigMap onto Overlord services, and use the ConfigMap to generate the pod template files we want.

<details>
<summary>Mounting to Overlord deployment</summary>

```yaml
  volumeMounts:
    - name: druid-pod-templates
      mountPath: /path/to/podTemplate/directory

  volumes:
    - name: druid-pod-templates
      configMap:
        name: druid-pod-templates
```
</details>

<details>
<summary>Example ConfigMap that generates the Base Pod Template</summary>

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: druid-pod-templates
data:
  basePodSpec.yaml: |-
    apiVersion: "v1"
    kind: "PodTemplate"
    template:
      metadata:
        labels:
          app.kubernetes.io/name: "druid-realtime-backend"
        annotations:
          sidecar.istio.io/proxyCPU: "512m"
      spec:
        containers:
        - name: main
          image: apache/druid:{{DRUIDVERSION}}
          command:
            - sh
            - -c
            - |
              /peon.sh /druid/data 1
          env:
            - name: druid_port
              value: 8100
            - name: druid_plaintextPort
              value: 8100
            - name: druid_tlsPort
              value: 8091
            - name: druid_peon_mode
              value: remote
            - name: druid_service
              value: "druid/peon"
            - name: druid_indexer_task_baseTaskDir
              value: /druid/data
            - name: druid_indexer_runner_type
              value: k8s
            - name: druid_indexer_task_encapsulatedTask
              value: true
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

```
</details>

#### Lazy Loading of Pod Templates

Whenever the Overlord wants to spin up a Kubernetes task pod, it will first read the relevant pod template file, and then create a task pod according to the specifications of the pod template file. This is helpful when you want to make configuration changes to the task pods (e.g. increase/decrease CPU limit or resources). You can edit the pod template files directly, and the next task pod spun up by the Overlord will reflect these changes in its configurations.

#### Pod template selection

The pod template adapter can select which pod template should be used for a task using the [task runner execution config](#dynamic-config)

##### Select based on task type

The `TaskTypePodTemplateSelectStrategy` strategy selects pod templates based on task type for execution purposes,
implementing the behavior that maps templates to specific task types. This is the default pod template selection
strategy. To explicitly select this strategy, set the `podTemplateSelectStrategy` in the dynamic execution config to

```json
{ "type": "default" }
```

Task specific pod templates can be specified as the runtime property 
`druid.indexer.runner.k8s.podTemplate.{taskType}: /path/to/taskSpecificPodSpec.yaml` where `{taskType}` is the name of the
task type. For example, `index_parallel`.

If you are trying to use the default image's environment variable parsing feature to set runtime properties, you need to add an extra escape underscore when specifying pod templates.
For example, set the environment variable `druid_indexer_runner_k8s_podTemplate_index__kafka` when you set the runtime property `druid.indexer.runner.k8s.podTemplate.index_kafka`


The following example shows a configuration for task-based pod template selection:

```properties
druid.indexer.runner.k8s.podTemplate.base=/path/to/basePodSpec.yaml
druid.indexer.runner.k8s.podTemplate.index_kafka=/path/to/kafkaPodSpec.yaml
```

##### Select based on one or more conditions

The `SelectorBasedPodTemplateSelectStrategy` strategy evaluates a series of criteria within `selectors` to determine
which pod template to use to run the task. Pod  templates are configured in the runtime properties like
`druid.indexer.runner.k8s.podTemplate.<selectionKey>=...`.

```json
{
  "type": "selectorBased",
  "selectors": [
    {
      "selectionKey": "podSpec1", 
      "context.tags":
      {
        "userProvidedTag": ["tag1", "tag2"]
      },
      "dataSource": ["wikipedia"]
    },
    {
      "selectionKey": "podSpec2",
      "type": ["index_kafka"]
    }
  ]
}
```

Selectors are processed in order. Druid selects the template based on the first matching selector. If a  task does not
match any selector in the list, it will use the `base` pod template.

For a task to match a selector, all the conditions within the selector must match. A selector can match on
- `type`: Type of the task
- `dataSource`: Destination datasource of the task.
- `context.tags`: Tags passed in the task's context.

##### Example

Set the following runtime properties to define the pod specs that can be used by Druid.

```properties
druid.indexer.runner.k8s.podTemplate.base=/path/to/basePodSpec.yaml
druid.indexer.runner.k8s.podTemplate.podSpec1=/path/to/podSpecWithHighMemRequests.yaml
druid.indexer.runner.k8s.podTemplate.podSpec2=/path/to/podSpecWithLowCpuRequests.yaml
```

Set the dynamic execution config to define the pod template selection strategy.

```json
{
  "type": "default",
  "podTemplateSelectStrategy": {
    "type": "selectorBased",
    "selectors": [
      {
        "selectionKey": "podSpec1",
        "context.tags": { "userProvidedTag": ["tag1", "tag2"] },
        "dataSource": ["wikipedia"]
      },
      {
        "selectionKey": "podSpec2",
        "type": ["index_kafka"]
      }
    ]
  }
}
```

Druid selects the pod templates as follows: 
1. Use `podSpecWithHighMemRequests.yaml` when both of the following conditions are met:
   1. The task context contains a tag with the key `userProvidedTag` that has the value `tag1` or `tag2`.
   2. The task targets the `wikipedia` datasource.
2. Use `podSpecWithLowCpuRequests.yaml` when the task type is `index_kafka`.
3. Use the `basePodSpec.yaml` for all other tasks.

In this example, if there is an `index_kafka` task for the `wikipedia` datasource with the tag `userProvidedTag: tag1`,
Druid selects the pod template `podSpecWithHighMemRequests.yaml`.

In the above example, for selection key `podSpec1` we didn't specify task `type`. This is equivalent to setting `type` to `null` or an empty array.
All three examples below are equivalent.

- Not setting `type`
    ```json
    {
      "selectionKey": "podSpec1",
      "context.tags": { "userProvidedTag": ["tag1", "tag2"] },
      "dataSource": ["wikipedia"]
    }
    ```

- Setting `type` to `null`
    ```json
    {
      "selectionKey": "podSpec1",
      "context.tags": { "userProvidedTag": ["tag1", "tag2"] },
      "dataSource": ["wikipedia"],
      "type": null
    }
    ```

- Setting `type` to an empty array
    ```json
    {
      "selectionKey": "podSpec1",
      "context.tags": { "userProvidedTag": ["tag1", "tag2"] },
      "dataSource": ["wikipedia"],
      "type": []
    }
    ```

In all the above cases, Druid will match the selector to any value of task type. Druid applies similar logic for `dataSource`. For `context.tags` setting `null` or an empty object `{}` is equivalent. 

#### Running Task Pods in Another Namespace

It is possible to run task pods in a different namespace from the rest of your Druid cluster.

If you are running multiple Druid clusters and would like to have a dedicated namespace for all your task pods, you can make the following changes to the runtime properties for your Overlord deployment:

- `druid.indexer.runner.namespace`: The namespace where the task pods will run. It can be the same as the namespace where your Druid cluster is deployed, or different from it. In the latter case, you need to define the following `overlordNamespace`.
- `druid.indexer.runner.overlordNamespace`: The namespace where the Overlord resides. This must be defined when tasks are scheduled in different namespace.
- `druid.indexer.runner.k8sTaskPodNamePrefix` (Optional):  Self-defined field to differentiate which task pods are created from which namespace. More information [here](#differentiating-task-pods-created-from-multiple-namespaces).

Warning: When `druid.indexer.runner.overlordNamespace` and `druid.indexer.runner.k8sTaskPodNamePrefix` is configured, users should ensure that all running tasks are stopped when changing these values. Failure to do so will cause the Overlord to lose track of running tasks, and re-launch them. This may lead to duplicate data and possibly metadata inconsistency issues.

Druid will tag Kubernetes jobs with a `druid.overlord.namespace` label. This label helps Druid filter out Kubernetes jobs belonging to other namespaces. Should you need to deploy a Druid cluster on a namespace `N1` that is already running tasks from another namespace `N2`, take note to set `druid.indexer.runner.overlordNamespace` to `druid.indexer.runner.namespace` (which is `N1`). Failure to do so will result in the cluster in `N1` detecting task pods created from both `N1` and `N2`.

##### Differentiating Task Pods Created From Multiple Namespaces

When we have task pods started by Overlord servers of different Druid clusters, running in different K8S namespaces, it will be difficult to tell which task pods are being started by which overlord or Druid cluster. You can specify a task name prefix, `druid.indexer.runner.k8sTaskPodNamePrefix`, to apply your specified prefix to all task pods created by your cluster.

After configuration, you can witness the change from `coordinatorissuedcompactdataso-0e74d5132781cc950eecf04--1-vbx6t` to `yourtaskprefix-0e74d5132781cc950eecf04--1-vbx6t` by either doing `kubectl get pods` or by viewing the "Location" column under the web console.

When configuring the `druid.indexer.runner.k8sTaskPodNamePrefix`, you should note that:
- The prefix will cut off at 30 characters, as the task pod names must respect a character limit of 63 in Kubernetes.
- Special characters `: - . _` will be ignored.
- The prefix will be converted to lowercase.
- All running tasks must be stopped during configuration. Failure to do so will cause the Overlord to lose track of running tasks, and re-launch them. This may lead to duplicate data and possibly metadata inconsistency issues.

##### Dealing with ZooKeeper Problems

Ensure that when you are running task pods in another namespace, your task pods are able to communicate with ZooKeeper which might be deployed in the same namespace with overlord. If you are using custom pod templates as described below, you can configure `druid.zk.service.host` to your tasks.

##### Dealing with Permissions

Should you require the needed permissions for interacting across Kubernetes namespaces, you can specify a kubeconfig file, and provided the necessary permissions. You can then use the `KUBECONFIG` environment variable to allow your Overlord deployment to find your kubeconfig file. Refer to the [Kubernetes documentation](https://kubernetes.io/docs/concepts/configuration/organize-cluster-access-kubeconfig/) for more information.

### Properties
| Property | Possible Values | Description | Default | Required |
| --- | --- | --- | --- | --- |
| `druid.indexer.runner.namespace` | `String` | If Overlord and task pods are running in different namespaces, specify the Overlord namespace. | - | Yes |
| `druid.indexer.runner.overlordNamespace` | `String` | Only applicable when using Custom Template Pod Adapter. If Overlord and task pods are running in different namespaces, specify the Overlord namespace. <br /> Warning: You need to stop all running tasks in Druid to change this property. Failure to do so will lead to duplicate data and metadata inconsistencies. | `""` | No |
| `druid.indexer.runner.k8sTaskPodNamePrefix` | `String` |  Use this if you want to change your task name to contain a more human-readable prefix. Maximum 30 characters. Special characters `: - . _` will be ignored. <br /> Warning: You need to stop all running tasks in Druid to change this property. Failure to do so will lead to duplicate data and metadata inconsistencies. | `""` | No |
| `druid.indexer.runner.debugJobs` | `boolean` | Boolean flag used to disable clean up of K8s jobs after tasks complete. | False | No |
| `druid.indexer.runner.sidecarSupport` | `boolean` | Deprecated, specify adapter type as runtime property `druid.indexer.runner.k8s.adapter.type: overlordMultiContainer` instead. If your overlord pod has sidecars, this will attempt to start the task with the same sidecars as the overlord pod. | False | No |
| `druid.indexer.runner.primaryContainerName` | `String` | If running with sidecars, the `primaryContainerName` should be that of your druid container like `druid-overlord`. | First container in `podSpec` list | No |
| `druid.indexer.runner.kubexitImage` | `String` | Used kubexit project to help shutdown sidecars when the main pod completes. Otherwise, jobs with sidecars never terminate. | karlkfi/kubexit:latest | No |
| `druid.indexer.runner.disableClientProxy` | `boolean` | Use this if you have a global http(s) proxy and you wish to bypass it. | false | No |
| `druid.indexer.runner.maxTaskDuration` | `Duration` | Max time a task is allowed to run for before getting killed. | `PT4H` | No |
| `druid.indexer.runner.taskCleanupDelay` | `Duration` | How long do jobs stay around before getting reaped from K8s. | `P2D` | No |
| `druid.indexer.runner.taskCleanupInterval` | `Duration` | How often to check for jobs to be reaped. | `PT10M` | No |
| `druid.indexer.runner.taskJoinTimeout` | `Duration` | Timeout for gathering metadata about existing tasks on startup. | `PT1M` | No |
| `druid.indexer.runner.k8sjobLaunchTimeout` | `Duration` | How long to wait to launch a K8s task before marking it as failed, on a resource constrained cluster it may take some time. | `PT1H` | No |
| `druid.indexer.runner.javaOptsArray` | `JsonArray` | java opts for the task. | `-Xmx1g` | No |
| `druid.indexer.runner.labels` | `JsonObject` | Additional labels you want to add to peon pod. | `{}` | No |
| `druid.indexer.runner.annotations` | `JsonObject` | Additional annotations you want to add to peon pod. | `{}` | No |
| `druid.indexer.runner.peonMonitors` | `JsonArray` | Overrides `druid.monitoring.monitors`. Use this property if you don't want to inherit monitors from the Overlord. | `[]` | No |
| `druid.indexer.runner.graceTerminationPeriodSeconds` | `Long` | Number of seconds you want to wait after a sigterm for container lifecycle hooks to complete. Keep at a smaller value if you want tasks to hold locks for shorter periods. | `PT30S` (K8s default) | No |
| `druid.indexer.runner.capacity` | `Integer` | Number of concurrent jobs that can be sent to Kubernetes. | `2147483647` | No |
| `druid.indexer.runner.cpuCoreInMicro` | `Integer` | Number of CPU micro core for the task. | `1000` | No |

### Metrics added

|Metric|Description|Dimensions|Normal value|
|------|-----------|----------|------------|
| `k8s/peon/startup/time` | Metric indicating the milliseconds for peon pod to startup. | `dataSource`, `taskId`, `taskType`, `groupId`, `taskStatus`, `tags` |Varies|

### Gotchas

- With the exception of task pods, all Druid Pods belonging to one Druid cluster must be inside the same Kubernetes namespace.

- You must have a role binding for the overlord's service account that provides the needed permissions for interacting with Kubernetes. An example spec could be:

```yaml
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
:::info
This feature is only available starting in Druid 28. If you require a rolling update to enable Kubernetes-based ingestion, first update your cluster to Druid 28 then apply the overlord configurations mentioned in this section.
:::

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

