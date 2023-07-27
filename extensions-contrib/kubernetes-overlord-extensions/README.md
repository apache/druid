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

# Kubernetes Task Scheduling Extension
The Kubernetes Task Scheduling extension allows a Druid cluster running on Kubernetes to schedule
its tasks as Kubernetes Jobs instead of sending them to workers (middle managers or indexers).

## How the Kubernetes Task Scheduler works.
The Kubernetes Task Scheduler replaces the HTTP or Zookeeper based task runner (which both send tasks to workers) on the overlord.
Based on which TaskAdapter (`druid.indexer.runner.k8s.adapter.type`) is selected, every task that the overlord runs is transformed into a Kubernetes Job that runs 
the Druid peon process for that task and then exits. The overlord then watches the K8s API for the Job's success or failure.

## Configuration

## Enabling
1. Create a role binding for the overlord's service account that provides the needed permissions for interacting with Kubernetes.
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

2. Configure the overlord's runtime properties to use the Kubernetes task scheduler.
    ```
    druid.indexer.runner.type=k8s
    druid.indexer.task.encapsulatedTask=true
    druid.indexer.runner.namespace=<druid-namespace>
    druid.indexer.runner.capacity=100 (number of concurrent jobs that can be sent to kubernetes at once)
    ```

3. Choose a Task Adapter (determines how tasks will be converted to a Kubernetes Job)
    #### Option 1. Copy the overlord's pod template to use as the base job template
    This adapter takes the overlord's own pod specification and injects needed information (like the task id) on top of it.

    Required overlord runtime properties 
    ```
    druid.indexer.runner.k8s.adapter.type=overlordSingleContainer (defaults to this if not specified)
    druid.indexer.runner.k8s.adapter.type=overlordMultiContainer (same as overlordSingleContainer except sidecars are respected)
    ```
    Additional overlord runtime properties
    ```
    druid.indexer.runner.javaOptsArray=[] (JVM args for the peon running in the Job, If -Xmx is specified, the adapter will attempt to scale the Kubernetes requests of the pod to correspond with it)
    druid.indexer.runner.labels={} (Additional labels to add to the Job)
    druid.indexer.runner.annotations={} (Additional annotations to add to the Job)
    druid.indexer.runner.peonMonitors={} (Override the list of Druid monitors for the peon, otherwise the overlord's list will be used)
    ```

    #### Option 2. Bring your own pod template to use as the base job pod template
    This adapter expects a [PodTemplate](https://kubernetes.io/docs/concepts/workloads/pods/#pod-templates) to be available on the overlord's file system.
    This pod template is used as the base of the job template (additional info like the task id is injected on top of it).
    You can override things like labels, environment variables, resources, annotation, or even the base image with this template.

    Required overlord runtime properties
    ```
    druid.indexer.runner.k8s.adapter.type=customTemplateAdapter
    druid.indexer.runner.k8s.podTemplate.base=/opt/druid/conf/druid/cluster/pod-template.yaml` (location of pod template on overlord)
    ```

    The following PodTemplate is an example that uses the regular druid docker image.
    ```
    apiVersion: "v1"
    kind: "PodTemplate"
    template:
      metadata:
        annotations:
          custom-annotation: "hello"
        labels:
          custom-label: "hello"
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
          image: apache/druid:26.0.0
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
    
    Runtime properties that need to be passed to the Job's peon process
    
    ```
    druid.port=8100 (what port the peon should run on)
    druid.peon.mode=remote
    druid.service=druid/peon (for metrics reporting)
    druid.indexer.task.baseTaskDir=/druid/data (this should match the argument to the ./peon.sh run command in the PodTemplate)
    druid.indexer.runner.type=k8s
    druid.indexer.task.encapsulatedTask=true
    ```

    Any runtime property or jvm config used by the peon process can also be passed here

    E.G. below is a example of a ConfigMap that can be used to generate the nodetype-config-volume mount in the above template.
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

