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

package org.apache.druid.k8s.middlemanager.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.kubernetes.client.PodLogs;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapBuilder;
import io.kubernetes.client.openapi.models.V1ConfigMapList;
import io.kubernetes.client.openapi.models.V1ConfigMapVolumeSource;
import io.kubernetes.client.openapi.models.V1ContainerPort;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1EnvVarBuilder;
import io.kubernetes.client.openapi.models.V1ObjectFieldSelector;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.openapi.models.V1OwnerReferenceBuilder;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodBuilder;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import io.kubernetes.client.util.Yaml;
import io.kubernetes.client.util.generic.GenericKubernetesApi;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Concrete {@link K8sApiClient} impl using k8s-client java lib.
 */
public class DefaultK8sApiClient implements K8sApiClient
{
  private static final Logger LOGGER = new Logger(DefaultK8sApiClient.class);

  private final ApiClient realK8sClient;
  private CoreV1Api coreV1Api;
  private final ObjectMapper jsonMapper;
  private GenericKubernetesApi<V1Pod, V1PodList> podClient;
  private PodLogs logs;

  @Inject
  public DefaultK8sApiClient(ApiClient realK8sClient, @Json ObjectMapper jsonMapper)
  {
    this.realK8sClient = realK8sClient;
    this.coreV1Api = new CoreV1Api(realK8sClient);
    this.jsonMapper = jsonMapper;
    this.podClient = new GenericKubernetesApi<>(V1Pod.class, V1PodList.class, "", "v1", "pods", realK8sClient);
    this.logs = new PodLogs(realK8sClient);
  }

  public void setCoreV1Api(CoreV1Api coreV1Api)
  {
    this.coreV1Api = coreV1Api;
  }

  public void setPodClient(GenericKubernetesApi<V1Pod, V1PodList> podClient)
  {
    this.podClient = podClient;
  }

  public void setPodLogsClient(PodLogs logs)
  {
    this.logs = logs;
  }

  /**
   * Let middlemanager to create peon pod and set peon pod OwnerReference to middlemanager
   * @param taskID used for pod name
   * @param image peon pod image
   * @param namespace peon pod image
   * @param labels peon pod labels used for pod disscovery
   * @param resourceLimit set request = limit here
   * @param taskDir location for task.json
   * @param args running commands
   * @param childPort peon port
   * @param tlsChildPort not support yet
   * @param tempLoc configmap mount location
   * @param peonPodRestartPolicy Never and lifecycle is crontroled by middlemanger
   * @param hostPath peon pod host in K8s network
   * @param mountPath configmap mountPath
   * @return peon pod created. could be null is pod creation failed.
   */
  @Override
  public V1Pod createPod(String taskID, String image, String namespace, Map<String, String> labels, Map<String, Quantity> resourceLimit, File taskDir, List<String> args, int childPort, int tlsChildPort, String tempLoc, String peonPodRestartPolicy, String hostPath, String mountPath, String serviceAccountName)
  {
    try {
      final String configMapVolumeName = "task-json-vol-tmp";
      V1VolumeMount configMapVolumeMount = new V1VolumeMount().name(configMapVolumeName).mountPath(tempLoc);
      V1ConfigMapVolumeSource configMapVolume = new V1ConfigMapVolumeSource().defaultMode(420).name(taskID);

      ArrayList<V1VolumeMount> v1VolumeMounts = new ArrayList<>();
      v1VolumeMounts.add(configMapVolumeMount);

      String commands = buildCommands(args);

      V1OwnerReference owner = new V1OwnerReferenceBuilder()
              .withName(System.getenv("POD_NAME"))
              .withApiVersion("v1")
              .withUid(System.getenv("POD_UID"))
              .withKind("Pod")
              .withController(true)
              .build();

      V1EnvVar podIpEnv = new V1EnvVarBuilder()
              .withName("POD_IP")
              .withNewValueFrom()
              .withFieldRef(new V1ObjectFieldSelector().fieldPath("status.podIP"))
              .endValueFrom()
              .build();

      V1EnvVar task_id = new V1EnvVarBuilder()
              .withName("TASK_ID")
              .withNewValue(taskID)
              .build();

      V1EnvVar mmPodName = new V1EnvVarBuilder()
              .withName("MM_POD_NAME")
              .withNewValue(System.getenv("POD_NAME"))
              .build();

      V1EnvVar mmNamespace = new V1EnvVarBuilder()
              .withName("MM_NAMESPACE")
              .withNewValue(namespace)
              .build();

      V1EnvVar task_dir = new V1EnvVarBuilder()
              .withName("TASK_DIR")
              .withNewValue(taskDir.getAbsolutePath()).build();

      V1EnvVar task_json_tmp_location = new V1EnvVarBuilder()
              .withName("TASK_JSON_TMP_LOCATION")
              .withNewValue(tempLoc + "/task.json").build();

      V1Pod pod;

      if (!mountPath.isEmpty() && !hostPath.isEmpty()) {

        final String hostVolumeName = "data-volume-for-druid-local";
        V1VolumeMount hostVolumeMount = new V1VolumeMount().name(hostVolumeName).mountPath(mountPath);
        v1VolumeMounts.add(hostVolumeMount);

        pod = new V1PodBuilder()
                .withNewMetadata()
                .withOwnerReferences(owner)
                .withNamespace(namespace)
                .withName(taskID)
                .withLabels(labels)
                .endMetadata()
                .withNewSpec()
                .withNewSecurityContext()
                .withFsGroup(0L)
                .withRunAsGroup(0L)
                .withRunAsUser(0L)
                .endSecurityContext()
                .addNewVolume()
                .withNewName(configMapVolumeName)
                .withConfigMap(configMapVolume)
                .endVolume()
                .addNewVolume()
                .withNewName(hostVolumeName)
                .withNewHostPath()
                .withNewPath(hostPath)
                .withNewType("")
                .endHostPath()
                .endVolume()
                .withNewRestartPolicy(peonPodRestartPolicy)
                .addNewContainer()
                .withPorts(new V1ContainerPort().protocol("TCP").containerPort(childPort).name("http"))
                .withNewSecurityContext()
                .withNewPrivileged(true)
                .endSecurityContext()
                .withCommand("/bin/sh", "-c")
                .withArgs(commands)
                .withName("peon")
                .withImage(image)
                .withImagePullPolicy("IfNotPresent")
                .withVolumeMounts(v1VolumeMounts)
                .withEnv(ImmutableList.of(podIpEnv, task_id, task_dir, task_json_tmp_location, mmPodName, mmNamespace))
                .withNewResources()
                .withRequests(resourceLimit)
                .withLimits(resourceLimit)
                .endResources()
                .endContainer()
                .endSpec()
                .build();
      } else {
        pod = new V1PodBuilder()
                .withNewMetadata()
                .withOwnerReferences(owner)
                .withNamespace(namespace)
                .withName(taskID)
                .withLabels(labels)
                .endMetadata()
                .withNewSpec()
                .withNewSecurityContext()
                .withFsGroup(0L)
                .withRunAsGroup(0L)
                .withRunAsUser(0L)
                .endSecurityContext()
                .addNewVolume()
                .withNewName(configMapVolumeName)
                .withConfigMap(configMapVolume)
                .endVolume()
                .withNewRestartPolicy(peonPodRestartPolicy)
                .addNewContainer()
                .withPorts(new V1ContainerPort().protocol("TCP").containerPort(childPort).name("http"))
                .withNewSecurityContext()
                .withNewPrivileged(true)
                .endSecurityContext()
                .withCommand("/bin/sh", "-c")
                .withArgs(commands)
                .withName("peon")
                .withImage(image)
                .withImagePullPolicy("IfNotPresent")
                .withVolumeMounts(v1VolumeMounts)
                .withEnv(ImmutableList.of(podIpEnv, task_id, task_dir, task_json_tmp_location, mmPodName, mmNamespace))
                .withNewResources()
                .withRequests(resourceLimit)
                .withLimits(resourceLimit)
                .endResources()
                .endContainer()
                .endSpec()
                .build();
      }

      return coreV1Api.createNamespacedPod(namespace, pod, null, null, null);
    }
    catch (ApiException ex) {
      LOGGER.warn(ex, "Failed to create pod[%s/%s], code[%d], error[%s].", namespace, taskID, ex.getCode(), ex.getResponseBody());
    }
    return null;
  }

  /**
   * convert commands into usable String:
   * 1. remove all the \n, \r, \t
   * 2. convert property=abc;def to property="abc;def"
   * 3. add prepareTaskFiles to prepare necessary files like task.json before running commands.
   * 4. convert property=[a,b,c] to property="[\"a\",\"b\",\"c\"]"
   * 5. convert " to \"
   * @param args commands
   * @return a runnable commands in pod.
   */
  private String buildCommands(List<String> args)
  {
    for (int i = 0; i < args.size(); i++) {
      String value = args.get(i);
      args.set(i, StringUtils.replace(value, "\n", ""));
      args.set(i, StringUtils.replace(value, "\r", ""));
      args.set(i, StringUtils.replace(value, "\t", ""));
      args.set(i, StringUtils.replace(value, "\"", "\\\""));
      String[] splits = args.get(i).split("=");
      if (splits.length > 1 && splits[1].contains(";")) {
        args.set(i, splits[0] + "=" + "\"" + splits[1] + "\"");
      }

      if (splits.length > 1 && (splits[1].startsWith("[") || splits[1].startsWith("{"))) {
        args.set(i, splits[0] + "=" + "\"" + splits[1] + "\"");
      }
    }

    StringBuilder builder = new StringBuilder();
    for (String arg : args) {
      builder.append(arg).append(" ");
    }

    String javaCommands = builder.toString().substring(0, builder.toString().length() - 1);
    String reportFile = args.get(args.size() - 1);
    String statusFile = args.get(args.size() - 2);
    final String postAction = ";cd `dirname " + reportFile
            + "`;kubectl cp report.json $MM_NAMESPACE/$MM_POD_NAME:" + reportFile
            + ";kubectl cp status.json $MM_NAMESPACE/$MM_POD_NAME:" + statusFile + ";";

    final String prepareTaskFiles = "mkdir -p /tmp/conf/;test -d /tmp/conf/druid && rm -r /tmp/conf/druid;cp -r /opt/druid/conf/druid /tmp/conf/druid;mkdir -p $TASK_DIR; cp $TASK_JSON_TMP_LOCATION $TASK_DIR;";
    return prepareTaskFiles + javaCommands + postAction;
  }

  /**
   * create a task.json configmap for peon pod.
   */
  @Override
  public V1ConfigMap createConfigMap(String namespace, String configMapName, Map<String, String> labels, Map<String, String> data)
  {
    V1OwnerReference owner = new V1OwnerReferenceBuilder()
            .withName(System.getenv("POD_NAME"))
            .withApiVersion("v1")
            .withUid(System.getenv("POD_UID"))
            .withKind("Pod")
            .withController(true)
            .build();

    V1ConfigMap configMap = new V1ConfigMapBuilder()
            .withNewMetadata()
            .withOwnerReferences(owner)
            .withName(configMapName)
            .withLabels(labels)
            .endMetadata()
            .withData(data)
            .build();

    try {
      return coreV1Api.createNamespacedConfigMap(namespace, configMap, null, null, null);
    }
    catch (ApiException ex) {
      LOGGER.warn(ex, "Failed to create configMap[%s/%s], code[%d], error[%s].", namespace, configMapName, ex.getCode(), ex.getResponseBody());
    }
    return null;
  }

  @Override
  public Boolean configMapIsExist(String namespace, String labelSelector)
  {
    try {
      V1ConfigMapList v1ConfigMapList = coreV1Api.listNamespacedConfigMap(namespace, null, null, null, null, labelSelector, null, null, null, null);
      if (v1ConfigMapList == null) {
        return false;
      }
      return !v1ConfigMapList.getItems().isEmpty();
    }
    catch (ApiException ex) {
      LOGGER.warn(ex, "Failed to get configMap[%s/%s], code[%d], error[%s].", namespace, labelSelector, ex.getCode(), ex.getResponseBody());
      return false;
    }
  }

  /**
   * There are five status for pod, including "Pending", "Running", "Succeeded", "Failed", "Unknown"
   * Just care about Pending status here.
   * @param peonPod
   * @param labelSelector
   */
  @Override
  public void waitForPodRunning(V1Pod peonPod, String labelSelector)
  {
    String namespace = peonPod.getMetadata().getNamespace();
    String podName = peonPod.getMetadata().getName();
    try {
      V1PodList v1PodList = coreV1Api.listNamespacedPod(namespace, null, null, null, null, labelSelector, null, null, null, null);
      while (v1PodList.getItems().isEmpty() || (v1PodList.getItems().size() > 0 && getPodStatus(v1PodList.getItems().get(0)).equalsIgnoreCase("Pending"))) {
        LOGGER.info("Still waiting for pod Running [%s/%s]", namespace, podName);
        Thread.sleep(3 * 1000);
        v1PodList = coreV1Api.listNamespacedPod(peonPod.getMetadata().getNamespace(), null, null, null, null, labelSelector, null, null, null, null);
      }
      LOGGER.info("Peon Pod Running : %s", Yaml.dump(peonPod));
    }
    catch (ApiException ex) {
      LOGGER.warn(ex, "Exception to wait for pod Running[%s/%s], code[%d], error[%s].", namespace, labelSelector, ex.getCode(), ex.getResponseBody());
    }
    catch (Exception ex) {
      LOGGER.warn(ex, "Exception when wait for pod Running [%s/%s]", namespace, podName);
    }
  }

  @Override
  public InputStream getPodLogs(V1Pod peonPod)
  {
    String namespace = peonPod.getMetadata().getNamespace();
    String podName = peonPod.getMetadata().getName();
    InputStream is = null;
    try {
      is = logs.streamNamespacedPodLog(peonPod);
    }
    catch (ApiException ex) {
      LOGGER.warn(ex, "Exception to get pod logs [%s/%s], code[%d], error[%s].", namespace, podName, ex.getCode(), ex.getResponseBody());
    }
    catch (IOException ex) {
      LOGGER.warn(ex, "Error when get pod logs [%s/%s].", namespace, podName);
    }
    return is;
  }

  @Override
  public String waitForPodFinished(V1Pod peonPod)
  {
    String phase = getPodStatus(peonPod);
    String namespace = "";
    String name = "";
    try {
      namespace = peonPod.getMetadata().getNamespace();
      name = peonPod.getMetadata().getName();

      while (!phase.equalsIgnoreCase("Failed") && !phase.equalsIgnoreCase("Succeeded")) {
        LOGGER.info("Still wait for peon pod finished [%s/%s] current status is [%s]", namespace, name, phase);
        Thread.sleep(3 * 1000);
        phase = getPodStatus(peonPod);
      }
    }
    catch (NullPointerException ex) {
      LOGGER.warn(ex, "NullPointerException  when wait for pod finished [%s/%s].", namespace, name);
    }
    catch (InterruptedException ex) {
      LOGGER.warn(ex, "InterruptedException  when wait for pod finished [%s/%s].", namespace, name);
    }
    return phase;
  }

  @Override
  public String getPodStatus(V1Pod peonPod)
  {
    String phase = "Failed";
    V1ObjectMeta mt = peonPod.getMetadata();
    try {
      phase = podClient.get(mt.getNamespace(), mt.getName()).getObject().getStatus().getPhase();
    }
    catch (NullPointerException ex) {
      LOGGER.warn(ex, "can't get [%s/%s] phase", mt.getNamespace(), mt.getName());
    }

    return phase;
  }

  @Override
  public V1Pod getPod(V1Pod peonPod)
  {
    V1ObjectMeta mt = peonPod.getMetadata();
    return podClient.get(mt.getNamespace(), mt.getName()).getObject();
  }

  @Override
  public void deleteConfigMap(V1Pod peonPod, String labels)
  {
    String namespace = peonPod.getMetadata().getNamespace();
    try {
      LOGGER.info("Start to delete pod related configMap : [%s/%s/%s]", namespace, peonPod.getMetadata().getName(), labels);
      coreV1Api.deleteCollectionNamespacedConfigMap(namespace, null, null, null, null, 0, labels, null, null, null, null, null, null);
      LOGGER.info("Pod related configMap deleted : [%s/%s/%s]", namespace, peonPod.getMetadata().getName(), labels);
    }
    catch (ApiException ex) {
      LOGGER.warn(ex, "Failed to delete configMap[%s/%s/%s], code[%d], error[%s].", namespace, peonPod.getMetadata().getName(), labels, ex.getCode(), ex.getResponseBody());
    }
  }

  @Override
  public void deletePod(V1Pod peonPod)
  {
    V1ObjectMeta mt = peonPod.getMetadata();
    podClient.delete(mt.getNamespace(), mt.getName());
    LOGGER.info("Peon Pod deleted : [%s/%s]", peonPod.getMetadata().getNamespace(), peonPod.getMetadata().getName());
  }
}

