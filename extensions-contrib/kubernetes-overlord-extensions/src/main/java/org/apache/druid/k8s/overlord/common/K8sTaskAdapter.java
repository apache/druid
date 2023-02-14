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

package org.apache.druid.k8s.overlord.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.EnvVarSourceBuilder;
import io.fabric8.kubernetes.api.model.ObjectFieldSelector;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.k8s.overlord.KubernetesTaskRunnerConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * This class transforms tasks to pods, and pods to tasks to assist with creating the job spec for a
 * peon task.
 * The two subclasses of this class are the SingleContainerTaskAdapter and the MultiContainerTaskAdapter
 * This class runs on the overlord, to convert a task into a job, it will take its own podSpec (the current running overlord)
 * keep volumees, secrets, env variables, config maps, etc. and add some additional information as well as provide a new
 * command for running the task.
 * The SingleContainerTaskAdapter only runs a task in a single container (no sidecars)
 * The MultiContainerTaskAdapter runs with all the sidecars the current running overlord runs with.  Thus, it needs
 * to add some extra coordination to shut down sidecar containers when the main pod exits.
 */

public abstract class K8sTaskAdapter implements TaskAdapter<Pod, Job>
{

  private static final EmittingLogger log = new EmittingLogger(K8sTaskAdapter.class);

  protected final KubernetesClientApi client;
  protected final KubernetesTaskRunnerConfig config;
  protected final ObjectMapper mapper;

  public K8sTaskAdapter(
      KubernetesClientApi client,
      KubernetesTaskRunnerConfig config,
      ObjectMapper mapper
  )
  {
    this.client = client;
    this.config = config;
    this.mapper = mapper;
  }

  @Override
  public Job fromTask(Task task, PeonCommandContext context) throws IOException
  {
    String myPodName = System.getenv("HOSTNAME");
    Pod pod = client.executeRequest(client -> client.pods().inNamespace(config.namespace).withName(myPodName).get());
    PodSpec podSpec = pod.getSpec();
    massageSpec(podSpec, config.primaryContainerName);
    return createJobFromPodSpec(podSpec, task, context);
  }

  @Override
  public Task toTask(Pod from) throws IOException
  {
    // all i have to do here is grab the main container...done
    PodSpec podSpec = from.getSpec();
    massageSpec(podSpec, "main");
    List<EnvVar> envVars = podSpec.getContainers().get(0).getEnv();
    Optional<EnvVar> taskJson = envVars.stream().filter(x -> "TASK_JSON".equals(x.getName())).findFirst();
    String contents = taskJson.map(envVar -> taskJson.get().getValue()).orElse(null);
    if (contents == null) {
      throw new IOException("No TASK_JSON environment variable found in pod: " + from.getMetadata().getName());
    }
    return mapper.readValue(Base64Compression.decompressBase64(contents), Task.class);
  }

  @VisibleForTesting
  abstract Job createJobFromPodSpec(PodSpec podSpec, Task task, PeonCommandContext context) throws IOException;

  protected Job buildJob(
      K8sTaskId k8sTaskId,
      Map<String, String> labels,
      Map<String, String> annotations,
      PodTemplateSpec podTemplate
  )
  {
    return new JobBuilder()
        .withNewMetadata()
        .withName(k8sTaskId.getK8sTaskId())
        .addToLabels(labels)
        .addToAnnotations(annotations)
        .endMetadata()
        .withNewSpec()
        .withTemplate(podTemplate)
        .withActiveDeadlineSeconds(config.maxTaskDuration.toStandardDuration().getStandardSeconds())
        .withBackoffLimit(0)
        .withTtlSecondsAfterFinished((int) config.taskCleanupDelay.toStandardDuration().getStandardSeconds())
        .endSpec()
        .build();
  }

  @VisibleForTesting
  static Optional<Long> getJavaOptValueBytes(String qualifier, List<String> commands)
  {
    Long result = null;
    Optional<String> lastHeapValue = commands.stream().filter(x -> x.startsWith(qualifier)).reduce((x, y) -> y);
    if (lastHeapValue.isPresent()) {
      result = HumanReadableBytes.parse(StringUtils.removeStart(lastHeapValue.get(), qualifier));
    }
    return Optional.ofNullable(result);
  }

  // sizes the container memory to [1.2 * (direct buffer size + Xmx)]
  @VisibleForTesting
  static long getContainerMemory(PeonCommandContext context)
  {
    List<String> javaOpts = context.getJavaOpts();
    Optional<Long> optionalXmx = getJavaOptValueBytes("-Xmx", javaOpts);
    long heapSize = HumanReadableBytes.parse("1g");
    if (optionalXmx.isPresent()) {
      heapSize = optionalXmx.get();
    }
    Optional<Long> optionalDbb = getJavaOptValueBytes("-XX:MaxDirectMemorySize=", javaOpts);
    long dbbSize = heapSize;
    if (optionalDbb.isPresent()) {
      dbbSize = optionalDbb.get();
    }
    return (long) ((dbbSize + heapSize) * 1.2);

  }

  protected void setupPorts(Container mainContainer)
  {
    mainContainer.getPorts().clear();
    ContainerPort tcpPort = new ContainerPort();
    tcpPort.setContainerPort(DruidK8sConstants.PORT);
    tcpPort.setName("druid-port");
    tcpPort.setProtocol("TCP");
    ContainerPort httpsPort = new ContainerPort();
    httpsPort.setContainerPort(DruidK8sConstants.TLS_PORT);
    httpsPort.setName("druid-tls-port");
    httpsPort.setProtocol("TCP");
    mainContainer.setPorts(Lists.newArrayList(httpsPort, tcpPort));
  }

  @VisibleForTesting
  void addEnvironmentVariables(Container mainContainer, PeonCommandContext context, String taskContents)
      throws JsonProcessingException
  {
    // if the peon monitors are set, override the overlord's monitors (if set) with the peon monitors
    if (!config.peonMonitors.isEmpty()) {
      mainContainer.getEnv().removeIf(x -> "druid_monitoring_monitors".equals(x.getName()));
      mainContainer.getEnv().add(new EnvVarBuilder()
                                     .withName("druid_monitoring_monitors")
                                     .withValue(mapper.writeValueAsString(config.peonMonitors))
                                     .build());
    }

    mainContainer.getEnv().addAll(Lists.newArrayList(
        new EnvVarBuilder()
            .withName(DruidK8sConstants.TASK_DIR_ENV)
            .withValue(context.getTaskDir().getAbsolutePath())
            .build(),
        new EnvVarBuilder()
            .withName(DruidK8sConstants.TASK_JSON_ENV)
            .withValue(taskContents)
            .build(),
        new EnvVarBuilder()
            .withName(DruidK8sConstants.JAVA_OPTS)
            .withValue(Joiner.on(" ").join(context.getJavaOpts()))
            .build(),
        new EnvVarBuilder()
            .withName(DruidK8sConstants.DRUID_HOST_ENV)
            .withValueFrom(new EnvVarSourceBuilder().withFieldRef(new ObjectFieldSelector(
                null,
                "status.podIP"
            )).build()).build(),
        new EnvVarBuilder()
            .withName(DruidK8sConstants.DRUID_HOSTNAME_ENV)
            .withValueFrom(new EnvVarSourceBuilder().withFieldRef(new ObjectFieldSelector(
                null,
                "metadata.name"
            )).build()).build()
    ));
  }

  protected Container setupMainContainer(
      PodSpec podSpec,
      PeonCommandContext context,
      long containerSize,
      String taskContents
  ) throws JsonProcessingException
  {
    // prepend the startup task.json extraction command
    List<String> mainCommand = Lists.newArrayList("sh", "-c");
    // update the command
    List<Container> containers = podSpec.getContainers();
    Container mainContainer = Iterables.getFirst(containers, null);
    if (mainContainer == null) {
      throw new IllegalArgumentException("Must have at least one container");
    }

    // remove probes
    mainContainer.setReadinessProbe(null);
    mainContainer.setLivenessProbe(null);

    setupPorts(mainContainer);
    addEnvironmentVariables(mainContainer, context, taskContents);

    mainContainer.setCommand(mainCommand);
    mainContainer.setArgs(Collections.singletonList(Joiner.on(" ").join(context.getComamnd())));

    mainContainer.setName("main");
    ImmutableMap<String, Quantity> resources = ImmutableMap.of(
        "cpu",
        new Quantity("1000", "m"),
        "memory",
        new Quantity(String.valueOf(containerSize))
    );
    mainContainer.setResources(new ResourceRequirementsBuilder().withRequests(resources).withLimits(resources).build());
    return mainContainer;
  }

  protected Map<String, String> addJobSpecificAnnotations(PeonCommandContext context, K8sTaskId k8sTaskId)
  {
    Map<String, String> annotations = config.annotations;
    annotations.put(DruidK8sConstants.TASK_ID, k8sTaskId.getOriginalTaskId());
    annotations.put(DruidK8sConstants.TLS_ENABLED, String.valueOf(context.isEnableTls()));
    return annotations;
  }

  protected Map<String, String> addJobSpecificLabels()
  {
    Map<String, String> labels = config.labels;
    labels.put(DruidK8sConstants.LABEL_KEY, "true");
    return labels;
  }

  protected PodTemplateSpec createTemplateFromSpec(
      K8sTaskId k8sTaskId,
      PodSpec podSpec,
      Map<String, String> annotations,
      Map<String, String> labels
  )
  {
    // clean up the podSpec
    podSpec.setNodeName(null);
    podSpec.setRestartPolicy("Never");
    podSpec.setHostname(k8sTaskId.getK8sTaskId());
    podSpec.setTerminationGracePeriodSeconds(config.graceTerminationPeriodSeconds);

    PodTemplateSpec podTemplate = new PodTemplateSpec();
    ObjectMeta objectMeta = new ObjectMeta();
    objectMeta.setAnnotations(annotations);
    objectMeta.setLabels(labels);
    podTemplate.setMetadata(objectMeta);
    podTemplate.setSpec(podSpec);
    return podTemplate;
  }

  @VisibleForTesting
  static void massageSpec(PodSpec spec, String primaryContainerName)
  {
    // find the primary container and make it first,
    if (StringUtils.isNotBlank(primaryContainerName)) {
      int i = 0;
      while (i < spec.getContainers().size()) {
        if (primaryContainerName.equals(spec.getContainers().get(i).getName())) {
          break;
        }
        i++;
      }
      // if the primaryContainer is not found, assume the primary container is the first container.
      if (i >= spec.getContainers().size()) {
        throw new IllegalArgumentException("Could not find container named: "
                                           + primaryContainerName
                                           + " in PodSpec");
      }
      Container primary = spec.getContainers().get(i);
      spec.getContainers().remove(i);
      spec.getContainers().add(0, primary);
    }
  }

}
