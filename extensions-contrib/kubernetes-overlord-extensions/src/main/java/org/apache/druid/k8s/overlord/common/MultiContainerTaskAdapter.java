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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.k8s.overlord.KubernetesTaskRunnerConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MultiContainerTaskAdapter extends K8sTaskAdapter
{
  public MultiContainerTaskAdapter(
      KubernetesClientApi client,
      KubernetesTaskRunnerConfig config,
      ObjectMapper mapper
  )
  {
    super(client, config, mapper);
  }

  @Override
  Job createJobFromPodSpec(PodSpec podSpec, Task task, PeonCommandContext context) throws IOException
  {
    K8sTaskId k8sTaskId = new K8sTaskId(task.getId());

    // get the container size from java_opts array
    long containerSize = getContainerMemory(context);

    // compress the task.json to set as an env variables
    String taskContents = Base64Compression.compressBase64(mapper.writeValueAsString(task));

    setupMainContainer(podSpec, context, containerSize, taskContents);

    // add any optional annotations or labels.
    Map<String, String> annotations = addJobSpecificAnnotations(context, k8sTaskId);
    Map<String, String> labels = addJobSpecificLabels();

    PodTemplateSpec podTemplate = createTemplateFromSpec(k8sTaskId, podSpec, annotations, labels);

    // add sidecar termination support
    addSideCarTerminationSupport(podTemplate);

    // and the init container
    podTemplate.getSpec().getInitContainers().add(getInitContainer());

    // create the job
    return buildJob(k8sTaskId, labels, annotations, podTemplate);
  }

  @VisibleForTesting
  private Container getInitContainer()
  {
    return new ContainerBuilder()
        .withName("kubexit")
        .withImage(config.kubexitImage)
        .withCommand("cp", "/bin/kubexit", "/kubexit/kubexit")
        .withVolumeMounts(new VolumeMountBuilder().withMountPath("/kubexit").withName("kubexit").build())
        .build();
  }

  static void reJiggerArgsAndCommand(Container container, boolean primary)
  {
    List<String> originalCommand = container.getCommand();
    List<String> originalArgs = container.getArgs();
    originalCommand.addAll(originalArgs);
    String newArgs;
    if (primary) {
      // for primary the command is /bin/sh -c, don't need this again, only grab args
      newArgs = Joiner.on(" ").join(originalArgs);
    } else {
      newArgs = Joiner.on(" ").join(originalCommand);
    }
    container.setCommand(Lists.newArrayList("/bin/sh", "-c"));
    String toExecute = "/kubexit/kubexit /bin/sh -c " + "\"" + StringEscapeUtils.escapeJava(newArgs) + "\"";
    // we don't care about exit code of sidecar containers
    if (!primary) {
      toExecute += " || true";
    }
    container.setArgs(Collections.singletonList(toExecute));
  }

  static void addSideCarTerminationSupport(PodTemplateSpec spec)
  {
    Volume graveyard = new VolumeBuilder().withName("graveyard")
                                          .withNewEmptyDir()
                                          .withMedium("Memory")
                                          .endEmptyDir()
                                          .build();
    Volume kubeExit = new VolumeBuilder().withName("kubexit")
                                         .withNewEmptyDir()
                                         .endEmptyDir()
                                         .build();
    spec.getSpec().getVolumes().add(graveyard);
    spec.getSpec().getVolumes().add(kubeExit);

    VolumeMount gMount = new VolumeMountBuilder().withMountPath("/graveyard").withName("graveyard").build();
    VolumeMount kMount = new VolumeMountBuilder().withMountPath("/kubexit").withName("kubexit").build();


    // get the main container
    List<Container> containers = spec.getSpec().getContainers();
    for (int i = 0; i < containers.size(); i++) {
      Container container = containers.get(i);
      container.getEnv().add(new EnvVar("KUBEXIT_NAME", container.getName(), null));
      container.getEnv().add(new EnvVar("KUBEXIT_GRAVEYARD", "/graveyard", null));
      container.getVolumeMounts().add(gMount);
      container.getVolumeMounts().add(kMount);
      if (i > 0) {
        container.getEnv().add(new EnvVar("KUBEXIT_DEATH_DEPS", containers.get(0).getName(), null));
        reJiggerArgsAndCommand(container, false);
      } else {
        reJiggerArgsAndCommand(container, true);
      }
    }
  }
}
