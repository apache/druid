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
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.k8s.overlord.KubernetesTaskRunnerConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class SingleContainerTaskAdapter extends K8sTaskAdapter
{
  public SingleContainerTaskAdapter(
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

    Container mainContainer = setupMainContainer(podSpec, context, containerSize, taskContents);

    // add any optional annotations or labels.
    Map<String, String> annotations = addJobSpecificAnnotations(context, k8sTaskId);
    Map<String, String> labels = addJobSpecificLabels();

    // remove all sidecars
    podSpec.setContainers(Collections.singletonList(mainContainer));

    // create the job
    return buildJob(k8sTaskId, labels, annotations, createTemplateFromSpec(k8sTaskId, podSpec, annotations, labels));
  }

}
