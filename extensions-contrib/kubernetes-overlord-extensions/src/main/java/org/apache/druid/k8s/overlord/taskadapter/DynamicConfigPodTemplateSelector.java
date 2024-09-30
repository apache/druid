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

package org.apache.druid.k8s.overlord.taskadapter;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import io.fabric8.kubernetes.api.model.PodTemplate;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.apache.druid.guice.IndexingServiceModuleHelper;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.k8s.overlord.execution.KubernetesTaskRunnerDynamicConfig;
import org.apache.druid.k8s.overlord.execution.PodTemplateSelectStrategy;

import java.io.File;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class DynamicConfigPodTemplateSelector implements PodTemplateSelector
{

  private static final String TASK_PROPERTY = IndexingServiceModuleHelper.INDEXER_RUNNER_PROPERTY_PREFIX
                                              + ".k8s.podTemplate.";

  private final Properties properties;
  private HashMap<String, PodTemplate> podTemplates;
  private Supplier<KubernetesTaskRunnerDynamicConfig> dynamicConfigRef;

  public DynamicConfigPodTemplateSelector(
      Properties properties,
      Supplier<KubernetesTaskRunnerDynamicConfig> dynamicConfigRef
  )
  {
    this.properties = properties;
    this.dynamicConfigRef = dynamicConfigRef;
    initializeTemplatesFromFileSystem();
  }

  private void initializeTemplatesFromFileSystem()
  {
    Set<String> taskAdapterTemplateKeys = getTaskAdapterTemplates(properties);
    if (!taskAdapterTemplateKeys.contains("base")) {
      throw new IAE(
          "Pod template task adapter requires a base pod template to be specified under druid.indexer.runner.k8s.podTemplate.base");
    }

    HashMap<String, PodTemplate> podTemplateMap = new HashMap<>();
    for (String taskAdapterTemplateKey : taskAdapterTemplateKeys) {
      Optional<PodTemplate> template = loadPodTemplate(taskAdapterTemplateKey, properties);
      if (template.isPresent()) {
        podTemplateMap.put(taskAdapterTemplateKey, template.get());
      }
    }
    podTemplates = podTemplateMap;
  }

  private Set<String> getTaskAdapterTemplates(Properties properties)
  {
    Set<String> taskAdapterTemplates = new HashSet<>();

    for (String runtimeProperty : properties.stringPropertyNames()) {
      if (runtimeProperty.startsWith(TASK_PROPERTY)) {
        String[] taskAdapterPropertyPaths = runtimeProperty.split("\\.");
        taskAdapterTemplates.add(taskAdapterPropertyPaths[taskAdapterPropertyPaths.length - 1]);
      }
    }

    return taskAdapterTemplates;
  }

  private Optional<PodTemplate> loadPodTemplate(String key, Properties properties)
  {
    String property = TASK_PROPERTY + key;
    String podTemplateFile = properties.getProperty(property);
    if (podTemplateFile == null) {
      throw new IAE("Pod template file not specified for [%s]", property);

    }
    try {
      return Optional.of(Serialization.unmarshal(
          Files.newInputStream(new File(podTemplateFile).toPath()),
          PodTemplate.class
      ));
    }
    catch (Exception e) {
      throw new IAE(e, "Failed to load pod template file for [%s] at [%s]", property, podTemplateFile);
    }
  }

  @Override
  public Optional<PodTemplateWithName> getPodTemplateForTask(Task task)
  {
    PodTemplateSelectStrategy podTemplateSelectStrategy;
    KubernetesTaskRunnerDynamicConfig dynamicConfig = dynamicConfigRef.get();
    if (dynamicConfig == null || dynamicConfig.getPodTemplateSelectStrategy() == null) {
      podTemplateSelectStrategy = KubernetesTaskRunnerDynamicConfig.DEFAULT_STRATEGY;
    } else {
      podTemplateSelectStrategy = dynamicConfig.getPodTemplateSelectStrategy();
    }

    return Optional.of(podTemplateSelectStrategy.getPodTemplateForTask(task, podTemplates));
  }
}
