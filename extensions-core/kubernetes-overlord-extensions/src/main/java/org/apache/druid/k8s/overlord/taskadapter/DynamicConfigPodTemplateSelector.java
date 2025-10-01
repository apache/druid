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
import org.apache.druid.k8s.overlord.KubernetesTaskRunnerEffectiveConfig;

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
  private final KubernetesTaskRunnerEffectiveConfig effectiveConfig;
  // Supplier allows Overlord to read the most recent pod template file without calling initializeTemplatesFromFileSystem() again.
  private HashMap<String, Supplier<PodTemplate>> podTemplates;

  public DynamicConfigPodTemplateSelector(
      Properties properties,
      KubernetesTaskRunnerEffectiveConfig effectiveConfig
  )
  {
    this.properties = properties;
    this.effectiveConfig = effectiveConfig;
    initializeTemplatesFromFileSystem();
  }

  private void initializeTemplatesFromFileSystem() throws IAE
  {
    Set<String> taskAdapterTemplateKeys = getTaskAdapterTemplatesKeys(properties);
    if (!taskAdapterTemplateKeys.contains("base")) {
      throw new IAE(
          "Pod template task adapter requires a base pod template to be specified under druid.indexer.runner.k8s.podTemplate.base");
    }

    HashMap<String, Supplier<PodTemplate>> podTemplateMap = new HashMap<>();
    for (String taskAdapterTemplateKey : taskAdapterTemplateKeys) {
      Supplier<PodTemplate> templateSupplier = () -> loadPodTemplate(taskAdapterTemplateKey, properties);
      validateTemplateSupplier(templateSupplier);
      podTemplateMap.put(taskAdapterTemplateKey, templateSupplier);
    }

    podTemplates = podTemplateMap;
  }

  private Set<String> getTaskAdapterTemplatesKeys(Properties properties)
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

  private PodTemplate loadPodTemplate(String key, Properties properties) throws IAE
  {
    String property = TASK_PROPERTY + key;
    String podTemplateFile = properties.getProperty(property);
    if (podTemplateFile == null) {
      throw new IAE("Pod template file not specified for [%s]", property);
    }

    try {
      // Use Optional to assert unmarshal result is non-null.
      Optional<PodTemplate> maybeTemplate = Optional.of(Serialization.unmarshal(
          Files.newInputStream(new File(podTemplateFile).toPath()),
          PodTemplate.class
      ));

      return maybeTemplate.get();
    }
    catch (Exception e) {
      throw new IAE(e, "Failed to load pod template file for [%s] at [%s]", property, podTemplateFile);
    }
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  private void validateTemplateSupplier(Supplier<PodTemplate> templateSupplier) throws IAE
  {
    templateSupplier.get();
  }

  @Override
  public Optional<PodTemplateWithName> getPodTemplateForTask(Task task)
  {
    return Optional.of(effectiveConfig.getPodTemplateSelectStrategy().getPodTemplateForTask(task, podTemplates));
  }
}
