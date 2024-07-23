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

package org.apache.druid.k8s.overlord.execution;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.fabric8.kubernetes.api.model.PodTemplate;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.k8s.overlord.taskadapter.PodTemplateWithName;

import javax.validation.constraints.NotNull;
import java.util.Map;

/**
 * Defines a strategy for selecting the Pod template of tasks based on specific conditions.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = TaskTypePodTemplateSelectStrategy.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "default", value = TaskTypePodTemplateSelectStrategy.class),
    @JsonSubTypes.Type(name = "selectorBased", value = SelectorBasedPodTemplateSelectStrategy.class),
})
public interface PodTemplateSelectStrategy
{
  /**
   * Determines the appropriate Pod template for a task by evaluating its properties. This selection
   * allows for customized resource allocation and management tailored to the task's specific requirements.
   *
   * @param task The task for which the Pod template is determined.
   * @return The PodTemplateWithName POJO that contains the name of the template selected and the template itself.
   */
  @NotNull PodTemplateWithName getPodTemplateForTask(Task task, Map<String, PodTemplate> templates);
}
