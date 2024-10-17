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

import com.fasterxml.jackson.annotation.JsonCreator;
import io.fabric8.kubernetes.api.model.PodTemplate;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.k8s.overlord.common.DruidK8sConstants;
import org.apache.druid.k8s.overlord.taskadapter.PodTemplateWithName;

import java.util.Map;

/**
 * This strategy defines how task template is selected based on their type for execution purposes.
 *
 * This implementation selects pod template by looking at the type of the task,
 * making it a straightforward, type-based template selection strategy.
 */
public class TaskTypePodTemplateSelectStrategy implements PodTemplateSelectStrategy
{

  @JsonCreator
  public TaskTypePodTemplateSelectStrategy()
  {
  }

  @Override
  public PodTemplateWithName getPodTemplateForTask(Task task, Map<String, PodTemplate> templates)
  {
    String templateKey = templates.containsKey(task.getType()) ? task.getType() : DruidK8sConstants.BASE_TEMPLATE_NAME;
    return new PodTemplateWithName(templateKey, templates.get(templateKey));
  }

  @Override
  public boolean equals(Object o)
  {
    return o instanceof TaskTypePodTemplateSelectStrategy;
  }

  @Override
  public int hashCode()
  {
    return 1; // Any constant will work here
  }

  @Override
  public String toString()
  {
    return "TaskTypePodTemplateSelectStrategy{" +
           '}';
  }
}
