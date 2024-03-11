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

package org.apache.druid.indexing.common.task;

import com.google.common.base.Joiner;
import org.apache.druid.query.DruidMetrics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The DefaultTaskLabelsProvider offers a basic implementation of the TaskLabelsProvider interface,
 * focusing on generating task labels and metric tags primarily from the task's inherent properties
 * and context. This default implementation serves as a straightforward example of how task
 * metadata can be extracted and utilized for task management and monitoring.
 */
public class DefaultTaskLabelsProvider implements TaskLabelsProvider
{
  public static final String TYPE = "default";

  /**
   * Generates a list of labels for the given task, predominantly based on the task's type.
   * If specific task labels are defined within the task's context, those are preferred.
   * Otherwise, the task's type itself is used as a fallback label, ensuring that every
   * task is categorized at a minimum by its type.
   *
   * @param task The Druid task for which labels are being generated.
   * @return A list of strings representing the task's labels. These labels are derived
   * from the task's type or directly from predefined labels within the task's context.
   */
  @Override
  public List<String> getTaskLabels(Task task)
  {
    TaskLabel taskLabel = task.getContextValue(Tasks.TASK_LABEL);
    List<String> taskLabels = new ArrayList<>();
    if (taskLabel == null || taskLabel.getLabels().isEmpty()) {
      taskLabels.add(task.getType());
    } else {
      taskLabels.addAll(taskLabel.getLabels());
    }

    return taskLabels;
  }

  /**
   * Extracts metric tags from the task's context. These tags are intended for use in
   * metrics reporting, providing additional dimensions for analyzing task performance.
   * The tags are directly sourced from the task's context, allowing for dynamic and
   * custom tagging of tasks based on operational needs.
   *
   * @param task The Druid task from which metric tags are extracted.
   * @return A map containing the metric tags, where keys are tag names and values are
   * the tag values. The returned map is sourced from the task's context.
   */
  @Override
  public Map<String, Object> getTaskMetricTags(Task task)
  {
    return task.getContextValue(DruidMetrics.TAGS, new HashMap<>());
  }

  /**
   * Compiles a unified configuration label for a Druid task by concatenating its individual labels,
   * separated by a dollar sign ('$'). This combined label aids in categorizing tasks for both scheduling
   * and resource allocation. It facilitates task management by providing a straightforward identifier
   * that encapsulates the task's key attributes.
   * <p>
   * This method employs a simple yet effective approach to generate a label that reflects the composite
   * nature of a task's characteristics. The resulting label can be utilized in various decision-making
   * processes related to task scheduling, ensuring tasks are matched with appropriate resources.
   *
   * @param task The Druid task for which the configuration label is being generated.
   * @return A string that amalgamates the task's individual labels into a single configuration label.
   * This label is intended to assist in the management and allocation of tasks within the system.
   */
  @Override
  public String getTaskConfigLabel(Task task)
  {
    return Joiner.on("$").join(getTaskLabels(task));
  }
}
