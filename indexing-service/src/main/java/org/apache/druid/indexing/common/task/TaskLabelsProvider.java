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

import org.apache.druid.guice.annotations.ExtensionPoint;

import java.util.List;
import java.util.Map;

/**
 * The TaskLabelsProvider interface is designed to supply labels and tags for tasks within Apache Druid.
 * Implementing this interface allows for the enrichment of task management capabilities, enhancing task
 * scheduling, monitoring, and reporting functionalities. It facilitates assigning meaningful metadata to
 * tasks, which can be leveraged for improved operational insights and system optimizations.
 */
@ExtensionPoint
public interface TaskLabelsProvider
{
  /**
   * Retrieves a list of labels for a specified task. These labels encapsulate various characteristics
   * and aspects of the task, aiding in task classification and management. Labels are utilized to
   * facilitate task laning and pod template selection, providing system-generated insights that do
   * not require end-user intervention but enhance internal task routing and execution strategies.
   *
   * @param task The Druid task for which labels are being requested.
   * @return A list of strings, each representing a label associated with the task. These labels
   * are intended for internal use to improve task management and scheduling.
   */
  List<String> getTaskLabels(Task task);

  /**
   * Generates a map of metric tags for a given task. Metric tags are key-value pairs that add
   * additional metadata to tasks for monitoring and reporting purposes. Unlike labels, tags can
   * be user-defined and are intended to be visible in metrics reports and task summaries, providing
   * a more granular view of task performance and characteristics.
   *
   * @param task The Druid task for which metric tags are being requested.
   * @return A map where keys are tag names and values are their corresponding values. These tags
   * enrich the task's metadata and are included in metrics reporting, allowing for detailed
   * analysis and tracking.
   */
  Map<String, Object> getTaskMetricTags(Task task);
}
