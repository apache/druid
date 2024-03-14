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

import java.util.Map;

/**
 * The TaskIdentitiesProvider interface helps add metric tags to tasks.
 * It's meant to make task management, monitoring, and reporting better by providing extra information
 * about tasks. Both user-defined and system-generated tags are included, making details about task
 * performance and characteristics clearer in reports and summaries.
 */
@ExtensionPoint
public interface TaskIdentitiesProvider
{
  String TASK_IDENTIFIER = "taskIdentifier";

  /**
   * Creates a map of metric tags for a given task. These tags add more information to tasks,
   * useful for monitoring and reporting. The tags include both information set by users and
   * information automatically added by the system. This makes sure tasks are well-described in
   * metrics reports and summaries, helping with detailed analysis and better task handling.
   *
   * @param task The Druid task that needs metric tags.
   * @return A map with tag names as keys and tag values as values. This extra information
   * helps with reporting metrics, providing a clear picture of tasks.
   */
  Map<String, Object> getTaskMetricTags(Task task);
}
