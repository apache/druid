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

import org.apache.druid.guice.annotations.UnstableApi;

/**
 * The TaskContextEnricher interface enhances Druid tasks by appending contextual information.
 * By infusing tasks with additional context, it aims to improve aspects of task management,
 * monitoring, and analysis. This contextual information aids in clarifying the intent and
 * specifics of tasks within metrics and reporting systems.
 */
@UnstableApi
public interface TaskContextEnricher
{
  /**
   * Augments a task's context with additional information. This method introduces or updates
   * context entries to better describe the task. Such enriched context is pivotal for generating
   * detailed task reports and for incorporating as dimensions within metrics reporting. It ensures
   * tasks are more accurately represented and managed by providing deeper insights into task execution
   * and performance.
   *
   * @param task The Druid task to be augmented with additional context. This process may either
   *             supplement existing context entries or introduce new ones, thereby refining the
   *             task's narrative and operational details.
   */
  void enrichContext(Task task);
}
