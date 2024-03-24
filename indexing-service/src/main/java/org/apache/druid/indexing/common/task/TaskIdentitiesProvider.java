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

/**
 * This interface is tasked with enriching tasks with tags.
 * This enrichment is aimed at enhancing task management, monitoring, and reporting
 * by providing additional information about tasks through tags. These tags
 * contribute to making details about task purpose and characteristics more
 * transparent in reports and metrics.
 */
@ExtensionPoint
public interface TaskIdentitiesProvider
{
  String TASK_IDENTIFIER = "taskIdentifier";

  /**
   * Enriches a task with tags. This method updates the task's context with additional
   * tags to aid in its description. The enriched tags are essential for the creation
   * of comprehensive task reports and for serving as dimensions in metrics reporting,
   * thus providing a clearer insight into task performance and facilitating
   * effective task management.
   *
   * @param task The Druid task to be enriched with tags. The process may update
   *             existing tags or add new ones to enhance the task's descriptive
   *             details.
   */
  void enrichTaskTags(Task task);
}
