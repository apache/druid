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
import org.apache.druid.indexing.common.task.Task;

/**
 * Interface for selecting a Pod template based on a given task.
 * Implementations of this interface are responsible for determining the appropriate
 * Pod template to use for a specific task based on task characteristics.
 */
public interface PodTemplateSelector
{
  /**
   * Selects a Pod template for the given task.
   * @param task The task for which to select a Pod template..
   * @return An Optional containing the selected PodTemplateWithName if a suitable
   *         template is found, or an empty Optional if no appropriate template
   *         is available for the given task.
   */
  Optional<PodTemplateWithName> getPodTemplateForTask(Task task);
}
