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
import org.apache.druid.indexing.common.task.Task;

import javax.annotation.Nullable;

/**
 * Defines a strategy for determining the execution behavior of tasks based on specific conditions.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = DefaultExecutionBehaviorStrategy.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "default", value = DefaultExecutionBehaviorStrategy.class),
    @JsonSubTypes.Type(name = "dynamicTask", value = DynamicTaskExecutionBehaviorStrategy.class),
})
public interface ExecutionBehaviorStrategy
{
  /**
   * Determines the category of a task based on its properties. The identified category is used
   * to map different Peon Pod templates, which allows for tailored resource allocation and management
   * according to the task's requirements.
   *
   * @param task the task to evaluate
   * @return the category of the task, or null if no category matches
   */
  @Nullable
  String getTaskCategory(Task task);
}
