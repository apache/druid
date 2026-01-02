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

package org.apache.druid.java.util.metrics;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Provides identifying information for a task. Implementations return {@code null}
 * when used in server processes that are not {@code CliPeon}.
 */
public interface TaskHolder
{
  /**
   * @return the datasource name for the task, or {@code null} if called from a server that is not {@code CliPeon}.
   */
  @Nullable
  String getDataSource();

  /**
   * @return the taskId, or {@code null} if called from a server that is not {@code CliPeon}.
   */
  @Nullable
  String getTaskId();

  /**
   * @return the type name of this task, or {@code null} if called from a server that is not {@code CliPeon}.
   */
  @Nullable
  String getTaskType();

  /**
   * @return the group ID of this task, or {@code null} if called from a server that is not {@code CliPeon}.
   */
  @Nullable
  String getGroupId();

  /**
   * @return a map of task holder dimensions, or an empty map if called from a server that is not {@code CliPeon}.
   */
  Map<String, String> getMetricDimensions();
}
