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

package org.apache.druid.server.metrics;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import javax.annotation.Nullable;

/**
 * A holder for task-specific properties applicable to {@code CliPeon} servers.
 * For other server types, the getter methods in this holder will return {@code null}.
 */
public class TaskPropertiesHolder
{
  public static final String DATA_SOURCE_BINDING = "druidDataSource";
  public static final String TASK_ID_BINDING = "druidTaskId";

  @Named(DATA_SOURCE_BINDING)
  @Inject(optional = true)
  final String dataSource = null;

  @Named(TASK_ID_BINDING)
  @Inject(optional = true)
  final String taskId = null;

  /**
   * @return the taskId for CliPeon servers; {@code null} for all other servers.
   */
  @Nullable
  public String getDataSource()
  {
    return dataSource;
  }

  /**
   * @return the dataSource for CliPeon servers; {@code null} for all other servers.
   */
  @Nullable
  public String getTaskId()
  {
    return taskId;
  }
}
