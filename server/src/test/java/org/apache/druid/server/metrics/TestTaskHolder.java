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

import org.apache.druid.java.util.metrics.TaskHolder;
import org.apache.druid.query.DruidMetrics;

import java.util.Map;

public class TestTaskHolder implements TaskHolder
{
  private final String dataSource;
  private final String taskId;
  private final String taskType;
  private final String groupId;

  public TestTaskHolder(final String dataSource, final String taskId, final String taskType, final String groupId)
  {
    this.dataSource = dataSource;
    this.taskId = taskId;
    this.taskType = taskType;
    this.groupId = groupId;
  }

  @Override
  public String getDataSource()
  {
    return dataSource;
  }

  @Override
  public String getTaskId()
  {
    return taskId;
  }

  @Override
  public String getTaskType()
  {
    return taskType;
  }

  @Override
  public String getGroupId()
  {
    return groupId;
  }

  @Override
  public Map<String, String> getMetricDimensions()
  {
    return Map.of(
        DruidMetrics.DATASOURCE, dataSource,
        DruidMetrics.TASK_ID, taskId,
        DruidMetrics.ID, taskId,
        DruidMetrics.TASK_TYPE, taskType,
        DruidMetrics.GROUP_ID, groupId
    );
  }
}
