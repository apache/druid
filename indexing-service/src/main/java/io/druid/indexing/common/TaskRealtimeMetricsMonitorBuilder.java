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

package io.druid.indexing.common;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.indexing.common.stats.RowIngestionMeters;
import io.druid.indexing.common.stats.TaskRealtimeMetricsMonitor;
import io.druid.indexing.common.task.Task;
import io.druid.query.DruidMetrics;
import io.druid.segment.realtime.FireDepartment;
import io.druid.segment.realtime.RealtimeMetricsMonitor;

public class TaskRealtimeMetricsMonitorBuilder
{
  private TaskRealtimeMetricsMonitorBuilder() {}

  public static RealtimeMetricsMonitor build(Task task, FireDepartment fireDepartment)
  {
    return new RealtimeMetricsMonitor(
        ImmutableList.of(fireDepartment),
        ImmutableMap.of(
            DruidMetrics.TASK_ID, new String[]{task.getId()},
            DruidMetrics.TASK_TYPE, new String[]{task.getType()}
        )
    );
  }

  public static TaskRealtimeMetricsMonitor build(Task task, FireDepartment fireDepartment, RowIngestionMeters meters)
  {
    return new TaskRealtimeMetricsMonitor(
        fireDepartment,
        meters,
        ImmutableMap.of(
            DruidMetrics.TASK_ID, new String[]{task.getId()},
            DruidMetrics.TASK_TYPE, new String[]{task.getType()}
        )
    );
  }
}
