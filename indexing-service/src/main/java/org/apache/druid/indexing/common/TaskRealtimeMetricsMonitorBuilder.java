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

package org.apache.druid.indexing.common;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexing.common.stats.TaskRealtimeMetricsMonitor;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.realtime.SegmentGenerationMetrics;

import java.util.Map;

public class TaskRealtimeMetricsMonitorBuilder
{
  private TaskRealtimeMetricsMonitorBuilder()
  {
  }

  public static TaskRealtimeMetricsMonitor build(
      Task task,
      SegmentGenerationMetrics metrics,
      RowIngestionMeters meters
  )
  {
    return new TaskRealtimeMetricsMonitor(
        metrics,
        meters,
        getMetricDimensions(task),
        task.getContextValue(DruidMetrics.TAGS)
    );
  }

  private static Map<String, String[]> getMetricDimensions(Task task)
  {
    final ImmutableMap.Builder<String, String[]> map = new ImmutableMap.Builder<>();
    map.put(DruidMetrics.DATASOURCE, new String[]{task.getDataSource()});
    map.put(DruidMetrics.TASK_ID, new String[]{task.getId()});
    map.put(DruidMetrics.TASK_TYPE, new String[]{task.getType()});
    map.put(DruidMetrics.GROUP_ID, new String[]{task.getGroupId()});

    if (task instanceof SeekableStreamIndexTask) {
      final SeekableStreamIndexTask<?, ?, ?> streamIndexTask = (SeekableStreamIndexTask<?, ?, ?>) task;
      if (streamIndexTask.getSupervisorId() != null) {
        map.put(DruidMetrics.SUPERVISOR_ID, new String[]{streamIndexTask.getSupervisorId()});
      }
    }

    return map.build();
  }
}
