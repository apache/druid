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

package org.apache.druid.indexing.worker;

import com.google.inject.Inject;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;
import org.apache.druid.query.DruidMetrics;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class WorkerTaskManagerMonitor extends AbstractMonitor
{
  private final WorkerTaskManager workerTaskManager;
  private static final String WORKER_RUNNING_TASK_COUNT_METRICS = "worker/task/running/count";
  private static final String WORKER_ASSIGNED_TASK_COUNT_METRIC = "worker/task/assigned/count";
  private static final String WORKER_COMPLETED_TASK_COUNT_METRIC = "worker/task/completed/count";

  @Inject
  public WorkerTaskManagerMonitor(WorkerTaskManager workerTaskManager)
  {
    this.workerTaskManager = workerTaskManager;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    final Map<String, Integer> runningTasks, assignedTasks, completedTasks;

    runningTasks = getDataSourceTasks(workerTaskManager.getRunningTasks(), WorkerTaskManager.TaskDetails::getDataSource);
    assignedTasks = getDataSourceTasks(workerTaskManager.getAssignedTasks(), Task::getDataSource);
    completedTasks = getDataSourceTasks(workerTaskManager.getCompletedTasks(), TaskAnnouncement::getTaskDataSource);

    final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
    emitWorkerTaskMetric(builder, emitter, WORKER_RUNNING_TASK_COUNT_METRICS, runningTasks);
    emitWorkerTaskMetric(builder, emitter, WORKER_ASSIGNED_TASK_COUNT_METRIC, assignedTasks);
    emitWorkerTaskMetric(builder, emitter, WORKER_COMPLETED_TASK_COUNT_METRIC, completedTasks);
    return true;
  }

  public void emitWorkerTaskMetric(ServiceMetricEvent.Builder builder, ServiceEmitter emitter, String metricName, Map<String, Integer> dataSourceTaskMap)
  {
    for (Map.Entry<String, Integer> dataSourceTaskCount : dataSourceTaskMap.entrySet()) {
      builder.setDimension(DruidMetrics.DATASOURCE, dataSourceTaskCount.getKey());
      emitter.emit(builder.setMetric(metricName, dataSourceTaskCount.getValue()));
    }
  }

  private <T> Map<String, Integer> getDataSourceTasks(Map<String, T> taskMap, Function<T, String> getDataSourceFunc)
  {
    String dataSource;
    final Map<String, Integer> dataSourceTaskMap = new HashMap<>();

    for (Map.Entry<String, T> task : taskMap.entrySet()) {
      dataSource = getDataSourceFunc.apply(task.getValue());
      dataSourceTaskMap.putIfAbsent(dataSource, 0);
      dataSourceTaskMap.put(dataSource, dataSourceTaskMap.get(dataSource) + 1);
    }
    return dataSourceTaskMap;
  }
}
