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
import com.google.inject.Injector;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;
import org.apache.druid.query.DruidMetrics;

import java.util.Map;
import java.util.Set;

public class WorkerTaskCountStatsMonitor extends AbstractMonitor
{
  private final WorkerTaskCountStatsProvider statsProvider;
  private final IndexerTaskCountStatsProvider indexerStatsProvider;
  private final String workerCategory;
  private final String workerVersion;
  private final boolean isMiddleManager;

  @Inject
  public WorkerTaskCountStatsMonitor(
      Injector injector,
      @Self Set<NodeRole> nodeRoles
  )
  {
    this.isMiddleManager = nodeRoles.contains(NodeRole.MIDDLE_MANAGER);
    if (isMiddleManager) {
      this.statsProvider = injector.getInstance(WorkerTaskCountStatsProvider.class);
      this.indexerStatsProvider = null;
      this.workerCategory = statsProvider.getWorkerCategory();
      this.workerVersion = statsProvider.getWorkerVersion();
    } else {
      this.indexerStatsProvider = injector.getInstance(IndexerTaskCountStatsProvider.class);
      this.statsProvider = null;
      this.workerCategory = null;
      this.workerVersion = null;
    }
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    if (isMiddleManager) {
      emit(emitter, "worker/task/failed/count", statsProvider.getWorkerFailedTaskCount());
      emit(emitter, "worker/task/success/count", statsProvider.getWorkerSuccessfulTaskCount());
      emit(emitter, "worker/taskSlot/idle/count", statsProvider.getWorkerIdleTaskSlotCount());
      emit(emitter, "worker/taskSlot/total/count", statsProvider.getWorkerTotalTaskSlotCount());
      emit(emitter, "worker/taskSlot/used/count", statsProvider.getWorkerUsedTaskSlotCount());
    } else {
      emit(emitter, "worker/task/running/count", indexerStatsProvider.getWorkerRunningTasks());
      emit(emitter, "worker/task/assigned/count", indexerStatsProvider.getWorkerAssignedTasks());
      emit(emitter, "worker/task/completed/count", indexerStatsProvider.getWorkerCompletedTasks());
    }
    return true;
  }

  private void emit(ServiceEmitter emitter, String metricName, Long value)
  {
    if (value != null) {
      final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
      builder.setDimension(DruidMetrics.CATEGORY, workerCategory);
      builder.setDimension(DruidMetrics.WORKER_VERSION, workerVersion);
      emitter.emit(builder.setMetric(metricName, value));
    }
  }

  public void emit(ServiceEmitter emitter, String metricName, Map<String, Long> dataSourceTaskMap)
  {
    for (Map.Entry<String, Long> dataSourceTaskCount : dataSourceTaskMap.entrySet()) {
      if (dataSourceTaskCount.getValue() != null) {
        ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
        builder.setDimension(DruidMetrics.DATASOURCE, dataSourceTaskCount.getKey());
        emitter.emit(builder.setMetric(metricName, dataSourceTaskCount.getValue()));
      }
    }
  }
}
