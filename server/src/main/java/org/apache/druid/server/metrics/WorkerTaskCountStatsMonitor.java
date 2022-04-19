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
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.LoadScope;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;

@LoadScope(roles = NodeRole.MIDDLE_MANAGER_JSON_NAME)
public class WorkerTaskCountStatsMonitor extends AbstractMonitor
{
  private static final Logger LOG = new Logger(WorkerTaskCountStatsMonitor.class);
  private final WorkerTaskCountStatsProvider statsProvider;
  private final String workerCategory;
  private final String workerVersion;

  @Inject
  public WorkerTaskCountStatsMonitor(
      WorkerTaskCountStatsProvider statsProvider
  )
  {
    this.statsProvider = statsProvider;
    this.workerCategory = statsProvider.getWorkerCategory();
    this.workerVersion = statsProvider.getWorkerVersion();
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    emit(emitter, "worker/task/failed/count", statsProvider.getWorkerFailedTaskCount());
    emit(emitter, "worker/task/success/count", statsProvider.getWorkerSuccessfulTaskCount());
    emit(emitter, "worker/taskSlot/idle/count", statsProvider.getWorkerIdleTaskSlotCount());
    emit(emitter, "worker/taskSlot/total/count", statsProvider.getWorkerTotalTaskSlotCount());
    emit(emitter, "worker/taskSlot/used/count", statsProvider.getWorkerUsedTaskSlotCount());
    return true;
  }

  private void emit(ServiceEmitter emitter, String metricName, Long value)
  {
    final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
    if (value != null) {
      builder.setDimension("category", workerCategory);
      builder.setDimension("version", workerVersion);
      LOG.info("%s (category: %s, version: %s): [%d]", metricName, workerCategory, workerVersion, value);
      emitter.emit(builder.build(metricName, value));
    }
  }
}
