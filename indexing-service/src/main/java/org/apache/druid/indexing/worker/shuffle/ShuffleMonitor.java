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

package org.apache.druid.indexing.worker.shuffle;

import org.apache.druid.indexing.worker.shuffle.ShuffleMetrics.PerDatasourceShuffleMetrics;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent.Builder;
import org.apache.druid.java.util.metrics.AbstractMonitor;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import java.util.Map;

public class ShuffleMonitor extends AbstractMonitor
{
  static final String SUPERVISOR_TASK_ID_DIMENSION = "supervisorTaskId";
  static final String SHUFFLE_BYTES_KEY = "ingest/shuffle/bytes";
  static final String SHUFFLE_REQUESTS_KEY = "ingest/shuffle/requests";

  /**
   * ShuffleMonitor can be instantiated in any node types if it is defined in
   * {@link org.apache.druid.server.metrics.MonitorsConfig}. Since {@link ShuffleMetrics} is defined
   * in the `indexing-service` module, some node types (such as broker) would fail to create it
   * if they don't have required dependencies. To avoid this problem, this variable is lazily initialized
   * only in the node types which has the {@link ShuffleModule}.
   */
  @MonotonicNonNull
  private ShuffleMetrics shuffleMetrics;

  public void setShuffleMetrics(ShuffleMetrics shuffleMetrics)
  {
    this.shuffleMetrics = shuffleMetrics;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    if (shuffleMetrics != null) {
      final Map<String, PerDatasourceShuffleMetrics> snapshot = shuffleMetrics.snapshotAndReset();
      snapshot.forEach((supervisorTaskId, perDatasourceShuffleMetrics) -> {
        final Builder metricBuilder = ServiceMetricEvent
            .builder()
            .setDimension(SUPERVISOR_TASK_ID_DIMENSION, supervisorTaskId);
        emitter.emit(metricBuilder.build(SHUFFLE_BYTES_KEY, perDatasourceShuffleMetrics.getShuffleBytes()));
        emitter.emit(metricBuilder.build(SHUFFLE_REQUESTS_KEY, perDatasourceShuffleMetrics.getShuffleRequests()));
      });
    }
    return true;
  }
}
