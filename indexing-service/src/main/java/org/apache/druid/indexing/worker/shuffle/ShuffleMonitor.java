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

import com.google.inject.Inject;
import org.apache.druid.indexing.worker.shuffle.ShuffleMetrics.PerDatasourceShuffleMetrics;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent.Builder;
import org.apache.druid.java.util.metrics.AbstractMonitor;

import java.util.Map;

public class ShuffleMonitor extends AbstractMonitor
{
  private static final String SUPERVISOR_TASK_ID_DIMENSION = "supervisorTaskId";
  private static final String SHUFFLE_BYTES_KEY = "shuffle/bytes";
  private static final String SHUFFLE_REQUESTS_KEY = "shuffle/requests";

  private final ShuffleMetrics shuffleMetrics;

  @Inject
  public ShuffleMonitor(ShuffleMetrics shuffleMetrics)
  {
    this.shuffleMetrics = shuffleMetrics;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    final Map<String, PerDatasourceShuffleMetrics> snapshot = shuffleMetrics.snapshot();
    snapshot.forEach((supervisorTaskId, perDatasourceShuffleMetrics) -> {
      final Builder metricBuilder = ServiceMetricEvent
          .builder()
          .setDimension(SUPERVISOR_TASK_ID_DIMENSION, supervisorTaskId);
      emitter.emit(metricBuilder.build(SHUFFLE_BYTES_KEY, perDatasourceShuffleMetrics.getShuffleBytes()));
      emitter.emit(metricBuilder.build(SHUFFLE_REQUESTS_KEY, perDatasourceShuffleMetrics.getShuffleRequests()));
    });

    return true;
  }
}
