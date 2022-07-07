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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.data.input.InputStats;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;


import java.util.Map;

public class InputStatsMonitor extends AbstractMonitor
{
  private final InputStats inputStats;
  private long lastReportedValue;
  private final Map<String, String[]> dimensions;

  @Inject
  public InputStatsMonitor(InputStats inputStats)
  {
    this(inputStats, ImmutableMap.of());
  }

  public InputStatsMonitor(InputStats inputStats, Map<String, String[]> dimensions)
  {
    this.inputStats = inputStats;
    this.dimensions = ImmutableMap.copyOf(dimensions);
    this.lastReportedValue = 0;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
    MonitorUtils.addDimensionsToBuilder(builder, dimensions);
    final long currentValue = inputStats.getProcessedBytes().get();
    emitter.emit(builder.build("ingest/events/processedBytes", currentValue - lastReportedValue));
    lastReportedValue = currentValue;
    return true;
  }
}
