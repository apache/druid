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
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;
import org.apache.druid.query.DruidMetrics;

import java.util.Collection;

@LoadScope(roles = NodeRole.OVERLORD_JSON_NAME)
public class SupervisorStatsMonitor extends AbstractMonitor
{
  private final SupervisorStatsProvider statsProvider;

  @Inject
  public SupervisorStatsMonitor(
      SupervisorStatsProvider statsProvider
  )
  {
    this.statsProvider = statsProvider;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    Collection<SupervisorStatsProvider.SupervisorStats> stats = statsProvider.getSupervisorStats();
    if (stats == null) {
      return true;
    }

    for (SupervisorStatsProvider.SupervisorStats stat : stats) {
      emitter.emit(
          ServiceMetricEvent.builder()
                            .setDimension(DruidMetrics.SUPERVISOR_ID, stat.getSupervisorId())
                            .setDimension(DruidMetrics.TYPE, stat.getType())
                            .setDimension("state", stat.getState())
                            .setDimension(DruidMetrics.DATASOURCE, stat.getDataSource())
                            .setDimensionIfNotNull(DruidMetrics.STREAM, stat.getStream())
                            .setDimensionIfNotNull("detailedState", stat.getDetailedState())
                            .setMetric("supervisor/count", 1)
      );
    }

    return true;
  }
}
