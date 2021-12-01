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

package org.apache.druid.server.coordinator.duty;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;

/**
 * Emit stats and metrics for the Primary Replicant Loader that can optionally be enabled on a cluster.
 */
public class EmitPrimaryReplicantLoaderStatsAndMetrics extends EmitClusterStatsAndMetrics
{
  private static final Logger log = new Logger(EmitClusterStatsAndMetrics.class);

  public EmitPrimaryReplicantLoaderStatsAndMetrics(DruidCoordinator coordinator)
  {
    super(coordinator);
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    DruidCluster cluster = params.getDruidCluster();
    CoordinatorStats stats = params.getCoordinatorStats();
    ServiceEmitter emitter = params.getEmitter();

    stats.forEachTieredStat(
        "assignedCount",
        (final String tier, final long count) -> {
          log.info(
              "[%s] : Assigned %s segments among %,d servers",
              tier,
              count,
              cluster.getHistoricalsByTier(tier).size()
          );

          emitTieredStat(emitter, "segment/assigned/count", tier, count);
        }
    );
    return params;
  }
}
