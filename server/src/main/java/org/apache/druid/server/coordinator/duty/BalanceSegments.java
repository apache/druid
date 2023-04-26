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

import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.StrategicSegmentAssigner;
import org.apache.druid.server.coordinator.balancer.TierSegmentBalancer;
import org.apache.druid.server.coordinator.loadqueue.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;

/**
 *
 */
public class BalanceSegments implements CoordinatorDuty
{
  private static final EmittingLogger log = new EmittingLogger(BalanceSegments.class);
  private final SegmentLoadQueueManager loadQueueManager;

  public BalanceSegments(SegmentLoadQueueManager loadQueueManager)
  {
    this.loadQueueManager = loadQueueManager;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    if (params.getUsedSegments().isEmpty()) {
      log.info("Skipping balance as there are no used segments.");
      return params;
    }

    final DruidCluster cluster = params.getDruidCluster();
    final CoordinatorDynamicConfig dynamicConfig = params.getCoordinatorDynamicConfig();
    final int maxSegmentsToMove = dynamicConfig.getMaxSegmentsToMove();
    if (maxSegmentsToMove <= 0) {
      log.info("Skipping balance as maxSegmentsToMove is [%d].", maxSegmentsToMove);
      return params;
    } else {
      log.info(
          "Balancing segments in tiers [%s] with maxSegmentsToMove=[%d], maxLifetime=[%d].",
          cluster.getTierNames(), maxSegmentsToMove, dynamicConfig.getReplicantLifetime()
      );
    }

    final StrategicSegmentAssigner segmentAssigner = new StrategicSegmentAssigner(
        loadQueueManager,
        params.getDruidCluster(),
        params.getSegmentReplicantLookup(),
        params.getReplicationManager(),
        params.getBalancerStrategy(),
        dynamicConfig
    );

    cluster.getHistoricals().forEach(
        (tier, servers) ->
            new TierSegmentBalancer(tier, servers, segmentAssigner, params).run()
    );

    segmentAssigner.makeAlerts();
    final CoordinatorRunStats stats = segmentAssigner.getStats();
    stats.logStatsAndErrors(log);

    return params.buildFromExisting().withCoordinatorStats(stats).build();
  }

}
