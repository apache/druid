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
import org.apache.druid.server.coordinator.SegmentLoader;
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
    reduceLifetimesAndAlert();

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

    final SegmentLoader loader = new SegmentLoader(
        loadQueueManager,
        params.getDruidCluster(),
        params.getSegmentReplicantLookup(),
        params.getReplicationManager(),
        params.getBalancerStrategy(),
        dynamicConfig.isUseRoundRobinSegmentAssignment()
    );

    cluster.getHistoricals().forEach(
        (tier, servers) ->
            new TierSegmentBalancer(tier, servers, loader, params).run()
    );

    loader.makeAlerts();
    final CoordinatorRunStats stats = loader.getStats();
    stats.forEachRow(
        (row, statValues) -> log.info("Stats for row[%s] are [%s]", row, statValues)
    );

    return params.buildFromExisting().withCoordinatorStats(stats).build();
  }

  /**
   * Reduces the lifetimes of segments currently being moved in all the tiers.
   * Raises alerts for segments stuck in the queue.
   */
  private void reduceLifetimesAndAlert()
  {
    loadQueueManager.reduceLifetimesOfMovingSegments().forEach((tier, movingState) -> {
      int numMovingSegments = movingState.getNumProcessingSegments();
      if (numMovingSegments <= 0) {
        return;
      }

      // Create alerts for stuck tiers
      if (movingState.getMinLifetime() <= 0) {
        log.makeAlert(
            "Balancing queue for tier [%s] has [%d] segments stuck.",
            tier,
            movingState.getNumExpiredSegments()
        ).addData("segments", movingState.getExpiredSegments()).emit();
      }
    });
  }
}
