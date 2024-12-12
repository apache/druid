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

import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.balancer.SegmentToMoveCalculator;
import org.apache.druid.server.coordinator.balancer.TierSegmentBalancer;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Stats;
import org.joda.time.Duration;

import java.util.Set;

/**
 * Coordinator Duty to balance segments across Historicals.
 */
public class BalanceSegments implements CoordinatorDuty
{
  private static final Logger log = new Logger(BalanceSegments.class);

  private final Duration coordinatorPeriod;

  public BalanceSegments(Duration coordinatorPeriod)
  {
    this.coordinatorPeriod = coordinatorPeriod;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    if (params.getUsedSegmentCount() <= 0) {
      return params;
    }

    final int maxSegmentsToMove = getMaxSegmentsToMove(params);
    params.getCoordinatorStats().add(Stats.Balancer.MAX_TO_MOVE, maxSegmentsToMove);
    if (maxSegmentsToMove <= 0) {
      return params;
    }

    params.getDruidCluster().getHistoricals().forEach(
        (tier, servers) -> new TierSegmentBalancer(tier, servers, maxSegmentsToMove, params).run()
    );

    CoordinatorRunStats runStats = params.getCoordinatorStats();
    params.getBalancerStrategy()
          .getStats()
          .forEachStat(runStats::add);

    return params;
  }

  /**
   * Recomputes the value of {@code maxSegmentsToMove} if smart segment loading
   * is enabled. {@code maxSegmentsToMove} defines only the upper bound, the actual
   * number of segments picked for moving is determined by the {@link TierSegmentBalancer}
   * based on the level of skew in the tier.
   */
  private int getMaxSegmentsToMove(DruidCoordinatorRuntimeParams params)
  {
    final CoordinatorDynamicConfig dynamicConfig = params.getCoordinatorDynamicConfig();
    if (dynamicConfig.isSmartSegmentLoading()) {
      final Pair<Integer, Integer> numHistoricalsAndSegments = getNumHistoricalsAndSegments(params.getDruidCluster());
      final int totalSegmentsInCluster = numHistoricalsAndSegments.rhs;

      final int numBalancerThreads = params.getSegmentLoadingConfig().getBalancerComputeThreads();
      final int maxSegmentsToMove = SegmentToMoveCalculator
          .computeMaxSegmentsToMovePerTier(totalSegmentsInCluster, numBalancerThreads, coordinatorPeriod);
      log.debug(
          "Computed maxSegmentsToMove[%,d] for total [%,d] segments on [%d] historicals.",
          maxSegmentsToMove, totalSegmentsInCluster, numHistoricalsAndSegments.lhs
      );

      return maxSegmentsToMove;
    } else {
      return dynamicConfig.getMaxSegmentsToMove();
    }
  }

  /**
   * Calculates the total number of historicals (active and decommissioning) and
   * the total number of segments on these historicals that would participate in
   * cost computations. This includes all replicas of all loaded, loading, dropping
   * and moving segments.
   * <p>
   * This is calculated here to ensure that all assignments done by the preceding
   * {@link RunRules} duty are accounted for.
   */
  private Pair<Integer, Integer> getNumHistoricalsAndSegments(DruidCluster cluster)
  {
    int numHistoricals = 0;
    int numSegments = 0;

    for (Set<ServerHolder> historicals : cluster.getHistoricals().values()) {
      for (ServerHolder historical : historicals) {
        ++numHistoricals;
        numSegments += historical.getServer().getNumSegments() + historical.getNumQueuedSegments();
      }
    }

    return Pair.of(numHistoricals, numSegments);
  }

}
