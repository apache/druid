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

package org.apache.druid.server.coordinator.loading;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;

/**
 * Contains recomputed configs from {@link CoordinatorDynamicConfig} based on
 * whether {@link CoordinatorDynamicConfig#isSmartSegmentLoading} is enabled or not.
 */
public class SegmentLoadingConfig
{
  private static final Logger log = new Logger(SegmentLoadingConfig.class);

  private final int maxSegmentsInLoadQueue;
  private final int replicationThrottleLimit;
  private final int maxReplicaAssignmentsInRun;
  private final int maxLifetimeInLoadQueue;

  private final int percentDecommSegmentsToMove;
  private final int balancerComputeThreads;

  private final boolean useRoundRobinSegmentAssignment;

  /**
   * Creates a new SegmentLoadingConfig with recomputed coordinator config values
   * based on whether {@link CoordinatorDynamicConfig#isSmartSegmentLoading()}
   * is enabled or not.
   */
  public static SegmentLoadingConfig create(CoordinatorDynamicConfig dynamicConfig, int numUsedSegments)
  {
    if (dynamicConfig.isSmartSegmentLoading()) {
      // Compute replicationThrottleLimit with a lower bound of 100
      final int throttlePercentage = 2;
      final int replicationThrottleLimit = Math.max(100, numUsedSegments * throttlePercentage / 100);
      final int balancerComputeThreads = computeNumBalancerThreads(numUsedSegments);

      log.info(
          "Smart segment loading is enabled. Calculated balancerComputeThreads[%d]"
          + " and replicationThrottleLimit[%,d] (%d%% of used segments[%,d]).",
          balancerComputeThreads, replicationThrottleLimit, throttlePercentage, numUsedSegments
      );

      return new SegmentLoadingConfig(
          0,
          replicationThrottleLimit,
          Integer.MAX_VALUE,
          60,
          100,
          true,
          balancerComputeThreads
      );
    } else {
      // Use the configured values
      return new SegmentLoadingConfig(
          dynamicConfig.getMaxSegmentsInNodeLoadingQueue(),
          dynamicConfig.getReplicationThrottleLimit(),
          dynamicConfig.getMaxNonPrimaryReplicantsToLoad(),
          dynamicConfig.getReplicantLifetime(),
          dynamicConfig.getDecommissioningMaxPercentOfMaxSegmentsToMove(),
          dynamicConfig.isUseRoundRobinSegmentAssignment(),
          dynamicConfig.getBalancerComputeThreads()
      );
    }
  }

  private SegmentLoadingConfig(
      int maxSegmentsInLoadQueue,
      int replicationThrottleLimit,
      int maxReplicaAssignmentsInRun,
      int maxLifetimeInLoadQueue,
      int percentDecommSegmentsToMove,
      boolean useRoundRobinSegmentAssignment,
      int balancerComputeThreads
  )
  {
    this.maxSegmentsInLoadQueue = maxSegmentsInLoadQueue;
    this.replicationThrottleLimit = replicationThrottleLimit;
    this.maxReplicaAssignmentsInRun = maxReplicaAssignmentsInRun;
    this.maxLifetimeInLoadQueue = maxLifetimeInLoadQueue;
    this.percentDecommSegmentsToMove = percentDecommSegmentsToMove;
    this.useRoundRobinSegmentAssignment = useRoundRobinSegmentAssignment;
    this.balancerComputeThreads = balancerComputeThreads;
  }

  public int getMaxSegmentsInLoadQueue()
  {
    return maxSegmentsInLoadQueue;
  }

  public int getReplicationThrottleLimit()
  {
    return replicationThrottleLimit;
  }

  public boolean isUseRoundRobinSegmentAssignment()
  {
    return useRoundRobinSegmentAssignment;
  }

  public int getMaxLifetimeInLoadQueue()
  {
    return maxLifetimeInLoadQueue;
  }

  public int getMaxReplicaAssignmentsInRun()
  {
    return maxReplicaAssignmentsInRun;
  }

  public int getPercentDecommSegmentsToMove()
  {
    return percentDecommSegmentsToMove;
  }

  public int getBalancerComputeThreads()
  {
    return balancerComputeThreads;
  }

  /**
   * Computes the number of threads to be used in the balancing executor.
   * The number of used segments in a cluster is generally a good indicator of
   * the cluster size and has been used here as a proxy for the actual number of
   * segments that would be involved in cost computations.
   * <p>
   * The number of threads increases by 1 first for every 50k segments, then for
   * every 75k segments and so on.
   *
   * @return Number of {@code balancerComputeThreads} in the range [1, 8].
   */
  public static int computeNumBalancerThreads(int numUsedSegments)
  {
    final int[] stepValues = {50, 50, 75, 75, 100, 100, 150, 150};

    int remainder = numUsedSegments / 1000;
    for (int step = 0; step < stepValues.length; ++step) {
      remainder -= stepValues[step];
      if (remainder < 0) {
        return step + 1;
      }
    }

    return stepValues.length;
  }

  /**
   * Calculates {@code maxSegmentsToMove} for the given number of segments in the
   * cluster.
   * <p>
   * Each balancer thread can perform 2 billion computations in every coordinator
   * cycle. Therefore,
   * <pre>
   * numComputations = maxSegmentsToMove * totalSegments
   *
   * maxSegmentsToMove = numComputations / totalSegments
   *                   = (nThreads * 2B) / totalSegments
   * </pre>
   *
   * @param totalSegmentsOnHistoricals Total number of all replicas of all segments
   *                                   loaded or queued across all historicals.
   * @return {@code maxSegmentsToMove} per tier in the range [100, 12.5% of totalSegments].
   */
  public static int computeMaxSegmentsToMove(int totalSegmentsOnHistoricals, int numBalancerThreads)
  {
    // Each thread can do ~2B computations in one cycle
    // = 2M * 1k = 2^21 * 1k (integer overflows here only if numThreads > 1000)
    int computationsInThousands = Math.max(1, numBalancerThreads << 21L);

    // maxSegmentsToMove(in k) = computations(in k) / totalSegments
    int divisor = totalSegmentsOnHistoricals;
    int quotient = computationsInThousands;
    while (divisor > 1) {
      divisor = divisor >> 1;
      quotient = quotient >> 1;
    }

    // Define the bounds for maxSegmentsToMove
    final int lowerBound = 100;
    final int upperBound = totalSegmentsOnHistoricals >> 3;

    if (upperBound < lowerBound) {
      return lowerBound;
    }

    // maxSegmentsToMove must be within [0, 12.5% of numUsedSegments]
    // TODO: handle overflow here, a tad more nicely
    return Math.max(lowerBound, Math.min(upperBound, quotient * 1000));
  }
}
