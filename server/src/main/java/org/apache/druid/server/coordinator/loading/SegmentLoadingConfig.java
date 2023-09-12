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
  private final int maxLifetimeInLoadQueue;

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
      final int throttlePercentage = 5;
      final int replicationThrottleLimit = Math.max(100, numUsedSegments * throttlePercentage / 100);
      final int numBalancerThreads = CoordinatorDynamicConfig.getDefaultBalancerComputeThreads();
      log.info(
          "Smart segment loading is enabled. Calculated replicationThrottleLimit[%,d]"
          + " (%d%% of used segments[%,d]) and numBalancerThreads[%d].",
          replicationThrottleLimit, throttlePercentage, numUsedSegments, numBalancerThreads
      );

      return new SegmentLoadingConfig(
          0,
          replicationThrottleLimit,
          60,
          true,
          numBalancerThreads
      );
    } else {
      // Use the configured values
      return new SegmentLoadingConfig(
          dynamicConfig.getMaxSegmentsInNodeLoadingQueue(),
          dynamicConfig.getReplicationThrottleLimit(),
          dynamicConfig.getReplicantLifetime(),
          dynamicConfig.isUseRoundRobinSegmentAssignment(),
          dynamicConfig.getBalancerComputeThreads()
      );
    }
  }

  private SegmentLoadingConfig(
      int maxSegmentsInLoadQueue,
      int replicationThrottleLimit,
      int maxLifetimeInLoadQueue,
      boolean useRoundRobinSegmentAssignment,
      int balancerComputeThreads
  )
  {
    this.maxSegmentsInLoadQueue = maxSegmentsInLoadQueue;
    this.replicationThrottleLimit = replicationThrottleLimit;
    this.maxLifetimeInLoadQueue = maxLifetimeInLoadQueue;
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

  public int getBalancerComputeThreads()
  {
    return balancerComputeThreads;
  }
}
