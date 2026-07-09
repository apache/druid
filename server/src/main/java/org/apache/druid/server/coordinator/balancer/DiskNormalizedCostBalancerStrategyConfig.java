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

package org.apache.druid.server.coordinator.balancer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.common.config.Configs;

import javax.annotation.Nullable;

/**
 * Configuration for {@link DiskNormalizedCostBalancerStrategy}.
 * <p>
 * Bound to the prefix
 * {@code druid.coordinator.balancer.diskNormalized}.
 */
public class DiskNormalizedCostBalancerStrategyConfig
{
  /**
   * Default projected disk utilization difference that the strategy ignores
   * before applying the exponential utilization penalty. A value of
   * {@code 0.05} means servers within 5 percentage points of the least-used
   * candidate are ordered by the normal cost strategy.
   */
  static final double DEFAULT_UTILIZATION_THRESHOLD = 0.05;

  /**
   * Projected disk utilization difference that the strategy ignores before
   * applying the exponential utilization penalty. For example, a value of
   * {@code 0.05} means servers within 5 percentage points of the least-used
   * candidate are ordered by the normal cost strategy.
   */
  @JsonProperty
  private final double utilizationThreshold;

  public DiskNormalizedCostBalancerStrategyConfig()
  {
    this(null);
  }

  @JsonCreator
  public DiskNormalizedCostBalancerStrategyConfig(
      @JsonProperty("utilizationThreshold") @Nullable Double utilizationThreshold
  )
  {
    this.utilizationThreshold = Configs.valueOrDefault(utilizationThreshold, DEFAULT_UTILIZATION_THRESHOLD);

    Preconditions.checkArgument(
        this.utilizationThreshold > 0.0 && this.utilizationThreshold < 1.0,
        "'druid.coordinator.balancer.diskNormalized.utilizationThreshold'[%s] must be in (0.0, 1.0)",
        this.utilizationThreshold
    );
  }

  public double getUtilizationThreshold()
  {
    return utilizationThreshold;
  }
}
