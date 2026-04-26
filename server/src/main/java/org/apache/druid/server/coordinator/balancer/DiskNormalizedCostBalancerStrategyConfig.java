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
   * Minimum fractional cost reduction required to move a segment off a server
   * that is already projected to hold it. For example, a value of {@code 0.05} means the
   * destination must be at least 5% cheaper than the source before a move
   * fires.
   */
  @JsonProperty
  private final double moveCostSavingsThreshold;

  public DiskNormalizedCostBalancerStrategyConfig()
  {
    this(null);
  }

  @JsonCreator
  public DiskNormalizedCostBalancerStrategyConfig(
      @JsonProperty("moveCostSavingsThreshold") @Nullable Double moveCostSavingsThreshold
  )
  {
    this.moveCostSavingsThreshold = Configs.valueOrDefault(moveCostSavingsThreshold, DiskNormalizedCostBalancerStrategy.DEFAULT_MOVE_COST_SAVINGS_THRESHOLD);

    Preconditions.checkArgument(
        this.moveCostSavingsThreshold >= 0.0 && this.moveCostSavingsThreshold < 1.0,
        "'druid.coordinator.balancer.diskNormalized.moveCostSavingsThreshold'[%s] must be in [0.0, 1.0)",
        this.moveCostSavingsThreshold
    );
  }

  public double getMoveCostSavingsThreshold()
  {
    return moveCostSavingsThreshold;
  }
}
