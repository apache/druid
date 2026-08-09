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

import javax.annotation.Nullable;

public class IntervalAwareBalancerStrategyFactory extends BalancerStrategyFactory
{
  /**
   * Whether to balance the segment count per interval independently for each
   * datasource ({@code true}, default) or across all datasources ({@code false}).
   */
  private final boolean perDatasource;

  @JsonCreator
  public IntervalAwareBalancerStrategyFactory(
      @JsonProperty("perDatasource") @Nullable Boolean perDatasource
  )
  {
    this.perDatasource = perDatasource == null || perDatasource;
  }

  @JsonProperty
  public boolean isPerDatasource()
  {
    return perDatasource;
  }

  @Override
  public BalancerStrategy createBalancerStrategy(int numBalancerThreads)
  {
    return new IntervalAwareBalancerStrategy(perDatasource);
  }
}
