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

package org.apache.druid.testing.cluster.overlord;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.LagStats;
import org.apache.druid.indexing.seekablestream.supervisor.LagAggregator;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of {@link LagAggregator} that supports the following:
 * <ul>
 * <li>Specify a {@code multiplier} to amplify the lag observed by the Overlord
 * for a given supervisor.</li>
 * <li>Support externally controllable lag injection for testing auto-scaling scenarios.</li>
 * </ul>
 */
public class FaultyLagAggregator implements LagAggregator
{
  private static final Logger log = new Logger(FaultyLagAggregator.class);

  // Global registry for external control from tests
  private static final ConcurrentHashMap<String, Long> INJECTED_LAG_REGISTRY = new ConcurrentHashMap<>();

  private final int lagMultiplier;
  private final String clientId;
  private final LagAggregator delegate = LagAggregator.DEFAULT;

  public FaultyLagAggregator(int lagMultiplier)
  {
    this.lagMultiplier = lagMultiplier;
    this.clientId = null;
    log.info("Multiplying lags by factor[%d].", lagMultiplier);
  }

  @JsonCreator
  public FaultyLagAggregator(
      @JsonProperty("lagMultiplier") int lagMultiplier,
      @JsonProperty("clientId") @Nullable String clientId
  )
  {
    this.lagMultiplier = lagMultiplier;
    this.clientId = clientId;

    if (clientId != null) {
      log.info("Controllable faulty lag aggregator with extenral ID[%s]", clientId);
    } else {
      log.info("Multiplying lags by factor[%d].", lagMultiplier);
    }
  }

  @JsonProperty
  public int getLagMultiplier()
  {
    return lagMultiplier;
  }

  @JsonProperty
  @Nullable
  public String getClientId()
  {
    return clientId;
  }

  @Override
  public <PartitionIdType> LagStats aggregate(Map<PartitionIdType, Long> partitionLags)
  {
    if (clientId != null && INJECTED_LAG_REGISTRY.containsKey(clientId)) {
      return aggregateWithInjectedLag(partitionLags);
    } else {
      return aggregateWithMultiplier(partitionLags);
    }
  }

  private <PartitionIdType> LagStats aggregateWithInjectedLag(Map<PartitionIdType, Long> partitionLags)
  {
    long injectedLag = INJECTED_LAG_REGISTRY.get(clientId);

    log.debug("Using injected lag[%d] for controller[%s]", injectedLag, clientId);

    // Return fixed lag values regardless of actual partition lags
    long totalLag = partitionLags.isEmpty() ? injectedLag : injectedLag * partitionLags.size();

    return new LagStats(injectedLag, totalLag, injectedLag);
  }

  private <PartitionIdType> LagStats aggregateWithMultiplier(Map<PartitionIdType, Long> partitionLags)
  {
    LagStats originalAggregate = delegate.aggregate(partitionLags);
    return new LagStats(
        originalAggregate.getMaxLag() * getLagMultiplier(),
        originalAggregate.getTotalLag() * getLagMultiplier(),
        originalAggregate.getAvgLag() * getLagMultiplier()
    );
  }

  public static void injectLag(String controllerId, long lag)
  {
    log.info("Injecting lag[%d] for controller[%s]", lag, controllerId);
    INJECTED_LAG_REGISTRY.put(controllerId, lag);
  }

  public static void clearAllInjectedLag()
  {
    log.info("Clearing all injected lag");
    INJECTED_LAG_REGISTRY.clear();
  }
}
