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

import java.util.Map;

public class FaultyLagAggregator implements LagAggregator
{
  private static final Logger log = new Logger(FaultyLagAggregator.class);

  private final int multiplier;
  private final LagAggregator delegate = LagAggregator.DEFAULT;

  @JsonCreator
  public FaultyLagAggregator(
      @JsonProperty("multiplier") int multiplier
  )
  {
    this.multiplier = multiplier;
  }

  @JsonProperty
  public int getMultiplier()
  {
    return multiplier;
  }

  @Override
  public <PartitionIdType> LagStats aggregate(Map<PartitionIdType, Long> partitionLags)
  {
    log.info("Calculating faulty lags with multiplier[%d]", multiplier);
    LagStats originalAggregate = delegate.aggregate(partitionLags);
    return new LagStats(
        originalAggregate.getMaxLag() * multiplier,
        originalAggregate.getTotalLag() * multiplier,
        originalAggregate.getAvgLag() * multiplier
    );
  }
}
