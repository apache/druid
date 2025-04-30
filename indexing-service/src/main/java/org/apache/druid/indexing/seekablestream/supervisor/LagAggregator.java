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

package org.apache.druid.indexing.seekablestream.supervisor;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.LagStats;

import java.util.Map;

/**
 * Calculates the maximum, average and total values of lag from the values of
 * lag for each stream partition for a given supervisor.
 * <p>
 * This interface is currently needed only to augment the capability of the
 * default implementation {@link DefaultLagAggregator} for testing purposes.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "default", value = LagAggregator.DefaultLagAggregator.class)
})
public interface LagAggregator
{
  LagAggregator DEFAULT = new DefaultLagAggregator();

  <PartitionIdType> LagStats aggregate(Map<PartitionIdType, Long> partitionLags);

  /**
   * Default implementation of LagAggregator which should be used for all
   * production use cases.
   */
  class DefaultLagAggregator implements LagAggregator
  {
    @Override
    public <PartitionIdType> LagStats aggregate(Map<PartitionIdType, Long> partitionLags)
    {
      long maxLag = 0, totalLag = 0, avgLag;
      for (long lag : partitionLags.values()) {
        if (lag > maxLag) {
          maxLag = lag;
        }
        totalLag += lag;
      }
      avgLag = partitionLags.isEmpty() ? 0 : totalLag / partitionLags.size();
      return new LagStats(maxLag, totalLag, avgLag);
    }
  }
}
