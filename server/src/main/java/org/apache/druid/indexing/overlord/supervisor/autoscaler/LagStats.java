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

package org.apache.druid.indexing.overlord.supervisor.autoscaler;

public class LagStats
{
  private final long maxLag;
  private final long totalLag;
  private final long avgLag;
  private final AggregateFunction aggregateForScaling;

  public LagStats(long maxLag, long totalLag, long avgLag)
  {
    this(maxLag, totalLag, avgLag, AggregateFunction.SUM);
  }

  public LagStats(long maxLag, long totalLag, long avgLag, AggregateFunction aggregateForScaling)
  {
    this.maxLag = maxLag;
    this.totalLag = totalLag;
    this.avgLag = avgLag;
    this.aggregateForScaling = aggregateForScaling == null ? AggregateFunction.SUM : aggregateForScaling;
  }

  public long getMaxLag()
  {
    return maxLag;
  }

  public long getTotalLag()
  {
    return totalLag;
  }

  public long getAvgLag()
  {
    return avgLag;
  }

  /**
   * The preferred scaling metric that supervisor may specify to be used.
   * This could be overrided by the autscaler.
   */
  public AggregateFunction getAggregateForScaling()
  {
    return aggregateForScaling;
  }

  public long getMetric(AggregateFunction metric)
  {
    switch (metric) {
      case MAX:
        return getMaxLag();
      case SUM:
        return getTotalLag();
      case AVERAGE:
        return getAvgLag();
    }
    throw new IllegalStateException("Unknown scale metric");
  }
}
