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

package org.apache.druid.query.groupby;

import org.junit.Assert;
import org.junit.Test;

public class GroupByStatsProviderTest
{
  @Test
  public void testAggregateStatsFromQueryMetrics()
  {
    GroupByStatsProvider statsProvider = new GroupByStatsProvider();

    DefaultGroupByQueryMetrics metricsWithAllStats = new DefaultGroupByQueryMetrics();
    metricsWithAllStats.mergeBufferAcquisitionTime(100L);
    metricsWithAllStats.bytesSpilledToStorage(2_048L);
    metricsWithAllStats.mergeDictionarySize(10L);

    DefaultGroupByQueryMetrics metricsWithPartialStats = new DefaultGroupByQueryMetrics();
    metricsWithPartialStats.bytesSpilledToStorage(1_024L);
    metricsWithPartialStats.mergeDictionarySize(5L);

    statsProvider.aggregateStats(metricsWithAllStats);
    statsProvider.aggregateStats(metricsWithPartialStats);

    GroupByStatsProvider.AggregateStats stats = statsProvider.getStatsSince();

    Assert.assertEquals(1, stats.getMergeBufferQueries());
    Assert.assertEquals(100L, stats.getMergeBufferAcquisitionTimeNs());
    Assert.assertEquals(2, stats.getSpilledQueries());
    Assert.assertEquals(3_072L, stats.getSpilledBytes());
    Assert.assertEquals(15L, stats.getMergeDictionarySize());
  }

  @Test
  public void testGetStatsSinceResetsCounters()
  {
    GroupByStatsProvider statsProvider = new GroupByStatsProvider();

    DefaultGroupByQueryMetrics metrics = new DefaultGroupByQueryMetrics();
    metrics.bytesSpilledToStorage(512L);
    metrics.mergeDictionarySize(7L);
    statsProvider.aggregateStats(metrics);

    Assert.assertEquals(512L, statsProvider.getStatsSince().getSpilledBytes());

    GroupByStatsProvider.AggregateStats reset = statsProvider.getStatsSince();
    Assert.assertEquals(0L, reset.getMergeBufferQueries());
    Assert.assertEquals(0L, reset.getSpilledQueries());
    Assert.assertEquals(0L, reset.getSpilledBytes());
    Assert.assertEquals(0L, reset.getMergeDictionarySize());
  }
}
