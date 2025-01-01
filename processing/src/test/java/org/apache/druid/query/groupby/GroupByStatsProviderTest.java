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

import org.apache.druid.query.QueryResourceId;
import org.junit.Assert;
import org.junit.Test;

public class GroupByStatsProviderTest
{
  @Test
  public void testMetricCollection()
  {
    GroupByStatsProvider statsProvider = new GroupByStatsProvider();

    QueryResourceId id1 = new QueryResourceId("q1");
    GroupByStatsProvider.PerQueryStats stats1 = statsProvider.getPerQueryStatsContainer(id1);

    stats1.mergeBufferAcquisitionTime(300);
    stats1.mergeBufferAcquisitionTime(400);
    stats1.spilledBytes(200);
    stats1.spilledBytes(400);
    stats1.dictionarySize(100);
    stats1.dictionarySize(200);

    QueryResourceId id2 = new QueryResourceId("q2");
    GroupByStatsProvider.PerQueryStats stats2 = statsProvider.getPerQueryStatsContainer(id2);

    stats2.mergeBufferAcquisitionTime(500);
    stats2.mergeBufferAcquisitionTime(600);
    stats2.spilledBytes(400);
    stats2.spilledBytes(600);
    stats2.dictionarySize(300);
    stats2.dictionarySize(400);

    GroupByStatsProvider.AggregateStats aggregateStats = statsProvider.getStatsSince();
    Assert.assertEquals(0L, aggregateStats.getMergeBufferQueries());
    Assert.assertEquals(0L, aggregateStats.getMergeBufferAcquisitionTimeNs());
    Assert.assertEquals(0L, aggregateStats.getSpilledQueries());
    Assert.assertEquals(0L, aggregateStats.getSpilledBytes());
    Assert.assertEquals(0L, aggregateStats.getMergeDictionarySize());

    statsProvider.closeQuery(id1);
    statsProvider.closeQuery(id2);

    aggregateStats = statsProvider.getStatsSince();
    Assert.assertEquals(2, aggregateStats.getMergeBufferQueries());
    Assert.assertEquals(1800L, aggregateStats.getMergeBufferAcquisitionTimeNs());
    Assert.assertEquals(2L, aggregateStats.getSpilledQueries());
    Assert.assertEquals(1600L, aggregateStats.getSpilledBytes());
    Assert.assertEquals(1000L, aggregateStats.getMergeDictionarySize());
  }
}
