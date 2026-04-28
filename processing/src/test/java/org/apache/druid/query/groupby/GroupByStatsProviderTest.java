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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
    stats1.maxMergeBufferUsedBytes(50);
    stats1.spilledBytes(200);
    stats1.spilledBytes(400);
    stats1.dictionarySize(100);
    stats1.dictionarySize(200);

    QueryResourceId id2 = new QueryResourceId("q2");
    GroupByStatsProvider.PerQueryStats stats2 = statsProvider.getPerQueryStatsContainer(id2);

    stats2.mergeBufferAcquisitionTime(500);
    stats2.mergeBufferAcquisitionTime(600);
    stats2.maxMergeBufferUsedBytes(100);
    stats2.spilledBytes(400);
    stats2.spilledBytes(600);
    stats2.dictionarySize(300);
    stats2.dictionarySize(400);

    GroupByStatsProvider.AggregateStats aggregateStats = statsProvider.getStatsSince();
    Assertions.assertEquals(0L, aggregateStats.getMergeBufferQueries());
    Assertions.assertEquals(0L, aggregateStats.getMergeBufferAcquisitionTimeNs());
    Assertions.assertEquals(0L, aggregateStats.getMaxMergeBufferAcquisitionTimeNs());
    Assertions.assertEquals(0L, aggregateStats.getTotalMergeBufferUsedBytes());
    Assertions.assertEquals(0L, aggregateStats.getMaxMergeBufferUsedBytes());
    Assertions.assertEquals(0L, aggregateStats.getSpilledQueries());
    Assertions.assertEquals(0L, aggregateStats.getSpilledBytes());
    Assertions.assertEquals(0L, aggregateStats.getMaxSpilledBytes());
    Assertions.assertEquals(0L, aggregateStats.getMergeDictionarySize());
    Assertions.assertEquals(0L, aggregateStats.getMaxMergeDictionarySize());

    statsProvider.closeQuery(id1);
    statsProvider.closeQuery(id2);

    aggregateStats = statsProvider.getStatsSince();
    Assertions.assertEquals(2, aggregateStats.getMergeBufferQueries());
    Assertions.assertEquals(1800L, aggregateStats.getMergeBufferAcquisitionTimeNs());
    Assertions.assertEquals(1100L, aggregateStats.getMaxMergeBufferAcquisitionTimeNs());
    Assertions.assertEquals(150L, aggregateStats.getTotalMergeBufferUsedBytes());
    Assertions.assertEquals(100L, aggregateStats.getMaxMergeBufferUsedBytes());
    Assertions.assertEquals(2L, aggregateStats.getSpilledQueries());
    Assertions.assertEquals(1600L, aggregateStats.getSpilledBytes());
    Assertions.assertEquals(1000L, aggregateStats.getMaxSpilledBytes());
    Assertions.assertEquals(1000L, aggregateStats.getMergeDictionarySize());
    Assertions.assertEquals(700L, aggregateStats.getMaxMergeDictionarySize());
  }

  @Test
  public void testMetricsWithMultipleQueries()
  {
    GroupByStatsProvider statsProvider = new GroupByStatsProvider();

    QueryResourceId r1 = new QueryResourceId("r1");
    GroupByStatsProvider.PerQueryStats stats1 = statsProvider.getPerQueryStatsContainer(r1);
    stats1.mergeBufferAcquisitionTime(2000);
    stats1.maxMergeBufferUsedBytes(50);
    stats1.spilledBytes(100);
    stats1.dictionarySize(200);

    QueryResourceId r2 = new QueryResourceId("r2");
    GroupByStatsProvider.PerQueryStats stats2 = statsProvider.getPerQueryStatsContainer(r2);
    stats2.mergeBufferAcquisitionTime(100);
    stats2.maxMergeBufferUsedBytes(500);
    stats2.spilledBytes(150);
    stats2.dictionarySize(250);

    QueryResourceId r3 = new QueryResourceId("r3");
    GroupByStatsProvider.PerQueryStats stats3 = statsProvider.getPerQueryStatsContainer(r3);
    stats3.mergeBufferAcquisitionTime(200);
    stats3.maxMergeBufferUsedBytes(100);
    stats3.spilledBytes(3000);
    stats3.dictionarySize(300);

    QueryResourceId r4 = new QueryResourceId("r4");
    GroupByStatsProvider.PerQueryStats stats4 = statsProvider.getPerQueryStatsContainer(r4);
    stats4.mergeBufferAcquisitionTime(300);
    stats4.maxMergeBufferUsedBytes(75);
    stats4.spilledBytes(200);
    stats4.dictionarySize(1500);

    statsProvider.closeQuery(r1);
    statsProvider.closeQuery(r2);
    statsProvider.closeQuery(r3);
    statsProvider.closeQuery(r4);

    GroupByStatsProvider.AggregateStats aggregateStats = statsProvider.getStatsSince();

    Assertions.assertEquals(2000L, aggregateStats.getMaxMergeBufferAcquisitionTimeNs());
    Assertions.assertEquals(500L, aggregateStats.getMaxMergeBufferUsedBytes());
    Assertions.assertEquals(3000L, aggregateStats.getMaxSpilledBytes());
    Assertions.assertEquals(1500L, aggregateStats.getMaxMergeDictionarySize());

    Assertions.assertEquals(4L, aggregateStats.getMergeBufferQueries());
    Assertions.assertEquals(2600L, aggregateStats.getMergeBufferAcquisitionTimeNs());
    Assertions.assertEquals(725L, aggregateStats.getTotalMergeBufferUsedBytes());
    Assertions.assertEquals(4L, aggregateStats.getSpilledQueries());
    Assertions.assertEquals(3450L, aggregateStats.getSpilledBytes());
    Assertions.assertEquals(2250L, aggregateStats.getMergeDictionarySize());
  }
}
