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

package org.apache.druid.server.scheduling;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.server.QueryLaningStrategy;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class HiLoQueryLaningStrategyTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private Druids.TimeseriesQueryBuilder queryBuilder;
  private HiLoQueryLaningStrategy strategy;

  @Before
  public void setup()
  {
    this.queryBuilder = Druids.newTimeseriesQueryBuilder()
                              .dataSource("test")
                              .intervals(ImmutableList.of(Intervals.ETERNITY))
                              .granularity(Granularities.DAY)
                              .aggregators(new CountAggregatorFactory("count"));

    this.strategy = new HiLoQueryLaningStrategy(10);
  }

  @Test
  public void testMaxLowThreadsRequired()
  {
    expectedException.expect(NullPointerException.class);
    expectedException.expectMessage("maxLowThreads must be set");
    QueryLaningStrategy strategy = new HiLoQueryLaningStrategy(null);
  }

  @Test
  public void testLaneLimits()
  {
    Object2IntMap<String> laneConfig = strategy.getLaneLimits();
    Assert.assertEquals(1, laneConfig.size());
    Assert.assertTrue(laneConfig.containsKey(HiLoQueryLaningStrategy.LOW));
    Assert.assertEquals(10, laneConfig.getInt(HiLoQueryLaningStrategy.LOW));
  }

  @Test
  public void testLaningNoPriority()
  {
    TimeseriesQuery interactive = queryBuilder.build();
    Assert.assertNull(QueryContexts.getLane(strategy.laneQuery(QueryPlus.wrap(interactive), ImmutableSet.of())));
  }

  @Test
  public void testLaningZeroPriority()
  {
    TimeseriesQuery interactive = queryBuilder.context(ImmutableMap.of(QueryContexts.PRIORITY_KEY, 0)).build();
    Assert.assertNull(QueryContexts.getLane(strategy.laneQuery(QueryPlus.wrap(interactive), ImmutableSet.of())));
  }

  @Test
  public void testLaningInteractivePriority()
  {
    TimeseriesQuery interactive = queryBuilder.context(ImmutableMap.of(QueryContexts.PRIORITY_KEY, 100)).build();
    Assert.assertNull(QueryContexts.getLane(strategy.laneQuery(QueryPlus.wrap(interactive), ImmutableSet.of())));
  }

  @Test
  public void testLaningLowPriority()
  {
    TimeseriesQuery interactive = queryBuilder.context(ImmutableMap.of(QueryContexts.PRIORITY_KEY, -1)).build();
    Assert.assertEquals(
        HiLoQueryLaningStrategy.LOW,
        QueryContexts.getLane(strategy.laneQuery(QueryPlus.wrap(interactive), ImmutableSet.of()))
    );
  }
}
