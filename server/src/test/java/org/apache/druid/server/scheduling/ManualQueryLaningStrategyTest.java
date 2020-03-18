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
import org.apache.druid.server.QueryScheduler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

@SuppressWarnings("ResultOfObjectAllocationIgnored")
public class ManualQueryLaningStrategyTest
{
  private Druids.TimeseriesQueryBuilder queryBuilder;
  private QueryLaningStrategy exactStrategy;
  private QueryLaningStrategy percentStrategy;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setup()
  {
    this.queryBuilder = Druids.newTimeseriesQueryBuilder()
                              .dataSource("test")
                              .intervals(ImmutableList.of(Intervals.ETERNITY))
                              .granularity(Granularities.DAY)
                              .aggregators(new CountAggregatorFactory("count"));
    this.exactStrategy =
        new ManualQueryLaningStrategy(ImmutableMap.of("one", 1, "ten", 10), null);
    this.percentStrategy =
        new ManualQueryLaningStrategy(ImmutableMap.of("one", 1, "ten", 10, "one-hundred", 100), true);
  }

  @Test
  public void testLanesMustBeSet()
  {
    expectedException.expect(NullPointerException.class);
    expectedException.expectMessage("lanes must be set");
    new ManualQueryLaningStrategy(null, null);
  }

  @Test
  public void testMustDefineAtLeastOneLane()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("lanes must define at least one lane");
    new ManualQueryLaningStrategy(ImmutableMap.of(), null);
  }

  @Test
  public void testMustNotUseTotalName()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Lane cannot be named 'total'");
    new ManualQueryLaningStrategy(ImmutableMap.of(QueryScheduler.TOTAL, 12), null);
  }

  @Test
  public void testMustNotUseDefaultName()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Lane cannot be named 'default'");
    new ManualQueryLaningStrategy(ImmutableMap.of("default", 12), null);
  }

  @Test
  public void testExactLaneLimitsMustBeAboveZero()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("All lane limits must be greater than 0");
    new ManualQueryLaningStrategy(ImmutableMap.of("zero", 0, "one", 1), null);
  }

  @Test
  public void testPercentLaneLimitsMustBeAboveZero()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("All lane limits must be in the range 1 to 100");
    new ManualQueryLaningStrategy(ImmutableMap.of("zero", 0, "one", 25), true);
  }

  @Test
  public void testPercentLaneLimitsMustBeLessThanOneHundred()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("All lane limits must be in the range 1 to 100");
    new ManualQueryLaningStrategy(ImmutableMap.of("one", 1, "one-hundred-and-one", 101), true);
  }

  @Test
  public void testExactLimits()
  {
    Object2IntMap<String> exactLanes = exactStrategy.getLaneLimits(50);
    Assert.assertEquals(1, exactLanes.getInt("one"));
    Assert.assertEquals(10, exactLanes.getInt("ten"));
  }

  @Test
  public void testPercentLimits()
  {
    Object2IntMap<String> exactLanes = percentStrategy.getLaneLimits(50);
    Assert.assertEquals(1, exactLanes.getInt("one"));
    Assert.assertEquals(5, exactLanes.getInt("ten"));
    Assert.assertEquals(50, exactLanes.getInt("one-hundred"));
  }

  @Test
  public void testDoesntSetLane()
  {
    TimeseriesQuery query = queryBuilder.context(ImmutableMap.of()).build();
    Assert.assertFalse(exactStrategy.computeLane(QueryPlus.wrap(query), ImmutableSet.of()).isPresent());
    Assert.assertFalse(percentStrategy.computeLane(QueryPlus.wrap(query), ImmutableSet.of()).isPresent());
  }

  @Test
  public void testPreservesManualLaneFromContextThatArentInMapAndIgnoresThem()
  {
    final String someLane = "some-lane";
    TimeseriesQuery query = queryBuilder.context(ImmutableMap.of(QueryContexts.LANE_KEY, someLane)).build();
    Assert.assertEquals(
        someLane,
        exactStrategy.computeLane(QueryPlus.wrap(query), ImmutableSet.of()).get()
    );
    Assert.assertEquals(
        someLane,
        percentStrategy.computeLane(QueryPlus.wrap(query), ImmutableSet.of()).get()
    );
  }

  @Test
  public void testPreservesManualLaneFromContext()
  {
    final String someLane = "ten";
    TimeseriesQuery query = queryBuilder.context(ImmutableMap.of(QueryContexts.LANE_KEY, someLane)).build();
    Assert.assertEquals(
        someLane,
        exactStrategy.computeLane(QueryPlus.wrap(query), ImmutableSet.of()).get()
    );
    Assert.assertEquals(
        someLane,
        percentStrategy.computeLane(QueryPlus.wrap(query), ImmutableSet.of()).get()
    );
  }
}
