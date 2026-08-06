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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("ResultOfObjectAllocationIgnored")
public class ManualQueryLaningStrategyTest
{
  private Druids.TimeseriesQueryBuilder queryBuilder;
  private QueryLaningStrategy exactStrategy;
  private QueryLaningStrategy percentStrategy;

  @BeforeEach
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
    Throwable exception = org.junit.jupiter.api.Assertions.assertThrows(NullPointerException.class, () -> {
      new ManualQueryLaningStrategy(null, null);
    });
    assertTrue(exception.getMessage().contains("lanes must be set"));
  }

  @Test
  public void testMustDefineAtLeastOneLane()
  {
    Throwable exception = org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> {
      new ManualQueryLaningStrategy(ImmutableMap.of(), null);
    });
    assertTrue(exception.getMessage().contains("lanes must define at least one lane"));
  }

  @Test
  public void testMustNotUseTotalName()
  {
    Throwable exception = org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> {
      new ManualQueryLaningStrategy(ImmutableMap.of(QueryScheduler.TOTAL, 12), null);
    });
    assertTrue(exception.getMessage().contains("Lane cannot be named 'total'"));
  }

  @Test
  public void testMustNotUseDefaultName()
  {
    Throwable exception = org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> {
      new ManualQueryLaningStrategy(ImmutableMap.of("default", 12), null);
    });
    assertTrue(exception.getMessage().contains("Lane cannot be named 'default'"));
  }

  @Test
  public void testExactLaneLimitsMustBeAboveZero()
  {
    Throwable exception = org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> {
      new ManualQueryLaningStrategy(ImmutableMap.of("zero", 0, "one", 1), null);
    });
    assertTrue(exception.getMessage().contains("All lane limits must be greater than 0"));
  }

  @Test
  public void testPercentLaneLimitsMustBeAboveZero()
  {
    Throwable exception = org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> {
      new ManualQueryLaningStrategy(ImmutableMap.of("zero", 0, "one", 25), true);
    });
    assertTrue(exception.getMessage().contains("All lane limits must be in the range 1 to 100"));
  }

  @Test
  public void testPercentLaneLimitsMustBeLessThanOneHundred()
  {
    Throwable exception = org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> {
      new ManualQueryLaningStrategy(ImmutableMap.of("one", 1, "one-hundred-and-one", 101), true);
    });
    assertTrue(exception.getMessage().contains("All lane limits must be in the range 1 to 100"));
  }

  @Test
  public void testExactLimits()
  {
    Object2IntMap<String> exactLanes = exactStrategy.getLaneLimits(50);
    Assertions.assertEquals(1, exactLanes.getInt("one"));
    Assertions.assertEquals(10, exactLanes.getInt("ten"));
  }

  @Test
  public void testPercentLimits()
  {
    Object2IntMap<String> exactLanes = percentStrategy.getLaneLimits(50);
    Assertions.assertEquals(1, exactLanes.getInt("one"));
    Assertions.assertEquals(5, exactLanes.getInt("ten"));
    Assertions.assertEquals(50, exactLanes.getInt("one-hundred"));
  }

  @Test
  public void testDoesntSetLane()
  {
    TimeseriesQuery query = queryBuilder.context(ImmutableMap.of()).build();
    Assertions.assertFalse(exactStrategy.computeLane(QueryPlus.wrap(query), ImmutableSet.of()).isPresent());
    Assertions.assertFalse(percentStrategy.computeLane(QueryPlus.wrap(query), ImmutableSet.of()).isPresent());
  }

  @Test
  public void testPreservesManualLaneFromContextThatArentInMapAndIgnoresThem()
  {
    final String someLane = "some-lane";
    TimeseriesQuery query = queryBuilder.context(ImmutableMap.of(QueryContexts.LANE_KEY, someLane)).build();
    Assertions.assertEquals(
        someLane,
        exactStrategy.computeLane(QueryPlus.wrap(query), ImmutableSet.of()).get()
    );
    Assertions.assertEquals(
        someLane,
        percentStrategy.computeLane(QueryPlus.wrap(query), ImmutableSet.of()).get()
    );
  }

  @Test
  public void testPreservesManualLaneFromContext()
  {
    final String someLane = "ten";
    TimeseriesQuery query = queryBuilder.context(ImmutableMap.of(QueryContexts.LANE_KEY, someLane)).build();
    Assertions.assertEquals(
        someLane,
        exactStrategy.computeLane(QueryPlus.wrap(query), ImmutableSet.of()).get()
    );
    Assertions.assertEquals(
        someLane,
        percentStrategy.computeLane(QueryPlus.wrap(query), ImmutableSet.of()).get()
    );
  }
}
