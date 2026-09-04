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
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.server.QueryLaningStrategy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class HiLoQueryLaningStrategyTest
{

  private Druids.TimeseriesQueryBuilder queryBuilder;
  private HiLoQueryLaningStrategy strategy;

  @BeforeEach
  public void setup()
  {
    this.queryBuilder = Druids.newTimeseriesQueryBuilder()
                              .dataSource("test")
                              .intervals(ImmutableList.of(Intervals.ETERNITY))
                              .granularity(Granularities.DAY)
                              .aggregators(new CountAggregatorFactory("count"));

    this.strategy = new HiLoQueryLaningStrategy(40);
  }

  @Test
  public void testMaxPercentageThreadsRequired()
  {
    Throwable exception = Assertions.assertThrows(NullPointerException.class, () -> {
      new HiLoQueryLaningStrategy(null);
    });
    Assertions.assertTrue(exception.getMessage().contains("maxLowPercent must be set"));
  }

  @Test
  public void testMaxLowPercentMustBeGreaterThanZero()
  {
    Throwable exception = Assertions.assertThrows(IllegalArgumentException.class, () -> {
      new HiLoQueryLaningStrategy(-1);
    });
    Assertions.assertTrue(exception.getMessage().contains("maxLowPercent must be in the range 1 to 100"));
  }


  @Test
  public void testMaxLowPercentMustBeLessThanOrEqual100()
  {
    Throwable exception = Assertions.assertThrows(IllegalArgumentException.class, () -> {
      new HiLoQueryLaningStrategy(9000);
    });
    Assertions.assertTrue(exception.getMessage().contains("maxLowPercent must be in the range 1 to 100"));
  }

  @Test
  public void testMaxLowPercentZero()
  {
    Throwable exception = Assertions.assertThrows(IllegalArgumentException.class, () -> {
      new HiLoQueryLaningStrategy(0);
    });
    Assertions.assertTrue(exception.getMessage().contains("maxLowPercent must be in the range 1 to 100"));
  }

  @Test
  public void testMaxLowPercent100()
  {
    QueryLaningStrategy strategy = new HiLoQueryLaningStrategy(100);
    Object2IntMap<String> laneConfig = strategy.getLaneLimits(25);
    Assertions.assertEquals(1, laneConfig.size());
    Assertions.assertTrue(laneConfig.containsKey(HiLoQueryLaningStrategy.LOW));
    Assertions.assertEquals(25, laneConfig.getInt(HiLoQueryLaningStrategy.LOW));
  }

  @Test
  public void testMaxLowPercentRoundsUp()
  {
    // will round up to 1
    QueryLaningStrategy strategyRoundLow = new HiLoQueryLaningStrategy(1);
    Object2IntMap<String> laneConfigRoundLow = strategyRoundLow.getLaneLimits(25);
    Assertions.assertEquals(1, laneConfigRoundLow.size());
    Assertions.assertTrue(laneConfigRoundLow.containsKey(HiLoQueryLaningStrategy.LOW));
    Assertions.assertEquals(1, laneConfigRoundLow.getInt(HiLoQueryLaningStrategy.LOW));

    // will not round, evenly divides
    QueryLaningStrategy strategy = new HiLoQueryLaningStrategy(96);
    Object2IntMap<String> laneConfig = strategy.getLaneLimits(25);
    Assertions.assertEquals(1, laneConfig.size());
    Assertions.assertTrue(laneConfig.containsKey(HiLoQueryLaningStrategy.LOW));
    Assertions.assertEquals(24, laneConfig.getInt(HiLoQueryLaningStrategy.LOW));

    // will round up
    QueryLaningStrategy strategyRounded = new HiLoQueryLaningStrategy(97);
    Object2IntMap<String> laneConfigRounded = strategyRounded.getLaneLimits(25);
    Assertions.assertEquals(1, laneConfigRounded.size());
    Assertions.assertTrue(laneConfigRounded.containsKey(HiLoQueryLaningStrategy.LOW));
    Assertions.assertEquals(25, laneConfigRounded.getInt(HiLoQueryLaningStrategy.LOW));
  }

  @Test
  public void testLaneLimits()
  {
    Object2IntMap<String> laneConfig = strategy.getLaneLimits(5);
    Assertions.assertEquals(1, laneConfig.size());
    Assertions.assertTrue(laneConfig.containsKey(HiLoQueryLaningStrategy.LOW));
    Assertions.assertEquals(2, laneConfig.getInt(HiLoQueryLaningStrategy.LOW));
  }

  @Test
  public void testLaningNoPriority()
  {
    TimeseriesQuery query = queryBuilder.build();
    Assertions.assertFalse(strategy.computeLane(QueryPlus.wrap(query), ImmutableSet.of()).isPresent());
  }

  @Test
  public void testLaningExplicitNullPriority()
  {
    TimeseriesQuery query = queryBuilder
        .context(QueryContext.builder().put(QueryContextParameters.PRIORITY, null).toMap())
        .build();
    Assertions.assertFalse(strategy.computeLane(QueryPlus.wrap(query), ImmutableSet.of()).isPresent());
  }

  @Test
  public void testLaningZeroPriority()
  {
    TimeseriesQuery query = queryBuilder.context(QueryContext.ofMap(QueryContextParameters.PRIORITY, 0)).build();
    Assertions.assertFalse(strategy.computeLane(QueryPlus.wrap(query), ImmutableSet.of()).isPresent());
  }

  @Test
  public void testLaningInteractivePriority()
  {
    TimeseriesQuery query = queryBuilder.context(QueryContext.ofMap(QueryContextParameters.PRIORITY, 100)).build();
    Assertions.assertFalse(strategy.computeLane(QueryPlus.wrap(query), ImmutableSet.of()).isPresent());
  }

  @Test
  public void testLaningInteractivePriority_String()
  {
    TimeseriesQuery query = queryBuilder.context(
        QueryContext.builder().putRaw(QueryContextParameters.PRIORITY.getName(), "100").toMap()
    ).build();
    Assertions.assertFalse(strategy.computeLane(QueryPlus.wrap(query), ImmutableSet.of()).isPresent());
  }

  @Test
  public void testLaningLowPriority()
  {
    TimeseriesQuery query = queryBuilder.context(QueryContext.ofMap(QueryContextParameters.PRIORITY, -1)).build();
    Assertions.assertTrue(strategy.computeLane(QueryPlus.wrap(query), ImmutableSet.of()).isPresent());
    Assertions.assertEquals(
        HiLoQueryLaningStrategy.LOW,
        strategy.computeLane(QueryPlus.wrap(query), ImmutableSet.of()).get()
    );
  }

  @Test
  public void testLaningLowPriority_String()
  {
    TimeseriesQuery query = queryBuilder.context(
        QueryContext.builder().putRaw(QueryContextParameters.PRIORITY.getName(), "-1").toMap()
    ).build();
    Assertions.assertTrue(strategy.computeLane(QueryPlus.wrap(query), ImmutableSet.of()).isPresent());
    Assertions.assertEquals(
        HiLoQueryLaningStrategy.LOW,
        strategy.computeLane(QueryPlus.wrap(query), ImmutableSet.of()).get()
    );
  }

  @Test
  public void testLaningPreservesManualSetLane()
  {
    TimeseriesQuery query = queryBuilder.context(
        QueryContext.ofMap(QueryContextParameters.PRIORITY, 100, QueryContextParameters.LANE, "low")
    ).build();
    Assertions.assertEquals(
        HiLoQueryLaningStrategy.LOW,
        strategy.computeLane(QueryPlus.wrap(query), ImmutableSet.of()).get()
    );
  }
}
