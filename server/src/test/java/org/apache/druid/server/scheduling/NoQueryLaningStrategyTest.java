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
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NoQueryLaningStrategyTest
{
  private Druids.TimeseriesQueryBuilder queryBuilder;
  private NoQueryLaningStrategy strategy;

  @Before
  public void setup()
  {
    this.queryBuilder = Druids.newTimeseriesQueryBuilder()
                              .dataSource("test")
                              .intervals(ImmutableList.of(Intervals.ETERNITY))
                              .granularity(Granularities.DAY)
                              .aggregators(new CountAggregatorFactory("count"));

    this.strategy = new NoQueryLaningStrategy();
  }

  @Test
  public void testDoesntSetLane()
  {
    TimeseriesQuery query = queryBuilder.context(ImmutableMap.of()).build();
    Assert.assertFalse(strategy.computeLane(QueryPlus.wrap(query), ImmutableSet.of()).isPresent());
  }

  @Test
  public void testPreservesManualLaneFromContext()
  {
    final String someLane = "some-lane";
    TimeseriesQuery query = queryBuilder.context(
        ImmutableMap.of(QueryContexts.PRIORITY_KEY, 100, QueryContexts.LANE_KEY, someLane)
    ).build();
    Assert.assertEquals(
        someLane,
        strategy.computeLane(QueryPlus.wrap(query), ImmutableSet.of()).get()
    );
  }
}
