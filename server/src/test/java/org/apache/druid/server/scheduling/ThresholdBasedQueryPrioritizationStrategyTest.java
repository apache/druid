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
import org.apache.druid.client.SegmentServerSelector;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.server.QueryPrioritizationStrategy;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ThresholdBasedQueryPrioritizationStrategyTest
{
  private final Integer adjustment = 10;
  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  private Druids.TimeseriesQueryBuilder queryBuilder;

  @Before
  public void setup()
  {
    this.queryBuilder = Druids.newTimeseriesQueryBuilder()
                              .dataSource("test")
                              .aggregators(new CountAggregatorFactory("count"));

  }

  @Test
  public void testPrioritizationPeriodThresholdInsidePeriod()
  {
    QueryPrioritizationStrategy strategy = new ThresholdBasedQueryPrioritizationStrategy(
        "P90D", null, null, null, null, adjustment);
    DateTime startDate = DateTimes.nowUtc().minusDays(1);
    DateTime endDate = DateTimes.nowUtc();
    TimeseriesQuery query = queryBuilder.intervals(ImmutableList.of(new Interval(startDate, endDate)))
                                        .granularity(Granularities.MINUTE)
                                        .context(ImmutableMap.of())
                                        .build();

    Assert.assertFalse(
        strategy.computePriority(QueryPlus.wrap(query), ImmutableSet.of()).isPresent()
    );
  }

  @Test
  public void testPrioritizationPeriodThresholdOutsidePeriod()
  {
    QueryPrioritizationStrategy strategy = new ThresholdBasedQueryPrioritizationStrategy(
        "P90D",
        null,
        null,
        null,
        null,
        adjustment
    );
    DateTime startDate = DateTimes.nowUtc().minusDays(100);
    DateTime endDate = DateTimes.nowUtc().minusDays(80);
    TimeseriesQuery query = queryBuilder.intervals(ImmutableList.of(new Interval(startDate, endDate)))
                                        .granularity(Granularities.HOUR)
                                        .context(ImmutableMap.of())
                                        .build();

    Assert.assertEquals(
        -adjustment,
        (int) strategy.computePriority(QueryPlus.wrap(query), ImmutableSet.of()).get()
    );
  }

  @Test
  public void testPrioritizationDurationThresholdInsideDuration()
  {
    QueryPrioritizationStrategy strategy = new ThresholdBasedQueryPrioritizationStrategy(
        null,
        "P7D",
        null,
        null,
        null,
        adjustment
    );
    DateTime startDate = DateTimes.nowUtc().minusDays(1);
    DateTime endDate = DateTimes.nowUtc();
    TimeseriesQuery query = queryBuilder.intervals(ImmutableList.of(new Interval(startDate, endDate)))
                                        .granularity(Granularities.MINUTE)
                                        .context(ImmutableMap.of())
                                        .build();

    Assert.assertFalse(
        strategy.computePriority(QueryPlus.wrap(query), ImmutableSet.of()).isPresent()
    );
  }

  @Test
  public void testPrioritizationDurationThresholdOutsideDuration()
  {
    QueryPrioritizationStrategy strategy = new ThresholdBasedQueryPrioritizationStrategy(
        null,
        "P7D",
        null,
        null,
        null,
        adjustment
    );
    DateTime startDate = DateTimes.nowUtc().minusDays(20);
    DateTime endDate = DateTimes.nowUtc();
    TimeseriesQuery query = queryBuilder.intervals(ImmutableList.of(new Interval(startDate, endDate)))
                                        .granularity(Granularities.HOUR)
                                        .context(ImmutableMap.of())
                                        .build();

    Assert.assertEquals(
        -adjustment,
        (int) strategy.computePriority(QueryPlus.wrap(query), ImmutableSet.of()).get()
    );
  }

  @Test
  public void testPrioritizationSegmentCountWithinThreshold()
  {
    QueryPrioritizationStrategy strategy = new ThresholdBasedQueryPrioritizationStrategy(
        null,
        null,
        2,
        null,
        null,
        adjustment
    );
    DateTime startDate = DateTimes.nowUtc().minusDays(1);
    DateTime endDate = DateTimes.nowUtc();
    TimeseriesQuery query = queryBuilder.intervals(ImmutableList.of(new Interval(startDate, endDate)))
                                        .granularity(Granularities.MINUTE)
                                        .context(ImmutableMap.of())
                                        .build();

    Assert.assertFalse(
        strategy.computePriority(
            QueryPlus.wrap(query),
            ImmutableSet.of(EasyMock.createMock(SegmentServerSelector.class))
        ).isPresent()
    );
  }

  @Test
  public void testPrioritizationSegmentCountOverThreshold()
  {
    QueryPrioritizationStrategy strategy = new ThresholdBasedQueryPrioritizationStrategy(
        null,
        null,
        2,
        null,
        null,
        adjustment
    );
    DateTime startDate = DateTimes.nowUtc().minusDays(20);
    DateTime endDate = DateTimes.nowUtc();
    TimeseriesQuery query = queryBuilder.intervals(ImmutableList.of(new Interval(startDate, endDate)))
                                        .granularity(Granularities.HOUR)
                                        .context(ImmutableMap.of())
                                        .build();

    Assert.assertEquals(
        -adjustment,
        (int) strategy.computePriority(
            QueryPlus.wrap(query),
            ImmutableSet.of(
                EasyMock.createMock(SegmentServerSelector.class),
                EasyMock.createMock(SegmentServerSelector.class),
                EasyMock.createMock(SegmentServerSelector.class)
            )
        ).get()
    );
  }

  @Test
  public void testPrioritizationSegmentRangeWithinThreshold()
  {
    QueryPrioritizationStrategy strategy = new ThresholdBasedQueryPrioritizationStrategy(
            null,
            null,
            null,
            "P7D",
            null,
            adjustment
    );
    DateTime startDate = DateTimes.nowUtc().minusDays(1);
    DateTime endDate = DateTimes.nowUtc();
    TimeseriesQuery query = queryBuilder.intervals(ImmutableList.of(new Interval(startDate, endDate)))
            .granularity(Granularities.MINUTE)
            .context(ImmutableMap.of())
            .build();
    SegmentServerSelector segmentServerSelector = EasyMock.createMock(SegmentServerSelector.class);
    EasyMock.expect(segmentServerSelector.getSegmentDescriptor()).andReturn(new SegmentDescriptor(new Interval(startDate, endDate), "", 0)).times(2);
    EasyMock.replay(segmentServerSelector);
    Assert.assertFalse(
            strategy.computePriority(QueryPlus.wrap(query), ImmutableSet.of(segmentServerSelector)).isPresent()
    );
  }

  @Test
  public void testPrioritizationSegmentRangeOverThreshold()
  {
    QueryPrioritizationStrategy strategy = new ThresholdBasedQueryPrioritizationStrategy(
            null,
            null,
            null,
            "P7D",
            null,
            adjustment
    );
    DateTime startDate = DateTimes.nowUtc().minusDays(20);
    DateTime endDate = DateTimes.nowUtc();
    TimeseriesQuery query = queryBuilder.intervals(ImmutableList.of(new Interval(startDate, endDate)))
            .granularity(Granularities.HOUR)
            .context(ImmutableMap.of())
            .build();
    SegmentServerSelector segmentServerSelector = EasyMock.createMock(SegmentServerSelector.class);
    EasyMock.expect(segmentServerSelector.getSegmentDescriptor()).andReturn(new SegmentDescriptor(new Interval(startDate, endDate), "", 0)).times(2);
    EasyMock.replay(segmentServerSelector);
    Assert.assertEquals(
            -adjustment,
            (int) strategy.computePriority(QueryPlus.wrap(query), ImmutableSet.of(segmentServerSelector)).get()
    );
  }

  @Test
  public void testPrioritizationWithExemptDatasource()
  {
    QueryPrioritizationStrategy strategy = new ThresholdBasedQueryPrioritizationStrategy(
            null,
            null,
            2,
            null,
            ImmutableSet.of("exemptDatasource"),
            adjustment
    );
    DateTime startDate = DateTimes.nowUtc().minusDays(20);
    DateTime endDate = DateTimes.nowUtc();
    TimeseriesQuery query = queryBuilder.intervals(ImmutableList.of(new Interval(startDate, endDate)))
            .granularity(Granularities.HOUR)
            .context(ImmutableMap.of())
            .build();

    Assert.assertFalse(
            strategy.computePriority(
                    QueryPlus.wrap(query),
                    ImmutableSet.of(
                            EasyMock.createMock(SegmentServerSelector.class),
                            EasyMock.createMock(SegmentServerSelector.class),
                            EasyMock.createMock(SegmentServerSelector.class)
                    )
            ).isPresent()
    );
  }

  @Test
  public void testPrioritizationWithNonExemptDatasource()
  {
    QueryPrioritizationStrategy strategy = new ThresholdBasedQueryPrioritizationStrategy(
            null,
            null,
            2,
            null,
            ImmutableSet.of("exemptDatasource"),
            adjustment
    );
    DateTime startDate = DateTimes.nowUtc().minusDays(20);
    DateTime endDate = DateTimes.nowUtc();
    TimeseriesQuery query = queryBuilder.dataSource("nonExemptDatasource")
            .intervals(ImmutableList.of(new Interval(startDate, endDate)))
            .granularity(Granularities.HOUR)
            .context(ImmutableMap.of())
            .build();

    // Since "test" is not in the exempt list, priority should be adjusted
    Assert.assertEquals(
            -adjustment,
            (int) strategy.computePriority(
                    QueryPlus.wrap(query),
                    ImmutableSet.of(
                            EasyMock.createMock(SegmentServerSelector.class),
                            EasyMock.createMock(SegmentServerSelector.class),
                            EasyMock.createMock(SegmentServerSelector.class)
                    )
            ).get()
    );
  }

}
