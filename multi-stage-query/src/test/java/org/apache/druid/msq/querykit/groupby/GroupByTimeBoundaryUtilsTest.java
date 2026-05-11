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

package org.apache.druid.msq.querykit.groupby;

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongMinAggregatorFactory;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.TimeBoundaryInspector;
import org.apache.druid.segment.column.ColumnHolder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GroupByTimeBoundaryUtilsTest
{
  private static final String DATASOURCE = "test";
  private static final DateTime MIN_TIME = new DateTime("2000-01-01", DateTimeZone.UTC);
  private static final DateTime MAX_TIME = new DateTime("2001-01-01", DateTimeZone.UTC);

  @Test
  public void testIsTimeBoundaryQuery_minAndMax()
  {
    final GroupByQuery query = makeTimeBoundaryGroupByQuery(true, true);
    assertTrue(GroupByTimeBoundaryUtils.isTimeBoundaryQuery(query));
  }

  @Test
  public void testIsTimeBoundaryQuery_minOnly()
  {
    final GroupByQuery query = makeTimeBoundaryGroupByQuery(true, false);
    assertTrue(GroupByTimeBoundaryUtils.isTimeBoundaryQuery(query));
  }

  @Test
  public void testIsTimeBoundaryQuery_maxOnly()
  {
    final GroupByQuery query = makeTimeBoundaryGroupByQuery(false, true);
    assertTrue(GroupByTimeBoundaryUtils.isTimeBoundaryQuery(query));
  }

  @Test
  public void testIsTimeBoundaryQuery_withDimensions()
  {
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource(DATASOURCE)
                    .setQuerySegmentSpec(
                        new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.ETERNITY))
                    )
                    .setGranularity(Granularities.ALL)
                    .setDimensions(
                        new org.apache.druid.query.dimension.DefaultDimensionSpec("dim1", "d0")
                    )
                    .setAggregatorSpecs(
                        new LongMinAggregatorFactory("a0", ColumnHolder.TIME_COLUMN_NAME)
                    )
                    .build();
    assertFalse(GroupByTimeBoundaryUtils.isTimeBoundaryQuery(query));
  }

  @Test
  public void testIsTimeBoundaryQuery_withFilter()
  {
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource(DATASOURCE)
                    .setQuerySegmentSpec(
                        new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.ETERNITY))
                    )
                    .setGranularity(Granularities.ALL)
                    .setDimFilter(new SelectorDimFilter("dim1", "abc", null))
                    .setAggregatorSpecs(
                        new LongMinAggregatorFactory("a0", ColumnHolder.TIME_COLUMN_NAME)
                    )
                    .build();
    assertFalse(GroupByTimeBoundaryUtils.isTimeBoundaryQuery(query));
  }

  @Test
  public void testIsTimeBoundaryQuery_withNonTimeBoundaryAgg()
  {
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource(DATASOURCE)
                    .setQuerySegmentSpec(
                        new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.ETERNITY))
                    )
                    .setGranularity(Granularities.ALL)
                    .setAggregatorSpecs(
                        new LongMinAggregatorFactory("a0", ColumnHolder.TIME_COLUMN_NAME),
                        new CountAggregatorFactory("a1")
                    )
                    .build();
    assertFalse(GroupByTimeBoundaryUtils.isTimeBoundaryQuery(query));
  }

  @Test
  public void testIsTimeBoundaryQuery_withNonTimeGranularity()
  {
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource(DATASOURCE)
                    .setQuerySegmentSpec(
                        new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.ETERNITY))
                    )
                    .setGranularity(Granularities.DAY)
                    .setAggregatorSpecs(
                        new LongMinAggregatorFactory("a0", ColumnHolder.TIME_COLUMN_NAME)
                    )
                    .build();
    assertFalse(GroupByTimeBoundaryUtils.isTimeBoundaryQuery(query));
  }

  @Test
  public void testIsTimeBoundaryQuery_noAggregators()
  {
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource(DATASOURCE)
                    .setQuerySegmentSpec(
                        new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.ETERNITY))
                    )
                    .setGranularity(Granularities.ALL)
                    .build();
    assertFalse(GroupByTimeBoundaryUtils.isTimeBoundaryQuery(query));
  }

  @Test
  public void testIsTimeBoundaryQuery_longMinOnNonTimeColumn()
  {
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource(DATASOURCE)
                    .setQuerySegmentSpec(
                        new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.ETERNITY))
                    )
                    .setGranularity(Granularities.ALL)
                    .setAggregatorSpecs(
                        new LongMinAggregatorFactory("a0", "some_other_column")
                    )
                    .build();
    assertFalse(GroupByTimeBoundaryUtils.isTimeBoundaryQuery(query));
  }

  @Test
  public void testNeedsMinTime()
  {
    assertTrue(GroupByTimeBoundaryUtils.needsMinTime(makeTimeBoundaryGroupByQuery(true, true)));
    assertTrue(GroupByTimeBoundaryUtils.needsMinTime(makeTimeBoundaryGroupByQuery(true, false)));
    assertFalse(GroupByTimeBoundaryUtils.needsMinTime(makeTimeBoundaryGroupByQuery(false, true)));
  }

  @Test
  public void testNeedsMaxTime()
  {
    assertTrue(GroupByTimeBoundaryUtils.needsMaxTime(makeTimeBoundaryGroupByQuery(true, true)));
    assertFalse(GroupByTimeBoundaryUtils.needsMaxTime(makeTimeBoundaryGroupByQuery(true, false)));
    assertTrue(GroupByTimeBoundaryUtils.needsMaxTime(makeTimeBoundaryGroupByQuery(false, true)));
  }

  @Test
  public void testCanUseTimeBoundaryInspector_happy()
  {
    final GroupByQuery query = makeTimeBoundaryGroupByQuery(true, true);
    final TimeBoundaryInspector tbi = makeExactTimeBoundaryInspector();
    final SegmentDescriptor descriptor = new SegmentDescriptor(Intervals.ETERNITY, "1", 0);

    assertTrue(GroupByTimeBoundaryUtils.canUseTimeBoundaryInspector(query, tbi, descriptor));
  }

  @Test
  public void testCanUseTimeBoundaryInspector_nullTbi()
  {
    final GroupByQuery query = makeTimeBoundaryGroupByQuery(true, true);
    final SegmentDescriptor descriptor = new SegmentDescriptor(Intervals.ETERNITY, "1", 0);

    assertFalse(GroupByTimeBoundaryUtils.canUseTimeBoundaryInspector(query, null, descriptor));
  }

  @Test
  public void testCanUseTimeBoundaryInspector_notExact()
  {
    final GroupByQuery query = makeTimeBoundaryGroupByQuery(true, true);
    final TimeBoundaryInspector tbi = makeInexactTimeBoundaryInspector();
    final SegmentDescriptor descriptor = new SegmentDescriptor(Intervals.ETERNITY, "1", 0);

    assertFalse(GroupByTimeBoundaryUtils.canUseTimeBoundaryInspector(query, tbi, descriptor));
  }

  @Test
  public void testCanUseTimeBoundaryInspector_descriptorDoesNotContainMinMax()
  {
    final GroupByQuery query = makeTimeBoundaryGroupByQuery(true, true);
    final TimeBoundaryInspector tbi = makeExactTimeBoundaryInspector();
    // Descriptor interval only covers part of the data
    final SegmentDescriptor descriptor = new SegmentDescriptor(
        Intervals.of("2000-06-01/2000-07-01"),
        "1",
        0
    );

    assertFalse(GroupByTimeBoundaryUtils.canUseTimeBoundaryInspector(query, tbi, descriptor));
  }

  @Test
  public void testComputeTimeBoundaryResult_minAndMax()
  {
    final GroupByQuery query = makeTimeBoundaryGroupByQuery(true, true);
    final TimeBoundaryInspector tbi = makeExactTimeBoundaryInspector();
    final ResultRow row = GroupByTimeBoundaryUtils.computeTimeBoundaryResult(query, tbi);

    final int aggStart = query.getResultRowAggregatorStart();
    assertEquals(MIN_TIME.getMillis(), row.get(aggStart));
    assertEquals(MAX_TIME.getMillis(), row.get(aggStart + 1));
  }

  @Test
  public void testComputeTimeBoundaryResult_minOnly()
  {
    final GroupByQuery query = makeTimeBoundaryGroupByQuery(true, false);
    final TimeBoundaryInspector tbi = makeExactTimeBoundaryInspector();
    final ResultRow row = GroupByTimeBoundaryUtils.computeTimeBoundaryResult(query, tbi);

    final int aggStart = query.getResultRowAggregatorStart();
    assertEquals(MIN_TIME.getMillis(), row.get(aggStart));
  }

  @Test
  public void testComputeTimeBoundaryResult_maxOnly()
  {
    final GroupByQuery query = makeTimeBoundaryGroupByQuery(false, true);
    final TimeBoundaryInspector tbi = makeExactTimeBoundaryInspector();
    final ResultRow row = GroupByTimeBoundaryUtils.computeTimeBoundaryResult(query, tbi);

    final int aggStart = query.getResultRowAggregatorStart();
    assertEquals(MAX_TIME.getMillis(), row.get(aggStart));
  }

  private static GroupByQuery makeTimeBoundaryGroupByQuery(final boolean includeMin, final boolean includeMax)
  {
    final GroupByQuery.Builder builder =
        GroupByQuery.builder()
                    .setDataSource(DATASOURCE)
                    .setQuerySegmentSpec(
                        new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.ETERNITY))
                    )
                    .setGranularity(Granularities.ALL);

    final com.google.common.collect.ImmutableList.Builder<org.apache.druid.query.aggregation.AggregatorFactory> aggs =
        com.google.common.collect.ImmutableList.builder();

    if (includeMin) {
      aggs.add(new LongMinAggregatorFactory("a0", ColumnHolder.TIME_COLUMN_NAME));
    }
    if (includeMax) {
      aggs.add(new LongMaxAggregatorFactory("a1", ColumnHolder.TIME_COLUMN_NAME));
    }

    builder.setAggregatorSpecs(aggs.build());
    return builder.build();
  }

  private static TimeBoundaryInspector makeExactTimeBoundaryInspector()
  {
    return new TimeBoundaryInspector()
    {
      @Override
      public DateTime getMinTime()
      {
        return MIN_TIME;
      }

      @Override
      public DateTime getMaxTime()
      {
        return MAX_TIME;
      }

      @Override
      public boolean isMinMaxExact()
      {
        return true;
      }
    };
  }

  private static TimeBoundaryInspector makeInexactTimeBoundaryInspector()
  {
    return new TimeBoundaryInspector()
    {
      @Override
      public DateTime getMinTime()
      {
        return MIN_TIME;
      }

      @Override
      public DateTime getMaxTime()
      {
        return MAX_TIME;
      }

      @Override
      public boolean isMinMaxExact()
      {
        return false;
      }
    };
  }
}
