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

package org.apache.druid.query.metadata;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.metadata.metadata.AggregatorMergeStrategy;
import org.apache.druid.query.metadata.metadata.ColumnAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.query.spec.LegacySegmentSpec;
import org.apache.druid.segment.AggregateProjectionMetadata;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.timeline.LogicalSegment;
import org.apache.druid.timeline.SegmentId;
import org.hamcrest.MatcherAssert;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.stream.Collectors;

public class SegmentMetadataQueryQueryToolChestTest
{
  private static final DataSource TEST_DATASOURCE = new TableDataSource("dummy");
  private static final Interval INTERVAL_2020 = Intervals.of("2020-01-01/2020-01-02");
  private static final Interval INTERVAL_2021 = Intervals.of("2021-01-01/2021-01-02");
  private static final SegmentId TEST_SEGMENT_ID1 = SegmentId.of(TEST_DATASOURCE.toString(), INTERVAL_2020, "test", 0);
  private static final SegmentId TEST_SEGMENT_ID2 = SegmentId.of(TEST_DATASOURCE.toString(), INTERVAL_2021, "test", 0);

  private static final AggregateProjectionMetadata.Schema PROJECTION_CHANNEL_ADDED_HOURLY = new AggregateProjectionMetadata.Schema(
      "name1-doesnot-matter",
      Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME,
      VirtualColumns.create(Granularities.toVirtualColumn(
          Granularities.HOUR,
          Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME
      )),
      ImmutableList.of(Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME, "channel"),
      new AggregatorFactory[]{
          new LongSumAggregatorFactory("channel_sum", "channel")
      },
      ImmutableList.of(
          OrderBy.ascending(Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME),
          OrderBy.ascending("channel")
      )
  );
  private static final AggregateProjectionMetadata.Schema PROJECTION_CHANNEL_ADDED_DAILY = new AggregateProjectionMetadata.Schema(
      "name2-doesnot-matter",
      Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME,
      VirtualColumns.create(Granularities.toVirtualColumn(
          Granularities.DAY,
          Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME
      )),
      ImmutableList.of(Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME, "channel"),
      new AggregatorFactory[]{
          new LongSumAggregatorFactory("channel_sum", "channel")
      },
      ImmutableList.of(
          OrderBy.ascending(Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME),
          OrderBy.ascending("channel")
      )
  );

  @Test
  public void testCacheStrategy() throws Exception
  {
    SegmentMetadataQuery query = new SegmentMetadataQuery(
        TEST_DATASOURCE,
        new LegacySegmentSpec("2015-01-01/2015-01-02"),
        null,
        null,
        null,
        null,
        false,
        null,
        AggregatorMergeStrategy.STRICT
    );

    CacheStrategy<SegmentAnalysis, SegmentAnalysis, SegmentMetadataQuery> strategy =
        new SegmentMetadataQueryQueryToolChest(new SegmentMetadataQueryConfig()).getCacheStrategy(query);

    // Test cache key generation
    byte[] expectedKey = {0x04, 0x09, 0x01, 0x0A, 0x00, 0x00, 0x00, 0x03, 0x00, 0x02, 0x04};
    byte[] actualKey = strategy.computeCacheKey(query);
    Assert.assertArrayEquals(expectedKey, actualKey);

    SegmentAnalysis result = new SegmentAnalysisBuilder(TEST_SEGMENT_ID1)
        .interval(Intervals.of("2011-01-12T00:00:00.000Z/2011-04-15T00:00:00.001Z"))
        .column(
            "placement",
            new ColumnAnalysis(
                ColumnType.STRING,
                ValueType.STRING.name(),
                true,
                false,
                10881,
                1,
                "preferred",
                "preferred",
                null
            )
        )
        .size(71982)
        .numRows(100)
        .build();

    Object preparedValue = strategy.prepareForSegmentLevelCache().apply(result);

    ObjectMapper objectMapper = new DefaultObjectMapper();
    SegmentAnalysis fromCacheValue = objectMapper.readValue(
        objectMapper.writeValueAsBytes(preparedValue),
        strategy.getCacheObjectClazz()
    );

    SegmentAnalysis fromCacheResult = strategy.pullFromSegmentLevelCache().apply(fromCacheValue);

    Assert.assertEquals(result, fromCacheResult);
  }

  @EnumSource(AggregatorMergeStrategy.class)
  @ParameterizedTest(name = "{index}: with AggregatorMergeStrategy {0}")
  public void testProjections(AggregatorMergeStrategy aggregatorMergeStrategy)
  {
    final SegmentAnalysis analysis1 = new SegmentAnalysisBuilder(TEST_SEGMENT_ID1)
        .projection("channel_sum", new AggregateProjectionMetadata(PROJECTION_CHANNEL_ADDED_HOURLY, 100))
        .build();
    final SegmentAnalysis analysis2 = new SegmentAnalysisBuilder(TEST_SEGMENT_ID2)
        .projection("channel_sum", new AggregateProjectionMetadata(PROJECTION_CHANNEL_ADDED_HOURLY, 200))
        .build();

    final SegmentAnalysis expected = new SegmentAnalysisBuilder(
        "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged")
        .projection("channel_sum", new AggregateProjectionMetadata(PROJECTION_CHANNEL_ADDED_HOURLY, 300))
        .build();
    Assert.assertEquals(expected, mergeWithStrategy(analysis1, analysis2, aggregatorMergeStrategy));
  }

  @EnumSource(AggregatorMergeStrategy.class)
  @ParameterizedTest(name = "{index}: with AggregatorMergeStrategy {0}")
  public void testProjectionsWithNull(AggregatorMergeStrategy aggregatorMergeStrategy)
  {
    final SegmentAnalysis analysis1 = new SegmentAnalysisBuilder(TEST_SEGMENT_ID1)
        .projection("channel_sum", new AggregateProjectionMetadata(PROJECTION_CHANNEL_ADDED_HOURLY, 100))
        .build();
    final SegmentAnalysis analysis1NullProjection = new SegmentAnalysisBuilder(TEST_SEGMENT_ID1).build();
    final SegmentAnalysis analysis2 = new SegmentAnalysisBuilder(TEST_SEGMENT_ID2)
        .projection("channel_sum", new AggregateProjectionMetadata(PROJECTION_CHANNEL_ADDED_HOURLY, 200))
        .build();
    final SegmentAnalysis analysis2NullProjection = new SegmentAnalysisBuilder(TEST_SEGMENT_ID2).build();

    Assert.assertNull(mergeWithStrategy(analysis1NullProjection, analysis2, aggregatorMergeStrategy).getProjections());
    Assert.assertNull(mergeWithStrategy(analysis1, analysis2NullProjection, aggregatorMergeStrategy).getProjections());
    Assert.assertNull(
        mergeWithStrategy(analysis1NullProjection, analysis2NullProjection, aggregatorMergeStrategy).getProjections()
    );
  }

  @EnumSource(AggregatorMergeStrategy.class)
  @ParameterizedTest(name = "{index}: with AggregatorMergeStrategy {0}")
  public void testProjectionsWithConflict(AggregatorMergeStrategy aggregatorMergeStrategy)
  {
    final SegmentAnalysis analysis1 = new SegmentAnalysisBuilder(TEST_SEGMENT_ID1)
        .projection("channel_sum", new AggregateProjectionMetadata(PROJECTION_CHANNEL_ADDED_HOURLY, 100))
        .projection("channel_sum_1", new AggregateProjectionMetadata(PROJECTION_CHANNEL_ADDED_HOURLY, 100))
        .projection("conflict_projection", new AggregateProjectionMetadata(PROJECTION_CHANNEL_ADDED_HOURLY, 100))
        .build();
    final SegmentAnalysis analysis2 = new SegmentAnalysisBuilder(TEST_SEGMENT_ID2)
        .projection("channel_sum", new AggregateProjectionMetadata(PROJECTION_CHANNEL_ADDED_HOURLY, 200))
        .projection("channel_sum_2", new AggregateProjectionMetadata(PROJECTION_CHANNEL_ADDED_DAILY, 200))
        .projection("conflict_projection", new AggregateProjectionMetadata(PROJECTION_CHANNEL_ADDED_DAILY, 200))
        .build();

    // conflict projection is ignored.
    final SegmentAnalysis expectedStrict = new SegmentAnalysisBuilder(
        "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged")
        .projection("channel_sum", new AggregateProjectionMetadata(PROJECTION_CHANNEL_ADDED_HOURLY, 300))
        .build();
    Assert.assertEquals(expectedStrict, mergeWithStrategy(analysis1, analysis2, aggregatorMergeStrategy));
  }

  @EnumSource(AggregatorMergeStrategy.class)
  @ParameterizedTest(name = "{index}: with AggregatorMergeStrategy {0}")
  public void testMergeAggregators(AggregatorMergeStrategy aggregatorMergeStrategy)
  {
    final SegmentAnalysis analysis1 = new SegmentAnalysisBuilder(TEST_SEGMENT_ID1)
        .aggregator("foo", new LongSumAggregatorFactory("foo", "foo"))
        .aggregator("baz", new DoubleSumAggregatorFactory("baz", "baz"))
        .build();
    final SegmentAnalysis analysis2 = new SegmentAnalysisBuilder(TEST_SEGMENT_ID2)
        .aggregator("foo", new LongSumAggregatorFactory("foo", "foo"))
        .aggregator("bar", new DoubleSumAggregatorFactory("bar", "bar"))
        .build();

    final SegmentAnalysis expected = new SegmentAnalysisBuilder(
        "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged")
        .aggregator("foo", new LongSumAggregatorFactory("foo", "foo"))
        .aggregator("bar", new DoubleSumAggregatorFactory("bar", "bar"))
        .aggregator("baz", new DoubleSumAggregatorFactory("baz", "baz"))
        .build();
    Assert.assertEquals(expected, mergeWithStrategy(analysis1, analysis2, aggregatorMergeStrategy));
  }

  @EnumSource(AggregatorMergeStrategy.class)
  @ParameterizedTest(name = "{index}: with AggregatorMergeStrategy {0}")
  public void testMergeAggregatorsWithIntervals(AggregatorMergeStrategy aggregatorMergeStrategy)
  {
    final SegmentAnalysis analysis1 = new SegmentAnalysisBuilder(TEST_SEGMENT_ID1)
        .interval(TEST_SEGMENT_ID1.getInterval())
        .aggregator("foo", new LongSumAggregatorFactory("foo", "foo"))
        .aggregator("baz", new DoubleSumAggregatorFactory("baz", "baz"))
        .build();
    final SegmentAnalysis analysis2 = new SegmentAnalysisBuilder(TEST_SEGMENT_ID2)
        .interval(TEST_SEGMENT_ID2.getInterval())
        .aggregator("foo", new LongSumAggregatorFactory("foo", "foo"))
        .aggregator("bar", new DoubleSumAggregatorFactory("bar", "bar"))
        .build();

    SegmentAnalysis expected = new SegmentAnalysisBuilder(
        "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged")
        .interval(TEST_SEGMENT_ID1.getInterval())
        .interval(TEST_SEGMENT_ID2.getInterval())
        .aggregator("foo", new LongSumAggregatorFactory("foo", "foo"))
        .aggregator("bar", new DoubleSumAggregatorFactory("bar", "bar"))
        .aggregator("baz", new DoubleSumAggregatorFactory("baz", "baz"))
        .build();
    Assert.assertEquals(expected, mergeWithStrategy(analysis1, analysis2, aggregatorMergeStrategy));
  }

  @EnumSource(AggregatorMergeStrategy.class)
  @ParameterizedTest(name = "{index}: with AggregatorMergeStrategy {0}")
  public void testMergeAggregatorsOneNullStrict(AggregatorMergeStrategy aggregatorMergeStrategy)
  {
    Assume.assumeTrue(aggregatorMergeStrategy == AggregatorMergeStrategy.STRICT);

    final SegmentAnalysis analysis1 = new SegmentAnalysisBuilder(TEST_SEGMENT_ID1).build();
    final SegmentAnalysis analysis2 = new SegmentAnalysisBuilder(TEST_SEGMENT_ID2)
        .aggregator("foo", new LongSumAggregatorFactory("foo", "foo"))
        .aggregator("bar", new DoubleSumAggregatorFactory("bar", "bar"))
        .build();

    final SegmentAnalysis expectedNullAggregators = new SegmentAnalysisBuilder(
        "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged").build();
    Assert.assertEquals(expectedNullAggregators, mergeWithStrategy(analysis1, analysis2, aggregatorMergeStrategy));
  }

  @EnumSource(AggregatorMergeStrategy.class)
  @ParameterizedTest(name = "{index}: with AggregatorMergeStrategy {0}")
  public void testMergeAggregatorsOneNullNotStrict(AggregatorMergeStrategy aggregatorMergeStrategy)
  {
    Assume.assumeTrue(aggregatorMergeStrategy != AggregatorMergeStrategy.STRICT);

    final SegmentAnalysis analysis1 = new SegmentAnalysisBuilder(TEST_SEGMENT_ID1).build();
    final SegmentAnalysis analysis2 = new SegmentAnalysisBuilder(TEST_SEGMENT_ID2)
        .aggregator("foo", new LongSumAggregatorFactory("foo", "foo"))
        .aggregator("bar", new DoubleSumAggregatorFactory("bar", "bar"))
        .build();

    final SegmentAnalysis expected = new SegmentAnalysisBuilder(
        "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged")
        .aggregator("foo", new LongSumAggregatorFactory("foo", "foo"))
        .aggregator("bar", new DoubleSumAggregatorFactory("bar", "bar"))
        .build();
    Assert.assertEquals(expected, mergeWithStrategy(analysis1, analysis2, aggregatorMergeStrategy));
  }

  @EnumSource(AggregatorMergeStrategy.class)
  @ParameterizedTest(name = "{index}: with AggregatorMergeStrategy {0}")
  public void testMergeAggregatorsAllNull(AggregatorMergeStrategy aggregatorMergeStrategy)
  {
    final SegmentAnalysis analysis1 = new SegmentAnalysisBuilder(TEST_SEGMENT_ID1).build();
    final SegmentAnalysis analysis2 = new SegmentAnalysisBuilder(TEST_SEGMENT_ID2).build();

    final SegmentAnalysis expected = new SegmentAnalysisBuilder(
        "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged").build();
    Assert.assertEquals(expected, mergeWithStrategy(analysis1, analysis2, aggregatorMergeStrategy));
  }

  @Test
  public void testMergeAggregatorsConflict()
  {
    final SegmentAnalysis analysis1 = new SegmentAnalysisBuilder(TEST_SEGMENT_ID1)
        .aggregator("foo", new LongSumAggregatorFactory("foo", "foo"))
        .aggregator("bar", new DoubleSumAggregatorFactory("bar", "bar"))
        .build();
    final SegmentAnalysis analysis2 = new SegmentAnalysisBuilder(TEST_SEGMENT_ID2)
        .aggregator("foo", new LongSumAggregatorFactory("foo", "foo"))
        .aggregator("bar", new DoubleMaxAggregatorFactory("bar", "bar"))
        .aggregator("baz", new LongMaxAggregatorFactory("baz", "baz"))
        .build();

    // Test strict merge, returns null aggregators as there's a conflict on "bar"
    final SegmentAnalysis expectedStrict = new SegmentAnalysisBuilder(
        "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged").build();
    Assert.assertEquals(expectedStrict, mergeWithStrategy(analysis1, analysis2, AggregatorMergeStrategy.STRICT));

    // Test lenient merge, returns a map with null for "bar" as it has a conflict
    final SegmentAnalysis expectedLenient = new SegmentAnalysisBuilder(
        "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged")
        .aggregator("foo", new LongSumAggregatorFactory("foo", "foo"))
        .aggregator("bar", null)
        .aggregator("baz", new LongMaxAggregatorFactory("baz", "baz"))
        .build();
    Assert.assertEquals(expectedLenient, mergeLenient(analysis1, analysis2));
    // Simulate multi-level lenient merge
    Assert.assertEquals(
        expectedLenient,
        mergeLenient(mergeLenient(analysis1, analysis2), mergeLenient(analysis1, analysis2))
    );
    // Simulate multi-level lenient merge (unmerged first)
    Assert.assertEquals(expectedLenient, mergeLenient(analysis1, mergeLenient(analysis1, analysis2)));
    // Simulate multi-level lenient merge (unmerged second)
    Assert.assertEquals(expectedLenient, mergeLenient(mergeLenient(analysis1, analysis2), analysis1));

    // Test earliest merge, returns a map with "bar" as DoubleSumAggregatorFactory since analysis1 is earlier
    final SegmentAnalysis expectedEarliest = new SegmentAnalysisBuilder(
        "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged")
        .aggregator("foo", new LongSumAggregatorFactory("foo", "foo"))
        .aggregator("bar", new DoubleSumAggregatorFactory("bar", "bar"))
        .aggregator("baz", new LongMaxAggregatorFactory("baz", "baz"))
        .build();
    Assert.assertEquals(expectedEarliest, mergeEarliest(analysis1, analysis2));
    // Simulate multi-level earliest merge
    Assert.assertEquals(
        expectedEarliest,
        mergeEarliest(mergeEarliest(analysis1, analysis2), mergeEarliest(analysis1, analysis2))
    );

    // Test latest merge, returns a map with "bar" as DoubleMaxAggregatorFactory since analysis2 is later
    final SegmentAnalysis expectedLatest = new SegmentAnalysisBuilder(
        "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged")
        .aggregator("foo", new LongSumAggregatorFactory("foo", "foo"))
        .aggregator("bar", new DoubleMaxAggregatorFactory("bar", "bar"))
        .aggregator("baz", new LongMaxAggregatorFactory("baz", "baz"))
        .build();
    Assert.assertEquals(expectedLatest, mergeLatest(analysis1, analysis2));
    // Simulate multi-level latest merge
    Assert.assertEquals(
        expectedLatest,
        mergeLatest(mergeLatest(analysis1, analysis2), mergeLatest(analysis1, analysis2))
    );
  }

  @Test
  public void testMergeAggregatorsConflictWithDifferentOrder()
  {
    final SegmentAnalysis analysis1 = new SegmentAnalysisBuilder(TEST_SEGMENT_ID2)
        .aggregator("foo", new LongSumAggregatorFactory("foo", "foo"))
        .aggregator("bar", new DoubleSumAggregatorFactory("bar", "bar"))
        .build();
    final SegmentAnalysis analysis2 = new SegmentAnalysisBuilder(TEST_SEGMENT_ID1)
        .aggregator("foo", new LongSumAggregatorFactory("foo", "foo"))
        .aggregator("bar", new DoubleMaxAggregatorFactory("bar", "bar"))
        .aggregator("baz", new LongMaxAggregatorFactory("baz", "baz"))
        .build();

    // Test strict merge, returns null aggregators as there's a conflict on "bar"
    Assert.assertEquals(
        new SegmentAnalysisBuilder("dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged")
            .build(),
        mergeStrict(analysis1, analysis2)
    );

    // Test lenient merge, returns a map with null for "bar" as it has a conflict
    final SegmentAnalysis expectedLenient = new SegmentAnalysisBuilder(
        "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged")
        .aggregator("foo", new LongSumAggregatorFactory("foo", "foo"))
        .aggregator("bar", null)
        .aggregator("baz", new LongMaxAggregatorFactory("baz", "baz"))
        .build();
    Assert.assertEquals(
        expectedLenient,
        mergeLenient(analysis1, analysis2)
    );
    // Simulate multi-level lenient merge
    Assert.assertEquals(
        expectedLenient,
        mergeLenient(
            mergeLenient(analysis1, analysis2),
            mergeLenient(analysis1, analysis2)
        )
    );

    // Test earliest merge, returns a map with "bar" as DoubleMaxAggregatorFactory since analysis2 is earlier
    final SegmentAnalysis expectedEarliest = new SegmentAnalysisBuilder(
        "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged")
        .aggregator("foo", new LongSumAggregatorFactory("foo", "foo"))
        .aggregator("bar", new DoubleMaxAggregatorFactory("bar", "bar"))
        .aggregator("baz", new LongMaxAggregatorFactory("baz", "baz"))
        .build();
    Assert.assertEquals(expectedEarliest, mergeEarliest(analysis1, analysis2));
    // Simulate multi-level earliest merge
    Assert.assertEquals(
        expectedEarliest,
        mergeEarliest(mergeEarliest(analysis1, analysis2), mergeEarliest(analysis1, analysis2))
    );

    // Test latest merge, returns a map with "bar" as DoubleSumAggregatorFactory since analysis1 is later
    final SegmentAnalysis expectedLatest = new SegmentAnalysisBuilder(
        "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged")
        .aggregator("foo", new LongSumAggregatorFactory("foo", "foo"))
        .aggregator("bar", new DoubleSumAggregatorFactory("bar", "bar"))
        .aggregator("baz", new LongMaxAggregatorFactory("baz", "baz"))
        .build();
    Assert.assertEquals(expectedLatest, mergeLatest(analysis1, analysis2));
    // Simulate multi-level latest merge
    Assert.assertEquals(
        expectedLatest,
        mergeLatest(mergeLatest(analysis1, analysis2), mergeLatest(analysis1, analysis2))
    );
  }

  @Test
  public void testMergeAggregatorsConflictWithEqualSegmentIntervalsAndDifferentPartitions()
  {
    Interval interval = Intervals.of("2023-01-01/2023-01-02");
    final SegmentId segmentId1 = SegmentId.of(TEST_DATASOURCE.toString(), interval, "test", 1);
    final SegmentId segmentId2 = SegmentId.of(TEST_DATASOURCE.toString(), interval, "test", 2);

    final SegmentAnalysis analysis1 = new SegmentAnalysisBuilder(segmentId1)
        .aggregator("foo", new LongSumAggregatorFactory("foo", "foo"))
        .aggregator("bar", new DoubleSumAggregatorFactory("bar", "bar"))
        .build();

    final SegmentAnalysis analysis2 = new SegmentAnalysisBuilder(segmentId2)
        .aggregator("foo", new LongSumAggregatorFactory("foo", "foo"))
        .aggregator("bar", new DoubleMaxAggregatorFactory("bar", "bar"))
        .aggregator("baz", new LongMaxAggregatorFactory("baz", "baz"))
        .build();

    // Test strict merge, returns null aggregators as there's a conflict on "bar"
    Assert.assertEquals(
        new SegmentAnalysisBuilder("dummy_2023-01-01T00:00:00.000Z_2023-01-02T00:00:00.000Z_merged_2")
            .build(),
        mergeStrict(analysis1, analysis2)
    );

    // Test lenient merge, returns a map with null for "bar" as it has a conflict
    SegmentAnalysis expectedLenient = new SegmentAnalysisBuilder(
        "dummy_2023-01-01T00:00:00.000Z_2023-01-02T00:00:00.000Z_merged_2")
        .aggregator("foo", new LongSumAggregatorFactory("foo", "foo"))
        .aggregator("bar", null)
        .aggregator("baz", new LongMaxAggregatorFactory("baz", "baz"))
        .build();
    Assert.assertEquals(expectedLenient, mergeLenient(analysis1, analysis2));
    // Simulate multi-level lenient merge
    Assert.assertEquals(
        expectedLenient,
        mergeLenient(mergeLenient(analysis1, analysis2), mergeLenient(analysis1, analysis2))
    );

    // Test earliest merge, returns a map with "bar" as DoubleSumAggregatorFactory since analysis1 has the earlier partition
    SegmentAnalysis expectedEarliest = new SegmentAnalysisBuilder(
        "dummy_2023-01-01T00:00:00.000Z_2023-01-02T00:00:00.000Z_merged_2")
        .aggregator("foo", new LongSumAggregatorFactory("foo", "foo"))
        .aggregator("bar", new DoubleSumAggregatorFactory("bar", "bar"))
        .aggregator("baz", new LongMaxAggregatorFactory("baz", "baz"))
        .build();
    Assert.assertEquals(expectedEarliest, mergeEarliest(analysis1, analysis2));
    // Simulate multi-level earliest merge
    Assert.assertEquals(
        expectedEarliest,
        mergeEarliest(mergeEarliest(analysis1, analysis2), mergeEarliest(analysis1, analysis2))
    );

    // Test latest merge, returns a map with "bar" as DoubleMaxAggregatorFactory since analysis2 has the later partition
    SegmentAnalysis expectedLatest = new SegmentAnalysisBuilder(
        "dummy_2023-01-01T00:00:00.000Z_2023-01-02T00:00:00.000Z_merged_2")
        .aggregator("foo", new LongSumAggregatorFactory("foo", "foo"))
        .aggregator("bar", new DoubleMaxAggregatorFactory("bar", "bar"))
        .aggregator("baz", new LongMaxAggregatorFactory("baz", "baz"))
        .build();
    Assert.assertEquals(expectedLatest, mergeLatest(analysis1, analysis2));
    // Simulate multi-level latest merge
    Assert.assertEquals(
        expectedLatest,
        mergeLatest(mergeLatest(analysis1, analysis2), mergeLatest(analysis1, analysis2))
    );
  }

  @Test
  public void testFilterSegments()
  {
    final SegmentMetadataQueryConfig config = new SegmentMetadataQueryConfig();
    final SegmentMetadataQueryQueryToolChest toolChest = new SegmentMetadataQueryQueryToolChest(config);

    final List<LogicalSegment> filteredSegments = toolChest.filterSegments(
        Druids.newSegmentMetadataQueryBuilder().dataSource("foo").merge(true).build(),
        ImmutableList
            .of(
                "2000-01-01/P1D",
                "2000-01-04/P1D",
                "2000-01-09/P1D",
                "2000-01-09/P1D"
            )
            .stream()
            .map(interval -> new LogicalSegment()
            {
              @Override
              public Interval getInterval()
              {
                return Intervals.of(interval);
              }

              @Override
              public Interval getTrueInterval()
              {
                return Intervals.of(interval);
              }
            })
            .collect(Collectors.toList())
    );

    Assert.assertEquals(Period.weeks(1), config.getDefaultHistory());
    Assert.assertEquals(
        ImmutableList.of(
            Intervals.of("2000-01-04/P1D"),
            Intervals.of("2000-01-09/P1D"),
            Intervals.of("2000-01-09/P1D")
        ),
        filteredSegments.stream().map(LogicalSegment::getInterval).collect(Collectors.toList())
    );
  }

  @SuppressWarnings("ArgumentParameterSwap")
  @EnumSource(AggregatorMergeStrategy.class)
  @ParameterizedTest(name = "{index}: with AggregatorMergeStrategy {0}")
  public void testMergeRollup(AggregatorMergeStrategy aggregatorMergeStrategy)
  {
    final SegmentAnalysis analysis1 = new SegmentAnalysisBuilder(TEST_SEGMENT_ID1).build();
    final SegmentAnalysis analysis2 = new SegmentAnalysisBuilder(TEST_SEGMENT_ID2).rollup(false).build();
    final SegmentAnalysis analysis3 = new SegmentAnalysisBuilder(TEST_SEGMENT_ID1).rollup(false).build();
    final SegmentAnalysis analysis4 = new SegmentAnalysisBuilder(TEST_SEGMENT_ID2).rollup(true).build();
    final SegmentAnalysis analysis5 = new SegmentAnalysisBuilder(TEST_SEGMENT_ID1).rollup(true).build();

    Assert.assertNull(mergeWithStrategy(analysis1, analysis2, aggregatorMergeStrategy).isRollup());
    Assert.assertNull(mergeWithStrategy(analysis1, analysis4, aggregatorMergeStrategy).isRollup());
    Assert.assertNull(mergeWithStrategy(analysis2, analysis4, aggregatorMergeStrategy).isRollup());
    Assert.assertFalse(mergeWithStrategy(analysis2, analysis3, aggregatorMergeStrategy).isRollup());
    Assert.assertTrue(mergeWithStrategy(analysis4, analysis5, aggregatorMergeStrategy).isRollup());
  }

  @EnumSource(AggregatorMergeStrategy.class)
  @ParameterizedTest(name = "{index}: with AggregatorMergeStrategy {0}")
  public void testInvalidMergeAggregatorsWithNullOrEmptyDatasource(AggregatorMergeStrategy aggregatorMergeStrategy)
  {
    final SegmentAnalysis analysis1 = new SegmentAnalysisBuilder(TEST_SEGMENT_ID1).build();
    final SegmentAnalysis analysis2 = new SegmentAnalysisBuilder(TEST_SEGMENT_ID2).build();

    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> SegmentMetadataQueryQueryToolChest.mergeAnalyses(
                null,
                analysis1,
                analysis2,
                aggregatorMergeStrategy
            )
        ),
        DruidExceptionMatcher.defensive().expectMessageIs("SegementMetadata queries require at least one datasource.")
    );

    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> SegmentMetadataQueryQueryToolChest.mergeAnalyses(
                ImmutableSet.of(),
                analysis1,
                analysis2,
                aggregatorMergeStrategy
            )
        ),
        DruidExceptionMatcher
            .defensive()
            .expectMessageIs(
                "SegementMetadata queries require at least one datasource.")
    );
  }

  @EnumSource(AggregatorMergeStrategy.class)
  @ParameterizedTest(name = "{index}: with AggregatorMergeStrategy {0}")
  public void testMergeWithUnionDatasource(AggregatorMergeStrategy aggregatorMergeStrategy)
  {
    final SegmentAnalysis analysis1 = new SegmentAnalysisBuilder(TEST_SEGMENT_ID1)
        .aggregator("foo", new LongSumAggregatorFactory("foo", "foo"))
        .aggregator("bar", new DoubleSumAggregatorFactory("bar", "bar"))
        .build();
    final SegmentAnalysis analysis2 = new SegmentAnalysisBuilder(TEST_SEGMENT_ID2)
        .aggregator("foo", new LongSumAggregatorFactory("foo", "foo"))
        .aggregator("bar", new DoubleSumAggregatorFactory("bar", "bar"))
        .build();

    final SegmentAnalysis expectedMergedAnalysis = new SegmentAnalysisBuilder(
        "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged")
        .aggregator("foo", new LongSumAggregatorFactory("foo", "foo"))
        .aggregator("bar", new DoubleSumAggregatorFactory("bar", "bar"))
        .build();

    UnionDataSource dataSource1 = new UnionDataSource(ImmutableList.of(
        new TableDataSource("foo"),
        new TableDataSource("dummy")
    ));
    UnionDataSource dataSource2 = new UnionDataSource(ImmutableList.of(
        new TableDataSource("dummy"),
        new TableDataSource("foo"),
        new TableDataSource("bar")
    ));

    Assert.assertEquals(
        expectedMergedAnalysis,
        SegmentMetadataQueryQueryToolChest.finalizeAnalysis(SegmentMetadataQueryQueryToolChest.mergeAnalyses(
            dataSource1.getTableNames(),
            analysis1,
            analysis2,
            aggregatorMergeStrategy
        ))
    );
    Assert.assertEquals(
        expectedMergedAnalysis,
        SegmentMetadataQueryQueryToolChest.finalizeAnalysis(
            SegmentMetadataQueryQueryToolChest.mergeAnalyses(
                dataSource2.getTableNames(),
                analysis1,
                analysis2,
                aggregatorMergeStrategy
            )
        )
    );
  }

  @EnumSource(AggregatorMergeStrategy.class)
  @ParameterizedTest(name = "{index}: with AggregatorMergeStrategy {0}")
  public void testMergeWithNullAnalyses(AggregatorMergeStrategy aggregatorMergeStrategy)
  {
    final SegmentAnalysis analysis1 = new SegmentAnalysisBuilder(TEST_SEGMENT_ID1).build();
    final SegmentAnalysis analysis2 = new SegmentAnalysisBuilder(TEST_SEGMENT_ID2).build();

    Assert.assertEquals(analysis1, mergeWithStrategy(analysis1, null, aggregatorMergeStrategy));
    Assert.assertEquals(analysis2, mergeWithStrategy(null, analysis2, aggregatorMergeStrategy));
    Assert.assertNull(
        SegmentMetadataQueryQueryToolChest
            .mergeAnalyses(TEST_DATASOURCE.getTableNames(), null, null, aggregatorMergeStrategy));
  }

  private static SegmentAnalysis mergeWithStrategy(
      SegmentAnalysis analysis1,
      SegmentAnalysis analysis2,
      AggregatorMergeStrategy strategy
  )
  {
    return SegmentMetadataQueryQueryToolChest.finalizeAnalysis(
        SegmentMetadataQueryQueryToolChest.mergeAnalyses(
            TEST_DATASOURCE.getTableNames(),
            analysis1,
            analysis2,
            strategy
        ));
  }

  private static SegmentAnalysis mergeStrict(SegmentAnalysis analysis1, SegmentAnalysis analysis2)
  {
    return mergeWithStrategy(analysis1, analysis2, AggregatorMergeStrategy.STRICT);
  }

  private static SegmentAnalysis mergeLenient(SegmentAnalysis analysis1, SegmentAnalysis analysis2)
  {
    return mergeWithStrategy(analysis1, analysis2, AggregatorMergeStrategy.LENIENT);
  }

  private static SegmentAnalysis mergeEarliest(SegmentAnalysis analysis1, SegmentAnalysis analysis2)
  {
    return mergeWithStrategy(analysis1, analysis2, AggregatorMergeStrategy.EARLIEST);
  }

  private static SegmentAnalysis mergeLatest(SegmentAnalysis analysis1, SegmentAnalysis analysis2)
  {
    return mergeWithStrategy(analysis1, analysis2, AggregatorMergeStrategy.LATEST);
  }
}
