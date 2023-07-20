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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
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
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.timeline.LogicalSegment;
import org.apache.druid.timeline.SegmentId;
import org.hamcrest.MatcherAssert;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SegmentMetadataQueryQueryToolChestTest
{
  private static final DataSource TEST_DATASOURCE = new TableDataSource("dummy");
  private static final SegmentId TEST_SEGMENT_ID1 = SegmentId.of(
      TEST_DATASOURCE.toString(),
      Intervals.of("2020-01-01/2020-01-02"),
      "test",
      0
  );
  private static final SegmentId TEST_SEGMENT_ID2 = SegmentId.of(
      TEST_DATASOURCE.toString(),
      Intervals.of("2021-01-01/2021-01-02"),
      "test",
      0
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

    SegmentAnalysis result = new SegmentAnalysis(
        TEST_SEGMENT_ID1.toString(),
        ImmutableList.of(Intervals.of("2011-01-12T00:00:00.000Z/2011-04-15T00:00:00.001Z")),
        new LinkedHashMap<>(
            ImmutableMap.of(
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
        ),
        71982,
        100,
        null,
        null,
        null,
        null
    );

    Object preparedValue = strategy.prepareForSegmentLevelCache().apply(result);

    ObjectMapper objectMapper = new DefaultObjectMapper();
    SegmentAnalysis fromCacheValue = objectMapper.readValue(
        objectMapper.writeValueAsBytes(preparedValue),
        strategy.getCacheObjectClazz()
    );

    SegmentAnalysis fromCacheResult = strategy.pullFromSegmentLevelCache().apply(fromCacheValue);

    Assert.assertEquals(result, fromCacheResult);
  }

  @Test
  public void testMergeAggregators()
  {
    final SegmentAnalysis analysis1 = new SegmentAnalysis(
        TEST_SEGMENT_ID1.toString(),
        null,
        new LinkedHashMap<>(),
        0,
        0,
        ImmutableMap.of(
            "foo", new LongSumAggregatorFactory("foo", "foo"),
            "baz", new DoubleSumAggregatorFactory("baz", "baz")
        ),
        null,
        null,
        null
    );
    final SegmentAnalysis analysis2 = new SegmentAnalysis(
        TEST_SEGMENT_ID2.toString(),
        null,
        new LinkedHashMap<>(),
        0,
        0,
        ImmutableMap.of(
            "foo", new LongSumAggregatorFactory("foo", "foo"),
            "bar", new DoubleSumAggregatorFactory("bar", "bar")
        ),
        null,
        null,
        null
    );

    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            ImmutableMap.of(
                "foo", new LongSumAggregatorFactory("foo", "foo"),
                "bar", new DoubleSumAggregatorFactory("bar", "bar"),
                "baz", new DoubleSumAggregatorFactory("baz", "baz")
            ),
            null,
            null,
            null
        ),
        mergeStrict(analysis1, analysis2)
    );

    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            ImmutableMap.of(
                "foo", new LongSumAggregatorFactory("foo", "foo"),
                "bar", new DoubleSumAggregatorFactory("bar", "bar"),
                "baz", new DoubleSumAggregatorFactory("baz", "baz")
            ),
            null,
            null,
            null
        ),
        mergeLenient(analysis1, analysis2)
    );

    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            ImmutableMap.of(
                "foo", new LongSumAggregatorFactory("foo", "foo"),
                "bar", new DoubleSumAggregatorFactory("bar", "bar"),
                "baz", new DoubleSumAggregatorFactory("baz", "baz")
            ),
            null,
            null,
            null
        ),
        mergeEarliest(analysis1, analysis2)
    );

    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            ImmutableMap.of(
                "foo", new LongSumAggregatorFactory("foo", "foo"),
                "bar", new DoubleSumAggregatorFactory("bar", "bar"),
                "baz", new DoubleSumAggregatorFactory("baz", "baz")
            ),
            null,
            null,
            null
        ),
        mergeLatest(analysis1, analysis2)
    );
  }

  @Test
  public void testMergeAggregatorsWithIntervals()
  {
    final SegmentAnalysis analysis1 = new SegmentAnalysis(
        TEST_SEGMENT_ID1.toString(),
        ImmutableList.of(TEST_SEGMENT_ID1.getInterval()),
        new LinkedHashMap<>(),
        0,
        0,
        ImmutableMap.of(
            "foo", new LongSumAggregatorFactory("foo", "foo"),
            "baz", new DoubleSumAggregatorFactory("baz", "baz")
        ),
        null,
        null,
        null
    );
    final SegmentAnalysis analysis2 = new SegmentAnalysis(
        TEST_SEGMENT_ID2.toString(),
        ImmutableList.of(TEST_SEGMENT_ID2.getInterval()),
        new LinkedHashMap<>(),
        0,
        0,
        ImmutableMap.of(
            "foo", new LongSumAggregatorFactory("foo", "foo"),
            "bar", new DoubleSumAggregatorFactory("bar", "bar")
        ),
        null,
        null,
        null
    );

    final List<Interval> expectedIntervals = new ArrayList<>();
    expectedIntervals.addAll(analysis1.getIntervals());
    expectedIntervals.addAll(analysis2.getIntervals());

    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged",
            expectedIntervals,
            new LinkedHashMap<>(),
            0,
            0,
            ImmutableMap.of(
                "foo", new LongSumAggregatorFactory("foo", "foo"),
                "bar", new DoubleSumAggregatorFactory("bar", "bar"),
                "baz", new DoubleSumAggregatorFactory("baz", "baz")
            ),
            null,
            null,
            null
        ),
        mergeStrict(analysis1, analysis2)
    );

    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged",
            expectedIntervals,
            new LinkedHashMap<>(),
            0,
            0,
            ImmutableMap.of(
                "foo", new LongSumAggregatorFactory("foo", "foo"),
                "bar", new DoubleSumAggregatorFactory("bar", "bar"),
                "baz", new DoubleSumAggregatorFactory("baz", "baz")
            ),
            null,
            null,
            null
        ),
        mergeLenient(analysis1, analysis2)
    );

    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged",
            expectedIntervals,
            new LinkedHashMap<>(),
            0,
            0,
            ImmutableMap.of(
                "foo", new LongSumAggregatorFactory("foo", "foo"),
                "baz", new DoubleSumAggregatorFactory("baz", "baz"),
                "bar", new DoubleSumAggregatorFactory("bar", "bar")
            ),
            null,
            null,
            null
        ),
        mergeEarliest(analysis1, analysis2)
    );

    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged",
            expectedIntervals,
            new LinkedHashMap<>(),
            0,
            0,
            ImmutableMap.of(
                "foo", new LongSumAggregatorFactory("foo", "foo"),
                "bar", new DoubleSumAggregatorFactory("bar", "bar"),
                "baz", new DoubleSumAggregatorFactory("baz", "baz")
            ),
            null,
            null,
            null
        ),
        mergeLatest(analysis1, analysis2)
    );
  }

  @Test
  public void testMergeAggregatorsOneNull()
  {
    final SegmentAnalysis analysis1 = new SegmentAnalysis(
        TEST_SEGMENT_ID1.toString(),
        null,
        new LinkedHashMap<>(),
        0,
        0,
        null,
        null,
        null,
        null
    );
    final SegmentAnalysis analysis2 = new SegmentAnalysis(
        TEST_SEGMENT_ID2.toString(),
        null,
        new LinkedHashMap<>(),
        0,
        0,
        ImmutableMap.of(
            "foo", new LongSumAggregatorFactory("foo", "foo"),
            "bar", new DoubleSumAggregatorFactory("bar", "bar")
        ),
        null,
        null,
        null
    );

    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            null,
            null,
            null,
            null
        ),
        mergeStrict(analysis1, analysis2)
    );

    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            ImmutableMap.of(
                "foo", new LongSumAggregatorFactory("foo", "foo"),
                "bar", new DoubleSumAggregatorFactory("bar", "bar")
            ),
            null,
            null,
            null
        ),
        mergeLenient(analysis1, analysis2)
    );

    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            ImmutableMap.of(
                "foo", new LongSumAggregatorFactory("foo", "foo"),
                "bar", new DoubleSumAggregatorFactory("bar", "bar")
            ),
            null,
            null,
            null
        ),
        mergeEarliest(analysis1, analysis2)
    );

    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            ImmutableMap.of(
                "foo", new LongSumAggregatorFactory("foo", "foo"),
                "bar", new DoubleSumAggregatorFactory("bar", "bar")
            ),
            null,
            null,
            null
        ),
        mergeLatest(analysis1, analysis2)
    );
  }

  @Test
  public void testMergeAggregatorsAllNull()
  {
    final SegmentAnalysis analysis1 = new SegmentAnalysis(
        TEST_SEGMENT_ID1.toString(),
        null,
        new LinkedHashMap<>(),
        0,
        0,
        null,
        null,
        null,
        null
    );
    final SegmentAnalysis analysis2 = new SegmentAnalysis(
        TEST_SEGMENT_ID2.toString(),
        null,
        new LinkedHashMap<>(),
        0,
        0,
        null,
        null,
        null,
        null
    );

    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            null,
            null,
            null,
            null
        ),
        mergeStrict(analysis1, analysis2)
    );

    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            null,
            null,
            null,
            null
        ),
        mergeLenient(analysis1, analysis2)
    );

    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            null,
            null,
            null,
            null
        ),
        mergeEarliest(analysis1, analysis2)
    );

    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            null,
            null,
            null,
            null
        ),
        mergeLatest(analysis1, analysis2)
    );
  }

  @Test
  public void testMergeAggregatorsConflict()
  {
    final SegmentAnalysis analysis1 = new SegmentAnalysis(
        TEST_SEGMENT_ID1.toString(),
        null,
        new LinkedHashMap<>(),
        0,
        0,
        ImmutableMap.of(
            "foo", new LongSumAggregatorFactory("foo", "foo"),
            "bar", new DoubleSumAggregatorFactory("bar", "bar")
        ),
        null,
        null,
        null
    );
    final SegmentAnalysis analysis2 = new SegmentAnalysis(
        TEST_SEGMENT_ID2.toString(),
        null,
        new LinkedHashMap<>(),
        0,
        0,
        ImmutableMap.of(
            "foo", new LongSumAggregatorFactory("foo", "foo"),
            "bar", new DoubleMaxAggregatorFactory("bar", "bar"),
            "baz", new LongMaxAggregatorFactory("baz", "baz")
        ),
        null,
        null,
        null
    );

    final Map<String, AggregatorFactory> expectedLenient = new HashMap<>();
    expectedLenient.put("foo", new LongSumAggregatorFactory("foo", "foo"));
    expectedLenient.put("bar", null);
    expectedLenient.put("baz", new LongMaxAggregatorFactory("baz", "baz"));

    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            null,
            null,
            null,
            null
        ),
        mergeStrict(analysis1, analysis2)
    );

    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            expectedLenient,
            null,
            null,
            null
        ),
        mergeLenient(analysis1, analysis2)
    );

    // Simulate multi-level lenient merge
    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            expectedLenient,
            null,
            null,
            null
        ),
        mergeLenient(
            mergeLenient(analysis1, analysis2),
            mergeLenient(analysis1, analysis2)
        )
    );

    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            ImmutableMap.of(
                "foo", new LongSumAggregatorFactory("foo", "foo"),
                "bar", new DoubleSumAggregatorFactory("bar", "bar"),
                "baz", new LongMaxAggregatorFactory("baz", "baz")
            ),
            null,
            null,
            null
        ),
        mergeEarliest(analysis1, analysis2)
    );

    // Simulate multi-level earliest merge
    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            ImmutableMap.of(
                "foo", new LongSumAggregatorFactory("foo", "foo"),
                "bar", new DoubleSumAggregatorFactory("bar", "bar"),
                "baz", new LongMaxAggregatorFactory("baz", "baz")
            ),
            null,
            null,
            null
        ),
        mergeEarliest(
            mergeEarliest(analysis1, analysis2),
            mergeEarliest(analysis1, analysis2)
        )
    );

    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            ImmutableMap.of(
                "foo", new LongSumAggregatorFactory("foo", "foo"),
                "bar", new DoubleMaxAggregatorFactory("bar", "bar"),
                "baz", new LongMaxAggregatorFactory("baz", "baz")
            ),
            null,
            null,
            null
        ),
        mergeLatest(analysis1, analysis2)
    );

    // Simulate multi-level latest merge
    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            ImmutableMap.of(
                "foo", new LongSumAggregatorFactory("foo", "foo"),
                "bar", new DoubleMaxAggregatorFactory("bar", "bar"),
                "baz", new LongMaxAggregatorFactory("baz", "baz")
            ),
            null,
            null,
            null
        ),
        mergeLatest(
            mergeLatest(analysis1, analysis2),
            mergeLatest(analysis1, analysis2)
        )
    );
  }

  @Test
  public void testMergeAggregatorsConflictWithDifferentOrder()
  {
    final SegmentAnalysis analysis1 = new SegmentAnalysis(
        TEST_SEGMENT_ID2.toString(),
        null,
        new LinkedHashMap<>(),
        0,
        0,
        ImmutableMap.of(
            "foo", new LongSumAggregatorFactory("foo", "foo"),
            "bar", new DoubleSumAggregatorFactory("bar", "bar")
        ),
        null,
        null,
        null
    );

    final SegmentAnalysis analysis2 = new SegmentAnalysis(
        TEST_SEGMENT_ID1.toString(),
        null,
        new LinkedHashMap<>(),
        0,
        0,
        ImmutableMap.of(
            "foo", new LongSumAggregatorFactory("foo", "foo"),
            "bar", new DoubleMaxAggregatorFactory("bar", "bar"),
            "baz", new LongMaxAggregatorFactory("baz", "baz")
        ),
        null,
        null,
        null
    );

    final Map<String, AggregatorFactory> expectedLenient = new HashMap<>();
    expectedLenient.put("foo", new LongSumAggregatorFactory("foo", "foo"));
    expectedLenient.put("bar", null);
    expectedLenient.put("baz", new LongMaxAggregatorFactory("baz", "baz"));

    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            null,
            null,
            null,
            null
        ),
        mergeStrict(analysis1, analysis2)
    );

    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            expectedLenient,
            null,
            null,
            null
        ),
        mergeLenient(analysis1, analysis2)
    );

    // Simulate multi-level lenient merge
    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            expectedLenient,
            null,
            null,
            null
        ),
        mergeLenient(
            mergeLenient(analysis1, analysis2),
            mergeLenient(analysis1, analysis2)
        )
    );

    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            ImmutableMap.of(
                "foo", new LongSumAggregatorFactory("foo", "foo"),
                "bar", new DoubleMaxAggregatorFactory("bar", "bar"),
                "baz", new LongMaxAggregatorFactory("baz", "baz")
            ),
            null,
            null,
            null
        ),
        mergeEarliest(analysis1, analysis2)
    );

    // Simulate multi-level earliest merge
    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            ImmutableMap.of(
                "foo", new LongSumAggregatorFactory("foo", "foo"),
                "bar", new DoubleMaxAggregatorFactory("bar", "bar"),
                "baz", new LongMaxAggregatorFactory("baz", "baz")
            ),
            null,
            null,
            null
        ),
        mergeEarliest(
            mergeEarliest(analysis1, analysis2),
            mergeEarliest(analysis1, analysis2)
        )
    );

    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            ImmutableMap.of(
                "foo", new LongSumAggregatorFactory("foo", "foo"),
                "bar", new DoubleSumAggregatorFactory("bar", "bar"),
                "baz", new LongMaxAggregatorFactory("baz", "baz")
            ),
            null,
            null,
            null
        ),
        mergeLatest(analysis1, analysis2)
    );

    // Simulate multi-level latest merge
    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            ImmutableMap.of(
                "foo", new LongSumAggregatorFactory("foo", "foo"),
                "bar", new DoubleSumAggregatorFactory("bar", "bar"),
                "baz", new LongMaxAggregatorFactory("baz", "baz")
            ),
            null,
            null,
            null
        ),
        mergeLatest(
            mergeLatest(analysis1, analysis2),
            mergeLatest(analysis1, analysis2)
        )
    );
  }

  @Test
  public void testMergeAggregatorsConflictWithEqualSegmentIntervalsAndDifferentPartitions()
  {
    final SegmentId segmentId1 = SegmentId.of(
        TEST_DATASOURCE.toString(),
        Intervals.of("2023-01-01/2023-01-02"),
        "test",
        1
    );
    final SegmentId segmentId2 = SegmentId.of(
        TEST_DATASOURCE.toString(),
        Intervals.of("2023-01-01/2023-01-02"),
        "test",
        2
    );

    final SegmentAnalysis analysis1 = new SegmentAnalysis(
        segmentId1.toString(),
        null,
        new LinkedHashMap<>(),
        0,
        0,
        ImmutableMap.of(
            "foo", new LongSumAggregatorFactory("foo", "foo"),
            "bar", new DoubleSumAggregatorFactory("bar", "bar")
        ),
        null,
        null,
        null
    );

    final SegmentAnalysis analysis2 = new SegmentAnalysis(
        segmentId2.toString(),
        null,
        new LinkedHashMap<>(),
        0,
        0,
        ImmutableMap.of(
            "foo", new LongSumAggregatorFactory("foo", "foo"),
            "bar", new DoubleMaxAggregatorFactory("bar", "bar"),
            "baz", new LongMaxAggregatorFactory("baz", "baz")
        ),
        null,
        null,
        null
    );

    final Map<String, AggregatorFactory> expectedLenient = new HashMap<>();
    expectedLenient.put("foo", new LongSumAggregatorFactory("foo", "foo"));
    expectedLenient.put("bar", null);
    expectedLenient.put("baz", new LongMaxAggregatorFactory("baz", "baz"));

    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2023-01-01T00:00:00.000Z_2023-01-02T00:00:00.000Z_merged_2",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            null,
            null,
            null,
            null
        ),
        mergeStrict(analysis1, analysis2)
    );

    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2023-01-01T00:00:00.000Z_2023-01-02T00:00:00.000Z_merged_2",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            expectedLenient,
            null,
            null,
            null
        ),
        mergeLenient(analysis1, analysis2)
    );

    // Simulate multi-level lenient merge
    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2023-01-01T00:00:00.000Z_2023-01-02T00:00:00.000Z_merged_2",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            expectedLenient,
            null,
            null,
            null
        ),
        mergeLenient(
            mergeLenient(analysis1, analysis2),
            mergeLenient(analysis1, analysis2)
        )
    );

    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2023-01-01T00:00:00.000Z_2023-01-02T00:00:00.000Z_merged_2",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            ImmutableMap.of(
                "foo", new LongSumAggregatorFactory("foo", "foo"),
                "bar", new DoubleSumAggregatorFactory("bar", "bar"),
                "baz", new LongMaxAggregatorFactory("baz", "baz")
            ),
            null,
            null,
            null
        ),
        mergeEarliest(analysis1, analysis2)
    );

    // Simulate multi-level earliest merge
    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2023-01-01T00:00:00.000Z_2023-01-02T00:00:00.000Z_merged_2",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            ImmutableMap.of(
                "foo", new LongSumAggregatorFactory("foo", "foo"),
                "bar", new DoubleSumAggregatorFactory("bar", "bar"),
                "baz", new LongMaxAggregatorFactory("baz", "baz")
            ),
            null,
            null,
            null
        ),
        mergeEarliest(
            mergeEarliest(analysis1, analysis2),
            mergeEarliest(analysis1, analysis2)
        )
    );

    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2023-01-01T00:00:00.000Z_2023-01-02T00:00:00.000Z_merged_2",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            ImmutableMap.of(
                "foo", new LongSumAggregatorFactory("foo", "foo"),
                "bar", new DoubleMaxAggregatorFactory("bar", "bar"),
                "baz", new LongMaxAggregatorFactory("baz", "baz")
            ),
            null,
            null,
            null
        ),
        mergeLatest(analysis1, analysis2)
    );

    // Simulate multi-level latest merge
    Assert.assertEquals(
        new SegmentAnalysis(
            "dummy_2023-01-01T00:00:00.000Z_2023-01-02T00:00:00.000Z_merged_2",
            null,
            new LinkedHashMap<>(),
            0,
            0,
            ImmutableMap.of(
                "foo", new LongSumAggregatorFactory("foo", "foo"),
                "bar", new DoubleMaxAggregatorFactory("bar", "bar"),
                "baz", new LongMaxAggregatorFactory("baz", "baz")
            ),
            null,
            null,
            null
        ),
        mergeLatest(
            mergeLatest(analysis1, analysis2),
            mergeLatest(analysis1, analysis2)
        )
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
  @Test
  public void testMergeRollup()
  {
    final SegmentAnalysis analysis1 = new SegmentAnalysis(
        TEST_SEGMENT_ID1.toString(),
        null,
        new LinkedHashMap<>(),
        0,
        0,
        null,
        null,
        null,
        null
    );
    final SegmentAnalysis analysis2 = new SegmentAnalysis(
        TEST_SEGMENT_ID2.toString(),
        null,
        new LinkedHashMap<>(),
        0,
        0,
        null,
        null,
        null,
        false
    );
    final SegmentAnalysis analysis3 = new SegmentAnalysis(
        TEST_SEGMENT_ID1.toString(),
        null,
        new LinkedHashMap<>(),
        0,
        0,
        null,
        null,
        null,
        false
    );
    final SegmentAnalysis analysis4 = new SegmentAnalysis(
        TEST_SEGMENT_ID2.toString(),
        null,
        new LinkedHashMap<>(),
        0,
        0,
        null,
        null,
        null,
        true
    );
    final SegmentAnalysis analysis5 = new SegmentAnalysis(
        TEST_SEGMENT_ID1.toString(),
        null,
        new LinkedHashMap<>(),
        0,
        0,
        null,
        null,
        null,
        true
    );

    Assert.assertNull(mergeStrict(analysis1, analysis2).isRollup());
    Assert.assertNull(mergeStrict(analysis1, analysis4).isRollup());
    Assert.assertNull(mergeStrict(analysis2, analysis4).isRollup());
    Assert.assertFalse(mergeStrict(analysis2, analysis3).isRollup());
    Assert.assertTrue(mergeStrict(analysis4, analysis5).isRollup());

    Assert.assertNull(mergeLenient(analysis1, analysis2).isRollup());
    Assert.assertNull(mergeLenient(analysis1, analysis4).isRollup());
    Assert.assertNull(mergeLenient(analysis2, analysis4).isRollup());
    Assert.assertFalse(mergeLenient(analysis2, analysis3).isRollup());
    Assert.assertTrue(mergeLenient(analysis4, analysis5).isRollup());

    Assert.assertNull(mergeEarliest(analysis1, analysis2).isRollup());
    Assert.assertNull(mergeEarliest(analysis1, analysis4).isRollup());
    Assert.assertNull(mergeEarliest(analysis2, analysis4).isRollup());
    Assert.assertFalse(mergeEarliest(analysis2, analysis3).isRollup());
    Assert.assertTrue(mergeEarliest(analysis4, analysis5).isRollup());

    Assert.assertNull(mergeLatest(analysis1, analysis2).isRollup());
    Assert.assertNull(mergeLatest(analysis1, analysis4).isRollup());
    Assert.assertNull(mergeLatest(analysis2, analysis4).isRollup());
    Assert.assertFalse(mergeLatest(analysis2, analysis3).isRollup());
    Assert.assertTrue(mergeLatest(analysis4, analysis5).isRollup());
  }

  @Test
  public void testInvalidMergeAggregatorsWithNullOrEmptyDatasource()
  {
    final SegmentAnalysis analysis1 = new SegmentAnalysis(
        TEST_SEGMENT_ID1.toString(),
        null,
        new LinkedHashMap<>(),
        0,
        0,
        null,
        null,
        null,
        null
    );
    final SegmentAnalysis analysis2 = new SegmentAnalysis(
        TEST_SEGMENT_ID2.toString(),
        null,
        new LinkedHashMap<>(),
        0,
        0,
        null,
        null,
        null,
        false
    );

    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> SegmentMetadataQueryQueryToolChest.mergeAnalyses(
                null,
                analysis1,
                analysis2,
                AggregatorMergeStrategy.STRICT
            )
        ),
        DruidExceptionMatcher
            .invalidInput()
            .expectMessageIs(
                "SegementMetadata queries require at least one datasource.")
    );

    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> SegmentMetadataQueryQueryToolChest.mergeAnalyses(
                ImmutableSet.of(),
                analysis1,
                analysis2,
                AggregatorMergeStrategy.STRICT
            )
        ),
        DruidExceptionMatcher
            .invalidInput()
            .expectMessageIs(
                "SegementMetadata queries require at least one datasource.")
    );
  }


  @Test
  public void testMergeWithUnionDatasource()
  {
    final SegmentAnalysis analysis1 = new SegmentAnalysis(
        TEST_SEGMENT_ID1.toString(),
        null,
        new LinkedHashMap<>(),
        0,
        0,
        ImmutableMap.of(
            "foo", new LongSumAggregatorFactory("foo", "foo"),
            "bar", new DoubleSumAggregatorFactory("bar", "bar")
        ),
        null,
        null,
        null
    );
    final SegmentAnalysis analysis2 = new SegmentAnalysis(
        TEST_SEGMENT_ID2.toString(),
        null,
        new LinkedHashMap<>(),
        0,
        0,
        ImmutableMap.of(
            "foo", new LongSumAggregatorFactory("foo", "foo"),
            "bar", new DoubleMaxAggregatorFactory("bar", "bar"),
            "baz", new LongMaxAggregatorFactory("baz", "baz")
        ),
        null,
        null,
        false
    );

    final SegmentAnalysis expectedMergedAnalysis = new SegmentAnalysis(
        "dummy_2021-01-01T00:00:00.000Z_2021-01-02T00:00:00.000Z_merged",
        null,
        new LinkedHashMap<>(),
        0,
        0,
        ImmutableMap.of(
            "foo", new LongSumAggregatorFactory("foo", "foo"),
            "bar", new DoubleMaxAggregatorFactory("bar", "bar"),
            "baz", new LongMaxAggregatorFactory("baz", "baz")
        ),
        null,
        null,
        null
    );

    Assert.assertEquals(
        expectedMergedAnalysis,
        SegmentMetadataQueryQueryToolChest.finalizeAnalysis(
            SegmentMetadataQueryQueryToolChest.mergeAnalyses(
                new UnionDataSource(
                    ImmutableList.of(
                        new TableDataSource("foo"),
                        new TableDataSource("dummy")
                    )
                ).getTableNames(),
                analysis1,
                analysis2,
                AggregatorMergeStrategy.LATEST
            )
        )
    );

    Assert.assertEquals(
        expectedMergedAnalysis,
        SegmentMetadataQueryQueryToolChest.finalizeAnalysis(
            SegmentMetadataQueryQueryToolChest.mergeAnalyses(
                new UnionDataSource(
                    ImmutableList.of(
                        new TableDataSource("dummy"),
                        new TableDataSource("foo"),
                        new TableDataSource("bar")
                    )
                ).getTableNames(),
                analysis1,
                analysis2,
                AggregatorMergeStrategy.LATEST
            )
        )
    );
  }

  @Test
  public void testMergeWithNullAnalyses()
  {
    final SegmentAnalysis analysis1 = new SegmentAnalysis(
        TEST_SEGMENT_ID1.toString(),
        null,
        new LinkedHashMap<>(),
        0,
        0,
        null,
        null,
        null,
        null
    );
    final SegmentAnalysis analysis2 = new SegmentAnalysis(
        TEST_SEGMENT_ID2.toString(),
        null,
        new LinkedHashMap<>(),
        0,
        0,
        null,
        null,
        null,
        false
    );

    Assert.assertEquals(
        analysis1,
        SegmentMetadataQueryQueryToolChest
            .mergeAnalyses(TEST_DATASOURCE.getTableNames(), analysis1, null, AggregatorMergeStrategy.STRICT)
    );
    Assert.assertEquals(
        analysis2,
        SegmentMetadataQueryQueryToolChest
            .mergeAnalyses(TEST_DATASOURCE.getTableNames(), null, analysis2, AggregatorMergeStrategy.STRICT)
    );
    Assert.assertNull(
        SegmentMetadataQueryQueryToolChest
            .mergeAnalyses(TEST_DATASOURCE.getTableNames(), null, null, AggregatorMergeStrategy.STRICT)
    );
    Assert.assertNull(
        SegmentMetadataQueryQueryToolChest
            .mergeAnalyses(TEST_DATASOURCE.getTableNames(), null, null, AggregatorMergeStrategy.LENIENT)
    );
    Assert.assertNull(
        SegmentMetadataQueryQueryToolChest
            .mergeAnalyses(TEST_DATASOURCE.getTableNames(), null, null, AggregatorMergeStrategy.EARLIEST)
    );
    Assert.assertNull(
        SegmentMetadataQueryQueryToolChest
            .mergeAnalyses(TEST_DATASOURCE.getTableNames(), null, null, AggregatorMergeStrategy.LATEST)
    );
  }

  private static SegmentAnalysis mergeStrict(SegmentAnalysis analysis1, SegmentAnalysis analysis2)
  {
    return SegmentMetadataQueryQueryToolChest.finalizeAnalysis(
        SegmentMetadataQueryQueryToolChest.mergeAnalyses(
            TEST_DATASOURCE.getTableNames(),
            analysis1,
            analysis2,
            AggregatorMergeStrategy.STRICT
        )
    );
  }

  private static SegmentAnalysis mergeLenient(SegmentAnalysis analysis1, SegmentAnalysis analysis2)
  {
    return SegmentMetadataQueryQueryToolChest.finalizeAnalysis(
        SegmentMetadataQueryQueryToolChest.mergeAnalyses(
            TEST_DATASOURCE.getTableNames(),
            analysis1,
            analysis2,
            AggregatorMergeStrategy.LENIENT
        )
    );
  }

  private static SegmentAnalysis mergeEarliest(SegmentAnalysis analysis1, SegmentAnalysis analysis2)
  {
    return SegmentMetadataQueryQueryToolChest.finalizeAnalysis(
        SegmentMetadataQueryQueryToolChest.mergeAnalyses(
            TEST_DATASOURCE.getTableNames(),
            analysis1,
            analysis2,
            AggregatorMergeStrategy.EARLIEST
        )
    );
  }

  private static SegmentAnalysis mergeLatest(SegmentAnalysis analysis1, SegmentAnalysis analysis2)
  {
    return SegmentMetadataQueryQueryToolChest.finalizeAnalysis(
        SegmentMetadataQueryQueryToolChest.mergeAnalyses(
            TEST_DATASOURCE.getTableNames(),
            analysis1,
            analysis2,
            AggregatorMergeStrategy.LATEST
        )
    );
  }
}
