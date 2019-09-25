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
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.Druids;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.metadata.metadata.ColumnAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.query.spec.LegacySegmentSpec;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.timeline.LogicalSegment;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SegmentMetadataQueryQueryToolChestTest
{
  @Test
  public void testCacheStrategy() throws Exception
  {
    SegmentMetadataQuery query = new SegmentMetadataQuery(
        new TableDataSource("dummy"),
        new LegacySegmentSpec("2015-01-01/2015-01-02"),
        null,
        null,
        null,
        null,
        false,
        false
    );

    CacheStrategy<SegmentAnalysis, SegmentAnalysis, SegmentMetadataQuery> strategy =
        new SegmentMetadataQueryQueryToolChest(new SegmentMetadataQueryConfig()).getCacheStrategy(query);

    // Test cache key generation
    byte[] expectedKey = {0x04, 0x01, (byte) 0xFF, 0x00, 0x02, 0x04};
    byte[] actualKey = strategy.computeCacheKey(query);
    Assert.assertArrayEquals(expectedKey, actualKey);

    SegmentAnalysis result = new SegmentAnalysis(
        "testSegment",
        ImmutableList.of(Intervals.of("2011-01-12T00:00:00.000Z/2011-04-15T00:00:00.001Z")),
        ImmutableMap.of(
            "placement",
            new ColumnAnalysis(
                ValueType.STRING.toString(),
                true,
                10881,
                1,
                "preferred",
                "preferred",
                null
            )
        ), 71982,
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
        "id",
        null,
        new HashMap<>(),
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
        "id",
        null,
        new HashMap<>(),
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
        ImmutableMap.of(
            "foo", new LongSumAggregatorFactory("foo", "foo"),
            "bar", new DoubleSumAggregatorFactory("bar", "bar"),
            "baz", new DoubleSumAggregatorFactory("baz", "baz")
        ),
        mergeStrict(analysis1, analysis2).getAggregators()
    );
    Assert.assertEquals(
        ImmutableMap.of(
            "foo", new LongSumAggregatorFactory("foo", "foo"),
            "bar", new DoubleSumAggregatorFactory("bar", "bar"),
            "baz", new DoubleSumAggregatorFactory("baz", "baz")
        ),
        mergeLenient(analysis1, analysis2).getAggregators()
    );
  }

  @Test
  public void testMergeAggregatorsOneNull()
  {
    final SegmentAnalysis analysis1 = new SegmentAnalysis(
        "id",
        null,
        new HashMap<>(),
        0,
        0,
        null,
        null,
        null,
        null
    );
    final SegmentAnalysis analysis2 = new SegmentAnalysis(
        "id",
        null,
        new HashMap<>(),
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

    Assert.assertNull(mergeStrict(analysis1, analysis2).getAggregators());
    Assert.assertEquals(
        ImmutableMap.of(
            "foo", new LongSumAggregatorFactory("foo", "foo"),
            "bar", new DoubleSumAggregatorFactory("bar", "bar")
        ),
        mergeLenient(analysis1, analysis2).getAggregators()
    );
  }

  @Test
  public void testMergeAggregatorsAllNull()
  {
    final SegmentAnalysis analysis1 = new SegmentAnalysis(
        "id",
        null,
        new HashMap<>(),
        0,
        0,
        null,
        null,
        null,
        null
    );
    final SegmentAnalysis analysis2 = new SegmentAnalysis(
        "id",
        null,
        new HashMap<>(),
        0,
        0,
        null,
        null,
        null,
        null
    );

    Assert.assertNull(mergeStrict(analysis1, analysis2).getAggregators());
    Assert.assertNull(mergeLenient(analysis1, analysis2).getAggregators());
  }

  @Test
  public void testMergeAggregatorsConflict()
  {
    final SegmentAnalysis analysis1 = new SegmentAnalysis(
        "id",
        null,
        new HashMap<>(),
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
        "id",
        null,
        new HashMap<>(),
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
    Assert.assertNull(mergeStrict(analysis1, analysis2).getAggregators());
    Assert.assertEquals(expectedLenient, mergeLenient(analysis1, analysis2).getAggregators());

    // Simulate multi-level merge
    Assert.assertEquals(
        expectedLenient,
        mergeLenient(
            mergeLenient(analysis1, analysis2),
            mergeLenient(analysis1, analysis2)
        ).getAggregators()
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
        "id",
        null,
        new HashMap<>(),
        0,
        0,
        null,
        null,
        null,
        null
    );
    final SegmentAnalysis analysis2 = new SegmentAnalysis(
        "id",
        null,
        new HashMap<>(),
        0,
        0,
        null,
        null,
        null,
        false
    );
    final SegmentAnalysis analysis3 = new SegmentAnalysis(
        "id",
        null,
        new HashMap<>(),
        0,
        0,
        null,
        null,
        null,
        false
    );
    final SegmentAnalysis analysis4 = new SegmentAnalysis(
        "id",
        null,
        new HashMap<>(),
        0,
        0,
        null,
        null,
        null,
        true
    );
    final SegmentAnalysis analysis5 = new SegmentAnalysis(
        "id",
        null,
        new HashMap<>(),
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
  }

  private static SegmentAnalysis mergeStrict(SegmentAnalysis analysis1, SegmentAnalysis analysis2)
  {
    return SegmentMetadataQueryQueryToolChest.finalizeAnalysis(
        SegmentMetadataQueryQueryToolChest.mergeAnalyses(
            analysis1,
            analysis2,
            false
        )
    );
  }

  private static SegmentAnalysis mergeLenient(SegmentAnalysis analysis1, SegmentAnalysis analysis2)
  {
    return SegmentMetadataQueryQueryToolChest.finalizeAnalysis(
        SegmentMetadataQueryQueryToolChest.mergeAnalyses(
            analysis1,
            analysis2,
            true
        )
    );
  }
}
