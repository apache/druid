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

package org.apache.druid.query.topn;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.collections.CloseableStupidPool;
import org.apache.druid.collections.SerializablePair;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QueryToolChestTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.TestQueryRunners;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.SerializablePairLongDouble;
import org.apache.druid.query.aggregation.SerializablePairLongFloat;
import org.apache.druid.query.aggregation.SerializablePairLongLong;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.query.aggregation.cardinality.CardinalityAggregator;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.aggregation.last.DoubleLastAggregatorFactory;
import org.apache.druid.query.aggregation.last.FloatLastAggregatorFactory;
import org.apache.druid.query.aggregation.last.LongLastAggregatorFactory;
import org.apache.druid.query.aggregation.last.StringLastAggregatorFactory;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.ConstantPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TopNQueryQueryToolChestTest extends InitializedNullHandlingTest
{

  private static final SegmentId SEGMENT_ID = SegmentId.dummy("testSegment");

  @BeforeClass
  public static void setUpClass()
  {
    NullHandling.initializeForTests();
  }

  @Test
  public void testCacheStrategy() throws Exception
  {
    doTestCacheStrategy(ColumnType.STRING, "val1");
    doTestCacheStrategy(ColumnType.FLOAT, 2.1f);
    doTestCacheStrategy(ColumnType.DOUBLE, 2.1d);
    doTestCacheStrategy(ColumnType.LONG, 2L);
  }

  @Test
  public void testCacheStrategyOrderByPostAggs() throws Exception
  {
    doTestCacheStrategyOrderByPost(ColumnType.STRING, "val1");
    doTestCacheStrategyOrderByPost(ColumnType.FLOAT, 2.1f);
    doTestCacheStrategyOrderByPost(ColumnType.DOUBLE, 2.1d);
    doTestCacheStrategyOrderByPost(ColumnType.LONG, 2L);
  }

  @Test
  public void testComputeCacheKeyWithDifferentPostAgg()
  {
    final TopNQuery query1 = new TopNQuery(
        new TableDataSource("dummy"),
        VirtualColumns.EMPTY,
        new DefaultDimensionSpec("test", "test"),
        new NumericTopNMetricSpec("post"),
        3,
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2015-01-01/2015-01-02"))),
        null,
        Granularities.ALL,
        ImmutableList.of(new CountAggregatorFactory("metric1")),
        ImmutableList.of(new ConstantPostAggregator("post", 10)),
        null
    );

    final TopNQuery query2 = new TopNQuery(
        new TableDataSource("dummy"),
        VirtualColumns.EMPTY,
        new DefaultDimensionSpec("test", "test"),
        new NumericTopNMetricSpec("post"),
        3,
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2015-01-01/2015-01-02"))),
        null,
        Granularities.ALL,
        ImmutableList.of(new CountAggregatorFactory("metric1")),
        ImmutableList.of(
            new ArithmeticPostAggregator(
                "post",
                "+",
                ImmutableList.of(
                    new FieldAccessPostAggregator(
                        null,
                        "metric1"
                    ),
                    new FieldAccessPostAggregator(
                        null,
                        "metric1"
                    )
                )
            )
        ),
        null
    );

    final CacheStrategy<Result<TopNResultValue>, Object, TopNQuery> strategy1 = new TopNQueryQueryToolChest(
        null,
        null
    ).getCacheStrategy(query1);

    final CacheStrategy<Result<TopNResultValue>, Object, TopNQuery> strategy2 = new TopNQueryQueryToolChest(
        null,
        null
    ).getCacheStrategy(query2);

    Assert.assertFalse(Arrays.equals(strategy1.computeCacheKey(query1), strategy2.computeCacheKey(query2)));
    Assert.assertFalse(Arrays.equals(
        strategy1.computeResultLevelCacheKey(query1),
        strategy2.computeResultLevelCacheKey(query2)
    ));
  }

  @Test
  public void testComputeResultLevelCacheKeyWithDifferentPostAgg()
  {
    final TopNQuery query1 = new TopNQuery(
        new TableDataSource("dummy"),
        VirtualColumns.EMPTY,
        new DefaultDimensionSpec("test", "test"),
        new LegacyTopNMetricSpec("metric1"),
        3,
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2015-01-01T18:00:00/2015-01-02T18:00:00"))),
        null,
        Granularities.ALL,
        ImmutableList.of(
            new LongSumAggregatorFactory("metric1", "metric1"),
            new LongSumAggregatorFactory("metric2", "metric2")
        ),
        ImmutableList.of(
            new ArithmeticPostAggregator(
                "post1",
                "/",
                ImmutableList.of(
                    new FieldAccessPostAggregator(
                        "metric1",
                        "metric1"
                    ),
                    new FieldAccessPostAggregator(
                        "metric2",
                        "metric2"
                    )
                )
            )
        ),
        null
    );

    final TopNQuery query2 = new TopNQuery(
        new TableDataSource("dummy"),
        VirtualColumns.EMPTY,
        new DefaultDimensionSpec("test", "test"),
        new LegacyTopNMetricSpec("metric1"),
        3,
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2015-01-01T18:00:00/2015-01-02T18:00:00"))),
        null,
        Granularities.ALL,
        ImmutableList.of(
            new LongSumAggregatorFactory("metric1", "metric1"),
            new LongSumAggregatorFactory("metric2", "metric2")
        ),
        ImmutableList.of(
            new ArithmeticPostAggregator(
                "post2",
                "+",
                ImmutableList.of(
                    new FieldAccessPostAggregator(
                        "metric1",
                        "metric1"
                    ),
                    new FieldAccessPostAggregator(
                        "metric2",
                        "metric2"
                    )
                )
            )
        ),
        null
    );

    final CacheStrategy<Result<TopNResultValue>, Object, TopNQuery> strategy1 = new TopNQueryQueryToolChest(
        null,
        null
    ).getCacheStrategy(query1);

    final CacheStrategy<Result<TopNResultValue>, Object, TopNQuery> strategy2 = new TopNQueryQueryToolChest(
        null,
        null
    ).getCacheStrategy(query2);

    //segment level cache key excludes postaggregates in topn
    Assert.assertTrue(Arrays.equals(strategy1.computeCacheKey(query1), strategy2.computeCacheKey(query2)));
    Assert.assertFalse(Arrays.equals(strategy1.computeCacheKey(query1), strategy1.computeResultLevelCacheKey(query1)));
    Assert.assertFalse(Arrays.equals(
        strategy1.computeResultLevelCacheKey(query1),
        strategy2.computeResultLevelCacheKey(query2)
    ));
  }

  @Test
  public void testMinTopNThreshold()
  {
    TopNQueryConfig config = new TopNQueryConfig();
    final TopNQueryQueryToolChest chest = new TopNQueryQueryToolChest(config);
    try (CloseableStupidPool<ByteBuffer> pool = TestQueryRunners.createDefaultNonBlockingPool()) {
      QueryRunnerFactory factory = new TopNQueryRunnerFactory(
          pool,
          chest,
          QueryRunnerTestHelper.NOOP_QUERYWATCHER
      );
      QueryRunner<Result<TopNResultValue>> runner = QueryRunnerTestHelper.makeQueryRunner(
          factory,
          new IncrementalIndexSegment(TestIndex.getIncrementalTestIndex(), SEGMENT_ID),
          null
      );

      Map<String, Object> context = new HashMap<>();
      context.put("minTopNThreshold", 500);

      TopNQueryBuilder builder = new TopNQueryBuilder()
          .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
          .granularity(QueryRunnerTestHelper.ALL_GRAN)
          .dimension(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION)
          .metric(QueryRunnerTestHelper.INDEX_METRIC)
          .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
          .aggregators(QueryRunnerTestHelper.COMMON_DOUBLE_AGGREGATORS);

      TopNQuery query1 = builder.threshold(10).context(null).build();
      MockQueryRunner mockRunner = new MockQueryRunner(runner);
      new TopNQueryQueryToolChest.ThresholdAdjustingQueryRunner(mockRunner, config).run(QueryPlus.wrap(query1));
      Assert.assertEquals(1000, mockRunner.query.getThreshold());

      TopNQuery query2 = builder.threshold(10).context(context).build();

      new TopNQueryQueryToolChest.ThresholdAdjustingQueryRunner(mockRunner, config).run(QueryPlus.wrap(query2));
      Assert.assertEquals(500, mockRunner.query.getThreshold());

      TopNQuery query3 = builder.threshold(2000).context(context).build();
      new TopNQueryQueryToolChest.ThresholdAdjustingQueryRunner(mockRunner, config).run(QueryPlus.wrap(query3));
      Assert.assertEquals(2000, mockRunner.query.getThreshold());
    }
  }

  @Test
  public void testResultArraySignature()
  {
    final TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(Granularities.ALL)
        .dimension(new DefaultDimensionSpec("col", "dim"))
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .aggregators(QueryRunnerTestHelper.COMMON_DOUBLE_AGGREGATORS)
        .postAggregators(QueryRunnerTestHelper.CONSTANT)
        .threshold(1)
        .build();

    Assert.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add("dim", ColumnType.STRING)
                    .add("rows", ColumnType.LONG)
                    .add("index", ColumnType.DOUBLE)
                    .add("uniques", null)
                    .add("const", ColumnType.LONG)
                    .build(),
        new TopNQueryQueryToolChest(null, null).resultArraySignature(query)
    );
  }

  @Test
  public void testResultsAsArrays()
  {
    final TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(Granularities.ALL)
        .dimension(new DefaultDimensionSpec("col", "dim"))
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .aggregators(QueryRunnerTestHelper.COMMON_DOUBLE_AGGREGATORS)
        .postAggregators(QueryRunnerTestHelper.CONSTANT)
        .threshold(1)
        .build();

    QueryToolChestTestHelper.assertArrayResultsEquals(
        ImmutableList.of(
            new Object[]{DateTimes.of("2000").getMillis(), "foo", 1L, 2L, 3L, 1L},
            new Object[]{DateTimes.of("2000").getMillis(), "bar", 4L, 5L, 6L, 1L}
        ),
        new TopNQueryQueryToolChest(null, null).resultsAsArrays(
            query,
            Sequences.simple(
                ImmutableList.of(
                    new Result<>(
                        DateTimes.of("2000"),
                        TopNResultValue.create(
                            ImmutableList.of(
                                new DimensionAndMetricValueExtractor(
                                    ImmutableMap.of("dim", "foo", "rows", 1L, "index", 2L, "uniques", 3L, "const", 1L)
                                ),
                                new DimensionAndMetricValueExtractor(
                                    ImmutableMap.of("dim", "bar", "rows", 4L, "index", 5L, "uniques", 6L, "const", 1L)
                                )
                            )
                        )
                    )
                )
            )
        )
    );
  }

  private AggregatorFactory getComplexAggregatorFactoryForValueType(final ValueType valueType)
  {
    switch (valueType) {
      case LONG:
        return new LongLastAggregatorFactory("complexMetric", "test", null);
      case DOUBLE:
        return new DoubleLastAggregatorFactory("complexMetric", "test", null);
      case FLOAT:
        return new FloatLastAggregatorFactory("complexMetric", "test", null);
      case STRING:
        return new StringLastAggregatorFactory("complexMetric", "test", null, null);
      default:
        throw new IllegalArgumentException("bad valueType: " + valueType);
    }
  }

  private SerializablePair getIntermediateComplexValue(final ValueType valueType, final Object dimValue)
  {
    switch (valueType) {
      case LONG:
        return new SerializablePairLongLong(123L, (long) dimValue);
      case DOUBLE:
        return new SerializablePairLongDouble(123L, (double) dimValue);
      case FLOAT:
        return new SerializablePairLongFloat(123L, (float) dimValue);
      case STRING:
        return new SerializablePairLongString(123L, (String) dimValue);
      default:
        throw new IllegalArgumentException("bad valueType: " + valueType);
    }
  }

  private HyperLogLogCollector getIntermediateHllCollector(final ValueType valueType, final Object dimValue)
  {
    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();
    switch (valueType) {
      case LONG:
        collector.add(CardinalityAggregator.HASH_FUNCTION.hashLong((Long) dimValue).asBytes());
        break;
      case DOUBLE:
        collector.add(CardinalityAggregator.HASH_FUNCTION.hashLong(Double.doubleToLongBits((Double) dimValue))
                                                         .asBytes());
        break;
      case FLOAT:
        collector.add(CardinalityAggregator.HASH_FUNCTION.hashInt(Float.floatToIntBits((Float) dimValue)).asBytes());
        break;
      case STRING:
        collector.add(CardinalityAggregator.HASH_FUNCTION.hashUnencodedChars((String) dimValue).asBytes());
        break;
      default:
        throw new IllegalArgumentException("bad valueType: " + valueType);
    }
    return collector;
  }

  private void doTestCacheStrategy(final ColumnType valueType, final Object dimValue) throws IOException
  {
    CacheStrategy<Result<TopNResultValue>, Object, TopNQuery> strategy =
        new TopNQueryQueryToolChest(null, null).getCacheStrategy(
            new TopNQuery(
                new TableDataSource("dummy"),
                VirtualColumns.EMPTY,
                new DefaultDimensionSpec("test", "test", valueType),
                new NumericTopNMetricSpec("metric1"),
                3,
                new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2015-01-01/2015-01-02"))),
                null,
                Granularities.ALL,
                ImmutableList.of(
                    new CountAggregatorFactory("metric1"),
                    getComplexAggregatorFactoryForValueType(valueType.getType())
                ),
                ImmutableList.of(new ConstantPostAggregator("post", 10)),
                null
            )
        );

    final Result<TopNResultValue> result1 = new Result<>(
        // test timestamps that result in integer size millis
        DateTimes.utc(123L),
        TopNResultValue.create(
            Collections.singletonList(
                ImmutableMap.of(
                    "test", dimValue,
                    "metric1", 2,
                    "complexMetric", getIntermediateComplexValue(valueType.getType(), dimValue)
                )
            )
        )
    );

    Object preparedValue = strategy.prepareForSegmentLevelCache().apply(
        result1
    );

    ObjectMapper objectMapper = TestHelper.makeJsonMapper();
    Object fromCacheValue = objectMapper.readValue(
        objectMapper.writeValueAsBytes(preparedValue),
        strategy.getCacheObjectClazz()
    );

    Result<TopNResultValue> fromCacheResult = strategy.pullFromSegmentLevelCache().apply(fromCacheValue);

    Assert.assertEquals(result1, fromCacheResult);

    final Result<TopNResultValue> result2 = new Result<>(
        // test timestamps that result in integer size millis
        DateTimes.utc(123L),
        TopNResultValue.create(
            Collections.singletonList(
                ImmutableMap.of(
                    "test", dimValue,
                    "metric1", 2,
                    "complexMetric", dimValue,
                    "post", 10
                )
            )
        )
    );

    // Please see the comments on aggregator serde and type handling in CacheStrategy.fetchAggregatorsFromCache()
    final Result<TopNResultValue> typeAdjustedResult2;
    if (valueType.is(ValueType.FLOAT)) {
      typeAdjustedResult2 = new Result<>(
          DateTimes.utc(123L),
          TopNResultValue.create(
              Collections.singletonList(
                  ImmutableMap.of(
                      "test", dimValue,
                      "metric1", 2,
                      "complexMetric", 2.1d,
                      "post", 10
                  )
              )
          )
      );
    } else if (valueType.is(ValueType.LONG)) {
      typeAdjustedResult2 = new Result<>(
          DateTimes.utc(123L),
          TopNResultValue.create(
              Collections.singletonList(
                  ImmutableMap.of(
                      "test", dimValue,
                      "metric1", 2,
                      "complexMetric", 2,
                      "post", 10
                  )
              )
          )
      );
    } else {
      typeAdjustedResult2 = result2;
    }


    Object preparedResultCacheValue = strategy.prepareForCache(true).apply(
        result2
    );

    Object fromResultCacheValue = objectMapper.readValue(
        objectMapper.writeValueAsBytes(preparedResultCacheValue),
        strategy.getCacheObjectClazz()
    );

    Result<TopNResultValue> fromResultCacheResult = strategy.pullFromCache(true).apply(fromResultCacheValue);
    Assert.assertEquals(typeAdjustedResult2, fromResultCacheResult);
  }

  private void doTestCacheStrategyOrderByPost(final ColumnType valueType, final Object dimValue) throws IOException
  {
    CacheStrategy<Result<TopNResultValue>, Object, TopNQuery> strategy =
        new TopNQueryQueryToolChest(null, null).getCacheStrategy(
            new TopNQuery(
                new TableDataSource("dummy"),
                VirtualColumns.EMPTY,
                new DefaultDimensionSpec("test", "test", valueType),
                new NumericTopNMetricSpec("post"),
                3,
                new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2015-01-01/2015-01-02"))),
                null,
                Granularities.ALL,
                ImmutableList.of(
                    new HyperUniquesAggregatorFactory("metric1", "test", false, false),
                    new CountAggregatorFactory("metric2")
                ),
                ImmutableList.of(
                    new ArithmeticPostAggregator(
                        "post",
                        "+",
                        ImmutableList.of(
                            new FinalizingFieldAccessPostAggregator(
                                "metric1",
                                "metric1"
                            ),
                            new FieldAccessPostAggregator(
                                "metric2",
                                "metric2"
                            )
                        )
                    )
                ),
                null
            )
        );

    HyperLogLogCollector collector = getIntermediateHllCollector(valueType.getType(), dimValue);

    final Result<TopNResultValue> result1 = new Result<>(
        // test timestamps that result in integer size millis
        DateTimes.utc(123L),
        TopNResultValue.create(
            Collections.singletonList(
                ImmutableMap.of(
                    "test", dimValue,
                    "metric1", collector,
                    "metric2", 2,
                    "post", collector.estimateCardinality() + 2
                )
            )
        )
    );

    Object preparedValue = strategy.prepareForSegmentLevelCache().apply(
        result1
    );

    ObjectMapper objectMapper = TestHelper.makeJsonMapper();
    Object fromCacheValue = objectMapper.readValue(
        objectMapper.writeValueAsBytes(preparedValue),
        strategy.getCacheObjectClazz()
    );

    Result<TopNResultValue> fromCacheResult = strategy.pullFromSegmentLevelCache().apply(fromCacheValue);

    Assert.assertEquals(result1, fromCacheResult);

    final Result<TopNResultValue> resultLevelCacheResult = new Result<>(
        // test timestamps that result in integer size millis
        DateTimes.utc(123L),
        TopNResultValue.create(
            Collections.singletonList(
                ImmutableMap.of(
                    "test", dimValue,
                    "metric1", collector.estimateCardinality(),
                    "metric2", 2,
                    "post", collector.estimateCardinality() + 2
                )
            )
        )
    );

    Object preparedResultCacheValue = strategy.prepareForCache(true).apply(
        resultLevelCacheResult
    );

    Object fromResultCacheValue = objectMapper.readValue(
        objectMapper.writeValueAsBytes(preparedResultCacheValue),
        strategy.getCacheObjectClazz()
    );

    Result<TopNResultValue> fromResultCacheResult = strategy.pullFromCache(true).apply(fromResultCacheValue);
    Assert.assertEquals(resultLevelCacheResult, fromResultCacheResult);
  }

  static class MockQueryRunner implements QueryRunner<Result<TopNResultValue>>
  {
    private final QueryRunner<Result<TopNResultValue>> runner;
    TopNQuery query = null;

    MockQueryRunner(QueryRunner<Result<TopNResultValue>> runner)
    {
      this.runner = runner;
    }

    @Override
    public Sequence<Result<TopNResultValue>> run(
        QueryPlus<Result<TopNResultValue>> queryPlus,
        ResponseContext responseContext
    )
    {
      this.query = (TopNQuery) queryPlus.getQuery();
      return runner.run(queryPlus, responseContext);
    }
  }
}
