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

package org.apache.druid.query.groupby;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.DefaultBlockingPool;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.collections.SerializablePair;
import org.apache.druid.collections.StupidPool;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QueryToolChestTestHelper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.SerializablePairLongDouble;
import org.apache.druid.query.aggregation.SerializablePairLongFloat;
import org.apache.druid.query.aggregation.SerializablePairLongLong;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.query.aggregation.last.DoubleLastAggregatorFactory;
import org.apache.druid.query.aggregation.last.FloatLastAggregatorFactory;
import org.apache.druid.query.aggregation.last.LongLastAggregatorFactory;
import org.apache.druid.query.aggregation.last.StringLastAggregatorFactory;
import org.apache.druid.query.aggregation.post.ConstantPostAggregator;
import org.apache.druid.query.aggregation.post.ExpressionPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.having.AndHavingSpec;
import org.apache.druid.query.groupby.having.DimFilterHavingSpec;
import org.apache.druid.query.groupby.having.EqualToHavingSpec;
import org.apache.druid.query.groupby.having.GreaterThanHavingSpec;
import org.apache.druid.query.groupby.having.HavingSpec;
import org.apache.druid.query.groupby.having.LessThanHavingSpec;
import org.apache.druid.query.groupby.having.NotHavingSpec;
import org.apache.druid.query.groupby.having.OrHavingSpec;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class GroupByQueryQueryToolChestTest extends InitializedNullHandlingTest
{
  @BeforeClass
  public static void setUpClass()
  {
    NullHandling.initializeForTests();
  }

  @Test
  public void testResultLevelCacheKeyWithPostAggregate()
  {
    final GroupByQuery query1 = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setPostAggregatorSpecs(
            ImmutableList.of(
                new ExpressionPostAggregator("post", "alias + 'x'", null, null, TestExprMacroTable.INSTANCE)
            )
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    final GroupByQuery query2 = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setPostAggregatorSpecs(
            ImmutableList.of(
                new ExpressionPostAggregator("post", "alias - 'x'", null, null, TestExprMacroTable.INSTANCE)
            )
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    final CacheStrategy<ResultRow, Object, GroupByQuery> strategy1 = new GroupByQueryQueryToolChest(
        null
    ).getCacheStrategy(query1);

    final CacheStrategy<ResultRow, Object, GroupByQuery> strategy2 = new GroupByQueryQueryToolChest(
        null
    ).getCacheStrategy(query2);

    Assert.assertTrue(Arrays.equals(strategy1.computeCacheKey(query1), strategy2.computeCacheKey(query2)));
    Assert.assertFalse(Arrays.equals(
        strategy1.computeResultLevelCacheKey(query1),
        strategy2.computeResultLevelCacheKey(query2)
    ));
  }

  @Test
  public void testResultLevelCacheKeyWithLimitSpec()
  {
    final GroupByQuery query1 = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setPostAggregatorSpecs(
            ImmutableList.of(
                new ExpressionPostAggregator("post", "alias + 'x'", null, null, TestExprMacroTable.INSTANCE)
            )
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setLimitSpec(
            new DefaultLimitSpec(
                ImmutableList.of(
                    new OrderByColumnSpec("post", OrderByColumnSpec.Direction.DESCENDING)
                ),
                Integer.MAX_VALUE
            )
        )
        .build();

    final GroupByQuery query2 = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setPostAggregatorSpecs(
            ImmutableList.of(
                new ExpressionPostAggregator("post", "alias - 'x'", null, null, TestExprMacroTable.INSTANCE)
            )
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setLimitSpec(
            new DefaultLimitSpec(
                ImmutableList.of(
                    new OrderByColumnSpec("post", OrderByColumnSpec.Direction.DESCENDING)
                ),
                Integer.MAX_VALUE
            )
        )
        .build();

    final CacheStrategy<ResultRow, Object, GroupByQuery> strategy1 = new GroupByQueryQueryToolChest(
        null
    ).getCacheStrategy(query1);

    final CacheStrategy<ResultRow, Object, GroupByQuery> strategy2 = new GroupByQueryQueryToolChest(
        null
    ).getCacheStrategy(query2);

    Assert.assertTrue(Arrays.equals(strategy1.computeCacheKey(query1), strategy2.computeCacheKey(query2)));
    Assert.assertFalse(Arrays.equals(
        strategy1.computeResultLevelCacheKey(query1),
        strategy2.computeResultLevelCacheKey(query2)
    ));
  }

  @Test
  public void testResultLevelCacheKeyWithHavingSpec()
  {
    final GroupByQuery query1 = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setPostAggregatorSpecs(
            ImmutableList.of(
                new ExpressionPostAggregator("post", "alias + 'x'", null, null, TestExprMacroTable.INSTANCE)
            )
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setLimitSpec(
            new DefaultLimitSpec(
                ImmutableList.of(
                    new OrderByColumnSpec("post", OrderByColumnSpec.Direction.DESCENDING)
                ),
                Integer.MAX_VALUE
            )
        )
        .setHavingSpec(new GreaterThanHavingSpec(QueryRunnerTestHelper.UNIQUE_METRIC, 8))
        .build();

    final GroupByQuery query2 = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setPostAggregatorSpecs(
            ImmutableList.of(
                new ExpressionPostAggregator("post", "alias + 'x'", null, null, TestExprMacroTable.INSTANCE)
            )
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setLimitSpec(
            new DefaultLimitSpec(
                ImmutableList.of(
                    new OrderByColumnSpec("post", OrderByColumnSpec.Direction.DESCENDING)
                ),
                Integer.MAX_VALUE
            )
        )
        .setHavingSpec(new GreaterThanHavingSpec(QueryRunnerTestHelper.UNIQUE_METRIC, 10))
        .build();

    final CacheStrategy<ResultRow, Object, GroupByQuery> strategy1 = new GroupByQueryQueryToolChest(
        null
    ).getCacheStrategy(query1);

    final CacheStrategy<ResultRow, Object, GroupByQuery> strategy2 = new GroupByQueryQueryToolChest(
        null
    ).getCacheStrategy(query2);

    Assert.assertTrue(Arrays.equals(strategy1.computeCacheKey(query1), strategy2.computeCacheKey(query2)));
    Assert.assertFalse(Arrays.equals(
        strategy1.computeResultLevelCacheKey(query1),
        strategy2.computeResultLevelCacheKey(query2)
    ));
  }

  @Test
  public void testResultLevelCacheKeyWithAndHavingSpec()
  {
    final List<HavingSpec> havings = Arrays.asList(
        new GreaterThanHavingSpec("agg", Double.valueOf(1.3)),
        new OrHavingSpec(
            Arrays.asList(
                new LessThanHavingSpec("lessAgg", Long.valueOf(1L)),
                new NotHavingSpec(new EqualToHavingSpec("equalAgg", Double.valueOf(2)))
            )
        )
    );
    final HavingSpec andHavingSpec = new AndHavingSpec(havings);

    final List<HavingSpec> havings2 = Arrays.asList(
        new GreaterThanHavingSpec("agg", Double.valueOf(13.0)),
        new OrHavingSpec(
            Arrays.asList(
                new LessThanHavingSpec("lessAgg", Long.valueOf(1L)),
                new NotHavingSpec(new EqualToHavingSpec("equalAgg", Double.valueOf(22)))
            )
        )
    );
    final HavingSpec andHavingSpec2 = new AndHavingSpec(havings2);

    final GroupByQuery query1 = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setPostAggregatorSpecs(
            ImmutableList.of(
                new ExpressionPostAggregator("post", "alias + 'x'", null, null, TestExprMacroTable.INSTANCE)
            )
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setLimitSpec(
            new DefaultLimitSpec(
                ImmutableList.of(
                    new OrderByColumnSpec("post", OrderByColumnSpec.Direction.DESCENDING)
                ),
                Integer.MAX_VALUE
            )
        )
        .setHavingSpec(andHavingSpec)
        .build();

    final GroupByQuery query2 = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setPostAggregatorSpecs(
            ImmutableList.of(
                new ExpressionPostAggregator("post", "alias + 'x'", null, null, TestExprMacroTable.INSTANCE)
            )
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setLimitSpec(
            new DefaultLimitSpec(
                ImmutableList.of(
                    new OrderByColumnSpec("post", OrderByColumnSpec.Direction.DESCENDING)
                ),
                Integer.MAX_VALUE
            )
        )
        .setHavingSpec(andHavingSpec2)
        .build();

    final CacheStrategy<ResultRow, Object, GroupByQuery> strategy1 = new GroupByQueryQueryToolChest(
        null
    ).getCacheStrategy(query1);

    final CacheStrategy<ResultRow, Object, GroupByQuery> strategy2 = new GroupByQueryQueryToolChest(
        null
    ).getCacheStrategy(query2);

    Assert.assertTrue(Arrays.equals(strategy1.computeCacheKey(query1), strategy2.computeCacheKey(query2)));
    Assert.assertFalse(Arrays.equals(
        strategy1.computeResultLevelCacheKey(query1),
        strategy2.computeResultLevelCacheKey(query2)
    ));
  }

  @Test
  public void testResultLevelCacheKeyWithHavingDimFilterHavingSpec()
  {
    final DimFilterHavingSpec havingSpec1 = new DimFilterHavingSpec(
        new AndDimFilter(
            ImmutableList.of(
                new OrDimFilter(
                    ImmutableList.of(
                        new BoundDimFilter("rows", "2", null, true, false, null, null, StringComparators.NUMERIC),
                        new SelectorDimFilter("idx", "217", null)
                    )
                ),
                new SelectorDimFilter("__time", String.valueOf(DateTimes.of("2011-04-01").getMillis()), null)
            )
        ),
        null
    );

    final DimFilterHavingSpec havingSpec2 = new DimFilterHavingSpec(
        new AndDimFilter(
            ImmutableList.of(
                new OrDimFilter(
                    ImmutableList.of(
                        new BoundDimFilter("rows", "2", null, true, false, null, null, StringComparators.NUMERIC),
                        new SelectorDimFilter("idx", "317", null)
                    )
                ),
                new SelectorDimFilter("__time", String.valueOf(DateTimes.of("2011-04-01").getMillis()), null)
            )
        ),
        null
    );
    final GroupByQuery query1 = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setPostAggregatorSpecs(
            ImmutableList.of(
                new ExpressionPostAggregator("post", "alias + 'x'", null, null, TestExprMacroTable.INSTANCE)
            )
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setLimitSpec(
            new DefaultLimitSpec(
                ImmutableList.of(
                    new OrderByColumnSpec("post", OrderByColumnSpec.Direction.DESCENDING)
                ),
                Integer.MAX_VALUE
            )
        )
        .setHavingSpec(havingSpec1)
        .build();

    final GroupByQuery query2 = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setPostAggregatorSpecs(
            ImmutableList.of(
                new ExpressionPostAggregator("post", "alias + 'x'", null, null, TestExprMacroTable.INSTANCE)
            )
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setLimitSpec(
            new DefaultLimitSpec(
                ImmutableList.of(
                    new OrderByColumnSpec("post", OrderByColumnSpec.Direction.DESCENDING)
                ),
                Integer.MAX_VALUE
            )
        )
        .setHavingSpec(havingSpec2)
        .build();

    final CacheStrategy<ResultRow, Object, GroupByQuery> strategy1 = new GroupByQueryQueryToolChest(
        null
    ).getCacheStrategy(query1);

    final CacheStrategy<ResultRow, Object, GroupByQuery> strategy2 = new GroupByQueryQueryToolChest(
        null
    ).getCacheStrategy(query2);

    Assert.assertTrue(Arrays.equals(strategy1.computeCacheKey(query1), strategy2.computeCacheKey(query2)));
    Assert.assertFalse(Arrays.equals(
        strategy1.computeResultLevelCacheKey(query1),
        strategy2.computeResultLevelCacheKey(query2)
    ));
  }

  @Test
  public void testResultLevelCacheKeyWithSubTotalsSpec()
  {
    final GroupByQuery query1 = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(Lists.newArrayList(
            new DefaultDimensionSpec("quality", "alias"),
            new DefaultDimensionSpec("market", "market")
        ))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.ROWS_COUNT,
                new LongSumAggregatorFactory("idx", "index"),
                new FloatSumAggregatorFactory("idxFloat", "indexFloat"),
                new DoubleSumAggregatorFactory("idxDouble", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setSubtotalsSpec(ImmutableList.of(
            ImmutableList.of("alias"),
            ImmutableList.of("market"),
            ImmutableList.of()
        ))
        .build();

    final GroupByQuery query2 = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(Lists.newArrayList(
            new DefaultDimensionSpec("quality", "alias"),
            new DefaultDimensionSpec("market", "market")
        ))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.ROWS_COUNT,
                new LongSumAggregatorFactory("idx", "index"),
                new FloatSumAggregatorFactory("idxFloat", "indexFloat"),
                new DoubleSumAggregatorFactory("idxDouble", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setSubtotalsSpec(ImmutableList.of(
            ImmutableList.of("alias"),
            ImmutableList.of()
        ))
        .build();

    final CacheStrategy<ResultRow, Object, GroupByQuery> strategy1 = new GroupByQueryQueryToolChest(
        null
    ).getCacheStrategy(query1);

    final CacheStrategy<ResultRow, Object, GroupByQuery> strategy2 = new GroupByQueryQueryToolChest(
        null
    ).getCacheStrategy(query2);

    Assert.assertTrue(Arrays.equals(strategy1.computeCacheKey(query1), strategy2.computeCacheKey(query2)));
    Assert.assertFalse(Arrays.equals(
        strategy1.computeResultLevelCacheKey(query1),
        strategy2.computeResultLevelCacheKey(query2)
    ));
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
  public void testMultiColumnCacheStrategy() throws Exception
  {
    final GroupByQuery query1 = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(ImmutableList.of(
            new DefaultDimensionSpec("test", "test", ColumnType.STRING),
            new DefaultDimensionSpec("v0", "v0", ColumnType.STRING)
        ))
        .setVirtualColumns(
            new ExpressionVirtualColumn("v0", "concat('foo', test)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
        )
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.ROWS_COUNT,
                getComplexAggregatorFactoryForValueType(ValueType.STRING)
            )
        )
        .setPostAggregatorSpecs(
            ImmutableList.of(new ConstantPostAggregator("post", 10))
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    CacheStrategy<ResultRow, Object, GroupByQuery> strategy =
        new GroupByQueryQueryToolChest(null).getCacheStrategy(
            query1
        );

    // test timestamps that result in integer size millis
    final ResultRow result1 = ResultRow.of(
        123L,
        "val1",
        "fooval1",
        1,
        getIntermediateComplexValue(ValueType.STRING, "val1")
    );

    Object preparedValue = strategy.prepareForSegmentLevelCache().apply(result1);

    ObjectMapper objectMapper = TestHelper.makeJsonMapper();
    Object fromCacheValue = objectMapper.readValue(
        objectMapper.writeValueAsBytes(preparedValue),
        strategy.getCacheObjectClazz()
    );

    ResultRow fromCacheResult = strategy.pullFromSegmentLevelCache().apply(fromCacheValue);

    Assert.assertEquals(result1, fromCacheResult);
  }

  @Test
  public void testResultSerde() throws Exception
  {
    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(Collections.singletonList(DefaultDimensionSpec.of("test")))
        .setAggregatorSpecs(Collections.singletonList(QueryRunnerTestHelper.ROWS_COUNT))
        .setPostAggregatorSpecs(Collections.singletonList(new ConstantPostAggregator("post", 10)))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    final GroupByQueryQueryToolChest toolChest = new GroupByQueryQueryToolChest(null);

    final ObjectMapper objectMapper = TestHelper.makeJsonMapper();
    final ObjectMapper arraysObjectMapper = toolChest.decorateObjectMapper(
        objectMapper,
        query.withOverriddenContext(ImmutableMap.of(GroupByQueryConfig.CTX_KEY_ARRAY_RESULT_ROWS, true))
    );
    final ObjectMapper mapsObjectMapper = toolChest.decorateObjectMapper(
        objectMapper,
        query.withOverriddenContext(ImmutableMap.of(GroupByQueryConfig.CTX_KEY_ARRAY_RESULT_ROWS, false))
    );

    final Object[] rowObjects = {DateTimes.of("2000").getMillis(), "foo", 100, 10.0};
    final ResultRow resultRow = ResultRow.of(rowObjects);

    Assert.assertArrayEquals(
        "standard mapper reads ResultRows",
        rowObjects,
        objectMapper.readValue(
            arraysObjectMapper.writeValueAsBytes(resultRow),
            Object[].class
        )
    );

    Assert.assertEquals(
        "standard mapper reads MapBasedRows",
        resultRow.toMapBasedRow(query),
        objectMapper.readValue(
            mapsObjectMapper.writeValueAsBytes(resultRow),
            Row.class
        )
    );

    Assert.assertEquals(
        "array mapper reads arrays",
        resultRow,
        arraysObjectMapper.readValue(
            arraysObjectMapper.writeValueAsBytes(resultRow),
            ResultRow.class
        )
    );

    Assert.assertEquals(
        "array mapper reads arrays (2)",
        resultRow,
        arraysObjectMapper.readValue(
            StringUtils.format("[%s, \"foo\", 100, 10.0]", DateTimes.of("2000").getMillis()),
            ResultRow.class
        )
    );

    Assert.assertEquals(
        "map mapper reads arrays",
        resultRow,
        mapsObjectMapper.readValue(
            arraysObjectMapper.writeValueAsBytes(resultRow),
            ResultRow.class
        )
    );

    Assert.assertEquals(
        "map mapper reads maps",
        resultRow,
        mapsObjectMapper.readValue(
            mapsObjectMapper.writeValueAsBytes(resultRow),
            ResultRow.class
        )
    );
  }

  @Test
  public void testResultSerdeIntermediateResultAsMapCompat() throws Exception
  {
    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(Collections.singletonList(DefaultDimensionSpec.of("test")))
        .setAggregatorSpecs(Collections.singletonList(QueryRunnerTestHelper.ROWS_COUNT))
        .setPostAggregatorSpecs(Collections.singletonList(new ConstantPostAggregator("post", 10)))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    final GroupByQueryQueryToolChest toolChest = new GroupByQueryQueryToolChest(
        null,
        () -> new GroupByQueryConfig()
        {
          @Override
          public boolean isIntermediateResultAsMapCompat()
          {
            return true;
          }
        },
        null
    );

    final ObjectMapper objectMapper = TestHelper.makeJsonMapper();
    final ObjectMapper arraysObjectMapper = toolChest.decorateObjectMapper(
        objectMapper,
        query.withOverriddenContext(ImmutableMap.of(GroupByQueryConfig.CTX_KEY_ARRAY_RESULT_ROWS, true))
    );
    final ObjectMapper mapsObjectMapper = toolChest.decorateObjectMapper(
        objectMapper,
        query.withOverriddenContext(ImmutableMap.of(GroupByQueryConfig.CTX_KEY_ARRAY_RESULT_ROWS, false))
    );

    final Object[] rowObjects = {DateTimes.of("2000").getMillis(), "foo", 100, 10.0};
    final ResultRow resultRow = ResultRow.of(rowObjects);


    Assert.assertEquals(
        resultRow,
        arraysObjectMapper.readValue(
            StringUtils.format("[%s, \"foo\", 100, 10.0]", DateTimes.of("2000").getMillis()),
            ResultRow.class
        )
    );

    Assert.assertArrayEquals(
        "standard mapper reads ResultRows",
        rowObjects,
        objectMapper.readValue(
            arraysObjectMapper.writeValueAsBytes(resultRow),
            Object[].class
        )
    );

    Assert.assertEquals(
        "standard mapper reads MapBasedRows",
        resultRow.toMapBasedRow(query),
        objectMapper.readValue(
            mapsObjectMapper.writeValueAsBytes(resultRow),
            Row.class
        )
    );

    Assert.assertEquals(
        "array mapper reads arrays",
        resultRow,
        arraysObjectMapper.readValue(
            arraysObjectMapper.writeValueAsBytes(resultRow),
            ResultRow.class
        )
    );

    Assert.assertEquals(
        "array mapper reads maps",
        resultRow,
        arraysObjectMapper.readValue(
            mapsObjectMapper.writeValueAsBytes(resultRow),
            ResultRow.class
        )
    );

    TestHelper.assertRow(
        "array mapper reads maps (2)",
        resultRow,
        arraysObjectMapper.readValue(
            StringUtils.format(
                "{\"version\":\"v1\","
                + "\"timestamp\":\"%s\","
                + "\"event\":"
                + "  {\"test\":\"foo\", \"rows\":100, \"post\":10.0}"
                + "}",
                DateTimes.of("2000")
            ),
            ResultRow.class
        )
    );

    Assert.assertEquals(
        "map mapper reads arrays",
        resultRow,
        mapsObjectMapper.readValue(
            arraysObjectMapper.writeValueAsBytes(resultRow),
            ResultRow.class
        )
    );

    Assert.assertEquals(
        "map mapper reads maps",
        resultRow,
        mapsObjectMapper.readValue(
            mapsObjectMapper.writeValueAsBytes(resultRow),
            ResultRow.class
        )
    );
  }

  @Test
  public void testResultArraySignatureAllGran()
  {
    final GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("col", "dim"))
        .setInterval(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setAggregatorSpecs(QueryRunnerTestHelper.COMMON_DOUBLE_AGGREGATORS)
        .setPostAggregatorSpecs(ImmutableList.of(QueryRunnerTestHelper.CONSTANT))
        .build();

    Assert.assertEquals(
        RowSignature.builder()
                    .add("dim", ColumnType.STRING)
                    .add("rows", ColumnType.LONG)
                    .add("index", ColumnType.DOUBLE)
                    .add("uniques", null)
                    .add("const", ColumnType.LONG)
                    .build(),
        new GroupByQueryQueryToolChest(null).resultArraySignature(query)
    );
  }

  @Test
  public void testResultArraySignatureDayGran()
  {
    final GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setGranularity(Granularities.DAY)
        .setDimensions(new DefaultDimensionSpec("col", "dim"))
        .setInterval(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setAggregatorSpecs(QueryRunnerTestHelper.COMMON_DOUBLE_AGGREGATORS)
        .setPostAggregatorSpecs(ImmutableList.of(QueryRunnerTestHelper.CONSTANT))
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
        new GroupByQueryQueryToolChest(null).resultArraySignature(query)
    );
  }

  @Test
  public void testResultsAsArraysAllGran()
  {
    final GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("col", "dim"))
        .setInterval(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setAggregatorSpecs(QueryRunnerTestHelper.COMMON_DOUBLE_AGGREGATORS)
        .setPostAggregatorSpecs(ImmutableList.of(QueryRunnerTestHelper.CONSTANT))
        .build();

    QueryToolChestTestHelper.assertArrayResultsEquals(
        ImmutableList.of(
            new Object[]{"foo", 1L, 2L, 3L, 1L},
            new Object[]{"bar", 4L, 5L, 6L, 1L}
        ),
        new GroupByQueryQueryToolChest(null).resultsAsArrays(
            query,
            Sequences.simple(
                ImmutableList.of(
                    makeRow(query, "2000", "dim", "foo", "rows", 1L, "index", 2L, "uniques", 3L, "const", 1L),
                    makeRow(query, "2000", "dim", "bar", "rows", 4L, "index", 5L, "uniques", 6L, "const", 1L)
                )
            )
        )
    );
  }

  @Test
  public void testResultsAsArraysDayGran()
  {
    final GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setGranularity(Granularities.DAY)
        .setDimensions(new DefaultDimensionSpec("col", "dim"))
        .setInterval(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setAggregatorSpecs(QueryRunnerTestHelper.COMMON_DOUBLE_AGGREGATORS)
        .setPostAggregatorSpecs(ImmutableList.of(QueryRunnerTestHelper.CONSTANT))
        .build();

    QueryToolChestTestHelper.assertArrayResultsEquals(
        ImmutableList.of(
            new Object[]{DateTimes.of("2000-01-01").getMillis(), "foo", 1L, 2L, 3L, 1L},
            new Object[]{DateTimes.of("2000-01-02").getMillis(), "bar", 4L, 5L, 6L, 1L}
        ),
        new GroupByQueryQueryToolChest(null).resultsAsArrays(
            query,
            Sequences.simple(
                ImmutableList.of(
                    makeRow(query, "2000-01-01", "dim", "foo", "rows", 1L, "index", 2L, "uniques", 3L, "const", 1L),
                    makeRow(query, "2000-01-02", "dim", "bar", "rows", 4L, "index", 5L, "uniques", 6L, "const", 1L)
                )
            )
        )
    );
  }

  @Test
  public void testCanPerformSubqueryOnGroupBys()
  {
    Assert.assertTrue(
        new GroupByQueryQueryToolChest(null).canPerformSubquery(
            new GroupByQuery.Builder()
                .setDataSource(
                    new QueryDataSource(
                        new GroupByQuery.Builder()
                            .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
                            .setInterval(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                            .setGranularity(Granularities.ALL)
                            .build()
                    )
                )
                .setInterval(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                .setGranularity(Granularities.ALL)
                .build()
        )
    );
  }

  @Test
  public void testCanPerformSubqueryOnTimeseries()
  {
    Assert.assertFalse(
        new GroupByQueryQueryToolChest(null).canPerformSubquery(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                  .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                  .granularity(Granularities.ALL)
                  .build()
        )
    );
  }

  @Test
  public void testCanPerformSubqueryOnGroupByOfTimeseries()
  {
    Assert.assertFalse(
        new GroupByQueryQueryToolChest(null).canPerformSubquery(
            new GroupByQuery.Builder()
                .setDataSource(
                    new QueryDataSource(
                        Druids.newTimeseriesQueryBuilder()
                              .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                              .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                              .granularity(Granularities.ALL)
                              .build()
                    )
                )
                .setInterval(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                .setGranularity(Granularities.ALL)
                .build()
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

  private void doTestCacheStrategy(final ColumnType valueType, final Object dimValue) throws IOException
  {
    final GroupByQuery query1 = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(Collections.singletonList(
            new DefaultDimensionSpec("test", "test", valueType)
        ))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.ROWS_COUNT,
                getComplexAggregatorFactoryForValueType(valueType.getType())
            )
        )
        .setPostAggregatorSpecs(
            new ConstantPostAggregator("post", 10),
            new ConstantPostAggregator("post2", 20)
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    CacheStrategy<ResultRow, Object, GroupByQuery> strategy =
        new GroupByQueryQueryToolChest(null).getCacheStrategy(
            query1
        );

    // test timestamps that result in integer size millis
    final ResultRow result1 = ResultRow.of(
        123L,
        dimValue,
        1,
        getIntermediateComplexValue(valueType.getType(), dimValue)
    );

    Object preparedValue = strategy.prepareForSegmentLevelCache().apply(result1);

    ObjectMapper objectMapper = TestHelper.makeJsonMapper();
    Object fromCacheValue = objectMapper.readValue(
        objectMapper.writeValueAsBytes(preparedValue),
        strategy.getCacheObjectClazz()
    );

    ResultRow fromCacheResult = strategy.pullFromSegmentLevelCache().apply(fromCacheValue);

    Assert.assertEquals(result1, fromCacheResult);

    // test timestamps that result in integer size millis
    final ResultRow result2 = ResultRow.of(123L, dimValue, 1, dimValue, 10, 20);

    // Please see the comments on aggregator serde and type handling in CacheStrategy.fetchAggregatorsFromCache()
    final ResultRow typeAdjustedResult2;
    if (valueType.is(ValueType.FLOAT)) {
      typeAdjustedResult2 = ResultRow.of(123L, dimValue, 1, 2.1d, 10, 20);
    } else if (valueType.is(ValueType.LONG)) {
      typeAdjustedResult2 = ResultRow.of(123L, dimValue, 1, 2, 10, 20);
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

    ResultRow fromResultCacheResult = strategy.pullFromCache(true).apply(fromResultCacheValue);
    Assert.assertEquals(typeAdjustedResult2, fromResultCacheResult);
  }

  @Test
  public void testQueryCacheKeyWithLimitSpec()
  {
    final GroupByQuery query1 = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setLimitSpec(
            new DefaultLimitSpec(
                null,
                100
            )
        )
        .overrideContext(ImmutableMap.of(GroupByQueryConfig.CTX_KEY_APPLY_LIMIT_PUSH_DOWN, "true"))
        .build();

    final GroupByQuery query2 = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setLimitSpec(
            new DefaultLimitSpec(
                null,
                1000
            )
        )
        .build();

    final GroupByQuery queryNoLimit = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    final CacheStrategy<ResultRow, Object, GroupByQuery> strategy1 = new GroupByQueryQueryToolChest(
        null
    ).getCacheStrategy(query1);

    final CacheStrategy<ResultRow, Object, GroupByQuery> strategy2 = new GroupByQueryQueryToolChest(
        null
    ).getCacheStrategy(query2);

    Assert.assertFalse(Arrays.equals(strategy1.computeCacheKey(query1), strategy2.computeCacheKey(query2)));
    Assert.assertFalse(Arrays.equals(
        strategy1.computeCacheKey(query1),
        strategy2.computeCacheKey(queryNoLimit)
    ));
  }

  @Test
  public void testQueryCacheKeyWithLimitSpecPushDownUsingContext()
  {
    final GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setLimitSpec(
            new DefaultLimitSpec(
                null,
                100
            )
        );
    final GroupByQuery query1 = builder
        .overrideContext(ImmutableMap.of(GroupByQueryConfig.CTX_KEY_APPLY_LIMIT_PUSH_DOWN, "true"))
        .build();
    final GroupByQuery query2 = builder
        .overrideContext(ImmutableMap.of(GroupByQueryConfig.CTX_KEY_APPLY_LIMIT_PUSH_DOWN, "false"))
        .build();

    final CacheStrategy<ResultRow, Object, GroupByQuery> strategy1 = new GroupByQueryQueryToolChest(
        null
    ).getCacheStrategy(query1);

    final CacheStrategy<ResultRow, Object, GroupByQuery> strategy2 = new GroupByQueryQueryToolChest(
        null
    ).getCacheStrategy(query2);

    Assert.assertFalse(Arrays.equals(strategy1.computeCacheKey(query1), strategy2.computeCacheKey(query2)));
    Assert.assertTrue(
        Arrays.equals(
            strategy1.computeResultLevelCacheKey(query1),
            strategy2.computeResultLevelCacheKey(query2)
        )
    );
  }

  private static ResultRow makeRow(final GroupByQuery query, final String timestamp, final Object... vals)
  {
    return GroupByQueryRunnerTestHelper.createExpectedRow(query, timestamp, vals);
  }

  @Test
  public void testIsQueryCacheableOnGroupByStrategyV2()
  {
    final GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setGranularity(Granularities.DAY)
        .setDimensions(new DefaultDimensionSpec("col", "dim"))
        .setInterval(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setAggregatorSpecs(QueryRunnerTestHelper.COMMON_DOUBLE_AGGREGATORS)
        .build();
    final DruidProcessingConfig processingConfig = new DruidProcessingConfig()
    {
      @Override
      public String getFormatString()
      {
        return "processing-%s";
      }
    };
    final GroupByQueryConfig queryConfig = new GroupByQueryConfig();
    final Supplier<GroupByQueryConfig> queryConfigSupplier = Suppliers.ofInstance(queryConfig);
    final Supplier<ByteBuffer> bufferSupplier =
        () -> ByteBuffer.allocateDirect(processingConfig.intermediateComputeSizeBytes());

    final NonBlockingPool<ByteBuffer> bufferPool = new StupidPool<>(
        "GroupByQueryEngine-bufferPool",
        bufferSupplier
    );
    final BlockingPool<ByteBuffer> mergeBufferPool = new DefaultBlockingPool<>(
        bufferSupplier,
        processingConfig.getNumMergeBuffers()
    );
    final GroupingEngine groupingEngine = new GroupingEngine(
        processingConfig,
        queryConfigSupplier,
        bufferPool,
        mergeBufferPool,
        TestHelper.makeJsonMapper(),
        new ObjectMapper(new SmileFactory()),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );
    final GroupByQueryQueryToolChest queryToolChest = new GroupByQueryQueryToolChest(groupingEngine);
    CacheStrategy<ResultRow, Object, GroupByQuery> cacheStrategy = queryToolChest.getCacheStrategy(query);
    Assert.assertTrue(
        "result level cache on broker server for GroupByStrategyV2 should be enabled",
        cacheStrategy.isCacheable(query, false, false)
    );
    Assert.assertFalse(
        "segment level cache on broker server for GroupByStrategyV2 should be disabled",
        cacheStrategy.isCacheable(query, false, true)
    );
    Assert.assertTrue(
        "segment level cache on data server for GroupByStrategyV2 should be enabled",
        cacheStrategy.isCacheable(query, true, true)
    );
  }
}
