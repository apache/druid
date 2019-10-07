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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.collections.SerializablePair;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
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
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class GroupByQueryQueryToolChestTest
{

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
                new ExpressionPostAggregator("post", "alias + 'x'", null, TestExprMacroTable.INSTANCE)
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
                new ExpressionPostAggregator("post", "alias - 'x'", null, TestExprMacroTable.INSTANCE)
            )
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    final CacheStrategy<ResultRow, Object, GroupByQuery> strategy1 = new GroupByQueryQueryToolChest(
        null,
        QueryRunnerTestHelper.sameThreadIntervalChunkingQueryRunnerDecorator()
    ).getCacheStrategy(query1);

    final CacheStrategy<ResultRow, Object, GroupByQuery> strategy2 = new GroupByQueryQueryToolChest(
        null,
        QueryRunnerTestHelper.sameThreadIntervalChunkingQueryRunnerDecorator()
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
                new ExpressionPostAggregator("post", "alias + 'x'", null, TestExprMacroTable.INSTANCE)
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
                new ExpressionPostAggregator("post", "alias - 'x'", null, TestExprMacroTable.INSTANCE)
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
        null,
        QueryRunnerTestHelper.sameThreadIntervalChunkingQueryRunnerDecorator()
    ).getCacheStrategy(query1);

    final CacheStrategy<ResultRow, Object, GroupByQuery> strategy2 = new GroupByQueryQueryToolChest(
        null,
        QueryRunnerTestHelper.sameThreadIntervalChunkingQueryRunnerDecorator()
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
                new ExpressionPostAggregator("post", "alias + 'x'", null, TestExprMacroTable.INSTANCE)
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
                new ExpressionPostAggregator("post", "alias + 'x'", null, TestExprMacroTable.INSTANCE)
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
        null,
        QueryRunnerTestHelper.sameThreadIntervalChunkingQueryRunnerDecorator()
    ).getCacheStrategy(query1);

    final CacheStrategy<ResultRow, Object, GroupByQuery> strategy2 = new GroupByQueryQueryToolChest(
        null,
        QueryRunnerTestHelper.sameThreadIntervalChunkingQueryRunnerDecorator()
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
                new ExpressionPostAggregator("post", "alias + 'x'", null, TestExprMacroTable.INSTANCE)
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
                new ExpressionPostAggregator("post", "alias + 'x'", null, TestExprMacroTable.INSTANCE)
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
        null,
        QueryRunnerTestHelper.sameThreadIntervalChunkingQueryRunnerDecorator()
    ).getCacheStrategy(query1);

    final CacheStrategy<ResultRow, Object, GroupByQuery> strategy2 = new GroupByQueryQueryToolChest(
        null,
        QueryRunnerTestHelper.sameThreadIntervalChunkingQueryRunnerDecorator()
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
                new ExpressionPostAggregator("post", "alias + 'x'", null, TestExprMacroTable.INSTANCE)
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
                new ExpressionPostAggregator("post", "alias + 'x'", null, TestExprMacroTable.INSTANCE)
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
        null,
        QueryRunnerTestHelper.sameThreadIntervalChunkingQueryRunnerDecorator()
    ).getCacheStrategy(query1);

    final CacheStrategy<ResultRow, Object, GroupByQuery> strategy2 = new GroupByQueryQueryToolChest(
        null,
        QueryRunnerTestHelper.sameThreadIntervalChunkingQueryRunnerDecorator()
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
        null,
        QueryRunnerTestHelper.sameThreadIntervalChunkingQueryRunnerDecorator()
    ).getCacheStrategy(query1);

    final CacheStrategy<ResultRow, Object, GroupByQuery> strategy2 = new GroupByQueryQueryToolChest(
        null,
        QueryRunnerTestHelper.sameThreadIntervalChunkingQueryRunnerDecorator()
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
    doTestCacheStrategy(ValueType.STRING, "val1");
    doTestCacheStrategy(ValueType.FLOAT, 2.1f);
    doTestCacheStrategy(ValueType.DOUBLE, 2.1d);
    doTestCacheStrategy(ValueType.LONG, 2L);
  }

  @Test
  public void testMultiColumnCacheStrategy() throws Exception
  {
    final GroupByQuery query1 = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(ImmutableList.of(
            new DefaultDimensionSpec("test", "test", ValueType.STRING),
            new DefaultDimensionSpec("v0", "v0", ValueType.STRING)
        ))
        .setVirtualColumns(
            new ExpressionVirtualColumn("v0", "concat('foo', test)", ValueType.STRING, TestExprMacroTable.INSTANCE)
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
        new GroupByQueryQueryToolChest(null, null).getCacheStrategy(
            query1
        );

    // test timestamps that result in integer size millis
    final ResultRow result1 = ResultRow.of(123L, "val1", "fooval1", 1, getIntermediateComplexValue(ValueType.STRING, "val1"));

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

    final GroupByQueryQueryToolChest toolChest = new GroupByQueryQueryToolChest(
        null,
        QueryRunnerTestHelper.sameThreadIntervalChunkingQueryRunnerDecorator()
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

    final Object[] rowObjects = {DateTimes.of("2000").getMillis(), "foo", 100, 10};
    final ResultRow resultRow = ResultRow.of(rowObjects);

    Assert.assertEquals(
        resultRow,
        arraysObjectMapper.readValue(
            StringUtils.format("[%s, \"foo\", 100, 10]", DateTimes.of("2000").getMillis()),
            ResultRow.class
        )
    );

    Assert.assertEquals(
        resultRow,
        arraysObjectMapper.readValue(
            StringUtils.format(
                "{\"version\":\"v1\","
                + "\"timestamp\":\"%s\","
                + "\"event\":"
                + "  {\"test\":\"foo\", \"rows\":100, \"post\":10}"
                + "}",
                DateTimes.of("2000")
            ),
            ResultRow.class
        )
    );

    Assert.assertArrayEquals(
        rowObjects,
        objectMapper.readValue(
            arraysObjectMapper.writeValueAsBytes(resultRow),
            Object[].class
        )
    );

    Assert.assertEquals(
        resultRow.toMapBasedRow(query),
        objectMapper.readValue(
            mapsObjectMapper.writeValueAsBytes(resultRow),
            Row.class
        )
    );

    Assert.assertEquals(
        "arrays read arrays",
        resultRow,
        arraysObjectMapper.readValue(
            arraysObjectMapper.writeValueAsBytes(resultRow),
            ResultRow.class
        )
    );

    Assert.assertEquals(
        "arrays read maps",
        resultRow,
        arraysObjectMapper.readValue(
            mapsObjectMapper.writeValueAsBytes(resultRow),
            ResultRow.class
        )
    );

    Assert.assertEquals(
        "maps read arrays",
        resultRow,
        mapsObjectMapper.readValue(
            arraysObjectMapper.writeValueAsBytes(resultRow),
            ResultRow.class
        )
    );

    Assert.assertEquals(
        "maps read maps",
        resultRow,
        mapsObjectMapper.readValue(
            mapsObjectMapper.writeValueAsBytes(resultRow),
            ResultRow.class
        )
    );
  }

  private AggregatorFactory getComplexAggregatorFactoryForValueType(final ValueType valueType)
  {
    switch (valueType) {
      case LONG:
        return new LongLastAggregatorFactory("complexMetric", "test");
      case DOUBLE:
        return new DoubleLastAggregatorFactory("complexMetric", "test");
      case FLOAT:
        return new FloatLastAggregatorFactory("complexMetric", "test");
      case STRING:
        return new StringLastAggregatorFactory("complexMetric", "test", null);
      default:
        throw new IllegalArgumentException("bad valueType: " + valueType);
    }
  }

  private SerializablePair getIntermediateComplexValue(final ValueType valueType, final Object dimValue)
  {
    switch (valueType) {
      case LONG:
      case DOUBLE:
      case FLOAT:
        return new SerializablePair<>(123L, dimValue);
      case STRING:
        return new SerializablePairLongString(123L, (String) dimValue);
      default:
        throw new IllegalArgumentException("bad valueType: " + valueType);
    }
  }

  private void doTestCacheStrategy(final ValueType valueType, final Object dimValue) throws IOException
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
                getComplexAggregatorFactoryForValueType(valueType)
            )
        )
        .setPostAggregatorSpecs(
            ImmutableList.of(new ConstantPostAggregator("post", 10))
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    CacheStrategy<ResultRow, Object, GroupByQuery> strategy =
        new GroupByQueryQueryToolChest(null, null).getCacheStrategy(
            query1
        );

    // test timestamps that result in integer size millis
    final ResultRow result1 = ResultRow.of(123L, dimValue, 1, getIntermediateComplexValue(valueType, dimValue));

    Object preparedValue = strategy.prepareForSegmentLevelCache().apply(result1);

    ObjectMapper objectMapper = TestHelper.makeJsonMapper();
    Object fromCacheValue = objectMapper.readValue(
        objectMapper.writeValueAsBytes(preparedValue),
        strategy.getCacheObjectClazz()
    );

    ResultRow fromCacheResult = strategy.pullFromSegmentLevelCache().apply(fromCacheValue);

    Assert.assertEquals(result1, fromCacheResult);

    // test timestamps that result in integer size millis
    final ResultRow result2 = ResultRow.of(123L, dimValue, 1, dimValue, 10);

    // Please see the comments on aggregator serde and type handling in CacheStrategy.fetchAggregatorsFromCache()
    final ResultRow typeAdjustedResult2;
    if (valueType == ValueType.FLOAT) {
      typeAdjustedResult2 = ResultRow.of(123L, dimValue, 1, 2.1d, 10);
    } else if (valueType == ValueType.LONG) {
      typeAdjustedResult2 = ResultRow.of(123L, dimValue, 1, 2, 10);
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
}
