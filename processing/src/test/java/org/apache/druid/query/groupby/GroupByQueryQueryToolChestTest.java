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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
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
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class GroupByQueryQueryToolChestTest
{

  @Test
  public void testResultLevelCacheKeyWithPostAggregate()
  {
    final GroupByQuery query1 = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setPostAggregatorSpecs(
            ImmutableList.of(
                new ExpressionPostAggregator("post", "alias + 'x'", null, TestExprMacroTable.INSTANCE)
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    final GroupByQuery query2 = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setPostAggregatorSpecs(
            ImmutableList.of(
                new ExpressionPostAggregator("post", "alias - 'x'", null, TestExprMacroTable.INSTANCE)
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    final CacheStrategy<Row, Object, GroupByQuery> strategy1 = new GroupByQueryQueryToolChest(
        null,
        QueryRunnerTestHelper.sameThreadIntervalChunkingQueryRunnerDecorator()
    ).getCacheStrategy(query1);

    final CacheStrategy<Row, Object, GroupByQuery> strategy2 = new GroupByQueryQueryToolChest(
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
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setPostAggregatorSpecs(
            ImmutableList.of(
                new ExpressionPostAggregator("post", "alias + 'x'", null, TestExprMacroTable.INSTANCE)
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
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
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setPostAggregatorSpecs(
            ImmutableList.of(
                new ExpressionPostAggregator("post", "alias - 'x'", null, TestExprMacroTable.INSTANCE)
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setLimitSpec(
            new DefaultLimitSpec(
                ImmutableList.of(
                    new OrderByColumnSpec("post", OrderByColumnSpec.Direction.DESCENDING)
                ),
                Integer.MAX_VALUE
            )
        )
        .build();

    final CacheStrategy<Row, Object, GroupByQuery> strategy1 = new GroupByQueryQueryToolChest(
        null,
        QueryRunnerTestHelper.sameThreadIntervalChunkingQueryRunnerDecorator()
    ).getCacheStrategy(query1);

    final CacheStrategy<Row, Object, GroupByQuery> strategy2 = new GroupByQueryQueryToolChest(
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
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setPostAggregatorSpecs(
            ImmutableList.of(
                new ExpressionPostAggregator("post", "alias + 'x'", null, TestExprMacroTable.INSTANCE)
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setLimitSpec(
            new DefaultLimitSpec(
                ImmutableList.of(
                    new OrderByColumnSpec("post", OrderByColumnSpec.Direction.DESCENDING)
                ),
                Integer.MAX_VALUE
            )
        )
        .setHavingSpec(new GreaterThanHavingSpec(QueryRunnerTestHelper.uniqueMetric, 8))
        .build();

    final GroupByQuery query2 = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setPostAggregatorSpecs(
            ImmutableList.of(
                new ExpressionPostAggregator("post", "alias + 'x'", null, TestExprMacroTable.INSTANCE)
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setLimitSpec(
            new DefaultLimitSpec(
                ImmutableList.of(
                    new OrderByColumnSpec("post", OrderByColumnSpec.Direction.DESCENDING)
                ),
                Integer.MAX_VALUE
            )
        )
        .setHavingSpec(new GreaterThanHavingSpec(QueryRunnerTestHelper.uniqueMetric, 10))
        .build();

    final CacheStrategy<Row, Object, GroupByQuery> strategy1 = new GroupByQueryQueryToolChest(
        null,
        QueryRunnerTestHelper.sameThreadIntervalChunkingQueryRunnerDecorator()
    ).getCacheStrategy(query1);

    final CacheStrategy<Row, Object, GroupByQuery> strategy2 = new GroupByQueryQueryToolChest(
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
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setPostAggregatorSpecs(
            ImmutableList.of(
                new ExpressionPostAggregator("post", "alias + 'x'", null, TestExprMacroTable.INSTANCE)
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
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
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setPostAggregatorSpecs(
            ImmutableList.of(
                new ExpressionPostAggregator("post", "alias + 'x'", null, TestExprMacroTable.INSTANCE)
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
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

    final CacheStrategy<Row, Object, GroupByQuery> strategy1 = new GroupByQueryQueryToolChest(
        null,
        QueryRunnerTestHelper.sameThreadIntervalChunkingQueryRunnerDecorator()
    ).getCacheStrategy(query1);

    final CacheStrategy<Row, Object, GroupByQuery> strategy2 = new GroupByQueryQueryToolChest(
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
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setPostAggregatorSpecs(
            ImmutableList.of(
                new ExpressionPostAggregator("post", "alias + 'x'", null, TestExprMacroTable.INSTANCE)
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
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
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setPostAggregatorSpecs(
            ImmutableList.of(
                new ExpressionPostAggregator("post", "alias + 'x'", null, TestExprMacroTable.INSTANCE)
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
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

    final CacheStrategy<Row, Object, GroupByQuery> strategy1 = new GroupByQueryQueryToolChest(
        null,
        QueryRunnerTestHelper.sameThreadIntervalChunkingQueryRunnerDecorator()
    ).getCacheStrategy(query1);

    final CacheStrategy<Row, Object, GroupByQuery> strategy2 = new GroupByQueryQueryToolChest(
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
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.newArrayList(
            new DefaultDimensionSpec("quality", "alias"),
            new DefaultDimensionSpec("market", "market")
        ))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index"),
                new FloatSumAggregatorFactory("idxFloat", "indexFloat"),
                new DoubleSumAggregatorFactory("idxDouble", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setSubtotalsSpec(ImmutableList.of(
            ImmutableList.of("alias"),
            ImmutableList.of("market"),
            ImmutableList.of()
        ))
        .build();

    final GroupByQuery query2 = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.newArrayList(
            new DefaultDimensionSpec("quality", "alias"),
            new DefaultDimensionSpec("market", "market")
        ))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index"),
                new FloatSumAggregatorFactory("idxFloat", "indexFloat"),
                new DoubleSumAggregatorFactory("idxDouble", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setSubtotalsSpec(ImmutableList.of(
            ImmutableList.of("alias"),
            ImmutableList.of()
        ))
        .build();

    final CacheStrategy<Row, Object, GroupByQuery> strategy1 = new GroupByQueryQueryToolChest(
        null,
        QueryRunnerTestHelper.sameThreadIntervalChunkingQueryRunnerDecorator()
    ).getCacheStrategy(query1);

    final CacheStrategy<Row, Object, GroupByQuery> strategy2 = new GroupByQueryQueryToolChest(
        null,
        QueryRunnerTestHelper.sameThreadIntervalChunkingQueryRunnerDecorator()
    ).getCacheStrategy(query2);

    Assert.assertTrue(Arrays.equals(strategy1.computeCacheKey(query1), strategy2.computeCacheKey(query2)));
    Assert.assertFalse(Arrays.equals(
        strategy1.computeResultLevelCacheKey(query1),
        strategy2.computeResultLevelCacheKey(query2)
    ));
  }

}
