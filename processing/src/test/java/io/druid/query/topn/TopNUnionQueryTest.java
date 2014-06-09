/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.topn;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.collections.StupidPool;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.TestQueryRunners;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.MaxAggregatorFactory;
import io.druid.query.aggregation.MinAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.segment.TestHelper;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class TopNUnionQueryTest
{
  private final QueryRunner runner;

  public TopNUnionQueryTest(
      QueryRunner runner
  )
  {
    this.runner = runner;
  }

  @Parameterized.Parameters
  public static Collection<?> constructorFeeder() throws IOException
  {
    List<Object> retVal = Lists.newArrayList();
    retVal.addAll(
        QueryRunnerTestHelper.makeUnionQueryRunners(
            new TopNQueryRunnerFactory(
                TestQueryRunners.getPool(),
                new TopNQueryQueryToolChest(new TopNQueryConfig()),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            )
        )
    );
    retVal.addAll(
        QueryRunnerTestHelper.makeUnionQueryRunners(
            new TopNQueryRunnerFactory(
                new StupidPool<ByteBuffer>(
                    new Supplier<ByteBuffer>()
                    {
                      @Override
                      public ByteBuffer get()
                      {
                        return ByteBuffer.allocate(2000);
                      }
                    }
                ),
                new TopNQueryQueryToolChest(new TopNQueryConfig()),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            )
        )
    );

    return retVal;
  }

  @Test
  public void testTopNUnionQuery()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.unionDataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(QueryRunnerTestHelper.providerDimension)
        .metric(QueryRunnerTestHelper.dependentPostAggMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    QueryRunnerTestHelper.commonAggregators,
                    Lists.newArrayList(
                        new MaxAggregatorFactory("maxIndex", "index"),
                        new MinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(
            Arrays.<PostAggregator>asList(
                QueryRunnerTestHelper.addRowsIndexConstant,
                QueryRunnerTestHelper.dependentPostAgg,
                QueryRunnerTestHelper.hyperUniqueFinalizingPostAgg
            )
        )
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                                .put(QueryRunnerTestHelper.providerDimension, "total_market")
                                .put("rows", 744L)
                                .put("index", 862719.3151855469D)
                                .put("addRowsIndexConstant", 863464.3151855469D)
                                .put(QueryRunnerTestHelper.dependentPostAggMetric, 864209.3151855469D)
                                .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                                .put("maxIndex", 1743.9217529296875D)
                                .put("minIndex", 792.3260498046875D)
                                .put(
                                    QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
                                    QueryRunnerTestHelper.UNIQUES_2 + 1.0
                                )
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put(QueryRunnerTestHelper.providerDimension, "upfront")
                                .put("rows", 744L)
                                .put("index", 768184.4240722656D)
                                .put("addRowsIndexConstant", 768929.4240722656D)
                                .put(QueryRunnerTestHelper.dependentPostAggMetric, 769674.4240722656D)
                                .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                                .put("maxIndex", 1870.06103515625D)
                                .put("minIndex", 545.9906005859375D)
                                .put(
                                    QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
                                    QueryRunnerTestHelper.UNIQUES_2 + 1.0
                                )
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put(QueryRunnerTestHelper.providerDimension, "spot")
                                .put("rows", 3348L)
                                .put("index", 382426.28929138184D)
                                .put("addRowsIndexConstant", 385775.28929138184D)
                                .put(QueryRunnerTestHelper.dependentPostAggMetric, 389124.28929138184D)
                                .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                                .put(
                                    QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
                                    QueryRunnerTestHelper.UNIQUES_9 + 1.0
                                )
                                .put("maxIndex", 277.2735290527344D)
                                .put("minIndex", 59.02102279663086D)
                                .build()
                )
            )
        )
    );

    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }


}
