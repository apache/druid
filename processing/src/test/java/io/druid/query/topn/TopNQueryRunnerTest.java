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
import com.metamx.common.guava.Sequences;
import io.druid.collections.StupidPool;
import io.druid.query.BySegmentResultValueClass;
import io.druid.query.Druids;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.TestQueryRunners;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.MaxAggregatorFactory;
import io.druid.query.aggregation.MinAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.ExtractionDimensionSpec;
import io.druid.query.extraction.RegexDimExtractionFn;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.TestHelper;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 */
@RunWith(Parameterized.class)
public class TopNQueryRunnerTest
{
  @Parameterized.Parameters
  public static Collection<?> constructorFeeder() throws IOException
  {
    List<Object> retVal = Lists.newArrayList();
    retVal.addAll(
        QueryRunnerTestHelper.makeQueryRunners(
            new TopNQueryRunnerFactory(
                TestQueryRunners.getPool(),
                new TopNQueryQueryToolChest(new TopNQueryConfig())
            )
        )
    );
    retVal.addAll(
        QueryRunnerTestHelper.makeQueryRunners(
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
                new TopNQueryQueryToolChest(new TopNQueryConfig())
            )
        )
    );

    return retVal;
  }

  private final QueryRunner runner;

  public TopNQueryRunnerTest(
      QueryRunner runner
  )
  {
    this.runner = runner;
  }

  private static final String providerDimension = "provider";

  @Test
  public void testFullOnTopN()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(providerDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
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
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put(providerDimension, "total_market")
                        .put("rows", 186L)
                        .put("index", 215679.82879638672D)
                        .put("addRowsIndexConstant", 215866.82879638672D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1743.9217529296875D)
                        .put("minIndex", 792.3260498046875D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put(providerDimension, "upfront")
                        .put("rows", 186L)
                        .put("index", 192046.1060180664D)
                        .put("addRowsIndexConstant", 192233.1060180664D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1870.06103515625D)
                        .put("minIndex", 545.9906005859375D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put(providerDimension, "spot")
                        .put("rows", 837L)
                        .put("index", 95606.57232284546D)
                        .put("addRowsIndexConstant", 96444.57232284546D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                        .put("maxIndex", 277.2735290527344D)
                        .put("minIndex", 59.02102279663086D)
                        .build()
                )
            )
        )
    );

    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testFullOnTopNOverPostAggs()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(providerDimension)
        .metric(QueryRunnerTestHelper.addRowsIndexConstantMetric)
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
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put(providerDimension, "total_market")
                        .put("rows", 186L)
                        .put("index", 215679.82879638672D)
                        .put("addRowsIndexConstant", 215866.82879638672D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1743.9217529296875D)
                        .put("minIndex", 792.3260498046875D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put(providerDimension, "upfront")
                        .put("rows", 186L)
                        .put("index", 192046.1060180664D)
                        .put("addRowsIndexConstant", 192233.1060180664D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1870.06103515625D)
                        .put("minIndex", 545.9906005859375D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put(providerDimension, "spot")
                        .put("rows", 837L)
                        .put("index", 95606.57232284546D)
                        .put("addRowsIndexConstant", 96444.57232284546D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                        .put("maxIndex", 277.2735290527344D)
                        .put("minIndex", 59.02102279663086D)
                        .build()
                )
            )
        )
    );

    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }


  @Test
  public void testFullOnTopNOverUniques()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(providerDimension)
        .metric(QueryRunnerTestHelper.uniqueMetric)
        .threshold(3)
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
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("provider", "spot")
                        .put("rows", 837L)
                        .put("index", 95606.57232284546D)
                        .put("addRowsIndexConstant", 96444.57232284546D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                        .put("maxIndex", 277.2735290527344D)
                        .put("minIndex", 59.02102279663086D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("provider", "total_market")
                        .put("rows", 186L)
                        .put("index", 215679.82879638672D)
                        .put("addRowsIndexConstant", 215866.82879638672D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1743.9217529296875D)
                        .put("minIndex", 792.3260498046875D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("provider", "upfront")
                        .put("rows", 186L)
                        .put("index", 192046.1060180664D)
                        .put("addRowsIndexConstant", 192233.1060180664D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1870.06103515625D)
                        .put("minIndex", 545.9906005859375D)
                        .build()
                )
            )
        )
    );

    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }


  @Test
  public void testTopN()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(providerDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();


    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        providerDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        providerDimension, "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        providerDimension, "spot",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    )
                )
            )
        )
    );

    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testTopNByUniques()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(providerDimension)
        .metric(new NumericTopNMetricSpec("uniques"))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();


    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "provider", "spot",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        "provider", "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        "provider", "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );

    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testTopNWithOrFilter1()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(providerDimension, "total_market", "upfront", "spot")
        .dimension(providerDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        providerDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        providerDimension, "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        providerDimension, "spot",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    )
                )
            )
        )
    );

    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testTopNWithOrFilter2()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(providerDimension, "total_market", "upfront")
        .dimension(providerDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        providerDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        providerDimension, "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );

    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testTopNWithFilter1()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(providerDimension, "upfront")
        .dimension(providerDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        providerDimension, "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );

    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testTopNWithFilter2()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(QueryRunnerTestHelper.qualityDimension, "mezzanine")
        .dimension(providerDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        providerDimension, "upfront",
                        "rows", 2L,
                        "index", 2591.68359375D,
                        "addRowsIndexConstant", 2594.68359375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.<String, Object>of(
                        providerDimension, "total_market",
                        "rows", 2L,
                        "index", 2508.39599609375D,
                        "addRowsIndexConstant", 2511.39599609375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.<String, Object>of(
                        providerDimension, "spot",
                        "rows", 2L,
                        "index", 220.63774871826172D,
                        "addRowsIndexConstant", 223.63774871826172D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    )
                )
            )
        )
    );

    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testTopNWithFilter2OneDay()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(QueryRunnerTestHelper.qualityDimension, "mezzanine")
        .dimension(providerDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(
            new MultipleIntervalSegmentSpec(
                Arrays.asList(new Interval("2011-04-01T00:00:00.000Z/2011-04-02T00:00:00.000Z"))
            )
        )
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        providerDimension, "upfront",
                        "rows", 1L,
                        "index", new Float(1447.341160).doubleValue(),
                        "addRowsIndexConstant", new Float(1449.341160).doubleValue(),
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.<String, Object>of(
                        providerDimension, "total_market",
                        "rows", 1L,
                        "index", new Float(1314.839715).doubleValue(),
                        "addRowsIndexConstant", new Float(1316.839715).doubleValue(),
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.<String, Object>of(
                        providerDimension, "spot",
                        "rows", 1L,
                        "index", new Float(109.705815).doubleValue(),
                        "addRowsIndexConstant", new Float(111.705815).doubleValue(),
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    )
                )
            )
        )
    );

    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testTopNWithNonExistentFilterInOr()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(providerDimension, "total_market", "upfront", "billyblank")
        .dimension(providerDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        providerDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        providerDimension, "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );

    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testTopNWithNonExistentFilter()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(providerDimension, "billyblank")
        .dimension(providerDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    TestHelper.assertExpectedResults(
        Lists.<Result<TopNResultValue>>newArrayList(
            new Result<TopNResultValue>(
                new DateTime("2011-04-01T00:00:00.000Z"),
                new TopNResultValue(Lists.<Map<String, Object>>newArrayList())
            )
        ),
        runner.run(query)
    );
  }

  @Test
  public void testTopNWithNonExistentFilterMultiDim()
  {
    AndDimFilter andDimFilter = Druids.newAndDimFilterBuilder()
        .fields(
            Lists.<DimFilter>newArrayList(
                Druids.newSelectorDimFilterBuilder()
                      .dimension(providerDimension)
                      .value("billyblank")
                      .build(),
                Druids.newSelectorDimFilterBuilder()
                      .dimension(QueryRunnerTestHelper.qualityDimension)
                      .value("mezzanine")
                      .build()
            )
        ).build();
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(andDimFilter)
        .dimension(providerDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    TestHelper.assertExpectedResults(
        Lists.<Result<TopNResultValue>>newArrayList(
            new Result<TopNResultValue>(
                new DateTime("2011-04-01T00:00:00.000Z"),
                new TopNResultValue(Lists.<Map<String, Object>>newArrayList())
            )
        ),
        runner.run(query)
    );
  }

  @Test
  public void testTopNWithMultiValueDimFilter1()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(QueryRunnerTestHelper.placementishDimension, "m")
        .dimension(providerDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    TestHelper.assertExpectedResults(
        Sequences.toList(
            runner.run(
                new TopNQueryBuilder()
                    .dataSource(QueryRunnerTestHelper.dataSource)
                    .granularity(QueryRunnerTestHelper.allGran)
                    .filters(QueryRunnerTestHelper.qualityDimension, "mezzanine")
                    .dimension(providerDimension)
                    .metric(QueryRunnerTestHelper.indexMetric)
                    .threshold(4)
                    .intervals(QueryRunnerTestHelper.firstToThird)
                    .aggregators(QueryRunnerTestHelper.commonAggregators)
                    .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                    .build()
            ), Lists.<Result<TopNResultValue>>newArrayList()
        ), runner.run(query)
    );
  }

  @Test
  public void testTopNWithMultiValueDimFilter2()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(QueryRunnerTestHelper.placementishDimension, "m", "a", "b")
        .dimension(QueryRunnerTestHelper.qualityDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    TestHelper.assertExpectedResults(
        Sequences.toList(
            runner.run(
                new TopNQueryBuilder()
                    .dataSource(QueryRunnerTestHelper.dataSource)
                    .granularity(QueryRunnerTestHelper.allGran)
                    .filters(QueryRunnerTestHelper.qualityDimension, "mezzanine", "automotive", "business")
                    .dimension(QueryRunnerTestHelper.qualityDimension)
                    .metric(QueryRunnerTestHelper.indexMetric)
                    .threshold(4)
                    .intervals(QueryRunnerTestHelper.firstToThird)
                    .aggregators(QueryRunnerTestHelper.commonAggregators)
                    .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                    .build()
            ), Lists.<Result<TopNResultValue>>newArrayList()
        )
        , runner.run(query)
    );
  }

  @Test
  public void testTopNWithMultiValueDimFilter3()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(QueryRunnerTestHelper.placementishDimension, "a")
        .dimension(QueryRunnerTestHelper.placementishDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    final ArrayList<Result<TopNResultValue>> expectedResults = Lists.newArrayList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "placementish", "a",
                        "rows", 2L,
                        "index", 283.31103515625D,
                        "addRowsIndexConstant", 286.31103515625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.<String, Object>of(
                        "placementish", "preferred",
                        "rows", 2L,
                        "index", 283.31103515625D,
                        "addRowsIndexConstant", 286.31103515625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    )
                )
            )
        )
    );

    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testTopNWithMultiValueDimFilter4()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(QueryRunnerTestHelper.placementishDimension, "a", "b")
        .dimension(QueryRunnerTestHelper.placementishDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    final ArrayList<Result<TopNResultValue>> expectedResults = Lists.newArrayList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "placementish", "preferred",
                        "rows", 4L,
                        "index", 514.868408203125D,
                        "addRowsIndexConstant", 519.868408203125D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        "placementish",
                        "a", "rows", 2L,
                        "index", 283.31103515625D,
                        "addRowsIndexConstant", 286.31103515625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.<String, Object>of(
                        "placementish", "b",
                        "rows", 2L,
                        "index", 231.557373046875D,
                        "addRowsIndexConstant", 234.557373046875D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    )
                )
            )
        )
    );

    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testTopNWithMultiValueDimFilter5()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(QueryRunnerTestHelper.placementishDimension, "preferred")
        .dimension(QueryRunnerTestHelper.placementishDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    final ArrayList<Result<TopNResultValue>> expectedResults = Lists.newArrayList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "placementish", "preferred",
                        "rows", 26L,
                        "index", 12459.361190795898D,
                        "addRowsIndexConstant", 12486.361190795898D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        "placementish", "p",
                        "rows", 6L,
                        "index", 5407.213653564453D,
                        "addRowsIndexConstant", 5414.213653564453D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.<String, Object>of(
                        "placementish", "m",
                        "rows", 6L,
                        "index", 5320.717338562012D,
                        "addRowsIndexConstant", 5327.717338562012D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.<String, Object>of(
                        "placementish", "t",
                        "rows", 4L,
                        "index", 422.3440856933594D,
                        "addRowsIndexConstant", 427.3440856933594D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );

    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testTopNLexicographic()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(providerDimension)
        .metric(new LexicographicTopNMetricSpec(""))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        providerDimension, "spot",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        providerDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        providerDimension, "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );

    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testTopNLexicographicWithPreviousStop()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(providerDimension)
        .metric(new LexicographicTopNMetricSpec("spot"))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        providerDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        providerDimension, "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );

    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testTopNLexicographicWithNonExistingPreviousStop()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(providerDimension)
        .metric(new LexicographicTopNMetricSpec("t"))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        providerDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        providerDimension, "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );

    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testTopNDimExtraction()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(
            new ExtractionDimensionSpec(
                providerDimension, providerDimension, new RegexDimExtractionFn("(.)")
            )
        )
        .metric("rows")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        providerDimension, "s",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        providerDimension, "t",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        providerDimension, "u",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );

    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testInvertedTopNQuery()
  {
    TopNQuery query =
        new TopNQueryBuilder()
            .dataSource(QueryRunnerTestHelper.dataSource)
            .granularity(QueryRunnerTestHelper.allGran)
            .dimension(providerDimension)
            .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec(QueryRunnerTestHelper.indexMetric)))
            .threshold(3)
            .intervals(QueryRunnerTestHelper.firstToThird)
            .aggregators(QueryRunnerTestHelper.commonAggregators)
            .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
            .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        providerDimension, "spot",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        providerDimension, "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        providerDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );

    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testTopNDependentPostAgg() {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(providerDimension)
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
                                .put(providerDimension, "total_market")
                                .put("rows", 186L)
                                .put("index", 215679.82879638672D)
                                .put("addRowsIndexConstant", 215866.82879638672D)
                                .put(QueryRunnerTestHelper.dependentPostAggMetric, 216053.82879638672D)
                                .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                                .put("maxIndex", 1743.9217529296875D)
                                .put("minIndex", 792.3260498046875D)
                                .put(
                                    QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
                                    QueryRunnerTestHelper.UNIQUES_2 + 1.0
                                )
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put(providerDimension, "upfront")
                                .put("rows", 186L)
                                .put("index", 192046.1060180664D)
                                .put("addRowsIndexConstant", 192233.1060180664D)
                                .put(QueryRunnerTestHelper.dependentPostAggMetric, 192420.1060180664D)
                                .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                                .put("maxIndex", 1870.06103515625D)
                                .put("minIndex", 545.9906005859375D)
                                .put(
                                    QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
                                    QueryRunnerTestHelper.UNIQUES_2 + 1.0
                                )
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put(providerDimension, "spot")
                                .put("rows", 837L)
                                .put("index", 95606.57232284546D)
                                .put("addRowsIndexConstant", 96444.57232284546D)
                                .put(QueryRunnerTestHelper.dependentPostAggMetric, 97282.57232284546D)
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

  @Test
  public void testTopNBySegmentResults()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
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
                QueryRunnerTestHelper.dependentPostAgg
            )
        )
        .context(ImmutableMap.<String, Object>of("finalize", true, "bySegment", true))
        .build();
    TopNResultValue topNResult = new TopNResultValue(
        Arrays.<Map<String, Object>>asList(
            ImmutableMap.<String, Object>builder()
                        .put(QueryRunnerTestHelper.providerDimension, "total_market")
                        .put("rows", 186L)
                        .put("index", 215679.82879638672D)
                        .put("addRowsIndexConstant", 215866.82879638672D)
                        .put(QueryRunnerTestHelper.dependentPostAggMetric, 216053.82879638672D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1743.9217529296875D)
                        .put("minIndex", 792.3260498046875D)
                        .build(),
            ImmutableMap.<String, Object>builder()
                        .put(QueryRunnerTestHelper.providerDimension, "upfront")
                        .put("rows", 186L)
                        .put("index", 192046.1060180664D)
                        .put("addRowsIndexConstant", 192233.1060180664D)
                        .put(QueryRunnerTestHelper.dependentPostAggMetric, 192420.1060180664D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1870.06103515625D)
                        .put("minIndex", 545.9906005859375D)
                        .build(),
            ImmutableMap.<String, Object>builder()
                        .put(QueryRunnerTestHelper.providerDimension, "spot")
                        .put("rows", 837L)
                        .put("index", 95606.57232284546D)
                        .put("addRowsIndexConstant", 96444.57232284546D)
                        .put(QueryRunnerTestHelper.dependentPostAggMetric, 97282.57232284546D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                        .put("maxIndex", 277.2735290527344D)
                        .put("minIndex", 59.02102279663086D)
                        .build()
        )
    );

    List<Result<BySegmentResultValueClass>> expectedResults = Arrays.asList(
        new Result<BySegmentResultValueClass>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new BySegmentResultValueClass(
                Arrays.asList(
                    new Result<TopNResultValue>(
                        new DateTime("2011-01-12T00:00:00.000Z"),
                        topNResult
                    )
                ),
                QueryRunnerTestHelper.segmentId,
                new Interval("1970-01-01T00:00:00.000Z/2020-01-01T00:00:00.000Z")
            )
        )
    );
    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }
}
