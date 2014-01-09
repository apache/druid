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
import io.druid.granularity.QueryGranularity;
import io.druid.query.Druids;
import io.druid.query.QueryRunner;
import io.druid.query.Result;
import io.druid.query.TestQueryRunners;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.MaxAggregatorFactory;
import io.druid.query.aggregation.MinAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.ConstantPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.dimension.ExtractionDimensionSpec;
import io.druid.query.extraction.RegexDimExtractionFn;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
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
        TopNQueryRunnerTestHelper.makeQueryRunners(
            new TopNQueryRunnerFactory(
                TestQueryRunners.getPool(),
                new TopNQueryQueryToolChest(new TopNQueryConfig())
            )
        )
    );
    retVal.addAll(
        TopNQueryRunnerTestHelper.makeQueryRunners(
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

  final String dataSource = "testing";
  final QueryGranularity gran = QueryGranularity.DAY;
  final QueryGranularity allGran = QueryGranularity.ALL;
  final String providerDimension = "provider";
  final String qualityDimension = "quality";
  final String placementishDimension = "placementish";
  final String indexMetric = "index";
  final String addRowsIndexConstantMetric = "addRowsIndexConstant";
  final CountAggregatorFactory rowsCount = new CountAggregatorFactory("rows");
  final LongSumAggregatorFactory indexLongSum = new LongSumAggregatorFactory("index", "index");
  final DoubleSumAggregatorFactory indexDoubleSum = new DoubleSumAggregatorFactory("index", "index");
  final ConstantPostAggregator constant = new ConstantPostAggregator("const", 1L);
  final FieldAccessPostAggregator rowsPostAgg = new FieldAccessPostAggregator("rows", "rows");
  final FieldAccessPostAggregator indexPostAgg = new FieldAccessPostAggregator("index", "index");
  final ArithmeticPostAggregator addRowsIndexConstant =
      new ArithmeticPostAggregator(
          "addRowsIndexConstant", "+", Lists.newArrayList(constant, rowsPostAgg, indexPostAgg)
      );
  final List<AggregatorFactory> commonAggregators = Arrays.asList(rowsCount, indexDoubleSum);


  final String[] expectedFullOnIndexValues = new String[]{
      "4500.0", "6077.949111938477", "4922.488838195801", "5726.140853881836", "4698.468170166016",
      "4651.030891418457", "4398.145851135254", "4596.068244934082", "4434.630561828613", "0.0",
      "6162.801361083984", "5590.292701721191", "4994.298484802246", "5179.679672241211", "6288.556800842285",
      "6025.663551330566", "5772.855537414551", "5346.517524719238", "5497.331253051758", "5909.684387207031",
      "5862.711364746094", "5958.373008728027", "5224.882194519043", "5456.789611816406", "5456.095397949219",
      "4642.481948852539", "5023.572692871094", "5155.821723937988", "5350.3723220825195", "5236.997489929199",
      "4910.097717285156", "4507.608840942383", "4659.80500793457", "5354.878845214844", "4945.796455383301",
      "6459.080368041992", "4390.493583679199", "6545.758262634277", "6922.801231384277", "6023.452911376953",
      "6812.107475280762", "6368.713348388672", "6381.748748779297", "5631.245086669922", "4976.192253112793",
      "6541.463027954102", "5983.8513107299805", "5967.189498901367", "5567.139289855957", "4863.5944747924805",
      "4681.164360046387", "6122.321441650391", "5410.308860778809", "4846.676376342773", "5333.872688293457",
      "5013.053741455078", "4836.85563659668", "5264.486434936523", "4581.821243286133", "4680.233596801758",
      "4771.363662719727", "5038.354717254639", "4816.808464050293", "4684.095504760742", "5023.663467407227",
      "5889.72257232666", "4984.973915100098", "5664.220512390137", "5572.653915405273", "5537.123138427734",
      "5980.422874450684", "6243.834693908691", "5372.147285461426", "5690.728981018066", "5827.796455383301",
      "6141.0769119262695", "6082.3237228393555", "5678.771339416504", "6814.467971801758", "6626.151596069336",
      "5833.2095947265625", "4679.222328186035", "5367.9403076171875", "5410.445640563965", "5689.197135925293",
      "5240.5018310546875", "4790.912239074707", "4992.670921325684", "4796.888023376465", "5479.439590454102",
      "5506.567192077637", "4743.144546508789", "4913.282669067383", "4723.869743347168"
  };

  final DateTime skippedDay = new DateTime("2011-01-21T00:00:00.000Z");

  final QuerySegmentSpec firstToThird = new MultipleIntervalSegmentSpec(
      Arrays.asList(new Interval("2011-04-01T00:00:00.000Z/2011-04-03T00:00:00.000Z"))
  );
  final QuerySegmentSpec fullOnInterval = new MultipleIntervalSegmentSpec(
      Arrays.asList(new Interval("1970-01-01T00:00:00.000Z/2020-01-01T00:00:00.000Z"))
  );


  @Test
  public void testFullOnTopN()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(allGran)
        .dimension(providerDimension)
        .metric(indexMetric)
        .threshold(4)
        .intervals(fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    commonAggregators,
                    Lists.newArrayList(
                        new MaxAggregatorFactory("maxIndex", "index"),
                        new MinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                                .put("provider", "total_market")
                                .put("rows", 186L)
                                .put("index", 215679.82879638672D)
                                .put("addRowsIndexConstant", 215866.82879638672D)
                                .put("maxIndex", 1743.9217529296875D)
                                .put("minIndex", 792.3260498046875D)
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put("provider", "upfront")
                                .put("rows", 186L)
                                .put("index", 192046.1060180664D)
                                .put("addRowsIndexConstant", 192233.1060180664D)
                                .put("maxIndex", 1870.06103515625D)
                                .put("minIndex", 545.9906005859375D)
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put("provider", "spot")
                                .put("rows", 837L)
                                .put("index", 95606.57232284546D)
                                .put("addRowsIndexConstant", 96444.57232284546D)
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
        .dataSource(dataSource)
        .granularity(allGran)
        .dimension(providerDimension)
        .metric(addRowsIndexConstantMetric)
        .threshold(4)
        .intervals(fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    commonAggregators,
                    Lists.newArrayList(
                        new MaxAggregatorFactory("maxIndex", "index"),
                        new MinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                                .put("provider", "total_market")
                                .put("rows", 186L)
                                .put("index", 215679.82879638672D)
                                .put("addRowsIndexConstant", 215866.82879638672D)
                                .put("maxIndex", 1743.9217529296875D)
                                .put("minIndex", 792.3260498046875D)
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put("provider", "upfront")
                                .put("rows", 186L)
                                .put("index", 192046.1060180664D)
                                .put("addRowsIndexConstant", 192233.1060180664D)
                                .put("maxIndex", 1870.06103515625D)
                                .put("minIndex", 545.9906005859375D)
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put("provider", "spot")
                                .put("rows", 837L)
                                .put("index", 95606.57232284546D)
                                .put("addRowsIndexConstant", 96444.57232284546D)
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
  public void testTopN()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(allGran)
        .dimension(providerDimension)
        .metric(indexMetric)
        .threshold(4)
        .intervals(firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
        .build();


    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "provider", "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D
                    ),
                    ImmutableMap.<String, Object>of(
                        "provider", "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D
                    ),
                    ImmutableMap.<String, Object>of(
                        "provider", "spot",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D
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
        .dataSource(dataSource)
        .granularity(allGran)
        .filters(providerDimension, "total_market", "upfront", "spot")
        .dimension(providerDimension)
        .metric(indexMetric)
        .threshold(4)
        .intervals(firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "provider", "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D
                    ),
                    ImmutableMap.<String, Object>of(
                        "provider", "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D
                    ),
                    ImmutableMap.<String, Object>of(
                        "provider", "spot",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D
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
        .dataSource(dataSource)
        .granularity(allGran)
        .filters(providerDimension, "total_market", "upfront")
        .dimension(providerDimension)
        .metric(indexMetric)
        .threshold(4)
        .intervals(firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "provider", "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D
                    ),
                    ImmutableMap.<String, Object>of(
                        "provider", "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D
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
        .dataSource(dataSource)
        .granularity(allGran)
        .filters(providerDimension, "upfront")
        .dimension(providerDimension)
        .metric(indexMetric)
        .threshold(4)
        .intervals(firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "provider", "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D
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
        .dataSource(dataSource)
        .granularity(allGran)
        .filters(qualityDimension, "mezzanine")
        .dimension(providerDimension)
        .metric(indexMetric)
        .threshold(4)
        .intervals(firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "provider", "upfront",
                        "rows", 2L,
                        "index", 2591.68359375D,
                        "addRowsIndexConstant", 2594.68359375D
                    ),
                    ImmutableMap.<String, Object>of(
                        "provider", "total_market",
                        "rows", 2L,
                        "index", 2508.39599609375D,
                        "addRowsIndexConstant", 2511.39599609375D
                    ),
                    ImmutableMap.<String, Object>of(
                        "provider", "spot",
                        "rows", 2L,
                        "index", 220.63774871826172D,
                        "addRowsIndexConstant", 223.63774871826172D
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
        .dataSource(dataSource)
        .granularity(allGran)
        .filters(qualityDimension, "mezzanine")
        .dimension(providerDimension)
        .metric(indexMetric)
        .threshold(4)
        .intervals(
            new MultipleIntervalSegmentSpec(
                Arrays.asList(new Interval("2011-04-01T00:00:00.000Z/2011-04-02T00:00:00.000Z"))
            )
        )
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "provider", "upfront",
                        "rows", 1L,
                        "index", new Float(1447.341160).doubleValue(),
                        "addRowsIndexConstant", new Float(1449.341160).doubleValue()
                    ),
                    ImmutableMap.<String, Object>of(
                        "provider", "total_market",
                        "rows", 1L,
                        "index", new Float(1314.839715).doubleValue(),
                        "addRowsIndexConstant", new Float(1316.839715).doubleValue()
                    ),
                    ImmutableMap.<String, Object>of(
                        "provider", "spot",
                        "rows", 1L,
                        "index", new Float(109.705815).doubleValue(),
                        "addRowsIndexConstant", new Float(111.705815).doubleValue()
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
        .dataSource(dataSource)
        .granularity(allGran)
        .filters(providerDimension, "total_market", "upfront", "billyblank")
        .dimension(providerDimension)
        .metric(indexMetric)
        .threshold(4)
        .intervals(firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "provider", "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D
                    ),
                    ImmutableMap.<String, Object>of(
                        "provider", "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D
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
        .dataSource(dataSource)
        .granularity(allGran)
        .filters(providerDimension, "billyblank")
        .dimension(providerDimension)
        .metric(indexMetric)
        .threshold(4)
        .intervals(firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
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
                                                    .dimension(qualityDimension)
                                                    .value("mezzanine")
                                                    .build()
                                          )
                                      ).build();
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(allGran)
        .filters(andDimFilter)
        .dimension(providerDimension)
        .metric(indexMetric)
        .threshold(4)
        .intervals(firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
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
        .dataSource(dataSource)
        .granularity(allGran)
        .filters(placementishDimension, "m")
        .dimension(providerDimension)
        .metric(indexMetric)
        .threshold(4)
        .intervals(firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
        .build();

    TestHelper.assertExpectedResults(
        Sequences.toList(
            runner.run(
                new TopNQueryBuilder()
                    .dataSource(dataSource)
                    .granularity(allGran)
                    .filters(qualityDimension, "mezzanine")
                    .dimension(providerDimension)
                    .metric(indexMetric)
                    .threshold(4)
                    .intervals(firstToThird)
                    .aggregators(commonAggregators)
                    .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
                    .build()
            ), Lists.<Result<TopNResultValue>>newArrayList()
        ), runner.run(query)
    );
  }

  @Test
  public void testTopNWithMultiValueDimFilter2()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(allGran)
        .filters(placementishDimension, "m", "a", "b")
        .dimension(qualityDimension)
        .metric(indexMetric)
        .threshold(4)
        .intervals(firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
        .build();

    TestHelper.assertExpectedResults(
        Sequences.toList(
            runner.run(
                new TopNQueryBuilder()
                    .dataSource(dataSource)
                    .granularity(allGran)
                    .filters(qualityDimension, "mezzanine", "automotive", "business")
                    .dimension(qualityDimension)
                    .metric(indexMetric)
                    .threshold(4)
                    .intervals(firstToThird)
                    .aggregators(commonAggregators)
                    .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
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
        .dataSource(dataSource)
        .granularity(allGran)
        .filters(placementishDimension, "a")
        .dimension(placementishDimension)
        .metric(indexMetric)
        .threshold(4)
        .intervals(firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
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
                        "addRowsIndexConstant", 286.31103515625D
                    ),
                    ImmutableMap.<String, Object>of(
                        "placementish", "preferred",
                        "rows", 2L,
                        "index", 283.31103515625D,
                        "addRowsIndexConstant", 286.31103515625D
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
        .dataSource(dataSource)
        .granularity(allGran)
        .filters(placementishDimension, "a", "b")
        .dimension(placementishDimension)
        .metric(indexMetric)
        .threshold(4)
        .intervals(firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
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
                        "addRowsIndexConstant", 519.868408203125D
                    ),
                    ImmutableMap.<String, Object>of(
                        "placementish",
                        "a", "rows", 2L,
                        "index", 283.31103515625D,
                        "addRowsIndexConstant", 286.31103515625D
                    ),
                    ImmutableMap.<String, Object>of(
                        "placementish", "b",
                        "rows", 2L,
                        "index", 231.557373046875D,
                        "addRowsIndexConstant", 234.557373046875D
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
        .dataSource(dataSource)
        .granularity(allGran)
        .filters(placementishDimension, "preferred")
        .dimension(placementishDimension)
        .metric(indexMetric)
        .threshold(4)
        .intervals(firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
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
                        "addRowsIndexConstant", 12486.361190795898D
                    ),
                    ImmutableMap.<String, Object>of(
                        "placementish", "p",
                        "rows", 6L,
                        "index", 5407.213653564453D,
                        "addRowsIndexConstant", 5414.213653564453D
                    ),
                    ImmutableMap.<String, Object>of(
                        "placementish", "m",
                        "rows", 6L,
                        "index", 5320.717338562012D,
                        "addRowsIndexConstant", 5327.717338562012D
                    ),
                    ImmutableMap.<String, Object>of(
                        "placementish", "t",
                        "rows", 4L,
                        "index", 422.3440856933594D,
                        "addRowsIndexConstant", 427.3440856933594D
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
        .dataSource(dataSource)
        .granularity(allGran)
        .dimension(providerDimension)
        .metric(new LexicographicTopNMetricSpec(""))
        .threshold(4)
        .intervals(firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
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
                        "addRowsIndexConstant", 2250.8768157958984D
                    ),
                    ImmutableMap.<String, Object>of(
                        "provider", "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D
                    ),
                    ImmutableMap.<String, Object>of(
                        "provider", "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D
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
        .dataSource(dataSource)
        .granularity(allGran)
        .dimension(providerDimension)
        .metric(new LexicographicTopNMetricSpec("spot"))
        .threshold(4)
        .intervals(firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "provider", "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D
                    ),
                    ImmutableMap.<String, Object>of(
                        "provider", "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D
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
        .dataSource(dataSource)
        .granularity(allGran)
        .dimension(providerDimension)
        .metric(new LexicographicTopNMetricSpec("t"))
        .threshold(4)
        .intervals(firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "provider", "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D
                    ),
                    ImmutableMap.<String, Object>of(
                        "provider", "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D
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
        .dataSource(dataSource)
        .granularity(allGran)
        .dimension(
            new ExtractionDimensionSpec(
                providerDimension, providerDimension, new RegexDimExtractionFn("(.)")
            )
        )
        .metric("rows")
        .threshold(4)
        .intervals(firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "provider", "s",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D
                    ),
                    ImmutableMap.<String, Object>of(
                        "provider", "t",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D
                    ),
                    ImmutableMap.<String, Object>of(
                        "provider", "u",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D
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
            .dataSource(dataSource)
            .granularity(allGran)
            .dimension(providerDimension)
            .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec(indexMetric)))
            .threshold(3)
            .intervals(firstToThird)
            .aggregators(commonAggregators)
            .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
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
                        "addRowsIndexConstant", 2250.8768157958984D
                    ),
                    ImmutableMap.<String, Object>of(
                        "provider", "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D
                    ),
                    ImmutableMap.<String, Object>of(
                        "provider", "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D
                    )
                )
            )
        )
    );

    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }
}