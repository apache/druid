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

package org.apache.druid.query.aggregation.histogram;

import com.google.common.collect.Lists;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.impl.DelimitedInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 */
@RunWith(Parameterized.class)
public class ApproximateHistogramAggregationTest extends InitializedNullHandlingTest
{
  private AggregationTestHelper helper;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public ApproximateHistogramAggregationTest(final GroupByQueryConfig config)
  {
    ApproximateHistogramDruidModule.registerSerde();
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        Lists.newArrayList(new ApproximateHistogramDruidModule().getJacksonModules()),
        config,
        tempFolder
    );
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
      constructors.add(new Object[]{config});
    }
    return constructors;
  }

  @After
  public void teardown() throws IOException
  {
    helper.close();
  }

  @Test
  public void testIngestWithNullsIgnoredAndQuery() throws Exception
  {
    MapBasedRow row = ingestAndQuery(true);
    Assert.assertEquals(92.782760, row.getMetric("index_min").floatValue(), 0.0001);
    Assert.assertEquals(135.109191, row.getMetric("index_max").floatValue(), 0.0001);
    Assert.assertEquals(133.69340, row.getMetric("index_quantile").floatValue(), 0.0001);
    Assert.assertEquals(
        new Quantiles(new float[]{0.2f, 0.7f}, new float[]{92.78276f, 103.195305f}, 92.78276f, 135.109191f),
        row.getRaw("index_quantiles")
    );
    Assert.assertEquals(
        "Histogram{breaks=[92.0, 94.0, 96.0, 98.0, 100.0, 106.0, 108.0, 134.0, 136.0], counts=[1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0]}",
        row.getRaw("index_buckets").toString()
    );
    Assert.assertEquals(
        "Histogram{breaks=[50.0, 100.0], counts=[3.0]}",
        row.getRaw("index_custom").toString()
    );
    Assert.assertEquals(
        "Histogram{breaks=[71.61954498291016, 92.78276062011719, 113.94597625732422, 135.10919189453125], counts=[1.0, 3.0, 1.0]}",
        row.getRaw("index_equal").toString()
    );
  }

  private MapBasedRow ingestAndQuery(boolean ignoreNulls) throws Exception
  {
    AggregatorFactory ingestionAgg = ignoreNulls
        ? new ApproximateHistogramFoldingAggregatorFactory("index_ah", "index", null, null, null, null, null)
        : new ApproximateHistogramAggregatorFactory("index_ah", "index", null, null, null, null, null);

    List<AggregatorFactory> metricSpec = List.of(ingestionAgg);

    GroupByQuery query = GroupByQuery.builder()
                                     .setDataSource("test_datasource")
                                     .setGranularity(Granularities.ALL)
                                     .setInterval("1970/2050")
                                     .setAggregatorSpecs(
                                         new ApproximateHistogramFoldingAggregatorFactory("index_ah", "index_ah", null, null, null, null, null)
                                     )
                                     .setPostAggregatorSpecs(
                                         new MinPostAggregator("index_min", "index_ah"),
                                         new MaxPostAggregator("index_max", "index_ah"),
                                         new QuantilePostAggregator("index_quantile", "index_ah", 0.99f),
                                         new QuantilesPostAggregator("index_quantiles", "index_ah", new float[]{0.2f, 0.7f}),
                                         new BucketsPostAggregator("index_buckets", "index_ah", 2.0f, 4.0f),
                                         new CustomBucketsPostAggregator("index_custom", "index_ah", new float[]{50.0f, 100.0f}),
                                         new EqualBucketsPostAggregator("index_equal", "index_ah", 3)
                                     )
                                     .build();

    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        this.getClass().getClassLoader().getResourceAsStream("sample.data.tsv"),
        new InputRowSchema(
            new TimestampSpec("timestamp", "auto", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(List.of())),
            ColumnsFilter.all()
        ),
        DelimitedInputFormat.forColumns(
            List.of("timestamp", "market", "quality", "placement", "placementish", "index")
        ),
        metricSpec,
        0,
        Granularities.NONE,
        50000,
        query
    );

    return seq.toList().get(0).toMapBasedRow(query);
  }
}
