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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.aggregation.AggregationTestHelper;
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

  @Test
  public void testIngestWithNullsToZeroAndQuery() throws Exception
  {
    // Nulls are ignored and not replaced with default for SQL compatible null handling.
    // This is already tested in testIngestWithNullsIgnoredAndQuery()
    if (NullHandling.replaceWithDefault()) {
      MapBasedRow row = ingestAndQuery(false);
      Assert.assertEquals(0.0F, row.getMetric("index_min"));
      Assert.assertEquals(135.109191, row.getMetric("index_max").floatValue(), 0.0001);
      Assert.assertEquals(131.428176, row.getMetric("index_quantile").floatValue(), 0.0001);
      Assert.assertEquals(
          new Quantiles(new float[]{0.2f, 0.7f}, new float[]{0.0f, 92.95146f}, 0.0f, 135.109191f),
          row.getRaw("index_quantiles")
      );
      Assert.assertEquals(
          "Histogram{breaks=[-2.0, 92.0, 94.0, 96.0, 98.0, 100.0, 106.0, 108.0, 134.0, 136.0], counts=[8.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0]}",
          row.getRaw("index_buckets").toString()
      );
      Assert.assertEquals(
          "Histogram{breaks=[50.0, 100.0], counts=[3.0]}",
          row.getRaw("index_custom").toString()
      );
      Assert.assertEquals(
          "Histogram{breaks=[-67.55459594726562, 0.0, 67.55459594726562, 135.10919189453125], counts=[8.0, 0.0, 5.0]}",
          row.getRaw("index_equal").toString()
      );
    }
  }

  private MapBasedRow ingestAndQuery(boolean ignoreNulls) throws Exception
  {
    String ingestionAgg = ignoreNulls ? "approxHistogramFold" : "approxHistogram";

    String metricSpec = "[{"
                        + "\"type\": \"" + ingestionAgg + "\","
                        + "\"name\": \"index_ah\","
                        + "\"fieldName\": \"index\""
                        + "}]";

    String parseSpec = "{"
                       + "\"type\" : \"string\","
                       + "\"parseSpec\" : {"
                       + "    \"format\" : \"tsv\","
                       + "    \"timestampSpec\" : {"
                       + "        \"column\" : \"timestamp\","
                       + "        \"format\" : \"auto\""
                       + "},"
                       + "    \"dimensionsSpec\" : {"
                       + "        \"dimensions\": [],"
                       + "        \"dimensionExclusions\" : [],"
                       + "        \"spatialDimensions\" : []"
                       + "    },"
                       + "    \"columns\": [\"timestamp\", \"market\", \"quality\", \"placement\", \"placementish\", \"index\"]"
                       + "  }"
                       + "}";

    String query = "{"
                   + "\"queryType\": \"groupBy\","
                   + "\"dataSource\": \"test_datasource\","
                   + "\"granularity\": \"ALL\","
                   + "\"dimensions\": [],"
                   + "\"aggregations\": ["
                   + "  { \"type\": \"approxHistogramFold\", \"name\": \"index_ah\", \"fieldName\": \"index_ah\" }"
                   + "],"
                   + "\"postAggregations\": ["
                   + "  { \"type\": \"min\", \"name\": \"index_min\", \"fieldName\": \"index_ah\"},"
                   + "  { \"type\": \"max\", \"name\": \"index_max\", \"fieldName\": \"index_ah\"},"
                   + "  { \"type\": \"quantile\", \"name\": \"index_quantile\", \"fieldName\": \"index_ah\", \"probability\" : 0.99 },"
                   + "  { \"type\": \"quantiles\", \"name\": \"index_quantiles\", \"fieldName\": \"index_ah\", \"probabilities\" : [0.2, 0.7] },"
                   + "  { \"type\": \"buckets\", \"name\": \"index_buckets\", \"fieldName\": \"index_ah\", \"bucketSize\" : 2.0, \"offset\": 4.0 },"
                   + "  { \"type\": \"customBuckets\", \"name\": \"index_custom\", \"fieldName\": \"index_ah\", \"breaks\" : [50.0, 100.0] },"
                   + "  { \"type\": \"equalBuckets\", \"name\": \"index_equal\", \"fieldName\": \"index_ah\", \"numBuckets\" : 3 }"
                   + "],"
                   + "\"intervals\": [ \"1970/2050\" ]"
                   + "}";

    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        this.getClass().getClassLoader().getResourceAsStream("sample.data.tsv"),
        parseSpec,
        metricSpec,
        0,
        Granularities.NONE,
        50000,
        query
    );

    return seq.toList().get(0).toMapBasedRow((GroupByQuery) helper.readQuery(query));
  }
}
