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
public class FixedBucketsHistogramAggregationTest
{
  private AggregationTestHelper helper;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public FixedBucketsHistogramAggregationTest(final GroupByQueryConfig config)
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
    MapBasedRow row = ingestAndQuery();
    if (!NullHandling.replaceWithDefault()) {
      Assert.assertEquals(92.782760, row.getMetric("index_min").floatValue(), 0.0001);
      Assert.assertEquals(135.109191, row.getMetric("index_max").floatValue(), 0.0001);
      Assert.assertEquals(135.9499969482422, row.getMetric("index_quantile").floatValue(), 0.0001);
    } else {
      Assert.assertEquals(0.0, row.getMetric("index_min"));
      Assert.assertEquals(135.109191, row.getMetric("index_max").floatValue(), 0.0001);
      Assert.assertEquals(135.8699951171875, row.getMetric("index_quantile").floatValue(), 0.0001);
    }
  }

  private MapBasedRow ingestAndQuery() throws Exception
  {
    String ingestionAgg = FixedBucketsHistogramAggregator.TYPE_NAME;

    String metricSpec = "[{"
                        + "\"type\": \"" + ingestionAgg + "\","
                        + "\"name\": \"index_fbh\","
                        + "\"numBuckets\": 200,"
                        + "\"lowerLimit\": 0,"
                        + "\"upperLimit\": 200,"
                        + "\"outlierHandlingMode\": \"overflow\","
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
                   + "  {"
                   + "   \"type\": \"fixedBucketsHistogram\","
                   + "   \"name\": \"index_fbh\","
                   + "   \"fieldName\": \"index_fbh\","
                   + "   \"numBuckets\": 200,"
                   + "   \"lowerLimit\": 0,"
                   + "   \"upperLimit\": 200,"
                   + "   \"outlierHandlingMode\": \"overflow\""
                   + "  }"
                   + "],"
                   + "\"postAggregations\": ["
                   + "  { \"type\": \"min\", \"name\": \"index_min\", \"fieldName\": \"index_fbh\"},"
                   + "  { \"type\": \"max\", \"name\": \"index_max\", \"fieldName\": \"index_fbh\"},"
                   + "  { \"type\": \"quantile\", \"name\": \"index_quantile\", \"fieldName\": \"index_fbh\", \"probability\" : 0.99 }"
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
