/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation.histogram;

import com.google.common.collect.Lists;

import io.druid.data.input.MapBasedRow;
import io.druid.granularity.QueryGranularities;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.aggregation.AggregationTestHelper;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 */
public class ApproximateHistogramAggregationTest
{
  private AggregationTestHelper helper;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public ApproximateHistogramAggregationTest()
  {
    ApproximateHistogramDruidModule module = new ApproximateHistogramDruidModule();
    module.configure(null);
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        Lists.newArrayList(module.getJacksonModules()),
        tempFolder
    );
  }

  @Test
  public void testIngestWithNullsIgnoredAndQuery() throws Exception
  {
    MapBasedRow row = ingestAndQuery(true);
    Assert.assertEquals(92.782760, row.getFloatMetric("index_min"), 0.0001);
    Assert.assertEquals(135.109191, row.getFloatMetric("index_max"), 0.0001);
    Assert.assertEquals(133.69340, row.getFloatMetric("index_quantile"), 0.0001);
  }

  @Test
  public void testIngestWithNullsToZeroAndQuery() throws Exception
  {
    MapBasedRow row = ingestAndQuery(false);
    Assert.assertEquals(0.0, row.getFloatMetric("index_min"), 0.0001);
    Assert.assertEquals(135.109191, row.getFloatMetric("index_max"), 0.0001);
    Assert.assertEquals(131.428176, row.getFloatMetric("index_quantile"), 0.0001);
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
                   + "  { \"type\": \"quantile\", \"name\": \"index_quantile\", \"fieldName\": \"index_ah\", \"probability\" : 0.99 }"
                   + "],"
                   + "\"intervals\": [ \"1970/2050\" ]"
                   + "}";

    Sequence seq = helper.createIndexAndRunQueryOnSegment(
        this.getClass().getClassLoader().getResourceAsStream("sample.data.tsv"),
        parseSpec,
        metricSpec,
        0,
        QueryGranularities.NONE,
        50000,
        query
    );

    return (MapBasedRow) Sequences.toList(seq, Lists.newArrayList()).get(0);
  }
}
