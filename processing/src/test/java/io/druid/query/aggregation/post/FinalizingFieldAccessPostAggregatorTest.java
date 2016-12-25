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

package io.druid.query.aggregation.post;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.data.input.MapBasedRow;
import io.druid.granularity.QueryGranularities;
import io.druid.jackson.AggregatorsModule;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.aggregation.AggregationTestHelper;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregator;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.groupby.GroupByQueryRunnerTest;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.List;
import java.util.Map;

public class FinalizingFieldAccessPostAggregatorTest
{
  @Rule
  public final TemporaryFolder tempFoler = new TemporaryFolder();

  @Test
  public void testComputeWithoutFinalizing()
  {
    Aggregator agg = new CountAggregator();
    agg.aggregate();
    agg.aggregate();
    agg.aggregate();

    Map<String, Object> metricValues = Maps.newHashMap();
    metricValues.put(agg.getName(), agg.get());

    PostAggregator postAgg = new FinalizingFieldAccessPostAggregator("final_rows", "rows");
    Assert.assertEquals(new Long(3L), postAgg.compute(metricValues));
  }

  @Test
  public void testComputedWithFinalizing()
  {
    AggregatorFactory aggFactory = EasyMock.createMock(AggregatorFactory.class);
    EasyMock.expect(aggFactory.finalizeComputation("test"))
            .andReturn(new Long(3L))
            .times(1);
    EasyMock.replay(aggFactory);

    FinalizingFieldAccessPostAggregator postAgg = new FinalizingFieldAccessPostAggregator("final_billy", "billy");
    postAgg.setDependentAggFactories(ImmutableMap.of("billy", aggFactory));

    Map<String, Object> metricValues = Maps.newHashMap();
    metricValues.put("billy", "test");

    Assert.assertEquals(new Long(3L), postAgg.compute(metricValues));
    EasyMock.verify(aggFactory);
  }

  @Test
  public void testComputedInArithmeticPostAggregator()
  {
    AggregatorFactory aggFactory = EasyMock.createMock(AggregatorFactory.class);
    EasyMock.expect(aggFactory.finalizeComputation("test"))
            .andReturn(new Long(3L))
            .times(1);
    EasyMock.replay(aggFactory);

    FinalizingFieldAccessPostAggregator postAgg = new FinalizingFieldAccessPostAggregator("final_billy", "billy");
    postAgg.setDependentAggFactories(ImmutableMap.of("billy", aggFactory));

    Map<String, Object> metricValues = Maps.newHashMap();
    metricValues.put("billy", "test");

    List<PostAggregator> postAggsList = Lists.newArrayList(
        new ConstantPostAggregator("roku", 6), postAgg);

    ArithmeticPostAggregator arithmeticPostAggregator = new ArithmeticPostAggregator("add", "+", postAggsList);

    Assert.assertEquals(new Double(9.0f), arithmeticPostAggregator.compute(metricValues));
    EasyMock.verify();
  }

  @Test
  public void testIngestAndQueryWithArithmeticPostAggregator() throws Exception
  {
    AggregationTestHelper helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        Lists.newArrayList(new AggregatorsModule()),
        GroupByQueryRunnerTest.testConfigs().get(0),
        tempFoler
    );

    String metricSpec = "[{\"type\": \"hyperUnique\", \"name\": \"hll_market\", \"fieldName\": \"market\"},"
                        + "{\"type\": \"hyperUnique\", \"name\": \"hll_quality\", \"fieldName\": \"quality\"}]";

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
                   + "  { \"type\": \"hyperUnique\", \"name\": \"hll_market\", \"fieldName\": \"hll_market\" },"
                   + "  { \"type\": \"hyperUnique\", \"name\": \"hll_quality\", \"fieldName\": \"hll_quality\" }"
                   + "],"
                   + "\"postAggregations\": ["
                   + "  { \"type\": \"arithmetic\", \"name\": \"uniq_add\", \"fn\": \"+\", \"fields\":["
                   + "    { \"type\": \"finalizingFieldAccess\", \"name\": \"uniq_market\", \"fieldName\": \"hll_market\" },"
                   + "    { \"type\": \"finalizingFieldAccess\", \"name\": \"uniq_quality\", \"fieldName\": \"hll_quality\" }]"
                   + "  }"
                   + "],"
                   + "\"intervals\": [ \"1970/2050\" ]"
                   + "}";

    Sequence seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("druid.sample.tsv").getFile()),
        parseSpec,
        metricSpec,
        0,
        QueryGranularities.NONE,
        50000,
        query
    );

    MapBasedRow row = (MapBasedRow) Sequences.toList(seq, Lists.newArrayList()).get(0);
    Assert.assertEquals(3.0, row.getFloatMetric("hll_market"), 0.1);
    Assert.assertEquals(9.0, row.getFloatMetric("hll_quality"), 0.1);
    Assert.assertEquals(12.0, row.getFloatMetric("uniq_add"), 0.1);
  }
}
