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

package org.apache.druid.query.aggregation.post;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import org.apache.druid.jackson.AggregatorsModule;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregator;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.segment.TestHelper;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FinalizingFieldAccessPostAggregatorTest
{
  @Rule
  public final TemporaryFolder tempFoler = new TemporaryFolder();

  @Test(expected = UnsupportedOperationException.class)
  public void testComputeWithoutFinalizing()
  {
    String aggName = "rows";
    Aggregator agg = new CountAggregator();
    agg.aggregate();
    agg.aggregate();
    agg.aggregate();

    Map<String, Object> metricValues = new HashMap<>();
    metricValues.put(aggName, agg.get());

    FinalizingFieldAccessPostAggregator postAgg = new FinalizingFieldAccessPostAggregator("final_rows", aggName);
    Assert.assertEquals(new Long(3L), postAgg.compute(metricValues));
  }

  @Test
  public void testComputedWithFinalizing()
  {
    String aggName = "biily";
    AggregatorFactory aggFactory = EasyMock.createMock(AggregatorFactory.class);
    EasyMock.expect(aggFactory.getComparator()).andReturn(Comparators.naturalNullsFirst()).once();
    EasyMock.expect(aggFactory.finalizeComputation("test")).andReturn(3L).once();
    EasyMock.replay(aggFactory);

    FinalizingFieldAccessPostAggregator postAgg = buildDecorated(
        "final_billy", aggName, ImmutableMap.of(aggName, aggFactory)
    );

    // Check that the class matches exactly; see https://github.com/apache/incubator-druid/issues/6063
    Assert.assertEquals(FinalizingFieldAccessPostAggregator.class, postAgg.getClass());

    Map<String, Object> metricValues = new HashMap<>();
    metricValues.put(aggName, "test");

    Assert.assertEquals(new Long(3L), postAgg.compute(metricValues));
    EasyMock.verify(aggFactory);
  }

  @Test
  public void testComputedInArithmeticPostAggregator()
  {
    String aggName = "billy";
    AggregatorFactory aggFactory = EasyMock.createMock(AggregatorFactory.class);
    EasyMock.expect(aggFactory.getComparator()).andReturn(Comparators.naturalNullsFirst()).once();
    EasyMock.expect(aggFactory.finalizeComputation("test")).andReturn(3L).once();
    EasyMock.replay(aggFactory);

    FinalizingFieldAccessPostAggregator postAgg = buildDecorated(
        "final_billy", aggName, ImmutableMap.of(aggName, aggFactory)
    );

    Map<String, Object> metricValues = new HashMap<>();
    metricValues.put(aggName, "test");

    List<PostAggregator> postAggsList = Lists.newArrayList(
        new ConstantPostAggregator("roku", 6), postAgg);

    ArithmeticPostAggregator arithmeticPostAggregator = new ArithmeticPostAggregator("add", "+", postAggsList);

    Assert.assertEquals(new Double(9.0f), arithmeticPostAggregator.compute(metricValues));
    EasyMock.verify();
  }

  @Test
  public void testComparatorsWithFinalizing()
  {
    String aggName = "billy";
    AggregatorFactory aggFactory = EasyMock.createMock(AggregatorFactory.class);
    EasyMock.expect(aggFactory.finalizeComputation("test_val1"))
            .andReturn(new Long(10L))
            .times(1);
    EasyMock.expect(aggFactory.finalizeComputation("test_val2"))
            .andReturn(new Long(21))
            .times(1);
    EasyMock.expect(aggFactory.finalizeComputation("test_val3"))
            .andReturn(new Long(3))
            .times(1);
    EasyMock.expect(aggFactory.finalizeComputation("test_val4"))
            .andReturn(null)
            .times(1);
    EasyMock.expect(aggFactory.getComparator())
            .andReturn(Ordering.natural().<Long>nullsLast())
            .times(1);
    EasyMock.replay(aggFactory);

    FinalizingFieldAccessPostAggregator postAgg = buildDecorated(
        "final_billy", aggName, ImmutableMap.of(aggName, aggFactory)
    );

    List<Object> computedValues = new ArrayList<>();
    computedValues.add(postAgg.compute(ImmutableMap.of(aggName, "test_val1")));
    computedValues.add(postAgg.compute(ImmutableMap.of(aggName, "test_val2")));
    computedValues.add(postAgg.compute(ImmutableMap.of(aggName, "test_val3")));
    computedValues.add(postAgg.compute(ImmutableMap.of(aggName, "test_val4")));

    Collections.sort(computedValues, postAgg.getComparator());
    Assert.assertArrayEquals(new Object[]{3L, 10L, 21L, null}, computedValues.toArray(new Object[0]));
    EasyMock.verify();
  }

  @Test
  public void testComparatorsWithFinalizingAndComparatorNull()
  {
    String aggName = "billy";
    AggregatorFactory aggFactory = EasyMock.createMock(AggregatorFactory.class);
    EasyMock.expect(aggFactory.getComparator())
            .andReturn(null)
            .times(1);
    EasyMock.replay(aggFactory);

    FinalizingFieldAccessPostAggregator postAgg = buildDecorated(
        "final_billy", "joe", ImmutableMap.of(aggName, aggFactory));

    List<Object> computedValues = new ArrayList<>();
    Map<String, Object> forNull = new HashMap<>();
    forNull.put("joe", null); // guava does not allow the value to be null.
    computedValues.add(postAgg.compute(ImmutableMap.of("joe", "test_val1")));
    computedValues.add(postAgg.compute(ImmutableMap.of("joe", "test_val2")));
    computedValues.add(postAgg.compute(forNull));
    computedValues.add(postAgg.compute(ImmutableMap.of("joe", "test_val4")));
    Collections.sort(computedValues, postAgg.getComparator());

    Assert.assertArrayEquals(
        new Object[]{null, "test_val1", "test_val2", "test_val4"},
        computedValues.toArray(new Object[0])
    );

    EasyMock.verify();
  }

  @Test
  public void testIngestAndQueryWithArithmeticPostAggregator() throws Exception
  {
    try (
        final AggregationTestHelper helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
            Collections.singletonList(new AggregatorsModule()),
            GroupByQueryRunnerTest.testConfigs().get(0),
            tempFoler
        )
    ) {

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

      Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
          new File(this.getClass().getClassLoader().getResource("druid.sample.tsv").getFile()),
          parseSpec,
          metricSpec,
          0,
          Granularities.NONE,
          50000,
          query
      );

      final ResultRow resultRow = seq.toList().get(0);
      Assert.assertEquals("hll_market", 3.0, ((Number) resultRow.get(0)).floatValue(), 0.1);
      Assert.assertEquals("hll_quality", 9.0, ((Number) resultRow.get(1)).floatValue(), 0.1);
      Assert.assertEquals("uniq_add", 12.0, ((Number) resultRow.get(2)).floatValue(), 0.1);
    }
  }

  @Test
  public void testSerde() throws IOException
  {
    final FinalizingFieldAccessPostAggregator original = new FinalizingFieldAccessPostAggregator("foo", "bar");
    final FinalizingFieldAccessPostAggregator decorated = original.decorate(
        ImmutableMap.of("bar", new CountAggregatorFactory("bar"))
    );
    final ObjectMapper objectMapper = TestHelper.makeJsonMapper();
    Assert.assertEquals(
        original,
        objectMapper.readValue(objectMapper.writeValueAsString(decorated), PostAggregator.class)
    );
  }

  private static FinalizingFieldAccessPostAggregator buildDecorated(
      String name,
      String fieldName,
      Map<String, AggregatorFactory> aggregators
  )
  {
    FinalizingFieldAccessPostAggregator ret = new FinalizingFieldAccessPostAggregator(name, fieldName);
    return ret.decorate(aggregators);
  }
}
