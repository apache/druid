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

package org.apache.druid.query.aggregation.datasketches.kll;

import com.fasterxml.jackson.core.JsonProcessingException;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.TestDoubleColumnSelectorImpl;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

public class KllDoublesSketchToCDFPostAggregatorTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testSerde() throws JsonProcessingException
  {
    final PostAggregator there = new KllDoublesSketchToCDFPostAggregator(
        "post",
        new FieldAccessPostAggregator("field1", "sketch"),
        new double[]{0.25, 0.75}
    );
    DefaultObjectMapper mapper = new DefaultObjectMapper();
    KllDoublesSketchToCDFPostAggregator andBackAgain = mapper.readValue(
        mapper.writeValueAsString(there),
        KllDoublesSketchToCDFPostAggregator.class
    );

    Assert.assertEquals(there, andBackAgain);
    Assert.assertArrayEquals(there.getCacheKey(), andBackAgain.getCacheKey());
  }

  @Test
  public void testToString()
  {
    final PostAggregator postAgg = new KllDoublesSketchToCDFPostAggregator(
        "post",
        new FieldAccessPostAggregator("field1", "sketch"),
        new double[]{0.25, 0.75}
    );

    Assert.assertEquals(
        "KllDoublesSketchToCDFPostAggregator{name='post', field=FieldAccessPostAggregator{name='field1', fieldName='sketch'}, splitPoints=[0.25, 0.75]}",
        postAgg.toString()
    );
  }

  @Test
  public void testComparator()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Comparing histograms is not supported");
    final PostAggregator postAgg = new KllDoublesSketchToCDFPostAggregator(
        "post",
        new FieldAccessPostAggregator("field1", "sketch"),
        new double[]{0.25, 0.75}
    );
    postAgg.getComparator();
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(KllDoublesSketchToCDFPostAggregator.class)
                  .withNonnullFields("name", "field", "splitPoints")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void emptySketch()
  {
    final TestDoubleColumnSelectorImpl selector = new TestDoubleColumnSelectorImpl(null);
    final Aggregator agg = new KllDoublesSketchBuildAggregator(selector, 8);

    final Map<String, Object> fields = new HashMap<>();
    fields.put("sketch", agg.get());

    final PostAggregator postAgg = new KllDoublesSketchToCDFPostAggregator(
        "cdf",
        new FieldAccessPostAggregator("field", "sketch"),
        new double[] {4}
    );

    final double[] histogram = (double[]) postAgg.compute(fields);
    Assert.assertNotNull(histogram);
    Assert.assertEquals(2, histogram.length);
    Assert.assertTrue(Double.isNaN(histogram[0]));
    Assert.assertTrue(Double.isNaN(histogram[1]));
  }

  @Test
  public void normalCase()
  {
    final double[] values = new double[] {1, 2, 3, 4, 5, 6};
    final TestDoubleColumnSelectorImpl selector = new TestDoubleColumnSelectorImpl(values);

    final Aggregator agg = new KllDoublesSketchBuildAggregator(selector, 8);
    //noinspection ForLoopReplaceableByForEach
    for (int i = 0; i < values.length; i++) {
      agg.aggregate();
      selector.increment();
    }

    final Map<String, Object> fields = new HashMap<>();
    fields.put("sketch", agg.get());

    final PostAggregator postAgg = new KllDoublesSketchToCDFPostAggregator(
        "cdf",
        new FieldAccessPostAggregator("field", "sketch"),
        new double[] {4} // half of the distribution is below 4
    );

    final double[] cdf = (double[]) postAgg.compute(fields);
    Assert.assertNotNull(cdf);
    Assert.assertEquals(2, cdf.length);
    Assert.assertEquals(0.5, cdf[0], 0);
    Assert.assertEquals(1.0, cdf[1], 0);
  }

  @Test
  public void testResultArraySignature()
  {
    final TimeseriesQuery query =
        Druids.newTimeseriesQueryBuilder()
              .dataSource("dummy")
              .intervals("2000/3000")
              .granularity(Granularities.HOUR)
              .aggregators(
                  new KllDoublesSketchAggregatorFactory("sketch", "col", 8, 1000000L)
              )
              .postAggregators(
                  new KllDoublesSketchToCDFPostAggregator(
                      "a",
                      new FieldAccessPostAggregator("field", "sketch"),
                      new double[] {4}
                  )
              )
              .build();

    Assert.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add("sketch", null)
                    .add("a", ColumnType.DOUBLE_ARRAY)
                    .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }
}
