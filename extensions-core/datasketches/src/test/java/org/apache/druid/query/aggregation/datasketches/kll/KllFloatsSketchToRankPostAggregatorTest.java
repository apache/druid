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
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.TestFloatColumnSelector;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class KllFloatsSketchToRankPostAggregatorTest
{
  @Test
  public void testSerde() throws JsonProcessingException
  {
    final PostAggregator there = new KllFloatsSketchToRankPostAggregator(
        "post",
        new FieldAccessPostAggregator("field1", "sketch"),
        0
    );
    DefaultObjectMapper mapper = new DefaultObjectMapper();
    KllFloatsSketchToRankPostAggregator andBackAgain = mapper.readValue(
        mapper.writeValueAsString(there),
        KllFloatsSketchToRankPostAggregator.class
    );

    Assert.assertEquals(there, andBackAgain);
    Assert.assertArrayEquals(there.getCacheKey(), andBackAgain.getCacheKey());
  }

  @Test
  public void testToString()
  {
    final PostAggregator postAgg = new KllFloatsSketchToRankPostAggregator(
        "post",
        new FieldAccessPostAggregator("field1", "sketch"),
        0
    );

    Assert.assertEquals(
        "KllFloatsSketchToRankPostAggregator{name='post', field=FieldAccessPostAggregator{name='field1', fieldName='sketch'}, value=0.0}",
        postAgg.toString()
    );
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(KllFloatsSketchToRankPostAggregator.class)
                  .withNonnullFields("name", "field", "value")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void emptySketch()
  {
    final TestFloatColumnSelector selector = new TestFloatColumnSelector(null);
    final Aggregator agg = new KllFloatsSketchBuildAggregator(selector, 8);

    final Map<String, Object> fields = new HashMap<>();
    fields.put("sketch", agg.get());

    final PostAggregator postAgg = new KllFloatsSketchToRankPostAggregator(
        "rank",
        new FieldAccessPostAggregator("field", "sketch"),
        0
    );

    final double rank = (double) postAgg.compute(fields);
    Assert.assertTrue(Double.isNaN(rank));
  }

  @Test
  public void normalCase()
  {
    final float[] values = new float[] {1, 2, 3, 4, 5, 6};
    final TestFloatColumnSelector selector = new TestFloatColumnSelector(values);

    final Aggregator agg = new KllFloatsSketchBuildAggregator(selector, 8);
    //noinspection ForLoopReplaceableByForEach
    for (int i = 0; i < values.length; i++) {
      agg.aggregate();
      selector.increment();
    }

    final Map<String, Object> fields = new HashMap<>();
    fields.put("sketch", agg.get());

    final PostAggregator postAgg = new KllFloatsSketchToRankPostAggregator(
        "rank",
        new FieldAccessPostAggregator("field", "sketch"),
        4
    );

    final double rank = (double) postAgg.compute(fields);
    Assert.assertEquals(0.5, rank, 0);
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
                  new KllFloatsSketchAggregatorFactory("sketch", "col", 8, 1000000L)
              )
              .postAggregators(
                  new KllFloatsSketchToRankPostAggregator(
                      "a",
                      new FieldAccessPostAggregator("field", "sketch"),
                      4
                  )
              )
              .build();

    Assert.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add("sketch", null)
                    .add("a", ColumnType.DOUBLE)
                    .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }
}
