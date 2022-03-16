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

package org.apache.druid.query.aggregation.datasketches.hll;

import com.fasterxml.jackson.core.JsonProcessingException;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;
import org.junit.Test;

public class HllSketchToEstimatePostAggregatorTest
{
  @Test
  public void testSerde() throws JsonProcessingException
  {
    final PostAggregator there = new HllSketchToEstimatePostAggregator(
        "post",
        new FieldAccessPostAggregator("field1", "sketch"),
        true
    );
    DefaultObjectMapper mapper = new DefaultObjectMapper();
    HllSketchToEstimatePostAggregator andBackAgain = mapper.readValue(
        mapper.writeValueAsString(there),
        HllSketchToEstimatePostAggregator.class
    );

    Assert.assertEquals(there, andBackAgain);
    Assert.assertArrayEquals(there.getCacheKey(), andBackAgain.getCacheKey());
  }

  @Test
  public void testToString()
  {
    final PostAggregator postAgg = new HllSketchToEstimatePostAggregator(
        "post",
        new FieldAccessPostAggregator("field1", "sketch"),
        true
    );

    Assert.assertEquals(
        "HllSketchToEstimatePostAggregator{name='post', field=FieldAccessPostAggregator{name='field1', fieldName='sketch'}, round=true}",
        postAgg.toString()
    );
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(HllSketchToEstimatePostAggregator.class)
                  .withNonnullFields("name", "field")
                  .usingGetClass()
                  .verify();
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
                  new CountAggregatorFactory("count"),
                  new HllSketchMergeAggregatorFactory(
                      "hllMerge",
                      "col",
                      null,
                      null,
                      false
                  )
              )
              .postAggregators(
                  new HllSketchToEstimatePostAggregator(
                      "hllEstimate",
                      new FieldAccessPostAggregator(null, "hllMerge"),
                      false
                  ),
                  new HllSketchToEstimatePostAggregator(
                      "hllEstimateRound",
                      new FieldAccessPostAggregator(null, "hllMerge"),
                      true
                  )
              )
              .build();

    Assert.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add("count", ColumnType.LONG)
                    .add("hllMerge", null)
                    .add("hllEstimate", ColumnType.DOUBLE)
                    .add("hllEstimateRound", ColumnType.LONG)
                    .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }
}
