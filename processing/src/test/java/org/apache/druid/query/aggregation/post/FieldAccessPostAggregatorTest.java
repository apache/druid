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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.CountAggregator;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class FieldAccessPostAggregatorTest extends InitializedNullHandlingTest
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testSerde() throws JsonProcessingException
  {
    FieldAccessPostAggregator postAgg = new FieldAccessPostAggregator("name", "column");
    Assert.assertEquals(
        postAgg,
        JSON_MAPPER.readValue(JSON_MAPPER.writeValueAsString(postAgg), FieldAccessPostAggregator.class)
    );
  }

  @Test
  public void testEqualsAndHashcode()
  {
    // type is computed by decorate
    EqualsVerifier.forClass(FieldAccessPostAggregator.class).usingGetClass().withIgnoredFields("type").verify();
  }

  @Test
  public void testGetTypeBeforeDecorate()
  {
    FieldAccessPostAggregator postAgg = new FieldAccessPostAggregator("name", "column");
    RowSignature signature = RowSignature.builder()
                                         .add("column", ColumnType.LONG)
                                         .build();
    Assert.assertEquals(ColumnType.LONG, postAgg.getType(signature));
  }

  @Test
  public void testGetTypeBeforeDecorateNil()
  {
    FieldAccessPostAggregator postAgg = new FieldAccessPostAggregator("name", "column");
    RowSignature signature = RowSignature.builder().build();
    Assert.assertNull(postAgg.getType(signature));
  }

  @Test
  public void testCompute()
  {
    final String aggName = "rows";
    FieldAccessPostAggregator fieldAccessPostAggregator;

    fieldAccessPostAggregator = new FieldAccessPostAggregator("To be, or not to be, that is the question:", "rows");
    CountAggregator agg = new CountAggregator();
    Map<String, Object> metricValues = new HashMap<String, Object>();
    metricValues.put(aggName, agg.get());
    Assert.assertEquals(new Long(0L), fieldAccessPostAggregator.compute(metricValues));

    agg.aggregate();
    agg.aggregate();
    agg.aggregate();
    metricValues.put(aggName, agg.get());
    Assert.assertEquals(new Long(3L), fieldAccessPostAggregator.compute(metricValues));
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
                  new DoubleSumAggregatorFactory("double", "col1"),
                  new FloatSumAggregatorFactory("float", "col2")
              )
              .postAggregators(
                  new FieldAccessPostAggregator("a", "count"),
                  new FieldAccessPostAggregator("b", "double"),
                  new FieldAccessPostAggregator("c", "float")
              )
              .build();

    Assert.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add("count", ColumnType.LONG)
                    .add("double", ColumnType.DOUBLE)
                    .add("float", ColumnType.FLOAT)
                    .add("a", ColumnType.LONG)
                    .add("b", ColumnType.DOUBLE)
                    .add("c", ColumnType.FLOAT)
                    .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }
}
