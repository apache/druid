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
import org.apache.druid.math.expr.SettableObjectBinding;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

public class ExpressionPostAggregatorTest extends InitializedNullHandlingTest
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testSerde() throws JsonProcessingException
  {
    ExpressionPostAggregator postAgg = new ExpressionPostAggregator(
        "p0",
        "2 + 3",
        null,
        TestExprMacroTable.INSTANCE
    );

    Assert.assertEquals(
        postAgg,
        JSON_MAPPER.readValue(JSON_MAPPER.writeValueAsString(postAgg), ExpressionPostAggregator.class)
    );
  }

  @Test
  public void testEqualsAndHashcode()
  {
    EqualsVerifier.forClass(ExpressionPostAggregator.class)
                  .usingGetClass()
                  .withIgnoredFields("macroTable", "finalizers", "parsed", "dependentFields", "cacheKey")
                  .verify();
  }

  @Test
  public void testOutputTypeAndCompute()
  {
    ExpressionPostAggregator postAgg = new ExpressionPostAggregator(
        "p0",
        "x + y",
        null,
        TestExprMacroTable.INSTANCE
    );

    RowSignature signature = RowSignature.builder()
                                         .add("x", ColumnType.LONG)
                                         .add("y", ColumnType.DOUBLE)
                                         .build();

    SettableObjectBinding binding = new SettableObjectBinding().withBinding("x", 2L)
                                                               .withBinding("y", 3.0);

    Assert.assertEquals(ColumnType.DOUBLE, postAgg.getType(signature));

    Assert.assertEquals(5.0, postAgg.compute(binding.asMap()));
  }

  @Test
  public void testNilOutputType()
  {
    ExpressionPostAggregator postAgg = new ExpressionPostAggregator(
        "p0",
        "x + y",
        null,
        TestExprMacroTable.INSTANCE
    );

    RowSignature signature = RowSignature.builder().build();

    // columns not existing in the output signature means they don't exist, so the output is also null
    Assert.assertNull(postAgg.getType(signature));
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
                  new ExpressionPostAggregator("a", "double + float", null, TestExprMacroTable.INSTANCE),
                  new ExpressionPostAggregator("b", "count + count", null, TestExprMacroTable.INSTANCE),
                  new ExpressionPostAggregator("c", "count + double", null, TestExprMacroTable.INSTANCE),
                  new ExpressionPostAggregator("d", "float + float", null, TestExprMacroTable.INSTANCE)
              )
              .build();

    Assert.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add("count", ColumnType.LONG)
                    .add("double", ColumnType.DOUBLE)
                    .add("float", ColumnType.FLOAT)
                    .add("a", ColumnType.DOUBLE)
                    .add("b", ColumnType.LONG)
                    .add("c", ColumnType.DOUBLE)
                    .add("d", ColumnType.DOUBLE) // floats don't exist in expressions
                    .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }
}
