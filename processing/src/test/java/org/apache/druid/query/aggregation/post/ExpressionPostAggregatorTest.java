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
import com.google.common.collect.ImmutableList;
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
        null,
        TestExprMacroTable.INSTANCE
    );

    Assert.assertEquals(
        postAgg,
        JSON_MAPPER.readValue(JSON_MAPPER.writeValueAsString(postAgg), ExpressionPostAggregator.class)
    );
  }

  @Test
  public void testSerdeOutputType() throws JsonProcessingException
  {
    ExpressionPostAggregator postAgg = new ExpressionPostAggregator(
        "p0",
        "2 + 3",
        null,
        ColumnType.LONG,
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
                  .withIgnoredFields("finalizers", "parsed", "dependentFields", "cacheKey", "partialTypeInformation", "expressionType")
                  .verify();
  }

  @Test
  public void testOutputTypeAndCompute()
  {
    ExpressionPostAggregator postAgg = new ExpressionPostAggregator(
        "p0",
        "x + y",
        null,
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
  public void testExplicitOutputTypeAndCompute()
  {
    ExpressionPostAggregator postAgg = new ExpressionPostAggregator(
        "p0",
        "x + y",
        null,
        ColumnType.FLOAT,
        TestExprMacroTable.INSTANCE
    );

    RowSignature signature = RowSignature.builder()
                                         .add("x", ColumnType.LONG)
                                         .add("y", ColumnType.FLOAT)
                                         .build();

    SettableObjectBinding binding = new SettableObjectBinding().withBinding("x", 2L)
                                                               .withBinding("y", 3.0);

    Assert.assertEquals(ColumnType.FLOAT, postAgg.getType(signature));

    Assert.assertEquals(5.0f, postAgg.compute(binding.asMap()));
  }

  @Test
  public void testExplicitOutputTypeAndComputeComparison()
  {
    ExpressionPostAggregator postAgg = new ExpressionPostAggregator(
        "p0",
        "array(x, y)",
        null,
        ColumnType.LONG_ARRAY,
        TestExprMacroTable.INSTANCE
    );

    RowSignature signature = RowSignature.builder()
                                         .add("x", ColumnType.LONG)
                                         .add("y", ColumnType.LONG)
                                         .build();

    SettableObjectBinding binding = new SettableObjectBinding().withBinding("x", 2L)
                                                               .withBinding("y", 3L);

    SettableObjectBinding binding2 = new SettableObjectBinding().withBinding("x", 3L)
                                                                .withBinding("y", 4L);

    Assert.assertEquals(ColumnType.LONG_ARRAY, postAgg.getType(signature));

    Assert.assertArrayEquals(new Object[]{2L, 3L}, (Object[]) postAgg.compute(binding.asMap()));
    Assert.assertArrayEquals(new Object[]{3L, 4L}, (Object[]) postAgg.compute(binding2.asMap()));

    Assert.assertEquals(
        -1,
        postAgg.getComparator().compare(postAgg.compute(binding.asMap()), postAgg.compute(binding2.asMap()))
    );
  }

  @Test
  public void testExplicitOutputTypeAndComputeArrayNoType()
  {
    ExpressionPostAggregator postAgg = new ExpressionPostAggregator(
        "p0",
        "array(x, y)",
        null,
        null,
        TestExprMacroTable.INSTANCE
    );

    RowSignature signature = RowSignature.builder()
                                         .add("x", ColumnType.STRING)
                                         .add("y", ColumnType.STRING)
                                         .build();

    SettableObjectBinding binding = new SettableObjectBinding().withBinding("x", "abc")
                                                               .withBinding("y", "def");

    Assert.assertEquals(ColumnType.STRING_ARRAY, postAgg.getType(signature));

    Assert.assertArrayEquals(new Object[]{"abc", "def"}, (Object[]) postAgg.compute(binding.asMap()));

    SettableObjectBinding binding2 = new SettableObjectBinding().withBinding("x", "abc")
                                                                .withBinding("y", "abc");

    // ordering by arrays doesn't work if no outputType is specified...
    Assert.assertThrows(
        ClassCastException.class,
        () -> postAgg.getComparator().compare(postAgg.compute(binding.asMap()), postAgg.compute(binding2.asMap()))
    );
  }

  @Test
  public void testExplicitOutputTypeAndComputeMultiValueDimension()
  {
    ExpressionPostAggregator postAgg = new ExpressionPostAggregator(
        "p0",
        "array(x, y)",
        null,
        ColumnType.STRING,
        TestExprMacroTable.INSTANCE
    );

    RowSignature signature = RowSignature.builder()
                                         .add("x", ColumnType.STRING)
                                         .add("y", ColumnType.STRING)
                                         .build();

    SettableObjectBinding binding = new SettableObjectBinding().withBinding("x", "abc")
                                                               .withBinding("y", "def");

    Assert.assertEquals(ColumnType.STRING, postAgg.getType(signature));

    Assert.assertEquals(ImmutableList.of("abc", "def"), postAgg.compute(binding.asMap()));
  }

  @Test
  public void testExplicitOutputTypeAndComputeMultiValueDimensionWithSingleElement()
  {
    ExpressionPostAggregator postAgg = new ExpressionPostAggregator(
        "p0",
        "array(x)",
        null,
        ColumnType.STRING,
        TestExprMacroTable.INSTANCE
    );

    RowSignature signature = RowSignature.builder()
                                         .add("x", ColumnType.STRING)
                                         .build();

    SettableObjectBinding binding = new SettableObjectBinding().withBinding("x", "abc");

    Assert.assertEquals(ColumnType.STRING, postAgg.getType(signature));

    Assert.assertEquals("abc", postAgg.compute(binding.asMap()));
  }

  @Test
  public void testNilOutputType()
  {
    ExpressionPostAggregator postAgg = new ExpressionPostAggregator(
        "p0",
        "x + y",
        null,
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
                  new ExpressionPostAggregator("a", "double + float", null, null, TestExprMacroTable.INSTANCE),
                  new ExpressionPostAggregator("b", "count + count", null, null, TestExprMacroTable.INSTANCE),
                  new ExpressionPostAggregator("c", "count + double", null, null, TestExprMacroTable.INSTANCE),
                  new ExpressionPostAggregator("d", "float + float", null, null, TestExprMacroTable.INSTANCE),
                  new ExpressionPostAggregator("e", "float + float", null, ColumnType.FLOAT, TestExprMacroTable.INSTANCE)
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
                    .add("e", ColumnType.FLOAT) // but can be explicitly specified
                    .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }
}
