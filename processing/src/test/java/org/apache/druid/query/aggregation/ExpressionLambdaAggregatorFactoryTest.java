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

package org.apache.druid.query.aggregation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class ExpressionLambdaAggregatorFactoryTest
{
  private static ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testSerde() throws IOException
  {
    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("some_column", "some_other_column"),
        "customAccumulator",
        "0.0",
        "<DOUBLE>[]",
        "customAccumulator + some_column + some_other_column",
        "customAccumulator + expr_agg_name",
        "if (o1 > o2, if (o1 == o2, 0, 1), -1)",
        "o + 100",
        2048,
        TestExprMacroTable.INSTANCE
    );

    Assert.assertEquals(agg, MAPPER.readValue(MAPPER.writeValueAsBytes(agg), ExpressionLambdaAggregatorFactory.class));
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(ExpressionLambdaAggregatorFactory.class)
                  .usingGetClass()
                  .withIgnoredFields(
                      "macroTable",
                      "initialValue",
                      "initialCombineValue",
                      "foldExpression",
                      "combineExpression",
                      "compareExpression",
                      "finalizeExpression"
                  )
                  .verify();
  }
}
