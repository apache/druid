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

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ExpressionLambdaAggregatorTest extends InitializedNullHandlingTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testEstimateString()
  {
    ExpressionLambdaAggregator.estimateAndCheckMaxBytes(ExprEval.ofType(ExpressionType.STRING, "hello"), 10);
  }

  @Test
  public void testEstimateStringTooBig()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage("Unable to serialize [STRING], size [12] is larger than max [5]");
    ExpressionLambdaAggregator.estimateAndCheckMaxBytes(ExprEval.ofType(ExpressionType.STRING, "too big"), 5);
  }

  @Test
  public void testEstimateStringArray()
  {
    ExpressionLambdaAggregator.estimateAndCheckMaxBytes(
        ExprEval.ofType(ExpressionType.STRING_ARRAY, new Object[] {"a", "b", "c", "d"}),
        30
    );
  }

  @Test
  public void testEstimateStringArrayTooBig()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage("Unable to serialize [ARRAY<STRING>], size [25] is larger than max [15]");
    ExpressionLambdaAggregator.estimateAndCheckMaxBytes(
        ExprEval.ofType(ExpressionType.STRING_ARRAY, new Object[] {"a", "b", "c", "d"}),
        15
    );
  }

  @Test
  public void testEstimateLongArray()
  {
    ExpressionLambdaAggregator.estimateAndCheckMaxBytes(
        ExprEval.ofType(ExpressionType.LONG_ARRAY, new Object[] {1L, 2L, 3L, 4L}),
        64
    );
  }

  @Test
  public void testEstimateLongArrayTooBig()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage("Unable to serialize [ARRAY<LONG>], size [41] is larger than max [24]");
    ExpressionLambdaAggregator.estimateAndCheckMaxBytes(
        ExprEval.ofType(ExpressionType.LONG_ARRAY, new Object[] {1L, 2L, 3L, 4L}),
        24
    );
  }

  @Test
  public void testEstimateDoubleArray()
  {
    ExpressionLambdaAggregator.estimateAndCheckMaxBytes(
        ExprEval.ofType(ExpressionType.DOUBLE_ARRAY, new Object[] {1.0, 2.0, 3.0, 4.0}),
        64
    );
  }

  @Test
  public void testEstimateDoubleArrayTooBig()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage("Unable to serialize [ARRAY<DOUBLE>], size [41] is larger than max [24]");
    ExpressionLambdaAggregator.estimateAndCheckMaxBytes(
        ExprEval.ofType(ExpressionType.DOUBLE_ARRAY, new Object[] {1.0, 2.0, 3.0, 4.0}),
        24
    );
  }
}
