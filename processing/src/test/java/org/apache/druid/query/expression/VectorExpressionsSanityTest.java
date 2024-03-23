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

package org.apache.druid.query.expression;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.VectorExprSanityTest;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.Map;

/**
 * randomize inputs to various vector expressions and make sure the results match nonvectorized expressions
 * <p>
 * this is not a replacement for correctness tests, but will ensure that vectorized and non-vectorized expression
 * evaluation is at least self consistent...
 */
public class VectorExpressionsSanityTest extends InitializedNullHandlingTest
{
  private static final Logger log = new Logger(VectorExpressionsSanityTest.class);
  private static final int NUM_ITERATIONS = 10;
  private static final int VECTOR_SIZE = 512;
  private static final TimestampShiftExprMacro TIMESTAMP_SHIFT_EXPR_MACRO = new TimestampShiftExprMacro();
  private static final DateTime DATE_TIME = DateTimes.of("2020-11-05T04:05:06");

  final Map<String, ExpressionType> types = ImmutableMap.<String, ExpressionType>builder()
                                                        .put("l1", ExpressionType.LONG)
                                                        .put("l2", ExpressionType.LONG)
                                                        .put("d1", ExpressionType.DOUBLE)
                                                        .put("d2", ExpressionType.DOUBLE)
                                                        .put("s1", ExpressionType.STRING)
                                                        .put("s2", ExpressionType.STRING)
                                                        .put("boolString1", ExpressionType.STRING)
                                                        .put("boolString2", ExpressionType.STRING)
                                                        .build();

  static void testExpression(String expr, Expr parsed, Map<String, ExpressionType> types)
  {
    log.debug("[%s]", expr);
    VectorExprSanityTest.testExpression(expr, parsed, types, NUM_ITERATIONS);
    VectorExprSanityTest.testSequentialBinding(expr, parsed, types);
  }

  @Test
  public void testTimeShiftFn()
  {
    int step = 1;
    Expr parsed = TIMESTAMP_SHIFT_EXPR_MACRO.apply(
        ImmutableList.of(
            ExprEval.of(DATE_TIME.getMillis()).toExpr(),
            ExprEval.of("P1M").toExpr(),
            ExprEval.of(step).toExpr()
        ));
    testExpression("time_shift(l1, 'P1M', 1)", parsed, types);
  }
}

