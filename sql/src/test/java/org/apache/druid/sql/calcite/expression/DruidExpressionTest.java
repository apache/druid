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

package org.apache.druid.sql.calcite.expression;

import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

public class DruidExpressionTest extends InitializedNullHandlingTest
{
  @Test
  public void test_doubleLiteral_asString()
  {
    Assert.assertEquals("0.0", DruidExpression.doubleLiteral(0));
    Assert.assertEquals("-2.0", DruidExpression.doubleLiteral(-2));
    Assert.assertEquals("2.0", DruidExpression.doubleLiteral(2));
    Assert.assertEquals("2.1", DruidExpression.doubleLiteral(2.1));
    Assert.assertEquals("2.12345678", DruidExpression.doubleLiteral(2.12345678));
    Assert.assertEquals("2.2E122", DruidExpression.doubleLiteral(2.2e122));
    Assert.assertEquals("NaN", DruidExpression.doubleLiteral(Double.NaN));
    Assert.assertEquals("Infinity", DruidExpression.doubleLiteral(Double.POSITIVE_INFINITY));
    Assert.assertEquals("-Infinity", DruidExpression.doubleLiteral(Double.NEGATIVE_INFINITY));
    //CHECKSTYLE.OFF: Regexp
    // Min/max double are banned by regexp due to often being inappropriate; but they are appropriate here.
    Assert.assertEquals("4.9E-324", DruidExpression.doubleLiteral(Double.MIN_VALUE));
    Assert.assertEquals("1.7976931348623157E308", DruidExpression.doubleLiteral(Double.MAX_VALUE));
    //CHECKSTYLE.ON: Regexp
    Assert.assertEquals("2.2250738585072014E-308", DruidExpression.doubleLiteral(Double.MIN_NORMAL));
  }

  @Test
  public void test_doubleLiteral_roundTrip()
  {
    final double[] doubles = {
        0,
        -2,
        2,
        2.1,
        2.12345678,
        2.2e122,
        Double.NaN,
        Double.POSITIVE_INFINITY,
        Double.NEGATIVE_INFINITY,
        //CHECKSTYLE.OFF: Regexp
        // Min/max double are banned by regexp due to often being inappropriate; but they are appropriate here.
        Double.MIN_VALUE,
        Double.MAX_VALUE,
        //CHECKSTYLE.ON: Regexp
        Double.MIN_NORMAL
    };

    for (double n : doubles) {
      final Expr expr = Parser.parse(DruidExpression.doubleLiteral(n), ExprMacroTable.nil());
      Assert.assertTrue(expr.isLiteral());
      MatcherAssert.assertThat(expr.getLiteralValue(), CoreMatchers.instanceOf(Double.class));
      Assert.assertEquals(n, (double) expr.getLiteralValue(), 0d);
    }
  }

  @Test
  public void test_longLiteral_asString()
  {
    Assert.assertEquals("0", DruidExpression.longLiteral(0));
    Assert.assertEquals("-2", DruidExpression.longLiteral(-2));
    Assert.assertEquals("2", DruidExpression.longLiteral(2));
    Assert.assertEquals("9223372036854775807", DruidExpression.longLiteral(Long.MAX_VALUE));
    Assert.assertEquals("-9223372036854775808", DruidExpression.longLiteral(Long.MIN_VALUE));
  }

  @Test
  public void longLiteral_roundTrip()
  {
    final long[] longs = {
        0,
        -2,
        2,
        Long.MAX_VALUE,
        Long.MIN_VALUE
    };

    for (long n : longs) {
      final Expr expr = Parser.parse(DruidExpression.longLiteral(n), ExprMacroTable.nil());
      Assert.assertTrue(expr.isLiteral());
      MatcherAssert.assertThat(expr.getLiteralValue(), CoreMatchers.instanceOf(Number.class));
      Assert.assertEquals(n, ((Number) expr.getLiteralValue()).longValue());
    }
  }
}
