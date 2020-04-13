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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class IPv4AddressParseExprMacroTest extends MacroTestBase
{
  private static final Expr VALID = ExprEval.of("192.168.0.1").toExpr();
  private static final long EXPECTED = 3232235521L;
  private static final Long NULL = NullHandling.replaceWithDefault() ? NullHandling.ZERO_LONG : null;

  private IPv4AddressParseExprMacro target;

  @Before
  public void setUp()
  {
    target = new IPv4AddressParseExprMacro();
  }

  @Test
  public void testTooFewArgs()
  {
    expectException(IllegalArgumentException.class, "must have 1 argument");

    target.apply(Collections.emptyList());
  }

  @Test
  public void testTooManyArgs()
  {
    expectException(IllegalArgumentException.class, "must have 1 argument");

    target.apply(Arrays.asList(VALID, VALID));
  }

  @Test
  public void testNullStringArg()
  {
    Expr nullString = ExprEval.of(null).toExpr();
    Assert.assertSame(NULL, eval(nullString));
  }

  @Test
  public void testNullLongArg()
  {
    Expr nullLong = ExprEval.ofLong(null).toExpr();
    Assert.assertEquals(NULL, eval(nullLong));
  }

  @Test
  public void testInvalidArgType()
  {
    Expr longArray = ExprEval.ofLongArray(new Long[]{1L, 2L}).toExpr();
    Assert.assertEquals(NULL, eval(longArray));
  }

  @Test
  public void testInvalidStringArgNotIPAddress()
  {
    Expr notIpAddress = ExprEval.of("druid.apache.org").toExpr();
    Assert.assertEquals(NULL, eval(notIpAddress));
  }

  @Test
  public void testInvalidStringArgIPv6Compatible()
  {
    Expr ipv6Compatible = ExprEval.of("::192.168.0.1").toExpr();
    Assert.assertEquals(NULL, eval(ipv6Compatible));
  }

  @Test
  public void testValidStringArgIPv6Mapped()
  {
    Expr ipv6Mapped = ExprEval.of("::ffff:192.168.0.1").toExpr();
    Assert.assertEquals(NULL, eval(ipv6Mapped));
  }

  @Test
  public void testValidStringArgIPv4()
  {
    Assert.assertEquals(EXPECTED, eval(VALID));
  }

  @Test
  public void testValidStringArgUnsignedInt()
  {
    Expr unsignedInt = ExprEval.of("3232235521").toExpr();
    Assert.assertEquals(NULL, eval(unsignedInt));
  }

  @Test
  public void testInvalidLongArgTooLow()
  {
    Expr tooLow = ExprEval.ofLong(-1L).toExpr();
    Assert.assertEquals(NULL, eval(tooLow));
  }

  @Test
  public void testValidLongArgLowest()
  {
    long lowest = 0L;
    Expr tooLow = ExprEval.ofLong(lowest).toExpr();
    Assert.assertEquals(lowest, eval(tooLow));
  }

  @Test
  public void testValidLongArgHighest()
  {
    long highest = 0xff_ff_ff_ffL;
    Expr tooLow = ExprEval.ofLong(highest).toExpr();
    Assert.assertEquals(highest, eval(tooLow));
  }

  @Test
  public void testInvalidLongArgTooHigh()
  {
    Expr tooHigh = ExprEval.ofLong(0x1_00_00_00_00L).toExpr();
    Assert.assertEquals(NULL, eval(tooHigh));
  }

  @Test
  public void testValidLongArg()
  {
    long value = EXPECTED;
    Expr valid = ExprEval.ofLong(value).toExpr();
    Assert.assertEquals(value, eval(valid));
  }

  private Object eval(Expr arg)
  {
    Expr expr = target.apply(Collections.singletonList(arg));
    ExprEval eval = expr.eval(ExprUtils.nilBindings());
    return eval.value();
  }
}
