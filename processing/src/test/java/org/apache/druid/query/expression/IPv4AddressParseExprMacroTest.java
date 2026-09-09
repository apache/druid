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

import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.InputBindings;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

public class IPv4AddressParseExprMacroTest extends MacroTestBase
{
  private static final Expr VALID = ExprEval.ofString("192.168.0.1").toExpr();
  private static final long EXPECTED = 3232235521L;

  public IPv4AddressParseExprMacroTest()
  {
    super(new IPv4AddressParseExprMacro());
  }

  @Test
  public void testTooFewArgs()
  {
    assertException(
        IllegalArgumentException.class,
        "requires 1 argument",
        () -> apply(Collections.emptyList())
    );
  }

  @Test
  public void testTooManyArgs()
  {
    assertException(
        IllegalArgumentException.class,
        "requires 1 argument",
        () -> apply(Arrays.asList(VALID, VALID))
    );
  }

  @Test
  public void testnullStringArg()
  {
    Expr nullString = ExprEval.ofString(null).toExpr();
    Assertions.assertNull(eval(nullString));
  }

  @Test
  public void testnullLongArg()
  {
    Expr nullLong = ExprEval.ofLong(null).toExpr();
    Assertions.assertNull(eval(nullLong));
  }

  @Test
  public void testInvalidArgType()
  {
    Expr longArray = ExprEval.ofLongArray(new Long[]{1L, 2L}).toExpr();
    Assertions.assertNull(eval(longArray));
  }

  @Test
  public void testInvalidStringArgNotIPAddress()
  {
    Expr notIpAddress = ExprEval.ofString("druid.apache.org").toExpr();
    Assertions.assertNull(eval(notIpAddress));
  }

  @Test
  public void testInvalidStringArgIPv6Compatible()
  {
    Expr ipv6Compatible = ExprEval.ofString("::192.168.0.1").toExpr();
    Assertions.assertNull(eval(ipv6Compatible));
  }

  @Test
  public void testValidStringArgIPv6Mapped()
  {
    Expr ipv6Mapped = ExprEval.ofString("::ffff:192.168.0.1").toExpr();
    Assertions.assertNull(eval(ipv6Mapped));
  }

  @Test
  public void testValidStringArgIPv4()
  {
    Assertions.assertEquals(EXPECTED, eval(VALID));
  }

  @Test
  public void testValidStringArgUnsignedInt()
  {
    Expr unsignedInt = ExprEval.ofString("3232235521").toExpr();
    Assertions.assertNull(eval(unsignedInt));
  }

  @Test
  public void testInvalidLongArgTooLow()
  {
    Expr tooLow = ExprEval.ofLong(-1L).toExpr();
    Assertions.assertNull(eval(tooLow));
  }

  @Test
  public void testValidLongArgLowest()
  {
    long lowest = 0L;
    Expr tooLow = ExprEval.ofLong(lowest).toExpr();
    Assertions.assertEquals(lowest, eval(tooLow));
  }

  @Test
  public void testValidLongArgHighest()
  {
    long highest = 0xff_ff_ff_ffL;
    Expr tooLow = ExprEval.ofLong(highest).toExpr();
    Assertions.assertEquals(highest, eval(tooLow));
  }

  @Test
  public void testInvalidLongArgTooHigh()
  {
    Expr tooHigh = ExprEval.ofLong(0x1_00_00_00_00L).toExpr();
    Assertions.assertNull(eval(tooHigh));
  }

  @Test
  public void testValidLongArg()
  {
    long value = EXPECTED;
    Expr valid = ExprEval.ofLong(value).toExpr();
    Assertions.assertEquals(value, eval(valid));
  }

  private Object eval(Expr arg)
  {
    Expr expr = apply(Collections.singletonList(arg));
    ExprEval eval = expr.eval(InputBindings.nilBindings());
    return eval.value();
  }
}
