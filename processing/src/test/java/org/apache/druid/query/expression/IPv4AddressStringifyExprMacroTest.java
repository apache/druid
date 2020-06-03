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
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class IPv4AddressStringifyExprMacroTest extends MacroTestBase
{
  private static final Expr VALID = ExprEval.of(3232235521L).toExpr();
  private static final String EXPECTED = "192.168.0.1";
  private static final String NULL = NullHandling.replaceWithDefault() ? "0.0.0.0" : null;

  public IPv4AddressStringifyExprMacroTest()
  {
    super(new IPv4AddressStringifyExprMacro());
  }

  @Test
  public void testTooFewArgs()
  {
    expectException(IllegalArgumentException.class, "must have 1 argument");

    apply(Collections.emptyList());
  }

  @Test
  public void testTooManyArgs()
  {
    expectException(IllegalArgumentException.class, "must have 1 argument");

    apply(Arrays.asList(VALID, VALID));
  }

  @Test
  public void testNullLongArg()
  {
    Expr nullNumeric = ExprEval.ofLong(null).toExpr();
    Assert.assertEquals(NULL, eval(nullNumeric));
  }

  @Test
  public void testInvalidArgType()
  {
    Expr longArray = ExprEval.ofLongArray(new Long[]{1L, 2L}).toExpr();
    Assert.assertNull(eval(longArray));
  }

  @Test
  public void testInvalidLongArgTooSmall()
  {
    Expr tooSmall = ExprEval.ofLong(-1L).toExpr();
    Assert.assertNull(eval(tooSmall));
  }

  @Test
  public void testValidLongArgLowest()
  {
    Expr tooSmall = ExprEval.ofLong(0L).toExpr();
    Assert.assertEquals("0.0.0.0", eval(tooSmall));
  }

  @Test
  public void testValidLongArg()
  {
    Assert.assertEquals(EXPECTED, eval(VALID));
  }

  @Test
  public void testValidLongArgHighest()
  {
    Expr tooSmall = ExprEval.ofLong(0xff_ff_ff_ffL).toExpr();
    Assert.assertEquals("255.255.255.255", eval(tooSmall));
  }

  @Test
  public void testInvalidLongArgTooLarge()
  {
    Expr tooLarge = ExprEval.ofLong(0x1_00_00_00_00L).toExpr();
    Assert.assertNull(eval(tooLarge));
  }

  @Test
  public void testNullStringArg()
  {
    Expr nullString = ExprEval.of(null).toExpr();
    Assert.assertNull(NULL, eval(nullString));
  }

  @Test
  public void testInvalidStringArgNotIPAddress()
  {
    Expr notIpAddress = ExprEval.of("druid.apache.org").toExpr();
    Assert.assertNull(eval(notIpAddress));
  }

  @Test
  public void testInvalidStringArgIPv6Compatible()
  {
    Expr ipv6Compatible = ExprEval.of("::192.168.0.1").toExpr();
    Assert.assertNull(eval(ipv6Compatible));
  }

  @Test
  public void testValidStringArgIPv6Mapped()
  {
    Expr ipv6Mapped = ExprEval.of("::ffff:192.168.0.1").toExpr();
    Assert.assertNull(eval(ipv6Mapped));
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
    Assert.assertNull(eval(unsignedInt));
  }

  private Object eval(Expr arg)
  {
    Expr expr = apply(Collections.singletonList(arg));
    ExprEval eval = expr.eval(ExprUtils.nilBindings());
    return eval.value();
  }
}
