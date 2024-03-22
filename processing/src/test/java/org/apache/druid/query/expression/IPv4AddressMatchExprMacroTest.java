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
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionValidationException;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.math.expr.Parser;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class IPv4AddressMatchExprMacroTest extends MacroTestBase
{
  private static final Expr IPV4 = ExprEval.of("192.168.0.1").toExpr();
  private static final Expr IPV4_LONG = ExprEval.of(3232235521L).toExpr();
  private static final Expr IPV4_UINT = ExprEval.of("3232235521").toExpr();
  private static final Expr IPV4_NETWORK = ExprEval.of("192.168.0.0").toExpr();
  private static final Expr IPV4_BROADCAST = ExprEval.of("192.168.255.255").toExpr();
  private static final Expr IPV6_COMPATIBLE = ExprEval.of("::192.168.0.1").toExpr();
  private static final Expr IPV6_MAPPED = ExprEval.of("::ffff:192.168.0.1").toExpr();
  private static final Expr SUBNET_192_168 = ExprEval.of("192.168.0.0/16").toExpr();
  private static final Expr SUBNET_10 = ExprEval.of("10.0.0.0/8").toExpr();
  private static final Expr NOT_LITERAL = Parser.parse("\"notliteral\"", ExprMacroTable.nil());

  public IPv4AddressMatchExprMacroTest()
  {
    super(new IPv4AddressMatchExprMacro());
  }

  @Test
  public void testTooFewArgs()
  {
    expectException(ExpressionValidationException.class, "requires 2 arguments");

    apply(Collections.emptyList());
  }

  @Test
  public void testTooManyArgs()
  {
    expectException(ExpressionValidationException.class, "requires 2 arguments");

    apply(Arrays.asList(IPV4, SUBNET_192_168, NOT_LITERAL));
  }

  @Test
  public void testSubnetArgNotLiteral()
  {
    expectException(ExpressionValidationException.class, "subnet argument must be a literal");

    apply(Arrays.asList(IPV4, NOT_LITERAL));
  }

  @Test
  public void testSubnetArgInvalid()
  {
    expectException(IllegalArgumentException.class, "subnet arg has an invalid format");

    Expr invalidSubnet = ExprEval.of("192.168.0.1/invalid").toExpr();
    apply(Arrays.asList(IPV4, invalidSubnet));
  }

  @Test
  public void testNullStringArg()
  {
    Expr nullString = ExprEval.of(null).toExpr();
    Assert.assertFalse(eval(nullString, SUBNET_192_168));
  }

  @Test
  public void testNullLongArg()
  {
    Expr nullLong = ExprEval.ofLong(null).toExpr();
    Assert.assertFalse(eval(nullLong, SUBNET_192_168));
  }

  @Test
  public void testInvalidArgType()
  {
    Expr longArray = ExprEval.ofLongArray(new Long[]{1L, 2L}).toExpr();
    Assert.assertFalse(eval(longArray, SUBNET_192_168));
  }

  @Test
  public void testMatchingStringArgIPv4()
  {
    Assert.assertTrue(eval(IPV4, SUBNET_192_168));
  }

  @Test
  public void testNotMatchingStringArgIPv4()
  {
    Assert.assertFalse(eval(IPV4, SUBNET_10));
  }

  @Test
  public void testMatchingStringArgIPv6Mapped()
  {
    Assert.assertFalse(eval(IPV6_MAPPED, SUBNET_192_168));
  }

  @Test
  public void testNotMatchingStringArgIPv6Mapped()
  {
    Assert.assertFalse(eval(IPV6_MAPPED, SUBNET_10));
  }

  @Test
  public void testMatchingStringArgIPv6Compatible()
  {
    Assert.assertFalse(eval(IPV6_COMPATIBLE, SUBNET_192_168));
  }

  @Test
  public void testNotMatchingStringArgIPv6Compatible()
  {
    Assert.assertFalse(eval(IPV6_COMPATIBLE, SUBNET_10));
  }

  @Test
  public void testNotIpAddress()
  {
    Expr notIpAddress = ExprEval.of("druid.apache.org").toExpr();
    Assert.assertFalse(eval(notIpAddress, SUBNET_192_168));
  }

  @Test
  public void testMatchingLongArg()
  {
    Assert.assertTrue(eval(IPV4_LONG, SUBNET_192_168));
  }

  @Test
  public void testNotMatchingLongArg()
  {
    Assert.assertFalse(eval(IPV4_LONG, SUBNET_10));
  }

  @Test
  public void testMatchingStringArgUnsignedInt()
  {
    Assert.assertFalse(eval(IPV4_UINT, SUBNET_192_168));
  }

  @Test
  public void testNotMatchingStringArgUnsignedInt()
  {
    Assert.assertFalse(eval(IPV4_UINT, SUBNET_10));
  }

  @Test
  public void testInclusive()
  {
    Expr subnet = SUBNET_192_168;
    Assert.assertTrue(eval(IPV4_NETWORK, subnet));
    Assert.assertTrue(eval(IPV4, subnet));
    Assert.assertTrue(eval(IPV4_BROADCAST, subnet));
  }

  @Test
  public void testMatchesPrefix()
  {
    Assert.assertTrue(eval(ExprEval.of("192.168.1.250").toExpr(), ExprEval.of("192.168.1.251/31").toExpr()));
    Assert.assertFalse(eval(ExprEval.of("192.168.1.240").toExpr(), ExprEval.of("192.168.1.251/31").toExpr()));
    Assert.assertFalse(eval(ExprEval.of("192.168.1.250").toExpr(), ExprEval.of("192.168.1.251/32").toExpr()));
    Assert.assertTrue(eval(ExprEval.of("192.168.1.251").toExpr(), ExprEval.of("192.168.1.251/32").toExpr()));

    Assert.assertTrue(eval(
        ExprEval.of(IPv4AddressExprUtils.parse("192.168.1.250").longValue()).toExpr(),
        ExprEval.of("192.168.1.251/31").toExpr()
    ));
    Assert.assertFalse(eval(
        ExprEval.of(IPv4AddressExprUtils.parse("192.168.1.240").longValue()).toExpr(),
        ExprEval.of("192.168.1.251/31").toExpr()
    ));
    Assert.assertFalse(eval(
        ExprEval.of(IPv4AddressExprUtils.parse("192.168.1.250").longValue()).toExpr(),
        ExprEval.of("192.168.1.251/32").toExpr()
    ));
    Assert.assertTrue(eval(
        ExprEval.of(IPv4AddressExprUtils.parse("192.168.1.251").longValue()).toExpr(),
        ExprEval.of("192.168.1.251/32").toExpr()
    ));
  }

  private boolean eval(Expr... args)
  {
    Expr expr = apply(Arrays.asList(args));
    ExprEval eval = expr.eval(InputBindings.nilBindings());
    return eval.asBoolean();
  }
}
