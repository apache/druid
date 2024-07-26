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
import org.apache.druid.math.expr.ExpressionProcessingException;
import org.apache.druid.math.expr.ExpressionValidationException;
import org.apache.druid.math.expr.InputBindings;
import org.junit.Assert;
import org.junit.Test;
 
import java.util.Arrays;
import java.util.Collections;
 
public class IPv6AddressMatchExprMacroTest extends MacroTestBase
{
  private static final Expr IPV6 = ExprEval.of("201:ef:168::").toExpr();
  private static final Expr IPV6_CIDR = ExprEval.of("201:ef:168::/32").toExpr();
 
  public IPv6AddressMatchExprMacroTest()
  {
    super(new IPv6AddressMatchExprMacro());
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
    Expr extraArgument = ExprEval.of("An extra argument").toExpr();
    expectException(ExpressionValidationException.class, "requires 2 arguments");
    apply(Arrays.asList(IPV6, IPV6_CIDR, extraArgument));
  }
 
  @Test
  public void testSubnetArgInvalid()
  {
    expectException(ExpressionProcessingException.class, "Function[ipv6_match] failed to parse address");
    Expr invalidSubnet = ExprEval.of("201:ef:168::/invalid").toExpr();
    apply(Arrays.asList(IPV6, invalidSubnet));
  }

  @Test
  public void testNullStringArg()
  {
    Expr nullString = ExprEval.of(null).toExpr();
    Assert.assertFalse(eval(nullString, IPV6_CIDR));
  }

  @Test
  public void testMatchingStringArgIPv6()
  {
    Assert.assertTrue(eval(IPV6, IPV6_CIDR));
  }
 
  @Test
  public void testNotMatchingStringArgIPv6()
  {
    Expr nonMatchingIpv6 = ExprEval.of("2002:ef:168::").toExpr();
    Assert.assertFalse(eval(nonMatchingIpv6, IPV6_CIDR));
  }
 
  @Test
  public void testNotIpAddress()
  {
    Expr notIpAddress = ExprEval.of("druid.apache.org").toExpr();
    Assert.assertFalse(eval(notIpAddress, IPV6_CIDR));
  }
 
  @Test
  public void testInclusive()
  {
    Expr subnet = IPV6_CIDR;
    Assert.assertTrue(eval(IPV6, subnet));
  }

  private boolean eval(Expr... args)
  {
    Expr expr = apply(Arrays.asList(args));
    ExprEval eval = expr.eval(InputBindings.nilBindings());
    return eval.asBoolean();
  }
}
