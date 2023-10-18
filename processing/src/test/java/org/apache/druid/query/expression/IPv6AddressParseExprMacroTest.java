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
import org.junit.Assert;
import org.junit.Test;
 
import java.util.Arrays;
import java.util.Collections;
 
public class IPv6AddressParseExprMacroTest extends MacroTestBase
{
  private static final String VALID_IPV6_STRING = "2345:e425:2ca1::a567:5673:23b5";
  private static final Expr VALID_IPV6_ADDRESS = ExprEval.of(VALID_IPV6_STRING).toExpr();
 
  public IPv6AddressParseExprMacroTest()
  {
    super(new IPv6AddressParseExprMacro());
  }
 
  @Test
  public void testTooFewArgs()
  {
    expectException(IllegalArgumentException.class, "requires 1 argument");
    apply(Collections.emptyList());
  }
 
  @Test
  public void testTooManyArgs()
  {
    expectException(IllegalArgumentException.class, "requires 1 argument");
    apply(Arrays.asList(VALID_IPV6_ADDRESS, VALID_IPV6_ADDRESS));
  }
 
  @Test
  public void testnullStringArg()
  {
    Expr nullString = ExprEval.of(null).toExpr();
    Assert.assertNull(eval(nullString));
  }
 
  @Test
  public void testInvalidArgType()
  {
    Expr nullLong = ExprEval.ofLong(23L).toExpr();
    Assert.assertNull(eval(nullLong));
  }
 
  @Test
  public void testInvalidStringArgNotIPAddress()
  {
    Expr notIpAddress = ExprEval.of("druid.apache.org").toExpr();
    Assert.assertNull(eval(notIpAddress));
  }

  @Test
  public void testValidStringArgIPv6()
  {
    Assert.assertEquals(VALID_IPV6_STRING, eval(VALID_IPV6_ADDRESS));
  }
 
  @Test
  public void testValidStringArgIPv6LowestAddress()
  {
    String lowestAddressString = "::";
    Expr lowestIpv6Address = ExprEval.of(lowestAddressString).toExpr();
    Assert.assertEquals(lowestAddressString, eval(lowestIpv6Address));
  }
 
  @Test
  public void testValidStringArgIPv6HighestAddress()
  {
    String highestIpv6String = "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff";
    Expr highestIpv6Address = ExprEval.of(highestIpv6String).toExpr();
    Assert.assertEquals(highestIpv6String, eval(highestIpv6Address));
  }
 
  private Object eval(Expr arg)
  {
    Expr expr = apply(Collections.singletonList(arg));
    ExprEval eval = expr.eval(InputBindings.nilBindings());
    return eval.value();
  }
}
