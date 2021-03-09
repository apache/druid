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

import org.apache.druid.com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.Parser;
import org.junit.Assert;
import org.junit.Test;

public class CaseInsensitiveExprMacroTest extends MacroTestBase
{
  public CaseInsensitiveExprMacroTest()
  {
    super(new CaseInsensitiveContainsExprMacro());
  }

  @Test
  public void testErrorZeroArguments()
  {
    expectException(IllegalArgumentException.class, "Function[icontains_string] must have 2 arguments");
    eval("icontains_string()", Parser.withMap(ImmutableMap.of()));
  }

  @Test
  public void testErrorThreeArguments()
  {
    expectException(IllegalArgumentException.class, "Function[icontains_string] must have 2 arguments");
    eval("icontains_string('a', 'b', 'c')", Parser.withMap(ImmutableMap.of()));
  }

  @Test
  public void testMatchSearchLowerCase()
  {
    final ExprEval<?> result = eval("icontains_string(a, 'OBA')", Parser.withMap(ImmutableMap.of("a", "foobar")));
    Assert.assertEquals(
        ExprEval.of(true, ExprType.LONG).value(),
        result.value()
    );
  }

  @Test
  public void testMatchSearchUpperCase()
  {
    final ExprEval<?> result = eval("icontains_string(a, 'oba')", Parser.withMap(ImmutableMap.of("a", "FOOBAR")));
    Assert.assertEquals(
        ExprEval.of(true, ExprType.LONG).value(),
        result.value()
    );
  }

  @Test
  public void testNoMatch()
  {
    final ExprEval<?> result = eval("icontains_string(a, 'bar')", Parser.withMap(ImmutableMap.of("a", "foo")));
    Assert.assertEquals(
        ExprEval.of(false, ExprType.LONG).value(),
        result.value()
    );
  }

  @Test
  public void testNullSearch()
  {
    if (NullHandling.sqlCompatible()) {
      expectException(IllegalArgumentException.class, "Function[icontains_string] substring must be a string literal");
    }

    final ExprEval<?> result = eval("icontains_string(a, null)", Parser.withMap(ImmutableMap.of("a", "foo")));
    Assert.assertEquals(
        ExprEval.of(true, ExprType.LONG).value(),
        result.value()
    );
  }

  @Test
  public void testEmptyStringSearch()
  {
    final ExprEval<?> result = eval("icontains_string(a, '')", Parser.withMap(ImmutableMap.of("a", "foo")));
    Assert.assertEquals(
        ExprEval.of(true, ExprType.LONG).value(),
        result.value()
    );
  }

  @Test
  public void testNullSearchOnEmptyString()
  {
    if (NullHandling.sqlCompatible()) {
      expectException(IllegalArgumentException.class, "Function[icontains_string] substring must be a string literal");
    }

    final ExprEval<?> result = eval("icontains_string(a, null)", Parser.withMap(ImmutableMap.of("a", "")));
    Assert.assertEquals(
        ExprEval.of(true, ExprType.LONG).value(),
        result.value()
    );
  }

  @Test
  public void testEmptyStringSearchOnEmptyString()
  {
    final ExprEval<?> result = eval("icontains_string(a, '')", Parser.withMap(ImmutableMap.of("a", "")));
    Assert.assertEquals(
        ExprEval.of(true, ExprType.LONG).value(),
        result.value()
    );
  }

  @Test
  public void testNullSearchOnNull()
  {
    if (NullHandling.sqlCompatible()) {
      expectException(IllegalArgumentException.class, "Function[icontains_string] substring must be a string literal");
    }

    final ExprEval<?> result = eval(
        "icontains_string(a, null)",
        Parser.withSuppliers(ImmutableMap.of("a", () -> null))
    );
    Assert.assertEquals(
        ExprEval.of(true, ExprType.LONG).value(),
        result.value()
    );
  }

  @Test
  public void testEmptyStringSearchOnNull()
  {
    final ExprEval<?> result = eval("icontains_string(a, '')", Parser.withSuppliers(ImmutableMap.of("a", () -> null)));
    Assert.assertEquals(
        ExprEval.of(!NullHandling.sqlCompatible(), ExprType.LONG).value(),
        result.value()
    );
  }

}
