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
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.InputBindings;
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
    expectException(IllegalArgumentException.class, "Function[icontains_string] requires 2 arguments");
    eval("icontains_string()", InputBindings.nilBindings());
  }

  @Test
  public void testErrorThreeArguments()
  {
    expectException(IllegalArgumentException.class, "Function[icontains_string] requires 2 arguments");
    eval("icontains_string('a', 'b', 'c')", InputBindings.nilBindings());
  }

  @Test
  public void testMatchSearchLowerCase()
  {
    final ExprEval<?> result = eval(
        "icontains_string(a, 'OBA')",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "foobar")
    );
    Assert.assertEquals(
        ExprEval.ofLongBoolean(true).value(),
        result.value()
    );
  }

  @Test
  public void testMatchSearchUpperCase()
  {
    final ExprEval<?> result = eval(
        "icontains_string(a, 'oba')",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "FOOBAR")
    );
    Assert.assertEquals(
        ExprEval.ofLongBoolean(true).value(),
        result.value()
    );
  }

  @Test
  public void testNoMatch()
  {
    final ExprEval<?> result = eval(
        "icontains_string(a, 'bar')",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "foo")
    );
    Assert.assertEquals(
        ExprEval.ofLongBoolean(false).value(),
        result.value()
    );
  }

  @Test
  public void testNullSearch()
  {
    if (NullHandling.sqlCompatible()) {
      expectException(IllegalArgumentException.class, "Function[icontains_string] substring must be a string literal");
    }

    final ExprEval<?> result = eval(
        "icontains_string(a, null)",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "foo")
    );
    Assert.assertEquals(
        ExprEval.ofLongBoolean(true).value(),
        result.value()
    );
  }

  @Test
  public void testEmptyStringSearch()
  {
    final ExprEval<?> result = eval(
        "icontains_string(a, '')",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "foo")
    );
    Assert.assertEquals(
        ExprEval.ofLongBoolean(true).value(),
        result.value()
    );
  }

  @Test
  public void testNullSearchOnEmptyString()
  {
    if (NullHandling.sqlCompatible()) {
      expectException(IllegalArgumentException.class, "Function[icontains_string] substring must be a string literal");
    }

    final ExprEval<?> result = eval(
        "icontains_string(a, null)",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "")
    );
    Assert.assertEquals(
        ExprEval.ofLongBoolean(true).value(),
        result.value()
    );
  }

  @Test
  public void testEmptyStringSearchOnEmptyString()
  {
    final ExprEval<?> result = eval(
        "icontains_string(a, '')",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "")
    );
    Assert.assertEquals(
        ExprEval.ofLongBoolean(true).value(),
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
        InputBindings.nilBindings()
    );
    Assert.assertEquals(
        ExprEval.ofLongBoolean(true).value(),
        result.value()
    );
  }

  @Test
  public void testEmptyStringSearchOnNull()
  {
    ExprEval<?> result = eval("icontains_string(a, '')", InputBindings.nilBindings());
    if (NullHandling.sqlCompatible()) {
      Assert.assertNull(result.value());
    } else {
      Assert.assertEquals(ExprEval.ofLongBoolean(true).value(), result.value());
    }
  }
}
