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

public class RegexpLikeExprMacroTest extends MacroTestBase
{
  public RegexpLikeExprMacroTest()
  {
    super(new RegexpLikeExprMacro());
  }

  @Test
  public void testErrorZeroArguments()
  {
    expectException(IllegalArgumentException.class, "Function[regexp_like] requires 2 arguments");
    eval("regexp_like()", InputBindings.nilBindings());
  }

  @Test
  public void testErrorThreeArguments()
  {
    expectException(IllegalArgumentException.class, "Function[regexp_like] requires 2 arguments");
    eval("regexp_like('a', 'b', 'c')", InputBindings.nilBindings());
  }

  @Test
  public void testMatch()
  {
    final ExprEval<?> result = eval(
        "regexp_like(a, 'f.o')",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "foo")
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
        "regexp_like(a, 'f.x')",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "foo")
    );
    Assert.assertEquals(
        ExprEval.ofLongBoolean(false).value(),
        result.value()
    );
  }

  @Test
  public void testNullPattern()
  {
    if (NullHandling.sqlCompatible()) {
      expectException(IllegalArgumentException.class, "Function[regexp_like] pattern must be a STRING literal");
    }

    final ExprEval<?> result = eval(
        "regexp_like(a, null)",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "foo")
    );
    Assert.assertEquals(
        ExprEval.ofLongBoolean(true).value(),
        result.value()
    );
  }

  @Test
  public void testEmptyStringPattern()
  {
    final ExprEval<?> result = eval(
        "regexp_like(a, '')",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "foo")
    );
    Assert.assertEquals(
        ExprEval.ofLongBoolean(true).value(),
        result.value()
    );
  }

  @Test
  public void testNullPatternOnEmptyString()
  {
    if (NullHandling.sqlCompatible()) {
      expectException(IllegalArgumentException.class, "Function[regexp_like] pattern must be a STRING literal");
    }

    final ExprEval<?> result = eval(
        "regexp_like(a, null)",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "")
    );
    Assert.assertEquals(
        ExprEval.ofLongBoolean(true).value(),
        result.value()
    );
  }

  @Test
  public void testEmptyStringPatternOnEmptyString()
  {
    final ExprEval<?> result = eval(
        "regexp_like(a, '')",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "")
    );
    Assert.assertEquals(
        ExprEval.ofLongBoolean(true).value(),
        result.value()
    );
  }

  @Test
  public void testNullPatternOnNull()
  {
    if (NullHandling.sqlCompatible()) {
      expectException(IllegalArgumentException.class, "Function[regexp_like] pattern must be a STRING literal");
    }

    final ExprEval<?> result = eval("regexp_like(a, null)", InputBindings.nilBindings());
    Assert.assertEquals(
        ExprEval.ofLongBoolean(true).value(),
        result.value()
    );
  }

  @Test
  public void testEmptyStringPatternOnNull()
  {
    final ExprEval<?> result = eval("regexp_like(a, '')", InputBindings.nilBindings());
    if (NullHandling.sqlCompatible()) {
      Assert.assertNull(result.value());
    } else {
      Assert.assertEquals(ExprEval.ofLongBoolean(true).value(), result.value());
    }
  }
}
