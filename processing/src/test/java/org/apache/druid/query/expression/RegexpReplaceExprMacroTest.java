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

public class RegexpReplaceExprMacroTest extends MacroTestBase
{
  public RegexpReplaceExprMacroTest()
  {
    super(new RegexpReplaceExprMacro());
  }

  @Test
  public void testErrorZeroArguments()
  {
    expectException(IllegalArgumentException.class, "Function[regexp_replace] requires 3 arguments");
    eval("regexp_replace()", InputBindings.nilBindings());
  }

  @Test
  public void testErrorFourArguments()
  {
    expectException(IllegalArgumentException.class, "Function[regexp_replace] requires 3 arguments");
    eval("regexp_replace('a', 'b', 'c', 'd')", InputBindings.nilBindings());
  }

  @Test
  public void testErrorNonStringPattern()
  {
    expectException(IllegalArgumentException.class, "Function[regexp_replace] pattern must be a string literal");
    eval(
        "regexp_replace(a, 1, 'x')",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "foo")
    );
  }

  @Test
  public void testErrorNullPattern()
  {
    if (NullHandling.sqlCompatible()) {
      expectException(
          IllegalArgumentException.class,
          "Function[regexp_replace] pattern must be a nonnull string literal"
      );
    }

    final ExprEval<?> result = eval(
        "regexp_replace(a, null, 'x')",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "foo")
    );

    // SQL-compat should have thrown an error by now.
    Assert.assertTrue(NullHandling.replaceWithDefault());
    Assert.assertEquals("xfxoxox", result.value());
  }

  @Test
  public void testNoMatch()
  {
    final ExprEval<?> result = eval(
        "regexp_replace(a, 'f.x', 'beep')",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "foo")
    );
    Assert.assertEquals("foo", result.value());
  }

  @Test
  public void testEmptyStringPattern()
  {
    final ExprEval<?> result = eval(
        "regexp_replace(a, '', 'x')",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "foo")
    );
    Assert.assertEquals("xfxoxox", result.value());
  }

  @Test
  public void testMultiLinePattern()
  {
    final ExprEval<?> result = eval(
        "regexp_replace(a, '^foo\\\\nbar$', 'xxx')",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "foo\nbar")
    );
    Assert.assertEquals("xxx", result.value());
  }

  @Test
  public void testMultiLinePatternNoMatch()
  {
    final ExprEval<?> result = eval(
        "regexp_replace(a, '^foo\\\\nbar$', 'xxx')",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "foo\nbarz")
    );
    Assert.assertEquals("foo\nbarz", result.value());
  }

  @Test
  public void testNullPatternOnEmptyString()
  {
    if (NullHandling.sqlCompatible()) {
      expectException(IllegalArgumentException.class, "Function[regexp_replace] pattern must be a STRING literal");
    }

    final ExprEval<?> result = eval(
        "regexp_replace(a, null, 'x')",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "")
    );

    // SQL-compat should have thrown an error by now.
    Assert.assertTrue(NullHandling.replaceWithDefault());
    Assert.assertEquals("x", result.value());
  }

  @Test
  public void testEmptyStringPatternOnEmptyString()
  {
    final ExprEval<?> result = eval(
        "regexp_replace(a, '', 'x')",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "")
    );
    Assert.assertEquals("x", result.value());
  }

  @Test
  public void testNullPatternOnNull()
  {
    if (NullHandling.sqlCompatible()) {
      expectException(
          IllegalArgumentException.class,
          "Function[regexp_replace] pattern must be a nonnull string literal"
      );
    }

    final ExprEval<?> result = eval("regexp_replace(a, null, 'x')", InputBindings.nilBindings());

    // SQL-compat should have thrown an error by now.
    Assert.assertTrue(NullHandling.replaceWithDefault());
    Assert.assertEquals("x", result.value());
  }

  @Test
  public void testEmptyStringPatternOnNull()
  {
    final ExprEval<?> result = eval("regexp_replace(a, '', 'x')", InputBindings.nilBindings());

    if (NullHandling.sqlCompatible()) {
      Assert.assertNull(result.value());
    } else {
      Assert.assertEquals("x", result.value());
    }
  }

  @Test
  public void testUrlIdReplacement()
  {
    final ExprEval<?> result = eval(
        "regexp_replace(regexp_replace(a, '\\\\?(.*)$', ''), '/(\\\\w+)(?=/|$)', '/*')",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "http://example.com/path/to?query")
    );

    if (NullHandling.sqlCompatible()) {
      Assert.assertNull(result.value());
    } else {
      Assert.assertEquals("http://example.com/*/*", result.value());
    }
  }
}
