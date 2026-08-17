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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.InputBindings;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RegexpReplaceExprMacroTest extends MacroTestBase
{
  public RegexpReplaceExprMacroTest()
  {
    super(new RegexpReplaceExprMacro());
  }

  @Test
  public void testErrorZeroArguments()
  {
    assertException(
        IllegalArgumentException.class,
        "Function[regexp_replace] requires 3 arguments",
        () -> eval("regexp_replace()", InputBindings.nilBindings())
    );
  }

  @Test
  public void testInvalidRegexpReplacePattern()
  {
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            DruidException.class,
            () -> eval("regexp_replace(a, '[Ab-cd-0]', 'xyz')", InputBindings.nilBindings())),
        DruidExceptionMatcher.invalidInput().expectMessageContains(
            "An invalid pattern [[Ab-cd-0]] was provided for the [regexp_replace] function,"
            + " error: [Illegal character range near index 7"
        )
    );
  }

  @Test
  public void testErrorFourArguments()
  {
    assertException(
        IllegalArgumentException.class,
        "Function[regexp_replace] requires 3 arguments",
        () -> eval("regexp_replace('a', 'b', 'c', 'd')", InputBindings.nilBindings())
    );
  }

  @Test
  public void testErrorNonStringPattern()
  {
    assertException(
        IllegalArgumentException.class,
        "Function[regexp_replace] pattern must be a string literal",
        () -> eval(
            "regexp_replace(a, 1, 'x')",
            InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "foo")
        )
    );
  }

  @Test
  public void testErrorNonStringReplacement()
  {
    assertException(
        IllegalArgumentException.class,
        "Function[regexp_replace] replacement must be a string literal",
        () -> eval(
            "regexp_replace(a, 'x', 1)",
            InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "foo")
        )
    );
  }

  @Test
  public void testNullPattern()
  {
    final ExprEval<?> result = eval(
        "regexp_replace(a, null, 'x')",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "foo")
    );

    Assertions.assertNull(result.value());
  }

  @Test
  public void testNoMatch()
  {
    final ExprEval<?> result = eval(
        "regexp_replace(a, 'f.x', 'beep')",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "foo")
    );
    Assertions.assertEquals("foo", result.value());
  }

  @Test
  public void testEmptyStringPattern()
  {
    final ExprEval<?> result = eval(
        "regexp_replace(a, '', 'x')",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "foo")
    );
    Assertions.assertEquals("xfxoxox", result.value());
  }

  @Test
  public void testMultiLinePattern()
  {
    final ExprEval<?> result = eval(
        "regexp_replace(a, '^foo\\\\nbar$', 'xxx')",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "foo\nbar")
    );
    Assertions.assertEquals("xxx", result.value());
  }

  @Test
  public void testMultiLinePatternNoMatch()
  {
    final ExprEval<?> result = eval(
        "regexp_replace(a, '^foo\\\\nbar$', 'xxx')",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "foo\nbarz")
    );
    Assertions.assertEquals("foo\nbarz", result.value());
  }

  @Test
  public void testNullPatternOnEmptyString()
  {
    final ExprEval<?> result = eval(
        "regexp_replace(a, null, 'x')",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "")
    );

    Assertions.assertNull(result.value());
  }

  @Test
  public void testEmptyStringPatternOnEmptyString()
  {
    final ExprEval<?> result = eval(
        "regexp_replace(a, '', 'x')",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "")
    );
    Assertions.assertEquals("x", result.value());
  }

  @Test
  public void testEmptyStringPatternOnEmptyStringDynamic()
  {
    final ExprEval<?> result = eval(
        "regexp_replace(a, pattern, replacement)",
        InputBindings.forInputSuppliers(
            ImmutableMap.of(
                "a", InputBindings.inputSupplier(ExpressionType.STRING, () -> ""),
                "pattern", InputBindings.inputSupplier(ExpressionType.STRING, () -> ""),
                "replacement", InputBindings.inputSupplier(ExpressionType.STRING, () -> "x")
            )
        )
    );
    Assertions.assertEquals("x", result.value());
  }

  @Test
  public void testNullPatternOnNull()
  {
    final ExprEval<?> result = eval("regexp_replace(a, null, 'x')", InputBindings.nilBindings());

    Assertions.assertNull(result.value());
  }

  @Test
  public void testNullPatternOnNullDynamic()
  {
    final ExprEval<?> result = eval(
        "regexp_replace(a, pattern, replacement)",
        InputBindings.forInputSuppliers(
            ImmutableMap.of("replacement", InputBindings.inputSupplier(ExpressionType.STRING, () -> "x"))
        )
    );

    Assertions.assertNull(result.value());
  }

  @Test
  public void testEmptyStringPatternOnNull()
  {
    final ExprEval<?> result = eval("regexp_replace(a, '', 'x')", InputBindings.nilBindings());

    Assertions.assertNull(result.value());
  }

  @Test
  public void testUrlIdReplacement()
  {
    final ExprEval<?> result = eval(
        "regexp_replace(regexp_replace(a, '\\\\?(.*)$', ''), '/(\\\\w+)(?=/|$)', '/*')",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "http://example.com/path/to?query")
    );

    Assertions.assertEquals("http://example.com/*/*", result.value());
  }

  @Test
  public void testUrlIdReplacementDynamic()
  {
    final ExprEval<?> result = eval(
        "regexp_replace(regexp_replace(a, pattern1, replacement1), pattern2, replacement2)",
        InputBindings.forInputSuppliers(
            ImmutableMap
                .<String, InputBindings.InputSupplier<?>>builder()
                .put("a", InputBindings.inputSupplier(ExpressionType.STRING, () -> "http://example.com/path/to?query"))
                .put("pattern1", InputBindings.inputSupplier(ExpressionType.STRING, () -> "\\?(.*)$"))
                .put("pattern2", InputBindings.inputSupplier(ExpressionType.STRING, () -> "/(\\w+)(?=/|$)"))
                .put("replacement1", InputBindings.inputSupplier(ExpressionType.STRING, () -> ""))
                .put("replacement2", InputBindings.inputSupplier(ExpressionType.STRING, () -> "/*"))
                .build()
        )
    );

    Assertions.assertEquals("http://example.com/*/*", result.value());
  }
}
