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

import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.InputBindings;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RegexpExtractExprMacroTest extends MacroTestBase
{
  public RegexpExtractExprMacroTest()
  {
    super(new RegexpExtractExprMacro());
  }

  @Test
  public void testErrorZeroArguments()
  {
    assertException(
        IllegalArgumentException.class,
        "Function[regexp_extract] requires 2 or 3 arguments",
        () -> eval("regexp_extract()", InputBindings.nilBindings())
    );
  }

  @Test
  public void testErrorFourArguments()
  {
    assertException(
        IllegalArgumentException.class,
        "Function[regexp_extract] requires 2 or 3 arguments",
        () -> eval("regexp_extract('a', 'b', 'c', 'd')", InputBindings.nilBindings())
    );
  }

  @Test
  public void testInvalidRegexpExtractPattern()
  {
    MatcherAssert.assertThat(
        Assertions.assertThrows(DruidException.class, () ->
            eval(
                "regexp_extract('pod-1234-node', '[ab-0-9]')",
                InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "foo")
            )
        ),
        DruidExceptionMatcher.invalidInput().expectMessageContains(
            "An invalid pattern [[ab-0-9]] was provided for the [regexp_extract] function,"
            + " error: [Illegal character range near index 4"
        )
    );
  }

  @Test
  public void testMatch()
  {
    final ExprEval<?> result = eval(
        "regexp_extract(a, 'f(.o)')",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "foo")
    );
    Assertions.assertEquals("foo", result.value());
  }

  @Test
  public void testMatchGroup0()
  {
    final ExprEval<?> result = eval(
        "regexp_extract(a, 'f(.o)', 0)",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "foo")
    );
    Assertions.assertEquals("foo", result.value());
  }

  @Test
  public void testMatchGroup1()
  {
    final ExprEval<?> result = eval(
        "regexp_extract(a, 'f(.o)', 1)",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "foo")
    );
    Assertions.assertEquals("oo", result.value());
  }

  @Test
  public void testMatchGroup2()
  {
    Throwable t = Assertions.assertThrows(
        IndexOutOfBoundsException.class,
        () -> eval(
            "regexp_extract(a, 'f(.o)', 2)",
            InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "foo")
        )
    );
    Assertions.assertEquals("No group 2", t.getMessage());
  }

  @Test
  public void testNoMatch()
  {
    final ExprEval<?> result = eval(
        "regexp_extract(a, 'f(.x)')",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "foo")
    );
    Assertions.assertNull(result.value());
  }

  @Test
  public void testMatchInMiddle()
  {
    final ExprEval<?> result = eval(
        "regexp_extract(a, '.o$')",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "foo")
    );
    Assertions.assertEquals("oo", result.value());
  }

  @Test
  public void testNullPattern()
  {
    assertException(
        IllegalArgumentException.class,
        "Function[regexp_extract] pattern must be a string literal",
        () -> eval(
            "regexp_extract(a, null)",
            InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "foo")
        )
    );
  }

  @Test
  public void testEmptyStringPattern()
  {
    final ExprEval<?> result = eval(
        "regexp_extract(a, '')",
        InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "foo")
    );
    Assertions.assertEquals("", result.value());
  }

  @Test
  public void testNumericPattern()
  {
    assertException(
        IllegalArgumentException.class,
        "Function[regexp_extract] pattern must be a string literal",
        () -> eval("regexp_extract(a, 1)", InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "foo"))
    );
  }

  @Test
  public void testNonLiteralPattern()
  {
    assertException(
        IllegalArgumentException.class,
        "Function[regexp_extract] pattern must be a string literal",
        () -> eval("regexp_extract(a, a)", InputBindings.forInputSupplier("a", ExpressionType.STRING, () -> "foo"))
    );
  }

  @Test
  public void testNullPatternOnNull()
  {
    assertException(
        IllegalArgumentException.class,
        "Function[regexp_extract] pattern must be a string literal",
        () -> eval("regexp_extract(a, null)", InputBindings.nilBindings())
    );
  }

  @Test
  public void testEmptyStringPatternOnNull()
  {
    final ExprEval<?> result = eval("regexp_extract(a, '')", InputBindings.nilBindings());
    Assertions.assertNull(result.value());
  }
}
