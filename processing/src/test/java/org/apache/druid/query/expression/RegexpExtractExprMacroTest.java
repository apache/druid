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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.InputBindings;
import org.junit.Assert;
import org.junit.Test;

public class RegexpExtractExprMacroTest extends MacroTestBase
{
  public RegexpExtractExprMacroTest()
  {
    super(new RegexpExtractExprMacro());
  }

  @Test
  public void testErrorZeroArguments()
  {
    expectException(IllegalArgumentException.class, "Function[regexp_extract] requires 2 or 3 arguments");
    eval("regexp_extract()", InputBindings.withMap(ImmutableMap.of()));
  }

  @Test
  public void testErrorFourArguments()
  {
    expectException(IllegalArgumentException.class, "Function[regexp_extract] requires 2 or 3 arguments");
    eval("regexp_extract('a', 'b', 'c', 'd')", InputBindings.withMap(ImmutableMap.of()));
  }

  @Test
  public void testMatch()
  {
    final ExprEval<?> result = eval("regexp_extract(a, 'f(.o)')", InputBindings.withMap(ImmutableMap.of("a", "foo")));
    Assert.assertEquals("foo", result.value());
  }

  @Test
  public void testMatchGroup0()
  {
    final ExprEval<?> result = eval("regexp_extract(a, 'f(.o)', 0)", InputBindings.withMap(ImmutableMap.of("a", "foo")));
    Assert.assertEquals("foo", result.value());
  }

  @Test
  public void testMatchGroup1()
  {
    final ExprEval<?> result = eval("regexp_extract(a, 'f(.o)', 1)", InputBindings.withMap(ImmutableMap.of("a", "foo")));
    Assert.assertEquals("oo", result.value());
  }

  @Test
  public void testMatchGroup2()
  {
    expectedException.expectMessage("No group 2");
    final ExprEval<?> result = eval("regexp_extract(a, 'f(.o)', 2)", InputBindings.withMap(ImmutableMap.of("a", "foo")));
  }

  @Test
  public void testNoMatch()
  {
    final ExprEval<?> result = eval("regexp_extract(a, 'f(.x)')", InputBindings.withMap(ImmutableMap.of("a", "foo")));
    Assert.assertNull(result.value());
  }

  @Test
  public void testMatchInMiddle()
  {
    final ExprEval<?> result = eval("regexp_extract(a, '.o$')", InputBindings.withMap(ImmutableMap.of("a", "foo")));
    Assert.assertEquals("oo", result.value());
  }

  @Test
  public void testNullPattern()
  {
    if (NullHandling.sqlCompatible()) {
      expectException(IllegalArgumentException.class, "Function[regexp_extract] pattern must be a string literal");
    }

    final ExprEval<?> result = eval("regexp_extract(a, null)", InputBindings.withMap(ImmutableMap.of("a", "foo")));
    Assert.assertNull(result.value());
  }

  @Test
  public void testEmptyStringPattern()
  {
    final ExprEval<?> result = eval("regexp_extract(a, '')", InputBindings.withMap(ImmutableMap.of("a", "foo")));
    Assert.assertEquals(NullHandling.emptyToNullIfNeeded(""), result.value());
  }

  @Test
  public void testNumericPattern()
  {
    expectException(IllegalArgumentException.class, "Function[regexp_extract] pattern must be a string literal");
    eval("regexp_extract(a, 1)", InputBindings.withMap(ImmutableMap.of("a", "foo")));
  }

  @Test
  public void testNonLiteralPattern()
  {
    expectException(IllegalArgumentException.class, "Function[regexp_extract] pattern must be a string literal");
    eval("regexp_extract(a, a)", InputBindings.withMap(ImmutableMap.of("a", "foo")));
  }

  @Test
  public void testNullPatternOnNull()
  {
    if (NullHandling.sqlCompatible()) {
      expectException(IllegalArgumentException.class, "Function[regexp_extract] pattern must be a string literal");
    }

    final ExprEval<?> result = eval("regexp_extract(a, null)", InputBindings.nilBindings());
    Assert.assertNull(result.value());
  }

  @Test
  public void testEmptyStringPatternOnNull()
  {
    final ExprEval<?> result = eval("regexp_extract(a, '')", InputBindings.nilBindings());
    Assert.assertNull(result.value());
  }
}
