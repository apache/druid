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
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.Parser;
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
    expectException(IllegalArgumentException.class, "Function[regexp_like] must have 2 arguments");
    eval("regexp_like()", Parser.withMap(ImmutableMap.of()));
  }

  @Test
  public void testErrorThreeArguments()
  {
    expectException(IllegalArgumentException.class, "Function[regexp_like] must have 2 arguments");
    eval("regexp_like('a', 'b', 'c')", Parser.withMap(ImmutableMap.of()));
  }

  @Test
  public void testMatch()
  {
    final ExprEval<?> result = eval("regexp_like(a, 'f.o')", Parser.withMap(ImmutableMap.of("a", "foo")));
    Assert.assertEquals(
        ExprEval.of(true, ExprType.LONG).value(),
        result.value()
    );
  }

  @Test
  public void testNoMatch()
  {
    final ExprEval<?> result = eval("regexp_like(a, 'f.x')", Parser.withMap(ImmutableMap.of("a", "foo")));
    Assert.assertEquals(
        ExprEval.of(false, ExprType.LONG).value(),
        result.value()
    );
  }

  @Test
  public void testNullPattern()
  {
    if (NullHandling.sqlCompatible()) {
      expectException(IllegalArgumentException.class, "Function[regexp_like] pattern must be a string literal");
    }

    final ExprEval<?> result = eval("regexp_like(a, null)", Parser.withMap(ImmutableMap.of("a", "foo")));
    Assert.assertEquals(
        ExprEval.of(true, ExprType.LONG).value(),
        result.value()
    );
  }

  @Test
  public void testEmptyStringPattern()
  {
    final ExprEval<?> result = eval("regexp_like(a, '')", Parser.withMap(ImmutableMap.of("a", "foo")));
    Assert.assertEquals(
        ExprEval.of(true, ExprType.LONG).value(),
        result.value()
    );
  }

  @Test
  public void testNullPatternOnEmptyString()
  {
    if (NullHandling.sqlCompatible()) {
      expectException(IllegalArgumentException.class, "Function[regexp_like] pattern must be a string literal");
    }

    final ExprEval<?> result = eval("regexp_like(a, null)", Parser.withMap(ImmutableMap.of("a", "")));
    Assert.assertEquals(
        ExprEval.of(true, ExprType.LONG).value(),
        result.value()
    );
  }

  @Test
  public void testEmptyStringPatternOnEmptyString()
  {
    final ExprEval<?> result = eval("regexp_like(a, '')", Parser.withMap(ImmutableMap.of("a", "")));
    Assert.assertEquals(
        ExprEval.of(true, ExprType.LONG).value(),
        result.value()
    );
  }

  @Test
  public void testNullPatternOnNull()
  {
    if (NullHandling.sqlCompatible()) {
      expectException(IllegalArgumentException.class, "Function[regexp_like] pattern must be a string literal");
    }

    final ExprEval<?> result = eval("regexp_like(a, null)", Parser.withSuppliers(ImmutableMap.of("a", () -> null)));
    Assert.assertEquals(
        ExprEval.of(true, ExprType.LONG).value(),
        result.value()
    );
  }

  @Test
  public void testEmptyStringPatternOnNull()
  {
    final ExprEval<?> result = eval("regexp_like(a, '')", Parser.withSuppliers(ImmutableMap.of("a", () -> null)));
    Assert.assertEquals(
        ExprEval.of(NullHandling.replaceWithDefault(), ExprType.LONG).value(),
        result.value()
    );
  }
}
