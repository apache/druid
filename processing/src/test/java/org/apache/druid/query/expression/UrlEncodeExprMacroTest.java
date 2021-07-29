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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.segment.virtual.ExpressionPlannerTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class UrlEncodeExprMacroTest extends MacroTestBase
{

  private static final ExprMacroTable MACRO_TABLE = new ExprMacroTable(ImmutableList.of(new UrlEncodeExprMacro()));

  public UrlEncodeExprMacroTest()
  {
    super(new UrlEncodeExprMacro());
  }

  @Test
  public void testNull()
  {
    Assert.assertNull(eval(null));
  }

  @Test
  public void testEmpty()
  {
    // allow test to work if property is set or not set
    if (System.getProperty("druid.generic.useDefaultValueForNull") != null) {
      Assert.assertEquals("", eval(""));
    } else {
      Assert.assertNull(eval(""));
    }
  }

  @Test
  public void testBlank()
  {
    Assert.assertEquals("+", eval(" "));
  }

  @Test
  public void testPercent()
  {
    Assert.assertEquals("%25", eval("%"));
    Assert.assertEquals("%25%25", eval("%%"));
  }

  @Test
  public void testUrls()
  {
    Assert.assertEquals("http%3A%2F%2Fdruid.apache.org", eval("http://druid.apache.org"));
    Assert.assertEquals("http%3A%2F%2Fdruid.apache.org%2Fa+b%3Fc%3D%25", eval("http://druid.apache.org/a b?c=%"));
    Assert.assertEquals("a+b", eval("a b"));
  }

  @Test(expected = IAE.class)
  public void testInvalidNoArguments()
  {
    Expr expr = apply(Collections.emptyList());
    expr.eval(ExprUtils.nilBindings());
  }

  @Test(expected = IAE.class)
  public void testInvalidNumberOfArguments()
  {
    Expr expr = apply(Arrays.asList(ExprEval.of("a").toExpr(), ExprEval.of("b").toExpr()));
    expr.eval(ExprUtils.nilBindings());
  }

  @Test
  public void testBindings()
  {
    Expr.ObjectBinding bindings = InputBindings.withMap(
        ImmutableMap.<String, Object>builder()
                    .put("url", "http://druid.apache.org")
                    .put("url_empty", "")
                    .put("number", 27L)
                    .build()
    );

    Expr expr = Parser.parse("urlencode(url)", MACRO_TABLE);

    Assert.assertEquals(ExprType.STRING, expr.getOutputType(ExpressionPlannerTest.SYNTHETIC_INSPECTOR));

    Assert.assertEquals("http%3A%2F%2Fdruid.apache.org", expr.eval(bindings).value());

    Assert.assertEquals(null, Parser.parse("urlencode(url_empty)", MACRO_TABLE).eval(bindings).value());
    Assert.assertEquals(null, Parser.parse("urlencode(number)", MACRO_TABLE).eval(bindings).value());
  }

  private Object eval(String url)
  {
    Expr expr = apply(Collections.singletonList(toExpr(url)));
    ExprEval eval = expr.eval(ExprUtils.nilBindings());
    return eval.value();
  }

  private Expr toExpr(String url)
  {
    return ExprEval.of(url).toExpr();
  }

}
