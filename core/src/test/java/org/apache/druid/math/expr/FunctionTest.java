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

package org.apache.druid.math.expr;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.junit.Assert;
import org.junit.Test;

public class FunctionTest
{
  private final Expr.ObjectBinding bindings = Parser.withMap(
      ImmutableMap.of(
          "x", "foo",
          "y", 2,
          "z", 3.1
      )
  );

  @Test
  public void testCaseSimple()
  {
    assertExpr("case_simple(x,'baz','is baz','foo','is foo','is other')", "is foo");
    assertExpr("case_simple(x,'baz','is baz','bar','is bar','is other')", "is other");
    assertExpr("case_simple(y,2,'is 2',3,'is 3','is other')", "is 2");
    assertExpr("case_simple(z,2,'is 2',3,'is 3','is other')", "is other");
  }

  @Test
  public void testCaseSearched()
  {
    assertExpr("case_searched(x=='baz','is baz',x=='foo','is foo','is other')", "is foo");
    assertExpr("case_searched(x=='baz','is baz',x=='bar','is bar','is other')", "is other");
    assertExpr("case_searched(y==2,'is 2',y==3,'is 3','is other')", "is 2");
    assertExpr("case_searched(z==2,'is 2',z==3,'is 3','is other')", "is other");
  }

  @Test
  public void testConcat()
  {
    assertExpr("concat(x,' ',y)", "foo 2");
    if (NullHandling.replaceWithDefault()) {
      assertExpr("concat(x,' ',nonexistent,' ',y)", "foo  2");
    } else {
      assertExpr("concat(x,' ',nonexistent,' ',y)", null);
    }

    assertExpr("concat(z)", "3.1");
    assertExpr("concat()", null);
  }

  @Test
  public void testReplace()
  {
    assertExpr("replace(x,'oo','ab')", "fab");
    assertExpr("replace(x,x,'ab')", "ab");
    assertExpr("replace(x,'oo',y)", "f2");
  }

  @Test
  public void testSubstring()
  {
    assertExpr("substring(x,0,2)", "fo");
    assertExpr("substring(x,1,2)", "oo");
    assertExpr("substring(x,y,1)", "o");
    assertExpr("substring(x,0,-1)", "foo");
    assertExpr("substring(x,0,100)", "foo");
  }

  @Test
  public void testStrlen()
  {
    assertExpr("strlen(x)", 3L);
    assertExpr("strlen(nonexistent)", NullHandling.defaultLongValue());
  }

  @Test
  public void testStrpos()
  {
    assertExpr("strpos(x, 'o')", 1L);
    assertExpr("strpos(x, 'o', 0)", 1L);
    assertExpr("strpos(x, 'o', 1)", 1L);
    assertExpr("strpos(x, 'o', 2)", 2L);
    assertExpr("strpos(x, 'o', 3)", -1L);
    assertExpr("strpos(x, '')", 0L);
    assertExpr("strpos(x, 'x')", -1L);
  }

  @Test
  public void testLower()
  {
    assertExpr("lower('FOO')", "foo");
  }

  @Test
  public void testUpper()
  {
    assertExpr("upper(x)", "FOO");
  }

  private void assertExpr(final String expression, final Object expectedResult)
  {
    final Expr expr = Parser.parse(expression, ExprMacroTable.nil());
    Assert.assertEquals(expression, expectedResult, expr.eval(bindings).value());
  }

  @Test
  public void testIsNull()
  {
    assertExpr("isnull(null)", 1L);
    assertExpr("isnull('abc')", 0L);
  }

  @Test
  public void testIsNotNull()
  {
    assertExpr("notnull(null)", 0L);
    assertExpr("notnull('abc')", 1L);
  }

  @Test
  public void testLpad()
  {
    assertExpr("lpad(x, 5, 'ab')", "abfoo");
    assertExpr("lpad(x, 4, 'ab')", "afoo");
    assertExpr("lpad(x, 2, 'ab')", "fo");
    assertExpr("lpad(x, 0, 'ab')", null);
    assertExpr("lpad(x, 5, null)", null);
    assertExpr("lpad(null, 5, x)", null);
  }

  @Test
  public void testRpad()
  {
    assertExpr("rpad(x, 5, 'ab')", "fooab");
    assertExpr("rpad(x, 4, 'ab')", "fooa");
    assertExpr("rpad(x, 2, 'ab')", "fo");
    assertExpr("rpad(x, 0, 'ab')", null);
    assertExpr("rpad(x, 5, null)", null);
    assertExpr("rpad(null, 5, x)", null);
  }
}
