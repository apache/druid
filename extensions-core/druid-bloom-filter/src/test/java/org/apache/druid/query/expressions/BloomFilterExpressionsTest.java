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

package org.apache.druid.query.expressions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.filter.BloomKFilter;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class BloomFilterExpressionsTest extends InitializedNullHandlingTest
{
  private static final String SOME_STRING = "foo";
  private static final long SOME_LONG = 1234L;
  private static final double SOME_DOUBLE = 1.234;
  private static final String[] SOME_STRING_ARRAY = new String[]{"hello", "world"};
  private static final Long[] SOME_LONG_ARRAY = new Long[]{1L, 2L, 3L, 4L};
  private static final Double[] SOME_DOUBLE_ARRAY = new Double[]{1.2, 3.4};

  BloomFilterExpressions.CreateExprMacro createMacro = new BloomFilterExpressions.CreateExprMacro();
  BloomFilterExpressions.AddExprMacro addMacro = new BloomFilterExpressions.AddExprMacro();
  BloomFilterExpressions.TestExprMacro testMacro = new BloomFilterExpressions.TestExprMacro();
  ExprMacroTable macroTable = new ExprMacroTable(ImmutableList.of(createMacro, addMacro, testMacro));

  Expr.ObjectBinding inputBindings = InputBindings.forInputSuppliers(
      new ImmutableMap.Builder<String, InputBindings.InputSupplier<?>>()
          .put("bloomy", InputBindings.inputSupplier(BloomFilterExpressions.BLOOM_FILTER_TYPE, () -> new BloomKFilter(100)))
          .put("string", InputBindings.inputSupplier(ExpressionType.STRING, () -> SOME_STRING))
          .put("long", InputBindings.inputSupplier(ExpressionType.LONG, () -> SOME_LONG))
          .put("double", InputBindings.inputSupplier(ExpressionType.DOUBLE, () -> SOME_DOUBLE))
          .put("string_array", InputBindings.inputSupplier(ExpressionType.STRING_ARRAY, () -> SOME_STRING_ARRAY))
          .put("long_array", InputBindings.inputSupplier(ExpressionType.LONG_ARRAY, () -> SOME_LONG_ARRAY))
          .put("double_array", InputBindings.inputSupplier(ExpressionType.DOUBLE_ARRAY, () -> SOME_DOUBLE_ARRAY))
          .build()
  );

  @Test
  public void testCreate()
  {
    Expr expr = Parser.parse("bloom_filter(100)", macroTable);
    ExprEval eval = expr.eval(inputBindings);

    Assertions.assertEquals(BloomFilterExpressions.BLOOM_FILTER_TYPE, eval.type());
    Assertions.assertTrue(eval.value() instanceof BloomKFilter);
    Assertions.assertEquals(1024, ((BloomKFilter) eval.value()).getBitSize());
  }

  @Test
  public void testAddString()
  {
    Expr expr = Parser.parse("bloom_filter_add('foo', bloomy)", macroTable);
    ExprEval eval = expr.eval(inputBindings);

    Assertions.assertEquals(BloomFilterExpressions.BLOOM_FILTER_TYPE, eval.type());
    Assertions.assertTrue(eval.value() instanceof BloomKFilter);
    Assertions.assertTrue(((BloomKFilter) eval.value()).testString(SOME_STRING));

    expr = Parser.parse("bloom_filter_add(string, bloomy)", macroTable);
    eval = expr.eval(inputBindings);

    Assertions.assertEquals(BloomFilterExpressions.BLOOM_FILTER_TYPE, eval.type());
    Assertions.assertTrue(eval.value() instanceof BloomKFilter);
    Assertions.assertTrue(((BloomKFilter) eval.value()).testString(SOME_STRING));
  }

  @Test
  public void testAddLong()
  {
    Expr expr = Parser.parse("bloom_filter_add(1234, bloomy)", macroTable);
    ExprEval eval = expr.eval(inputBindings);

    Assertions.assertEquals(BloomFilterExpressions.BLOOM_FILTER_TYPE, eval.type());
    Assertions.assertTrue(eval.value() instanceof BloomKFilter);
    Assertions.assertTrue(((BloomKFilter) eval.value()).testLong(SOME_LONG));

    expr = Parser.parse("bloom_filter_add(long, bloomy)", macroTable);
    eval = expr.eval(inputBindings);

    Assertions.assertEquals(BloomFilterExpressions.BLOOM_FILTER_TYPE, eval.type());
    Assertions.assertTrue(eval.value() instanceof BloomKFilter);
    Assertions.assertTrue(((BloomKFilter) eval.value()).testLong(SOME_LONG));
  }

  @Test
  public void testAddDouble()
  {
    Expr expr = Parser.parse("bloom_filter_add(1.234, bloomy)", macroTable);
    ExprEval eval = expr.eval(inputBindings);

    Assertions.assertEquals(BloomFilterExpressions.BLOOM_FILTER_TYPE, eval.type());
    Assertions.assertTrue(eval.value() instanceof BloomKFilter);
    Assertions.assertTrue(((BloomKFilter) eval.value()).testDouble(SOME_DOUBLE));

    expr = Parser.parse("bloom_filter_add(double, bloomy)", macroTable);
    eval = expr.eval(inputBindings);

    Assertions.assertEquals(BloomFilterExpressions.BLOOM_FILTER_TYPE, eval.type());
    Assertions.assertTrue(eval.value() instanceof BloomKFilter);
    Assertions.assertTrue(((BloomKFilter) eval.value()).testDouble(SOME_DOUBLE));
  }

  @Test
  public void testFilter()
  {
    Expr expr = Parser.parse("bloom_filter_test(1.234, bloom_filter_add(1.234, bloomy))", macroTable);
    ExprEval eval = expr.eval(inputBindings);
    Assertions.assertEquals(ExpressionType.LONG, eval.type());
    Assertions.assertTrue(eval.asBoolean());

    expr = Parser.parse("bloom_filter_test(1234, bloom_filter_add(1234, bloomy))", macroTable);
    eval = expr.eval(inputBindings);
    Assertions.assertTrue(eval.asBoolean());

    expr = Parser.parse("bloom_filter_test('foo', bloom_filter_add('foo', bloomy))", macroTable);
    eval = expr.eval(inputBindings);
    Assertions.assertTrue(eval.asBoolean());

    expr = Parser.parse("bloom_filter_test('bar', bloom_filter_add('foo', bloomy))", macroTable);
    eval = expr.eval(inputBindings);
    Assertions.assertFalse(eval.asBoolean());

    expr = Parser.parse("bloom_filter_test(1234, bloom_filter_add('foo', bloomy))", macroTable);
    eval = expr.eval(inputBindings);
    Assertions.assertFalse(eval.asBoolean());

    expr = Parser.parse("bloom_filter_test(1.23, bloom_filter_add('foo', bloomy))", macroTable);
    eval = expr.eval(inputBindings);
    Assertions.assertFalse(eval.asBoolean());


    expr = Parser.parse("bloom_filter_test(1234, bloom_filter_add(1234, bloom_filter(100)))", macroTable);
    eval = expr.eval(inputBindings);
    Assertions.assertTrue(eval.asBoolean());

    expr = Parser.parse("bloom_filter_test(4321, bloom_filter_add(1234, bloom_filter(100)))", macroTable);
    eval = expr.eval(inputBindings);
    Assertions.assertFalse(eval.asBoolean());

    expr = Parser.parse("bloom_filter_test(4321, bloom_filter_add(bloom_filter_add(1234, bloom_filter(100)), bloom_filter_add(4321, bloom_filter(100))))", macroTable);
    eval = expr.eval(inputBindings);
    Assertions.assertTrue(eval.asBoolean());
  }


  @Test
  public void testCreateWrongArgsCount()
  {
    Throwable exception = assertThrows(IAE.class, () ->
      Parser.parse("bloom_filter()", macroTable));
    assertTrue(exception.getMessage().contains("Function[bloom_filter] requires 1 argument"));
  }

  @Test
  public void testAddWrongArgsCount()
  {
    Throwable exception = assertThrows(IAE.class, () ->
      Parser.parse("bloom_filter_add(1)", macroTable));
    assertTrue(exception.getMessage().contains("Function[bloom_filter_add] requires 2 arguments"));
  }

  @Test
  public void testAddWrongArgType()
  {
    Throwable exception = assertThrows(IAE.class, () ->
      Parser.parse("bloom_filter_add(1, 2)", macroTable));
    assertTrue(exception.getMessage().contains("Function[bloom_filter_add] must take a bloom filter as the second argument"));
  }

  @Test
  public void testAddWrongArgType2()
  {
    Throwable exception = assertThrows(IAE.class, () -> {
      Expr expr = Parser.parse("bloom_filter_add(ARRAY<LONG>[], bloomy)", macroTable);
      expr.eval(inputBindings);
    });
    assertTrue(exception.getMessage().contains("Function[bloom_filter_add] cannot add [ARRAY<LONG>] to a bloom filter"));
  }

  @Test
  public void testTestWrongArgsCount()
  {
    Throwable exception = assertThrows(IAE.class, () ->
      Parser.parse("bloom_filter_test(1)", macroTable));
    assertTrue(exception.getMessage().contains("Function[bloom_filter_test] requires 2 arguments"));
  }

  @Test
  public void testTestWrongArgsType()
  {
    Throwable exception = assertThrows(IAE.class, () ->
      Parser.parse("bloom_filter_test(1, 2)", macroTable));
    assertTrue(exception.getMessage().contains("Function[bloom_filter_test] must take a bloom filter as the second argument"));
  }
}
