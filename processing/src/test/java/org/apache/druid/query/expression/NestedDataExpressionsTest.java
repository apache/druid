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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionProcessingException;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.ExpressionTypeFactory;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.segment.nested.StructuredData;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class NestedDataExpressionsTest extends InitializedNullHandlingTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();
  private static final ExprMacroTable MACRO_TABLE = new ExprMacroTable(
      ImmutableList.of(
          new NestedDataExpressions.JsonPathsExprMacro(),
          new NestedDataExpressions.JsonKeysExprMacro(),
          new NestedDataExpressions.JsonObjectExprMacro(),
          new NestedDataExpressions.JsonValueExprMacro(),
          new NestedDataExpressions.JsonQueryExprMacro(),
          new NestedDataExpressions.JsonQueryArrayExprMacro(),
          new NestedDataExpressions.ToJsonStringExprMacro(JSON_MAPPER),
          new NestedDataExpressions.ParseJsonExprMacro(JSON_MAPPER),
          new NestedDataExpressions.TryParseJsonExprMacro(JSON_MAPPER)
      )
  );
  private static final Map<String, Object> NEST = ImmutableMap.of(
      "x", 100L,
      "y", 200L,
      "z", 300L
  );

  private static final Map<String, Object> NESTER = ImmutableMap.of(
      "x", ImmutableList.of("a", "b", "c"),
      "y", ImmutableMap.of("a", "hello", "b", "world")
  );

  private static final Map<String, Object> NESTERER = ImmutableMap.of(
      "x",
      ImmutableMap.of(
          "a1", Arrays.asList(1, null, 3),
          "a2", Arrays.asList(1.1, null, 3.3),
          "a3", Arrays.asList("a", null, "b", "100")
      ),
      "y",
      ImmutableList.of(
          ImmutableMap.of("x", 1L, "y", 1.1),
          ImmutableMap.of("x", 2L, "y", 2.2),
          ImmutableMap.of("x", 3L, "y", 3.3)
      )
  );

  Expr.ObjectBinding inputBindings = InputBindings.forInputSuppliers(
      new ImmutableMap.Builder<String, InputBindings.InputSupplier<?>>()
          .put("nest", InputBindings.inputSupplier(ExpressionType.NESTED_DATA, () -> NEST))
          .put("nestWrapped", InputBindings.inputSupplier(ExpressionType.NESTED_DATA, () -> new StructuredData(NEST)))
          .put("nester", InputBindings.inputSupplier(ExpressionType.NESTED_DATA, () -> NESTER))
          .put("nesterer", InputBindings.inputSupplier(ExpressionType.NESTED_DATA, () -> NESTERER))
          .put("string", InputBindings.inputSupplier(ExpressionType.STRING, () -> "abcdef"))
          .put("long", InputBindings.inputSupplier(ExpressionType.LONG, () -> 1234L))
          .put("double", InputBindings.inputSupplier(ExpressionType.DOUBLE, () -> 1.234))
          .put("nullString", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
          .put("nullLong", InputBindings.inputSupplier(ExpressionType.LONG, () -> null))
          .put("nullDouble", InputBindings.inputSupplier(ExpressionType.DOUBLE, () -> null))
          .build()
  );

  @Test
  public void testJsonObjectExpression()
  {
    Expr expr = Parser.parse("json_object('x',100,'y',200,'z',300)", MACRO_TABLE);
    ExprEval eval = expr.eval(inputBindings);
    Assert.assertEquals(NEST, eval.value());

    expr = Parser.parse("json_object('x',array('a','b','c'),'y',json_object('a','hello','b','world'))", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    // decompose because of array equals
    Assert.assertArrayEquals(new Object[]{"a", "b", "c"}, (Object[]) ((Map) eval.value()).get("x"));
    Assert.assertEquals(ImmutableMap.of("a", "hello", "b", "world"), ((Map) eval.value()).get("y"));
  }

  @Test
  public void testJsonKeysExpression()
  {
    Expr expr = Parser.parse("json_keys(nest, '$.')", MACRO_TABLE);
    ExprEval eval = expr.eval(inputBindings);
    Assert.assertEquals(ExpressionType.STRING_ARRAY, eval.type());
    Assert.assertArrayEquals(new Object[]{"x", "y", "z"}, (Object[]) eval.value());


    expr = Parser.parse("json_keys(nester, '$.x')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(ExpressionType.STRING_ARRAY, eval.type());
    Assert.assertArrayEquals(new Object[]{"0", "1", "2"}, (Object[]) eval.value());

    expr = Parser.parse("json_keys(nester, '$.y')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(ExpressionType.STRING_ARRAY, eval.type());
    Assert.assertArrayEquals(new Object[]{"a", "b"}, (Object[]) eval.value());

    expr = Parser.parse("json_keys(nester, '$.x.a')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertNull(eval.value());

    expr = Parser.parse("json_keys(nester, '$.x.a.b')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertNull(eval.value());
  }

  @Test
  public void testJsonPathsExpression()
  {
    Expr expr = Parser.parse("json_paths(nest)", MACRO_TABLE);
    ExprEval eval = expr.eval(inputBindings);
    Assert.assertEquals(ExpressionType.STRING_ARRAY, eval.type());
    Assert.assertArrayEquals(new Object[]{"$.x", "$.y", "$.z"}, (Object[]) eval.value());

    expr = Parser.parse("json_paths(nester)", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(ExpressionType.STRING_ARRAY, eval.type());
    Assert.assertArrayEquals(new Object[]{"$.x", "$.y.a", "$.y.b"}, (Object[]) eval.value());
  }

  @Test
  public void testJsonValueExpression()
  {
    Expr expr = Parser.parse("json_value(nest, '$.x')", MACRO_TABLE);
    ExprEval eval = expr.eval(inputBindings);
    Assert.assertEquals(100L, eval.value());
    Assert.assertEquals(ExpressionType.LONG, eval.type());

    expr = Parser.parse("json_value(nester, '$.x')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertArrayEquals(new Object[]{"a", "b", "c"}, (Object[]) eval.value());
    Assert.assertEquals(ExpressionType.STRING_ARRAY, eval.type());

    expr = Parser.parse("json_value(nester, '$.x[1]')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals("b", eval.value());
    Assert.assertEquals(ExpressionType.STRING, eval.type());

    expr = Parser.parse("json_value(nester, '$.x[23]')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertNull(eval.value());

    expr = Parser.parse("json_value(nester, '$.x[1].b')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertNull(eval.value());

    expr = Parser.parse("json_value(nester, '$.y[1]')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertNull(eval.value());

    expr = Parser.parse("json_value(nester, '$.y.a')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals("hello", eval.value());
    Assert.assertEquals(ExpressionType.STRING, eval.type());

    expr = Parser.parse("json_value(nester, '$.y.a', 'LONG')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertNull(eval.value());
    Assert.assertEquals(ExpressionType.LONG, eval.type());

    expr = Parser.parse("json_value(nester, '$.y.a.b.c[12]')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertNull(eval.value());

    expr = Parser.parse("json_value(long, '$')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(1234L, eval.value());
    Assert.assertEquals(ExpressionType.LONG, eval.type());

    expr = Parser.parse("json_value(long, '$', 'STRING')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals("1234", eval.value());
    Assert.assertEquals(ExpressionType.STRING, eval.type());

    expr = Parser.parse("json_value(nest, '$.x')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(100L, eval.value());
    Assert.assertEquals(ExpressionType.LONG, eval.type());

    expr = Parser.parse("json_value(nest, '$.x', 'DOUBLE')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(100.0, eval.value());
    Assert.assertEquals(ExpressionType.DOUBLE, eval.type());

    expr = Parser.parse("json_value(nest, '$.x', 'STRING')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals("100", eval.value());
    Assert.assertEquals(ExpressionType.STRING, eval.type());

    expr = Parser.parse("json_value(nesterer, '$.x.a1')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertArrayEquals(new Object[]{1L, null, 3L}, (Object[]) eval.value());
    Assert.assertEquals(ExpressionType.LONG_ARRAY, eval.type());

    expr = Parser.parse("json_value(nesterer, '$.x.a1', 'ARRAY<STRING>')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertArrayEquals(new Object[]{"1", null, "3"}, (Object[]) eval.value());
    Assert.assertEquals(ExpressionType.STRING_ARRAY, eval.type());

    expr = Parser.parse("json_value(nesterer, '$.x.a1', 'ARRAY<DOUBLE>')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertArrayEquals(new Object[]{1.0, null, 3.0}, (Object[]) eval.value());
    Assert.assertEquals(ExpressionType.DOUBLE_ARRAY, eval.type());

    expr = Parser.parse("json_value(nesterer, '$.x.a2')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertArrayEquals(new Object[]{1.1, null, 3.3}, (Object[]) eval.value());
    Assert.assertEquals(ExpressionType.DOUBLE_ARRAY, eval.type());

    expr = Parser.parse("json_value(nesterer, '$.x.a2', 'ARRAY<LONG>')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertArrayEquals(new Object[]{1L, null, 3L}, (Object[]) eval.value());
    Assert.assertEquals(ExpressionType.LONG_ARRAY, eval.type());

    expr = Parser.parse("json_value(nesterer, '$.x.a2', 'ARRAY<STRING>')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertArrayEquals(new Object[]{"1.1", null, "3.3"}, (Object[]) eval.value());
    Assert.assertEquals(ExpressionType.STRING_ARRAY, eval.type());

    expr = Parser.parse("json_value(nesterer, '$.x.a3')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertArrayEquals(new Object[]{"a", null, "b", "100"}, (Object[]) eval.value());
    Assert.assertEquals(ExpressionType.STRING_ARRAY, eval.type());

    expr = Parser.parse("json_value(nesterer, '$.x.a3', 'ARRAY<LONG>')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertArrayEquals(
        new Object[]{null, null, null, 100L},
        (Object[]) eval.value()
    );
    Assert.assertEquals(ExpressionType.LONG_ARRAY, eval.type());

    // arrays of objects are not primitive
    expr = Parser.parse("json_value(nesterer, '$.y')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertNull(eval.value());

    expr = Parser.parse("json_value(json_object('k1', array(1,2,3), 'k2', array('a', 'b', 'c')), '$.k1', 'ARRAY<STRING>')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertArrayEquals(new Object[]{"1", "2", "3"}, (Object[]) eval.value());
    Assert.assertEquals(ExpressionType.STRING_ARRAY, eval.type());

    expr = Parser.parse("json_value(nester, array_offset(json_paths(nester), 0))", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertArrayEquals(new Object[]{"a", "b", "c"}, (Object[]) eval.value());
    Assert.assertEquals(ExpressionType.STRING_ARRAY, eval.type());
  }

  @Test
  public void testJsonQueryExpression()
  {
    Expr expr = Parser.parse("json_query(nest, '$.x')", MACRO_TABLE);
    ExprEval eval = expr.eval(inputBindings);
    Assert.assertEquals(100L, eval.value());
    Assert.assertEquals(ExpressionType.NESTED_DATA, eval.type());

    expr = Parser.parse("json_query(nester, '$.x')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(NESTER.get("x"), eval.value());
    Assert.assertEquals(ExpressionType.NESTED_DATA, eval.type());

    expr = Parser.parse("json_query(nester, '$.x[1]')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals("b", eval.value());
    Assert.assertEquals(ExpressionType.NESTED_DATA, eval.type());

    expr = Parser.parse("json_query(nester, '$.x[23]')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertNull(eval.value());

    expr = Parser.parse("json_query(nester, '$.x[1].b')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertNull(eval.value());

    expr = Parser.parse("json_query(nester, '$.y[1]')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertNull(eval.value());

    expr = Parser.parse("json_query(nester, '$.y.a')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals("hello", eval.value());
    Assert.assertEquals(ExpressionType.NESTED_DATA, eval.type());

    expr = Parser.parse("json_query(nester, '$.y.a.b.c[12]')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertNull(eval.value());

    expr = Parser.parse("json_query(long, '$')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(1234L, eval.value());
    Assert.assertEquals(ExpressionType.NESTED_DATA, eval.type());

    expr = Parser.parse("json_query(nester, array_offset(json_paths(nester), 0))", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(NESTER.get("x"), eval.value());
    Assert.assertEquals(ExpressionType.NESTED_DATA, eval.type());
  }

  @Test
  public void testJsonQueryArrayExpression()
  {
    final ExpressionType nestedArray = ExpressionTypeFactory.getInstance().ofArray(ExpressionType.NESTED_DATA);

    Expr expr = Parser.parse("json_query_array(nest, '$.x')", MACRO_TABLE);
    ExprEval eval = expr.eval(inputBindings);
    Assert.assertArrayEquals(new Object[]{100L}, (Object[]) eval.value());
    Assert.assertEquals(nestedArray, eval.type());

    expr = Parser.parse("json_query_array(nester, '$.x')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertArrayEquals(((List) NESTER.get("x")).toArray(), (Object[]) eval.value());
    Assert.assertEquals(nestedArray, eval.type());

    expr = Parser.parse("json_query_array(nester, array_offset(json_paths(nester), 0))", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertArrayEquals(((List) NESTER.get("x")).toArray(), (Object[]) eval.value());
    Assert.assertEquals(nestedArray, eval.type());

    expr = Parser.parse("json_query_array(nesterer, '$.y')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertArrayEquals(((List) NESTERER.get("y")).toArray(), (Object[]) eval.value());
    Assert.assertEquals(nestedArray, eval.type());

    expr = Parser.parse("array_length(json_query_array(nesterer, '$.y'))", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(3L, eval.value());
    Assert.assertEquals(ExpressionType.LONG, eval.type());
  }

  @Test
  public void testParseJsonTryParseJson() throws JsonProcessingException
  {
    Expr expr = Parser.parse("parse_json(null)", MACRO_TABLE);
    ExprEval eval = expr.eval(inputBindings);
    Assert.assertEquals(null, eval.value());
    Assert.assertEquals(ExpressionType.NESTED_DATA, eval.type());

    expr = Parser.parse("parse_json('null')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(null, eval.value());
    Assert.assertEquals(ExpressionType.NESTED_DATA, eval.type());

    Assert.assertThrows(ExpressionProcessingException.class, () -> Parser.parse("parse_json('{')", MACRO_TABLE));
    expr = Parser.parse("try_parse_json('{')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(null, eval.value());
    Assert.assertEquals(ExpressionType.NESTED_DATA, eval.type());

    Assert.assertThrows(ExpressionProcessingException.class, () -> Parser.parse("parse_json('hello world')", MACRO_TABLE));
    expr = Parser.parse("try_parse_json('hello world')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(null, eval.value());
    Assert.assertEquals(ExpressionType.NESTED_DATA, eval.type());

    expr = Parser.parse("parse_json('\"hello world\"')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals("hello world", eval.value());
    Assert.assertEquals(ExpressionType.NESTED_DATA, eval.type());

    expr = Parser.parse("parse_json('1')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(1, eval.value());
    Assert.assertEquals(ExpressionType.NESTED_DATA, eval.type());

    expr = Parser.parse("parse_json('true')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(true, eval.value());
    Assert.assertEquals(ExpressionType.NESTED_DATA, eval.type());

    expr = Parser.parse("parse_json('{\"foo\":1}')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals("{\"foo\":1}", JSON_MAPPER.writeValueAsString(eval.value()));
    Assert.assertEquals(ExpressionType.NESTED_DATA, eval.type());
  }

  @Test
  public void testToJsonStringParseJson()
  {
    Expr expr = Parser.parse("to_json_string(nest)", MACRO_TABLE);
    ExprEval eval = expr.eval(inputBindings);
    Assert.assertEquals("{\"x\":100,\"y\":200,\"z\":300}", eval.value());
    Assert.assertEquals(ExpressionType.STRING, eval.type());

    expr = Parser.parse("parse_json(to_json_string(nest))", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    // round trip ends up as integers initially...
    for (String key : NEST.keySet()) {
      Map val = (Map) eval.value();
      Assert.assertEquals(NEST.get(key), ((Integer) val.get(key)).longValue());
    }
    Assert.assertEquals(ExpressionType.NESTED_DATA, eval.type());

    expr = Parser.parse("json_value(parse_json('{\"x\":100,\"y\":200,\"z\":300}'), '$.x')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(100L, eval.value());
    Assert.assertEquals(ExpressionType.LONG, eval.type());

    expr = Parser.parse("to_json_string(json_object('x', nestWrapped))", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals("{\"x\":{\"x\":100,\"y\":200,\"z\":300}}", eval.value());

    expr = Parser.parse("to_json_string(json_object('xs', array(nest, nestWrapped)))", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals("{\"xs\":[{\"x\":100,\"y\":200,\"z\":300},{\"x\":100,\"y\":200,\"z\":300}]}", eval.value());
  }
}
