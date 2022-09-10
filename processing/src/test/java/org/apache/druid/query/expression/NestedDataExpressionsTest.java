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
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionProcessingException;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.segment.nested.StructuredData;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

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

  Expr.ObjectBinding inputBindings = InputBindings.withTypedSuppliers(
      new ImmutableMap.Builder<String, Pair<ExpressionType, Supplier<Object>>>()
          .put("nest", new Pair<>(NestedDataExpressions.TYPE, () -> NEST))
          .put("nestWrapped", new Pair<>(NestedDataExpressions.TYPE, () -> new StructuredData(NEST)))
          .put("nester", new Pair<>(NestedDataExpressions.TYPE, () -> NESTER))
          .put("string", new Pair<>(ExpressionType.STRING, () -> "abcdef"))
          .put("long", new Pair<>(ExpressionType.LONG, () -> 1234L))
          .put("double", new Pair<>(ExpressionType.DOUBLE, () -> 1.234))
          .put("nullString", new Pair<>(ExpressionType.STRING, () -> null))
          .put("nullLong", new Pair<>(ExpressionType.LONG, () -> null))
          .put("nullDouble", new Pair<>(ExpressionType.DOUBLE, () -> null))
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
    Assert.assertArrayEquals(new Object[]{"$.y", "$.z", "$.x"}, (Object[]) eval.value());

    expr = Parser.parse("json_paths(nester)", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(ExpressionType.STRING_ARRAY, eval.type());
    Assert.assertArrayEquals(new Object[]{"$.x[0]", "$.x[1]", "$.x[2]", "$.y.b", "$.y.a"}, (Object[]) eval.value());
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
    Assert.assertNull(eval.value());

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
    Assert.assertEquals(NullHandling.defaultLongValue(), eval.value());
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
  }

  @Test
  public void testJsonQueryExpression()
  {
    Expr expr = Parser.parse("json_query(nest, '$.x')", MACRO_TABLE);
    ExprEval eval = expr.eval(inputBindings);
    Assert.assertEquals(100L, eval.value());
    Assert.assertEquals(NestedDataExpressions.TYPE, eval.type());

    expr = Parser.parse("json_query(nester, '$.x')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(NESTER.get("x"), eval.value());
    Assert.assertEquals(NestedDataExpressions.TYPE, eval.type());

    expr = Parser.parse("json_query(nester, '$.x[1]')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals("b", eval.value());
    Assert.assertEquals(NestedDataExpressions.TYPE, eval.type());

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
    Assert.assertEquals(NestedDataExpressions.TYPE, eval.type());

    expr = Parser.parse("json_query(nester, '$.y.a.b.c[12]')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertNull(eval.value());

    expr = Parser.parse("json_query(long, '$')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(1234L, eval.value());
    Assert.assertEquals(NestedDataExpressions.TYPE, eval.type());
  }

  @Test
  public void testParseJsonTryParseJson() throws JsonProcessingException
  {
    Expr expr = Parser.parse("parse_json(null)", MACRO_TABLE);
    ExprEval eval = expr.eval(inputBindings);
    Assert.assertEquals(null, eval.value());
    Assert.assertEquals(NestedDataExpressions.TYPE, eval.type());

    expr = Parser.parse("parse_json('null')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(null, eval.value());
    Assert.assertEquals(NestedDataExpressions.TYPE, eval.type());

    Assert.assertThrows(ExpressionProcessingException.class, () -> Parser.parse("parse_json('{')", MACRO_TABLE));
    expr = Parser.parse("try_parse_json('{')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(null, eval.value());
    Assert.assertEquals(NestedDataExpressions.TYPE, eval.type());

    Assert.assertThrows(ExpressionProcessingException.class, () -> Parser.parse("parse_json('hello world')", MACRO_TABLE));
    expr = Parser.parse("try_parse_json('hello world')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(null, eval.value());
    Assert.assertEquals(NestedDataExpressions.TYPE, eval.type());

    expr = Parser.parse("parse_json('\"hello world\"')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals("hello world", eval.value());
    Assert.assertEquals(NestedDataExpressions.TYPE, eval.type());

    expr = Parser.parse("parse_json('1')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(1, eval.value());
    Assert.assertEquals(NestedDataExpressions.TYPE, eval.type());

    expr = Parser.parse("parse_json('true')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals(true, eval.value());
    Assert.assertEquals(NestedDataExpressions.TYPE, eval.type());

    expr = Parser.parse("parse_json('{\"foo\":1}')", MACRO_TABLE);
    eval = expr.eval(inputBindings);
    Assert.assertEquals("{\"foo\":1}", JSON_MAPPER.writeValueAsString(eval.value()));
    Assert.assertEquals(NestedDataExpressions.TYPE, eval.type());
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
    Assert.assertEquals(NestedDataExpressions.TYPE, eval.type());

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
