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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 */
public class EvalTest extends InitializedNullHandlingTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private long evalLong(String x, Expr.ObjectBinding bindings)
  {
    ExprEval ret = eval(x, bindings);
    assertEquals(ExpressionType.LONG, ret.type());
    return ret.asLong();
  }

  private double evalDouble(String x, Expr.ObjectBinding bindings)
  {
    ExprEval ret = eval(x, bindings);
    assertEquals(ExpressionType.DOUBLE, ret.type());
    return ret.asDouble();
  }

  private ExprEval eval(String x, Expr.ObjectBinding bindings)
  {
    return Parser.parse(x, ExprMacroTable.nil()).eval(bindings);
  }

  @Test
  public void testDoubleEval()
  {
    Expr.ObjectBinding bindings = InputBindings.withMap(ImmutableMap.of("x", 2.0d));
    assertEquals(2.0, evalDouble("x", bindings), 0.0001);
    assertEquals(2.0, evalDouble("\"x\"", bindings), 0.0001);
    assertEquals(304.0, evalDouble("300 + \"x\" * 2", bindings), 0.0001);

    try {
      ExpressionProcessing.initializeForStrictBooleansTests(false);
      Assert.assertFalse(evalDouble("1.0 && 0.0", bindings) > 0.0);
      Assert.assertTrue(evalDouble("1.0 && 2.0", bindings) > 0.0);

      Assert.assertTrue(evalDouble("1.0 || 0.0", bindings) > 0.0);
      Assert.assertFalse(evalDouble("0.0 || 0.0", bindings) > 0.0);

      Assert.assertTrue(evalDouble("2.0 > 1.0", bindings) > 0.0);
      Assert.assertTrue(evalDouble("2.0 >= 2.0", bindings) > 0.0);
      Assert.assertTrue(evalDouble("1.0 < 2.0", bindings) > 0.0);
      Assert.assertTrue(evalDouble("2.0 <= 2.0", bindings) > 0.0);
      Assert.assertTrue(evalDouble("2.0 == 2.0", bindings) > 0.0);
      Assert.assertTrue(evalDouble("2.0 != 1.0", bindings) > 0.0);

      Assert.assertTrue(evalDouble("!-1.0", bindings) > 0.0);
      Assert.assertTrue(evalDouble("!0.0", bindings) > 0.0);
      Assert.assertFalse(evalDouble("!2.0", bindings) > 0.0);
    }
    finally {
      ExpressionProcessing.initializeForTests(null);
    }
    try {
      ExpressionProcessing.initializeForStrictBooleansTests(true);
      Assert.assertEquals(0L, evalLong("1.0 && 0.0", bindings));
      Assert.assertEquals(1L, evalLong("1.0 && 2.0", bindings));

      Assert.assertEquals(1L, evalLong("1.0 || 0.0", bindings));
      Assert.assertEquals(0L, evalLong("0.0 || 0.0", bindings));

      Assert.assertEquals(1L, evalLong("2.0 > 1.0", bindings));
      Assert.assertEquals(1L, evalLong("2.0 >= 2.0", bindings));
      Assert.assertEquals(1L, evalLong("1.0 < 2.0", bindings));
      Assert.assertEquals(1L, evalLong("2.0 <= 2.0", bindings));
      Assert.assertEquals(1L, evalLong("2.0 == 2.0", bindings));
      Assert.assertEquals(1L, evalLong("2.0 != 1.0", bindings));

      Assert.assertEquals(1L, evalLong("!-1.0", bindings));
      Assert.assertEquals(1L, evalLong("!0.0", bindings));
      Assert.assertEquals(0L, evalLong("!2.0", bindings));

      assertEquals(3.5, evalDouble("2.0 + 1.5", bindings), 0.0001);
      assertEquals(0.5, evalDouble("2.0 - 1.5", bindings), 0.0001);
      assertEquals(3.0, evalDouble("2.0 * 1.5", bindings), 0.0001);
      assertEquals(4.0, evalDouble("2.0 / 0.5", bindings), 0.0001);
      assertEquals(0.2, evalDouble("2.0 % 0.3", bindings), 0.0001);
      assertEquals(8.0, evalDouble("2.0 ^ 3.0", bindings), 0.0001);
      assertEquals(-1.5, evalDouble("-1.5", bindings), 0.0001);


      assertEquals(2.0, evalDouble("sqrt(4.0)", bindings), 0.0001);
      assertEquals(2.0, evalDouble("if(1.0, 2.0, 3.0)", bindings), 0.0001);
      assertEquals(3.0, evalDouble("if(0.0, 2.0, 3.0)", bindings), 0.0001);
    }
    finally {
      ExpressionProcessing.initializeForTests(null);
    }
  }

  @Test
  public void testLongEval()
  {
    Expr.ObjectBinding bindings = InputBindings.withMap(ImmutableMap.of("x", 9223372036854775807L));

    assertEquals(9223372036854775807L, evalLong("x", bindings));
    assertEquals(9223372036854775807L, evalLong("\"x\"", bindings));
    assertEquals(92233720368547759L, evalLong("\"x\" / 100 + 1", bindings));

    Assert.assertFalse(evalLong("9223372036854775807 && 0", bindings) > 0);
    Assert.assertTrue(evalLong("9223372036854775807 && 9223372036854775806", bindings) > 0);

    Assert.assertTrue(evalLong("9223372036854775807 || 0", bindings) > 0);
    Assert.assertFalse(evalLong("-9223372036854775807 || -9223372036854775807", bindings) > 0);
    Assert.assertTrue(evalLong("-9223372036854775807 || 9223372036854775807", bindings) > 0);
    Assert.assertFalse(evalLong("0 || 0", bindings) > 0);

    Assert.assertTrue(evalLong("9223372036854775807 > 9223372036854775806", bindings) > 0);
    Assert.assertTrue(evalLong("9223372036854775807 >= 9223372036854775807", bindings) > 0);
    Assert.assertTrue(evalLong("9223372036854775806 < 9223372036854775807", bindings) > 0);
    Assert.assertTrue(evalLong("9223372036854775807 <= 9223372036854775807", bindings) > 0);
    Assert.assertTrue(evalLong("9223372036854775807 == 9223372036854775807", bindings) > 0);
    Assert.assertTrue(evalLong("9223372036854775807 != 9223372036854775806", bindings) > 0);

    assertEquals(9223372036854775807L, evalLong("9223372036854775806 + 1", bindings));
    assertEquals(9223372036854775806L, evalLong("9223372036854775807 - 1", bindings));
    assertEquals(9223372036854775806L, evalLong("4611686018427387903 * 2", bindings));
    assertEquals(4611686018427387903L, evalLong("9223372036854775806 / 2", bindings));
    assertEquals(7L, evalLong("9223372036854775807 % 9223372036854775800", bindings));
    assertEquals(9223372030926249001L, evalLong("3037000499 ^ 2", bindings));
    assertEquals(-9223372036854775807L, evalLong("-9223372036854775807", bindings));

    Assert.assertTrue(evalLong("!-9223372036854775807", bindings) > 0);
    Assert.assertTrue(evalLong("!0", bindings) > 0);
    Assert.assertFalse(evalLong("!9223372036854775807", bindings) > 0);

    assertEquals(3037000499L, evalLong("cast(sqrt(9223372036854775807), 'long')", bindings));
    assertEquals(1L, evalLong("if(x == 9223372036854775807, 1, 0)", bindings));
    assertEquals(0L, evalLong("if(x - 1 == 9223372036854775807, 1, 0)", bindings));

    assertEquals(1271030400000L, evalLong("timestamp('2010-04-12')", bindings));
    assertEquals(1270998000000L, evalLong("timestamp('2010-04-12T+09:00')", bindings));
    assertEquals(1271055781000L, evalLong("timestamp('2010-04-12T07:03:01')", bindings));
    assertEquals(1271023381000L, evalLong("timestamp('2010-04-12T07:03:01+09:00')", bindings));
    assertEquals(1271023381419L, evalLong("timestamp('2010-04-12T07:03:01.419+09:00')", bindings));

    assertEquals(1271030400L, evalLong("unix_timestamp('2010-04-12')", bindings));
    assertEquals(1270998000L, evalLong("unix_timestamp('2010-04-12T+09:00')", bindings));
    assertEquals(1271055781L, evalLong("unix_timestamp('2010-04-12T07:03:01')", bindings));
    assertEquals(1271023381L, evalLong("unix_timestamp('2010-04-12T07:03:01+09:00')", bindings));
    assertEquals(1271023381L, evalLong("unix_timestamp('2010-04-12T07:03:01.419+09:00')", bindings));
    assertEquals(
        NullHandling.replaceWithDefault() ? "NULL" : "",
        eval("nvl(if(x == 9223372036854775807, '', 'x'), 'NULL')", bindings).asString()
    );
    assertEquals("x", eval("nvl(if(x == 9223372036854775806, '', 'x'), 'NULL')", bindings).asString());
  }

  @Test
  public void testArrayToScalar()
  {
    assertEquals(1L, ExprEval.ofLongArray(new Long[]{1L}).asLong());
    assertEquals(1.0, ExprEval.ofLongArray(new Long[]{1L}).asDouble(), 0.0);
    assertEquals(1, ExprEval.ofLongArray(new Long[]{1L}).asInt());
    assertEquals(true, ExprEval.ofLongArray(new Long[]{1L}).asBoolean());
    assertEquals("1", ExprEval.ofLongArray(new Long[]{1L}).asString());


    assertEquals(null, ExprEval.ofLongArray(new Long[]{null}).asString());

    assertEquals(0L, ExprEval.ofLongArray(new Long[]{1L, 2L}).asLong());
    assertEquals(0.0, ExprEval.ofLongArray(new Long[]{1L, 2L}).asDouble(), 0.0);
    assertEquals("[1, 2]", ExprEval.ofLongArray(new Long[]{1L, 2L}).asString());
    assertEquals(0, ExprEval.ofLongArray(new Long[]{1L, 2L}).asInt());
    assertEquals(false, ExprEval.ofLongArray(new Long[]{1L, 2L}).asBoolean());

    assertEquals(1.1, ExprEval.ofDoubleArray(new Double[]{1.1}).asDouble(), 0.0);
    assertEquals(1L, ExprEval.ofDoubleArray(new Double[]{1.1}).asLong());
    assertEquals("1.1", ExprEval.ofDoubleArray(new Double[]{1.1}).asString());
    assertEquals(1, ExprEval.ofDoubleArray(new Double[]{1.1}).asInt());
    assertEquals(true, ExprEval.ofDoubleArray(new Double[]{1.1}).asBoolean());

    assertEquals(null, ExprEval.ofDoubleArray(new Double[]{null}).asString());

    assertEquals(0.0, ExprEval.ofDoubleArray(new Double[]{1.1, 2.2}).asDouble(), 0.0);
    assertEquals(0L, ExprEval.ofDoubleArray(new Double[]{1.1, 2.2}).asLong());
    assertEquals("[1.1, 2.2]", ExprEval.ofDoubleArray(new Double[]{1.1, 2.2}).asString());
    assertEquals(0, ExprEval.ofDoubleArray(new Double[]{1.1, 2.2}).asInt());
    assertEquals(false, ExprEval.ofDoubleArray(new Double[]{1.1, 2.2}).asBoolean());

    assertEquals("foo", ExprEval.ofStringArray(new String[]{"foo"}).asString());

    assertEquals("1", ExprEval.ofStringArray(new String[]{"1"}).asString());
    assertEquals(1L, ExprEval.ofStringArray(new String[]{"1"}).asLong());
    assertEquals(1.0, ExprEval.ofStringArray(new String[]{"1"}).asDouble(), 0.0);
    assertEquals(1, ExprEval.ofStringArray(new String[]{"1"}).asInt());
    assertEquals(false, ExprEval.ofStringArray(new String[]{"1"}).asBoolean());
    assertEquals(true, ExprEval.ofStringArray(new String[]{"true"}).asBoolean());

    assertEquals("[1, 2.2]", ExprEval.ofStringArray(new String[]{"1", "2.2"}).asString());
    assertEquals(0L, ExprEval.ofStringArray(new String[]{"1", "2.2"}).asLong());
    assertEquals(0.0, ExprEval.ofStringArray(new String[]{"1", "2.2"}).asDouble(), 0.0);
    assertEquals(0, ExprEval.ofStringArray(new String[]{"1", "2.2"}).asInt());
    assertEquals(false, ExprEval.ofStringArray(new String[]{"1", "2.2"}).asBoolean());

    // test casting arrays to scalars
    assertEquals(1L, ExprEval.ofLongArray(new Long[]{1L}).castTo(ExpressionType.LONG).value());
    assertEquals(NullHandling.defaultLongValue(), ExprEval.ofLongArray(new Long[]{null}).castTo(ExpressionType.LONG).value());
    assertEquals(1.0, ExprEval.ofLongArray(new Long[]{1L}).castTo(ExpressionType.DOUBLE).asDouble(), 0.0);
    assertEquals("1", ExprEval.ofLongArray(new Long[]{1L}).castTo(ExpressionType.STRING).value());

    assertEquals(1.1, ExprEval.ofDoubleArray(new Double[]{1.1}).castTo(ExpressionType.DOUBLE).asDouble(), 0.0);
    assertEquals(NullHandling.defaultDoubleValue(), ExprEval.ofDoubleArray(new Double[]{null}).castTo(ExpressionType.DOUBLE).value());
    assertEquals(1L, ExprEval.ofDoubleArray(new Double[]{1.1}).castTo(ExpressionType.LONG).value());
    assertEquals("1.1", ExprEval.ofDoubleArray(new Double[]{1.1}).castTo(ExpressionType.STRING).value());

    assertEquals("foo", ExprEval.ofStringArray(new String[]{"foo"}).castTo(ExpressionType.STRING).value());
    assertEquals(NullHandling.defaultLongValue(), ExprEval.ofStringArray(new String[]{"foo"}).castTo(ExpressionType.LONG).value());
    assertEquals(NullHandling.defaultDoubleValue(), ExprEval.ofStringArray(new String[]{"foo"}).castTo(ExpressionType.DOUBLE).value());
    assertEquals("1", ExprEval.ofStringArray(new String[]{"1"}).castTo(ExpressionType.STRING).value());
    assertEquals(1L, ExprEval.ofStringArray(new String[]{"1"}).castTo(ExpressionType.LONG).value());
    assertEquals(1.0, ExprEval.ofStringArray(new String[]{"1"}).castTo(ExpressionType.DOUBLE).value());
  }

  @Test
  public void testStringArrayToScalarStringBadCast()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("invalid type STRING");
    ExprEval.ofStringArray(new String[]{"foo", "bar"}).castTo(ExpressionType.STRING);
  }

  @Test
  public void testStringArrayToScalarLongBadCast()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("invalid type LONG");
    ExprEval.ofStringArray(new String[]{"foo", "bar"}).castTo(ExpressionType.LONG);
  }

  @Test
  public void testStringArrayToScalarDoubleBadCast()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("invalid type DOUBLE");
    ExprEval.ofStringArray(new String[]{"foo", "bar"}).castTo(ExpressionType.DOUBLE);
  }

  @Test
  public void testLongArrayToScalarStringBadCast()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("invalid type STRING");
    ExprEval.ofLongArray(new Long[]{1L, 2L}).castTo(ExpressionType.STRING);
  }

  @Test
  public void testLongArrayToScalarLongBadCast()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("invalid type LONG");
    ExprEval.ofLongArray(new Long[]{1L, 2L}).castTo(ExpressionType.LONG);
  }

  @Test
  public void testLongArrayToScalarDoubleBadCast()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("invalid type DOUBLE");
    ExprEval.ofLongArray(new Long[]{1L, 2L}).castTo(ExpressionType.DOUBLE);
  }

  @Test
  public void testDoubleArrayToScalarStringBadCast()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("invalid type STRING");
    ExprEval.ofDoubleArray(new Double[]{1.1, 2.2}).castTo(ExpressionType.STRING);
  }

  @Test
  public void testDoubleArrayToScalarLongBadCast()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("invalid type LONG");
    ExprEval.ofDoubleArray(new Double[]{1.1, 2.2}).castTo(ExpressionType.LONG);
  }

  @Test
  public void testDoubleArrayToScalarDoubleBadCast()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("invalid type DOUBLE");
    ExprEval.ofDoubleArray(new Double[]{1.1, 2.2}).castTo(ExpressionType.DOUBLE);
  }

  @Test
  public void testIsNumericNull()
  {
    if (NullHandling.sqlCompatible()) {
      Assert.assertFalse(ExprEval.ofLong(1L).isNumericNull());
      Assert.assertTrue(ExprEval.ofLong(null).isNumericNull());

      Assert.assertFalse(ExprEval.ofDouble(1.0).isNumericNull());
      Assert.assertTrue(ExprEval.ofDouble(null).isNumericNull());

      Assert.assertTrue(ExprEval.of(null).isNumericNull());
      Assert.assertTrue(ExprEval.of("one").isNumericNull());
      Assert.assertFalse(ExprEval.of("1").isNumericNull());

      Assert.assertFalse(ExprEval.ofLongArray(new Long[]{1L}).isNumericNull());
      Assert.assertTrue(ExprEval.ofLongArray(new Long[]{null, 2L, 3L}).isNumericNull());
      Assert.assertTrue(ExprEval.ofLongArray(new Long[]{null}).isNumericNull());

      Assert.assertFalse(ExprEval.ofDoubleArray(new Double[]{1.1}).isNumericNull());
      Assert.assertTrue(ExprEval.ofDoubleArray(new Double[]{null, 1.1, 2.2}).isNumericNull());
      Assert.assertTrue(ExprEval.ofDoubleArray(new Double[]{null}).isNumericNull());

      Assert.assertFalse(ExprEval.ofStringArray(new String[]{"1"}).isNumericNull());
      Assert.assertTrue(ExprEval.ofStringArray(new String[]{null, "1", "2"}).isNumericNull());
      Assert.assertTrue(ExprEval.ofStringArray(new String[]{"one"}).isNumericNull());
      Assert.assertTrue(ExprEval.ofStringArray(new String[]{null}).isNumericNull());
    } else {
      Assert.assertFalse(ExprEval.ofLong(1L).isNumericNull());
      Assert.assertFalse(ExprEval.ofLong(null).isNumericNull());

      Assert.assertFalse(ExprEval.ofDouble(1.0).isNumericNull());
      Assert.assertFalse(ExprEval.ofDouble(null).isNumericNull());

      // strings are still null
      Assert.assertTrue(ExprEval.of(null).isNumericNull());
      Assert.assertTrue(ExprEval.of("one").isNumericNull());
      Assert.assertFalse(ExprEval.of("1").isNumericNull());

      // arrays can still have nulls
      Assert.assertFalse(ExprEval.ofLongArray(new Long[]{1L}).isNumericNull());
      Assert.assertTrue(ExprEval.ofLongArray(new Long[]{null, 2L, 3L}).isNumericNull());
      Assert.assertTrue(ExprEval.ofLongArray(new Long[]{null}).isNumericNull());

      Assert.assertFalse(ExprEval.ofDoubleArray(new Double[]{1.1}).isNumericNull());
      Assert.assertTrue(ExprEval.ofDoubleArray(new Double[]{null, 1.1, 2.2}).isNumericNull());
      Assert.assertTrue(ExprEval.ofDoubleArray(new Double[]{null}).isNumericNull());

      Assert.assertFalse(ExprEval.ofStringArray(new String[]{"1"}).isNumericNull());
      Assert.assertTrue(ExprEval.ofStringArray(new String[]{null, "1", "2"}).isNumericNull());
      Assert.assertTrue(ExprEval.ofStringArray(new String[]{"one"}).isNumericNull());
      Assert.assertTrue(ExprEval.ofStringArray(new String[]{null}).isNumericNull());
    }
  }

  @Test
  public void testBooleanReturn()
  {
    Expr.ObjectBinding bindings = InputBindings.withMap(
        ImmutableMap.of("x", 100L, "y", 100L, "z", 100D, "w", 100D)
    );

    try {
      ExpressionProcessing.initializeForStrictBooleansTests(false);
      ExprEval eval = Parser.parse("x==z", ExprMacroTable.nil()).eval(bindings);
      Assert.assertTrue(eval.asBoolean());
      assertEquals(ExpressionType.DOUBLE, eval.type());

      eval = Parser.parse("x!=z", ExprMacroTable.nil()).eval(bindings);
      Assert.assertFalse(eval.asBoolean());
      assertEquals(ExpressionType.DOUBLE, eval.type());

      eval = Parser.parse("z==w", ExprMacroTable.nil()).eval(bindings);
      Assert.assertTrue(eval.asBoolean());
      assertEquals(ExpressionType.DOUBLE, eval.type());

      eval = Parser.parse("z!=w", ExprMacroTable.nil()).eval(bindings);
      Assert.assertFalse(eval.asBoolean());
      assertEquals(ExpressionType.DOUBLE, eval.type());
    }
    finally {
      ExpressionProcessing.initializeForTests(null);
    }
    try {
      ExpressionProcessing.initializeForStrictBooleansTests(true);
      ExprEval eval = Parser.parse("x==y", ExprMacroTable.nil()).eval(bindings);
      Assert.assertTrue(eval.asBoolean());
      assertEquals(ExpressionType.LONG, eval.type());

      eval = Parser.parse("x!=y", ExprMacroTable.nil()).eval(bindings);
      Assert.assertFalse(eval.asBoolean());
      assertEquals(ExpressionType.LONG, eval.type());

      eval = Parser.parse("x==z", ExprMacroTable.nil()).eval(bindings);
      Assert.assertTrue(eval.asBoolean());
      assertEquals(ExpressionType.LONG, eval.type());

      eval = Parser.parse("x!=z", ExprMacroTable.nil()).eval(bindings);
      Assert.assertFalse(eval.asBoolean());
      assertEquals(ExpressionType.LONG, eval.type());

      eval = Parser.parse("z==w", ExprMacroTable.nil()).eval(bindings);
      Assert.assertTrue(eval.asBoolean());
      assertEquals(ExpressionType.LONG, eval.type());

      eval = Parser.parse("z!=w", ExprMacroTable.nil()).eval(bindings);
      Assert.assertFalse(eval.asBoolean());
      assertEquals(ExpressionType.LONG, eval.type());
    }
    finally {
      ExpressionProcessing.initializeForTests(null);
    }
  }

  @Test
  public void testLogicalOperators()
  {
    Expr.ObjectBinding bindings = InputBindings.withMap(
        ImmutableMap.of()
    );

    try {
      ExpressionProcessing.initializeForStrictBooleansTests(true);
      assertEquals(1L, eval("'true' && 'true'", bindings).value());
      assertEquals(0L, eval("'true' && 'false'", bindings).value());
      assertEquals(0L, eval("'false' && 'true'", bindings).value());
      assertEquals(0L, eval("'troo' && 'true'", bindings).value());
      assertEquals(0L, eval("'false' && 'false'", bindings).value());

      assertEquals(1L, eval("'true' || 'true'", bindings).value());
      assertEquals(1L, eval("'true' || 'false'", bindings).value());
      assertEquals(1L, eval("'false' || 'true'", bindings).value());
      assertEquals(1L, eval("'troo' || 'true'", bindings).value());
      assertEquals(0L, eval("'false' || 'false'", bindings).value());

      assertEquals(1L, eval("1 && 1", bindings).value());
      assertEquals(1L, eval("100 && 11", bindings).value());
      assertEquals(0L, eval("1 && 0", bindings).value());
      assertEquals(0L, eval("0 && 1", bindings).value());
      assertEquals(0L, eval("0 && 0", bindings).value());

      assertEquals(1L, eval("1 || 1", bindings).value());
      assertEquals(1L, eval("100 || 11", bindings).value());
      assertEquals(1L, eval("1 || 0", bindings).value());
      assertEquals(1L, eval("0 || 1", bindings).value());
      assertEquals(1L, eval("111 || 0", bindings).value());
      assertEquals(1L, eval("0 || 111", bindings).value());
      assertEquals(0L, eval("0 || 0", bindings).value());

      assertEquals(1L, eval("1.0 && 1.0", bindings).value());
      assertEquals(1L, eval("0.100 && 1.1", bindings).value());
      assertEquals(0L, eval("1.0 && 0.0", bindings).value());
      assertEquals(0L, eval("0.0 && 1.0", bindings).value());
      assertEquals(0L, eval("0.0 && 0.0", bindings).value());

      assertEquals(1L, eval("1.0 || 1.0", bindings).value());
      assertEquals(1L, eval("0.2 || 0.3", bindings).value());
      assertEquals(1L, eval("1.0 || 0.0", bindings).value());
      assertEquals(1L, eval("0.0 || 1.0", bindings).value());
      assertEquals(1L, eval("1.11 || 0.0", bindings).value());
      assertEquals(1L, eval("0.0 || 0.111", bindings).value());
      assertEquals(0L, eval("0.0 || 0.0", bindings).value());

      assertEquals(1L, eval("null || 1", bindings).value());
      assertEquals(1L, eval("1 || null", bindings).value());
      assertEquals(NullHandling.defaultLongValue(), eval("null || 0", bindings).value());
      assertEquals(NullHandling.defaultLongValue(), eval("0 || null", bindings).value());
      // null/null is evaluated as string typed
      assertEquals(NullHandling.defaultLongValue(), eval("null || null", bindings).value());

      assertEquals(NullHandling.defaultLongValue(), eval("null && 1", bindings).value());
      assertEquals(NullHandling.defaultLongValue(), eval("1 && null", bindings).value());
      assertEquals(0L, eval("null && 0", bindings).value());
      assertEquals(0L, eval("0 && null", bindings).value());
      // null/null is evaluated as string typed
      assertEquals(NullHandling.defaultLongValue(), eval("null && null", bindings).value());
    }
    finally {
      // reset
      ExpressionProcessing.initializeForTests(null);
    }

    try {
      // turn on legacy insanity mode
      ExpressionProcessing.initializeForStrictBooleansTests(false);

      assertEquals("true", eval("'true' && 'true'", bindings).value());
      assertEquals("false", eval("'true' && 'false'", bindings).value());
      assertEquals("false", eval("'false' && 'true'", bindings).value());
      assertEquals("troo", eval("'troo' && 'true'", bindings).value());
      assertEquals("false", eval("'false' && 'false'", bindings).value());

      assertEquals("true", eval("'true' || 'true'", bindings).value());
      assertEquals("true", eval("'true' || 'false'", bindings).value());
      assertEquals("true", eval("'false' || 'true'", bindings).value());
      assertEquals("true", eval("'troo' || 'true'", bindings).value());
      assertEquals("false", eval("'false' || 'false'", bindings).value());

      assertEquals(1.0, eval("1.0 && 1.0", bindings).value());
      assertEquals(1.1, eval("0.100 && 1.1", bindings).value());
      assertEquals(0.0, eval("1.0 && 0.0", bindings).value());
      assertEquals(0.0, eval("0.0 && 1.0", bindings).value());
      assertEquals(0.0, eval("0.0 && 0.0", bindings).value());

      assertEquals(1.0, eval("1.0 || 1.0", bindings).value());
      assertEquals(0.2, eval("0.2 || 0.3", bindings).value());
      assertEquals(1.0, eval("1.0 || 0.0", bindings).value());
      assertEquals(1.0, eval("0.0 || 1.0", bindings).value());
      assertEquals(1.11, eval("1.11 || 0.0", bindings).value());
      assertEquals(0.111, eval("0.0 || 0.111", bindings).value());
      assertEquals(0.0, eval("0.0 || 0.0", bindings).value());

      assertEquals(1L, eval("1 && 1", bindings).value());
      assertEquals(11L, eval("100 && 11", bindings).value());
      assertEquals(0L, eval("1 && 0", bindings).value());
      assertEquals(0L, eval("0 && 1", bindings).value());
      assertEquals(0L, eval("0 && 0", bindings).value());

      assertEquals(1L, eval("1 || 1", bindings).value());
      assertEquals(100L, eval("100 || 11", bindings).value());
      assertEquals(1L, eval("1 || 0", bindings).value());
      assertEquals(1L, eval("0 || 1", bindings).value());
      assertEquals(111L, eval("111 || 0", bindings).value());
      assertEquals(111L, eval("0 || 111", bindings).value());
      assertEquals(0L, eval("0 || 0", bindings).value());

      assertEquals(1.0, eval("1.0 && 1.0", bindings).value());
      assertEquals(1.1, eval("0.100 && 1.1", bindings).value());
      assertEquals(0.0, eval("1.0 && 0.0", bindings).value());
      assertEquals(0.0, eval("0.0 && 1.0", bindings).value());
      assertEquals(0.0, eval("0.0 && 0.0", bindings).value());

      assertEquals(1.0, eval("1.0 || 1.0", bindings).value());
      assertEquals(0.2, eval("0.2 || 0.3", bindings).value());
      assertEquals(1.0, eval("1.0 || 0.0", bindings).value());
      assertEquals(1.0, eval("0.0 || 1.0", bindings).value());
      assertEquals(1.11, eval("1.11 || 0.0", bindings).value());
      assertEquals(0.111, eval("0.0 || 0.111", bindings).value());
      assertEquals(0.0, eval("0.0 || 0.0", bindings).value());

      assertEquals(1L, eval("null || 1", bindings).value());
      assertEquals(1L, eval("1 || null", bindings).value());
      assertEquals(0L, eval("null || 0", bindings).value());
      Assert.assertNull(eval("0 || null", bindings).value());
      Assert.assertNull(eval("null || null", bindings).value());

      Assert.assertNull(eval("null && 1", bindings).value());
      Assert.assertNull(eval("1 && null", bindings).value());
      Assert.assertNull(eval("null && 0", bindings).value());
      assertEquals(0L, eval("0 && null", bindings).value());
      assertNull(eval("null && null", bindings).value());
    }
    finally {
      // reset
      ExpressionProcessing.initializeForTests(null);
    }
  }
}
