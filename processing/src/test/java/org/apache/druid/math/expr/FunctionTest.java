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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.NestedDataModule;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Expr.ObjectBinding;
import org.apache.druid.segment.column.TypeStrategies;
import org.apache.druid.segment.column.TypeStrategiesTest;
import org.apache.druid.segment.column.TypeStrategy;
import org.apache.druid.segment.nested.StructuredData;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class FunctionTest extends InitializedNullHandlingTest
{
  private Expr.ObjectBinding bestEffortBindings;
  private Expr.ObjectBinding typedBindings;
  private Expr.ObjectBinding[] allBindings;


  @BeforeClass
  public static void setupClass()
  {
    TypeStrategies.registerComplex(
        TypeStrategiesTest.NULLABLE_TEST_PAIR_TYPE.getComplexTypeName(),
        new TypeStrategiesTest.NullableLongPairTypeStrategy()
    );
    NestedDataModule.registerHandlersAndSerde();
  }

  @Before
  public void setup()
  {
    ImmutableMap.Builder<String, ExpressionType> inputTypesBuilder = ImmutableMap.builder();
    inputTypesBuilder.put("x", ExpressionType.STRING)
                     .put("y", ExpressionType.LONG)
                     .put("z", ExpressionType.DOUBLE)
                     .put("d", ExpressionType.DOUBLE)
                     .put("maxLong", ExpressionType.LONG)
                     .put("minLong", ExpressionType.LONG)
                     .put("f", ExpressionType.DOUBLE)
                     .put("nan", ExpressionType.DOUBLE)
                     .put("inf", ExpressionType.DOUBLE)
                     .put("-inf", ExpressionType.DOUBLE)
                     .put("o", ExpressionType.LONG)
                     .put("od", ExpressionType.DOUBLE)
                     .put("of", ExpressionType.DOUBLE)
                     .put("a", ExpressionType.STRING_ARRAY)
                     .put("b", ExpressionType.LONG_ARRAY)
                     .put("c", ExpressionType.DOUBLE_ARRAY)
                     .put("someComplex", ExpressionType.fromColumnType(TypeStrategiesTest.NULLABLE_TEST_PAIR_TYPE))
                     .put("str1", ExpressionType.STRING)
                     .put("str2", ExpressionType.STRING)
                     .put("nestedArray", ExpressionType.NESTED_DATA);

    final StructuredData nestedArray = StructuredData.wrap(
        ImmutableList.of(
            ImmutableMap.of("x", 2L, "y", 3.3),
            ImmutableMap.of("x", 4L, "y", 6.6)
        )
    );
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    builder.put("x", "foo")
           .put("y", 2)
           .put("z", 3.1)
           .put("d", 34.56D)
           .put("maxLong", Long.MAX_VALUE)
           .put("minLong", Long.MIN_VALUE)
           .put("f", 12.34F)
           .put("nan", Double.NaN)
           .put("inf", Double.POSITIVE_INFINITY)
           .put("-inf", Double.NEGATIVE_INFINITY)
           .put("o", 0)
           .put("od", 0D)
           .put("of", 0F)
           .put("a", new String[]{"foo", "bar", "baz", "foobar"})
           .put("b", new Long[]{1L, 2L, 3L, 4L, 5L})
           .put("c", new Double[]{3.1, 4.2, 5.3})
           .put("someComplex", new TypeStrategiesTest.NullableLongPair(1L, 2L))
           .put("str1", "v1")
           .put("str2", "v2")
           .put("nestedArray", nestedArray);
    bestEffortBindings = InputBindings.forMap(builder.build());
    typedBindings = InputBindings.forMap(
        builder.build(), InputBindings.inspectorFromTypeMap(inputTypesBuilder.build())
    );
    allBindings = new Expr.ObjectBinding[]{bestEffortBindings, typedBindings};
  }

  @Test
  public void testUnknownErrorsAreWrappedAndReported()
  {
    final Expr expr = Parser.parse("abs(x)", ExprMacroTable.nil());

    ObjectBinding bind = new ObjectBinding()
    {

      @Override
      public ExpressionType getType(String name)
      {
        return ExpressionType.LONG_ARRAY;
      }

      @Override
      public Object get(String name)
      {
        throw new RuntimeException("nested-exception");
      }
    };
    DruidException e = Assert.assertThrows(
        DruidException.class,
        () -> {
          expr.eval(bind);
        }
    );

    assertEquals("Function[abs] encountered unknown exception.", e.getMessage());
    assertNotNull(e.getCause());
    assertEquals("nested-exception", e.getCause().getMessage());
  }

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
      assertArrayExpr("concat(x,' ',nonexistent,' ',y)", null);
    }

    assertExpr("concat(z)", "3.1");
    assertArrayExpr("concat()", null);
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
    assertExpr("strlen(nonexistent)", null);
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
    assertExpr("lpad(x, -1, 'ab')", NullHandling.replaceWithDefault() ? null : "");
    assertExpr("lpad(null, 5, 'ab')", null);
    assertExpr("lpad(x, 2, '')", NullHandling.replaceWithDefault() ? null : "fo");
    assertExpr("lpad(x, 6, '')", NullHandling.replaceWithDefault() ? null : "foo");
    assertExpr("lpad('', 3, '*')", NullHandling.replaceWithDefault() ? null : "***");
    assertExpr("lpad(x, 2, null)", null);
    assertExpr("lpad(a, 4, '*')", "[foo");
    assertExpr("lpad(a, 2, '*')", "[f");
    assertExpr("lpad(a, 2, '')", NullHandling.replaceWithDefault() ? null : "[f");
    assertExpr("lpad(b, 4, '*')", "[1, ");
    assertExpr("lpad(b, 2, '')", NullHandling.replaceWithDefault() ? null : "[1");
    assertExpr("lpad(b, 2, null)", null);
    assertExpr("lpad(x, 5, x)", "fofoo");
    assertExpr("lpad(x, 5, y)", "22foo");
    assertExpr("lpad(x, 5, z)", "3.foo");
    assertExpr("lpad(y, 5, x)", "foof2");
    assertExpr("lpad(z, 5, y)", "223.1");
  }

  @Test
  public void testRpad()
  {
    assertExpr("rpad(x, 5, 'ab')", "fooab");
    assertExpr("rpad(x, 4, 'ab')", "fooa");
    assertExpr("rpad(x, 2, 'ab')", "fo");
    assertExpr("rpad(x, -1, 'ab')", NullHandling.replaceWithDefault() ? null : "");
    assertExpr("rpad(null, 5, 'ab')", null);
    assertExpr("rpad(x, 2, '')", NullHandling.replaceWithDefault() ? null : "fo");
    assertExpr("rpad(x, 6, '')", NullHandling.replaceWithDefault() ? null : "foo");
    assertExpr("rpad('', 3, '*')", NullHandling.replaceWithDefault() ? null : "***");
    assertExpr("rpad(x, 2, null)", null);
    assertExpr("rpad(a, 2, '*')", "[f");
    assertExpr("rpad(a, 2, '')", NullHandling.replaceWithDefault() ? null : "[f");
    assertExpr("rpad(b, 4, '*')", "[1, ");
    assertExpr("rpad(b, 2, '')", NullHandling.replaceWithDefault() ? null : "[1");
    assertExpr("rpad(b, 2, null)", null);
    assertExpr("rpad(x, 5, x)", "foofo");
    assertExpr("rpad(x, 5, y)", "foo22");
    assertExpr("rpad(x, 5, z)", "foo3.");
    assertExpr("rpad(y, 5, x)", "2foof");
    assertExpr("rpad(z, 5, y)", "3.122");
  }

  @Test
  public void testArrayConstructor()
  {
    assertArrayExpr("array(1, 2, 3, 4)", new Long[]{1L, 2L, 3L, 4L});
    assertArrayExpr("array(1, 2, 3, 'bar')", new Long[]{1L, 2L, 3L, null});
    assertArrayExpr("array(1.0)", new Double[]{1.0});
    assertArrayExpr("array('foo', 'bar')", new String[]{"foo", "bar"});
    assertArrayExpr(
        "array(a, b)",
        new Object[]{
            new Object[]{"foo", "bar", "baz", "foobar"},
            new Object[]{"1", "2", "3", "4", "5"}
        }
    );
  }

  @Test
  public void testArrayLength()
  {
    assertExpr("array_length([1,2,3])", 3L);
    assertExpr("array_length(a)", 4L);
    // nested types only work with typed bindings right now, and pretty limited support for stuff
    assertExpr("array_length(nestedArray)", 2L, typedBindings);
  }

  @Test
  public void testArrayOffset()
  {
    assertExpr("array_offset([1, 2, 3], 2)", 3L);
    assertArrayExpr("array_offset([1, 2, 3], 3)", null);
    assertExpr("array_offset(a, 2)", "baz");
    // nested types only work with typed bindings right now, and pretty limited support for stuff
    assertExpr("array_offset(nestedArray, 1)", ImmutableMap.of("x", 4L, "y", 6.6), typedBindings);
  }

  @Test
  public void testArrayOrdinal()
  {
    assertExpr("array_ordinal([1, 2, 3], 3)", 3L);
    assertArrayExpr("array_ordinal([1, 2, 3], 4)", null);
    assertExpr("array_ordinal(a, 3)", "baz");
    // nested types only work with typed bindings right now, and pretty limited support for stuff
    assertExpr("array_ordinal(nestedArray, 2)", ImmutableMap.of("x", 4L, "y", 6.6), typedBindings);
  }

  @Test
  public void testArrayOffsetOf()
  {
    assertExpr("array_offset_of([1, 2, 3], 3)", 2L);
    assertExpr("array_offset_of([1, 2, 3], 4)", NullHandling.replaceWithDefault() ? -1L : null);
    assertExpr("array_offset_of(a, 'baz')", 2L);
  }

  @Test
  public void testArrayOrdinalOf()
  {
    assertExpr("array_ordinal_of([1, 2, 3], 3)", 3L);
    assertExpr("array_ordinal_of([1, 2, 3], 4)", NullHandling.replaceWithDefault() ? -1L : null);
    assertExpr("array_ordinal_of(a, 'baz')", 3L);
  }

  @Test
  public void testArrayContains()
  {
    assertExpr("array_contains([1, 2, 3], 2)", 1L);
    assertExpr("array_contains([1, 2, 3], 4)", 0L);
    assertExpr("array_contains([1, 2, 3], [2, 3])", 1L);
    assertExpr("array_contains([1, 2, 3], [3, 4])", 0L);
    assertExpr("array_contains(b, [3, 4])", 1L);
  }

  @Test
  public void testArrayOverlap()
  {
    assertExpr("array_overlap([1, 2, 3], [2, 4, 6])", 1L);
    assertExpr("array_overlap([1, 2, 3], [4, 5, 6])", 0L);
  }

  @Test
  public void testArrayAppend()
  {
    assertArrayExpr("array_append([1, 2, 3], 4)", new Long[]{1L, 2L, 3L, 4L});
    assertArrayExpr("array_append([1, 2, 3], 'bar')", new Long[]{1L, 2L, 3L, null});
    assertArrayExpr("array_append([], 1)", new String[]{"1"});
    assertArrayExpr("array_append(<LONG>[], 1)", new Long[]{1L});
  }

  @Test
  public void testArrayConcat()
  {
    assertArrayExpr("array_concat([1, 2, 3], [2, 4, 6])", new Long[]{1L, 2L, 3L, 2L, 4L, 6L});
    assertArrayExpr("array_concat([1, 2, 3], 4)", new Long[]{1L, 2L, 3L, 4L});
    assertArrayExpr("array_concat(0, [1, 2, 3])", new Long[]{0L, 1L, 2L, 3L});
    assertArrayExpr("array_concat(map(y -> y * 3, b), [1, 2, 3])", new Long[]{3L, 6L, 9L, 12L, 15L, 1L, 2L, 3L});
    assertArrayExpr("array_concat(0, 1)", new Long[]{0L, 1L});
  }

  @Test
  public void testArraySetAdd()
  {
    assertArrayExpr("array_set_add([1, 2, 3], 4)", new Long[]{1L, 2L, 3L, 4L});
    assertArrayExpr("array_set_add([1, 2, 3], 'bar')", new Long[]{null, 1L, 2L, 3L});
    assertArrayExpr("array_set_add([1, 2, 2], 1)", new Long[]{1L, 2L});
    assertArrayExpr("array_set_add([], 1)", new String[]{"1"});
    assertArrayExpr("array_set_add(<LONG>[], 1)", new Long[]{1L});
    assertArrayExpr("array_set_add(<LONG>[], null)", new Long[]{null});
  }

  @Test
  public void testArraySetAddAll()
  {
    assertArrayExpr("array_set_add_all([1, 2, 3], [2, 4, 6])", new Long[]{1L, 2L, 3L, 4L, 6L});
    assertArrayExpr("array_set_add_all([1, 2, 3], 4)", new Long[]{1L, 2L, 3L, 4L});
    assertArrayExpr("array_set_add_all(0, [1, 2, 3])", new Long[]{0L, 1L, 2L, 3L});
    assertArrayExpr("array_set_add_all(map(y -> y * 3, b), [1, 2, 3])", new Long[]{1L, 2L, 3L, 6L, 9L, 12L, 15L});
    assertArrayExpr("array_set_add_all(0, 1)", new Long[]{0L, 1L});
  }

  @Test
  public void testArrayToString()
  {
    assertExpr("array_to_string([1, 2, 3], ',')", "1,2,3");
    assertExpr("array_to_string([1], '|')", "1");
    assertExpr("array_to_string(a, '|')", "foo|bar|baz|foobar");
  }

  @Test
  public void testStringToArray()
  {
    assertArrayExpr("string_to_array('1,2,3', ',')", new String[]{"1", "2", "3"});
    assertArrayExpr("string_to_array(null, ',')", null);
    assertArrayExpr("string_to_array('1', ',')", new String[]{"1"});
    assertArrayExpr("string_to_array(array_to_string(a, ','), ',')", new String[]{"foo", "bar", "baz", "foobar"});
  }

  @Test
  public void testArrayCastLegacy()
  {
    assertArrayExpr("cast([1, 2, 3], 'STRING_ARRAY')", new String[]{"1", "2", "3"});
    assertArrayExpr("cast([1, 2, 3], 'DOUBLE_ARRAY')", new Double[]{1.0, 2.0, 3.0});
    assertArrayExpr("cast(c, 'LONG_ARRAY')", new Long[]{3L, 4L, 5L});
    assertArrayExpr(
        "cast(string_to_array(array_to_string(b, ','), ','), 'LONG_ARRAY')",
        new Long[]{1L, 2L, 3L, 4L, 5L}
    );
    assertArrayExpr("cast(['1.0', '2.0', '3.0'], 'LONG_ARRAY')", new Long[]{1L, 2L, 3L});
  }

  @Test
  public void testArrayCast()
  {
    assertArrayExpr("cast([1, 2, 3], 'ARRAY<STRING>')", new String[]{"1", "2", "3"});
    assertArrayExpr("cast([1, 2, 3], 'ARRAY<DOUBLE>')", new Double[]{1.0, 2.0, 3.0});
    assertArrayExpr("cast(c, 'ARRAY<LONG>')", new Long[]{3L, 4L, 5L});
    assertArrayExpr(
        "cast(string_to_array(array_to_string(b, ','), ','), 'ARRAY<LONG>')",
        new Long[]{1L, 2L, 3L, 4L, 5L}
    );
    assertArrayExpr("cast(['1.0', '2.0', '3.0'], 'ARRAY<LONG>')", new Long[]{1L, 2L, 3L});
  }

  @Test
  public void testArraySlice()
  {
    assertArrayExpr("array_slice([1, 2, 3, 4], 1, 3)", new Long[]{2L, 3L});
    assertArrayExpr("array_slice([1.0, 2.1, 3.2, 4.3], 2)", new Double[]{3.2, 4.3});
    assertArrayExpr("array_slice(['a', 'b', 'c', 'd'], 4, 6)", new String[]{null, null});
    assertArrayExpr("array_slice([1, 2, 3, 4], 2, 2)", new Long[]{});
    assertArrayExpr("array_slice([1, 2, 3, 4], 5, 7)", null);
    assertArrayExpr("array_slice([1, 2, 3, 4], 2, 1)", null);
  }

  @Test
  public void testArrayPrepend()
  {
    assertArrayExpr("array_prepend(4, [1, 2, 3])", new Long[]{4L, 1L, 2L, 3L});
    assertArrayExpr("array_prepend('bar', [1, 2, 3])", new Long[]{null, 1L, 2L, 3L});
    assertArrayExpr("array_prepend(1, [])", new String[]{"1"});
    assertArrayExpr("array_prepend(1, <LONG>[])", new Long[]{1L});
    assertArrayExpr("array_prepend(1, <DOUBLE>[])", new Double[]{1.0});
  }

  @Test
  public void testRoundWithNonNumericValuesShouldReturn0()
  {
    assertExpr("round(nan)", 0D);
    assertExpr("round(nan, 5)", 0D);
    //CHECKSTYLE.OFF: Regexp
    assertExpr("round(inf)", Double.MAX_VALUE);
    assertExpr("round(inf, 4)", Double.MAX_VALUE);
    assertExpr("round(-inf)", -1 * Double.MAX_VALUE);
    assertExpr("round(-inf, 3)", -1 * Double.MAX_VALUE);
    assertExpr("round(-inf, -5)", -1 * Double.MAX_VALUE);
    //CHECKSTYLE.ON: Regexp

    // Calculations that result in non numeric numbers
    assertExpr("round(0/od)", 0D);
    assertExpr("round(od/od)", 0D);
    //CHECKSTYLE.OFF: Regexp
    assertExpr("round(1/od)", Double.MAX_VALUE);
    assertExpr("round(-1/od)", -1 * Double.MAX_VALUE);
    //CHECKSTYLE.ON: Regexp

    assertExpr("round(0/of)", 0D);
    assertExpr("round(of/of)", 0D);
    //CHECKSTYLE.OFF: Regexp
    assertExpr("round(1/of)", Double.MAX_VALUE);
    assertExpr("round(-1/of)", -1 * Double.MAX_VALUE);
    //CHECKSTYLE.ON: Regexp
  }

  @Test
  public void testRoundWithLong()
  {
    assertExpr("round(y)", 2L);
    assertExpr("round(y, 2)", 2L);
    assertExpr("round(y, -1)", 0L);
  }

  @Test
  public void testRoundWithDouble()
  {
    assertExpr("round(d)", 35D);
    assertExpr("round(d, 2)", 34.56D);
    assertExpr("round(d, y)", 34.56D);
    assertExpr("round(d, 1)", 34.6D);
    assertExpr("round(d, -1)", 30D);
  }

  @Test
  public void testRoundWithFloat()
  {
    assertExpr("round(f)", 12D);
    assertExpr("round(f, 2)", 12.34D);
    assertExpr("round(f, y)", 12.34D);
    assertExpr("round(f, 1)", 12.3D);
    assertExpr("round(f, -1)", 10D);
  }

  @Test
  public void testRoundWithExtremeNumbers()
  {
    assertExpr("round(maxLong)", BigDecimal.valueOf(Long.MAX_VALUE).setScale(0, RoundingMode.HALF_UP).longValue());
    assertExpr("round(minLong)", BigDecimal.valueOf(Long.MIN_VALUE).setScale(0, RoundingMode.HALF_UP).longValue());
    // overflow
    assertExpr(
        "round(maxLong + 1, 1)",
        BigDecimal.valueOf(Long.MIN_VALUE).setScale(1, RoundingMode.HALF_UP).longValue()
    );
    // underflow
    assertExpr(
        "round(minLong - 1, -2)",
        BigDecimal.valueOf(Long.MAX_VALUE).setScale(-2, RoundingMode.HALF_UP).longValue()
    );

    assertExpr(
        "round(CAST(maxLong, 'DOUBLE') + 1, 1)",
        BigDecimal.valueOf(((double) Long.MAX_VALUE) + 1).setScale(1, RoundingMode.HALF_UP).doubleValue()
    );
    assertExpr(
        "round(CAST(minLong, 'DOUBLE') - 1, -2)",
        BigDecimal.valueOf(((double) Long.MIN_VALUE) - 1).setScale(-2, RoundingMode.HALF_UP).doubleValue()
    );
  }

  @Test
  public void testRoundWithNullValueOrInvalid()
  {
    Set<Pair<String, String>> invalidArguments = ImmutableSet.of(
        Pair.of("null", "STRING"),
        Pair.of("x", "STRING"),
        Pair.of("b", "ARRAY<LONG>"),
        Pair.of("c", "ARRAY<DOUBLE>"),
        Pair.of("a", "ARRAY<STRING>")
    );
    for (Pair<String, String> argAndType : invalidArguments) {
      if (NullHandling.sqlCompatible()) {
        assertExpr(StringUtils.format("round(%s)", argAndType.lhs), null);
      } else {
        Throwable t = Assert.assertThrows(
            DruidException.class,
            () -> assertExpr(StringUtils.format("round(%s)", argAndType.lhs), null)
        );
        Assert.assertEquals(
            StringUtils.format(
                "Function[round] first argument should be a LONG or DOUBLE but got %s instead",
                argAndType.rhs
            ),
            t.getMessage()
        );
      }
    }
  }

  @Test
  public void testRoundWithInvalidSecondArgument()
  {
    Set<Pair<String, String>> invalidArguments = ImmutableSet.of(
        Pair.of("1.2", "DOUBLE"),
        Pair.of("x", "STRING"),
        Pair.of("a", "ARRAY<STRING>"),
        Pair.of("c", "ARRAY<DOUBLE>")

    );
    for (Pair<String, String> argAndType : invalidArguments) {
      Throwable t = Assert.assertThrows(
          DruidException.class,
          () -> assertExpr(StringUtils.format("round(d, %s)", argAndType.lhs), null)
      );
      Assert.assertEquals(
          StringUtils.format(
              "Function[round] second argument should be a LONG but got %s instead",
              argAndType.rhs
          ),
          t.getMessage()
      );
    }
  }

  @Test
  public void testGreatest()
  {
    // Same types
    assertExpr("greatest(y, 0)", 2L);
    assertExpr("greatest(34.0, z, 5.0, 767.0)", 767.0);
    assertExpr("greatest('B', x, 'A')", "foo");

    // Different types
    assertExpr("greatest(-1, z, 'A')", "A");
    assertExpr("greatest(-1, z)", 3.1);
    assertExpr("greatest(1, 'A')", "A");

    // Invalid types
    Throwable t = Assert.assertThrows(
        DruidException.class,
        () -> assertExpr("greatest(1, ['A'])", null)
    );
    Assert.assertEquals("Function[greatest] does not accept ARRAY<STRING> types", t.getMessage());

    // Null handling
    assertExpr("greatest()", null);
    assertExpr("greatest(null, null)", null);
    assertExpr("greatest(1, null, 'A')", "A");
  }

  @Test
  public void testLeast()
  {
    // Same types
    assertExpr("least(y, 0)", 0L);
    assertExpr("least(34.0, z, 5.0, 767.0)", 3.1);
    assertExpr("least('B', x, 'A')", "A");

    // Different types
    assertExpr("least(-1, z, 'A')", "-1");
    assertExpr("least(-1, z)", -1.0);
    assertExpr("least(1, 'A')", "1");

    // Invalid types
    Throwable t = Assert.assertThrows(
        DruidException.class,
        () -> assertExpr("least(1, [2, 3])", null)
    );
    Assert.assertEquals("Function[least] does not accept ARRAY<LONG> types", t.getMessage());

    // Null handling
    assertExpr("least()", null);
    assertExpr("least(null, null)", null);
    assertExpr("least(1, null, 'A')", "1");
  }

  @Test
  public void testSizeFormat()
  {
    assertExpr("human_readable_binary_byte_format(-1024)", "-1.00 KiB");
    assertExpr("human_readable_binary_byte_format(1024)", "1.00 KiB");
    assertExpr("human_readable_binary_byte_format(1024*1024)", "1.00 MiB");
    assertExpr("human_readable_binary_byte_format(1024*1024*1024)", "1.00 GiB");
    assertExpr("human_readable_binary_byte_format(1024*1024*1024*1024)", "1.00 TiB");
    assertExpr("human_readable_binary_byte_format(1024*1024*1024*1024*1024)", "1.00 PiB");

    assertExpr("human_readable_decimal_byte_format(-1000)", "-1.00 KB");
    assertExpr("human_readable_decimal_byte_format(1000)", "1.00 KB");
    assertExpr("human_readable_decimal_byte_format(1000*1000)", "1.00 MB");
    assertExpr("human_readable_decimal_byte_format(1000*1000*1000)", "1.00 GB");
    assertExpr("human_readable_decimal_byte_format(1000*1000*1000*1000)", "1.00 TB");

    assertExpr("human_readable_decimal_format(-1000)", "-1.00 K");
    assertExpr("human_readable_decimal_format(1000)", "1.00 K");
    assertExpr("human_readable_decimal_format(1000*1000)", "1.00 M");
    assertExpr("human_readable_decimal_format(1000*1000*1000)", "1.00 G");
    assertExpr("human_readable_decimal_format(1000*1000*1000*1000)", "1.00 T");
  }

  @Test
  public void testSizeFormatWithDifferentPrecision()
  {
    assertExpr("human_readable_binary_byte_format(1024, 0)", "1 KiB");
    assertExpr("human_readable_binary_byte_format(1024*1024, 1)", "1.0 MiB");
    assertExpr("human_readable_binary_byte_format(1024*1024*1024, 2)", "1.00 GiB");
    assertExpr("human_readable_binary_byte_format(1024*1024*1024*1024, 3)", "1.000 TiB");

    assertExpr("human_readable_decimal_byte_format(1234, 0)", "1 KB");
    assertExpr("human_readable_decimal_byte_format(1234*1000, 1)", "1.2 MB");
    assertExpr("human_readable_decimal_byte_format(1234*1000*1000, 2)", "1.23 GB");
    assertExpr("human_readable_decimal_byte_format(1234*1000*1000*1000, 3)", "1.234 TB");

    assertExpr("human_readable_decimal_format(1234, 0)", "1 K");
    assertExpr("human_readable_decimal_format(1234*1000,1)", "1.2 M");
    assertExpr("human_readable_decimal_format(1234*1000*1000,2)", "1.23 G");
    assertExpr("human_readable_decimal_format(1234*1000*1000*1000,3)", "1.234 T");
  }

  @Test
  public void testSizeFormatWithEdgeCases()
  {
    //a nonexist value is null which is treated as 0
    assertExpr("human_readable_binary_byte_format(nonexist)", NullHandling.sqlCompatible() ? null : "0 B");

    //f = 12.34
    assertExpr("human_readable_binary_byte_format(f)", "12 B");

    //nan is Double.NaN
    assertExpr("human_readable_binary_byte_format(nan)", "0 B");

    //inf = Double.POSITIVE_INFINITY
    assertExpr("human_readable_binary_byte_format(inf)", "8.00 EiB");

    //inf = Double.NEGATIVE_INFINITY
    assertExpr("human_readable_binary_byte_format(-inf)", "-8.00 EiB");

    // o = 0
    assertExpr("human_readable_binary_byte_format(o)", "0 B");

    // od = 0D
    assertExpr("human_readable_binary_byte_format(od)", "0 B");

    // of = 0F
    assertExpr("human_readable_binary_byte_format(of)", "0 B");
  }

  @Test
  public void testSizeForatInvalidArgumentType()
  {
    if (NullHandling.replaceWithDefault()) {
      //x = "foo"
      Throwable t = Assert.assertThrows(
          DruidException.class,
          () -> Parser.parse("human_readable_binary_byte_format(x)", ExprMacroTable.nil())
                      .eval(bestEffortBindings)
      );
      Assert.assertEquals(
          "Function[human_readable_binary_byte_format] needs a number as its first argument but got STRING instead",
          t.getMessage()
      );
    }

    // x = "foo"
    Throwable t = Assert.assertThrows(
        DruidException.class,
        () -> Parser.parse("human_readable_binary_byte_format(1024, x)", ExprMacroTable.nil()).eval(bestEffortBindings)
    );
    Assert.assertEquals(
        "Function[human_readable_binary_byte_format] needs a LONG as its second argument but got STRING instead",
        t.getMessage()
    );
    //of = 0F
    t = Assert.assertThrows(
        DruidException.class,
        () -> Parser.parse("human_readable_binary_byte_format(1024, of)", ExprMacroTable.nil()).eval(bestEffortBindings)
    );
    Assert.assertEquals(
        "Function[human_readable_binary_byte_format] needs a LONG as its second argument but got DOUBLE instead",
        t.getMessage()
    );

    //of = 0F
    t = Assert.assertThrows(
        DruidException.class,
        () -> Parser.parse("human_readable_binary_byte_format(1024, nonexist)", ExprMacroTable.nil())
                    .eval(bestEffortBindings)
    );
    Assert.assertEquals(
        "Function[human_readable_binary_byte_format] needs a LONG as its second argument but got STRING instead",
        t.getMessage()
    );
  }

  @Test
  public void testSizeFormatInvalidPrecision()
  {
    Throwable t = Assert.assertThrows(
        DruidException.class,
        () -> Parser.parse("human_readable_binary_byte_format(1024, maxLong)", ExprMacroTable.nil())
                    .eval(bestEffortBindings)
    );

    Assert.assertEquals(
        "Function[human_readable_binary_byte_format] given precision[9223372036854775807] must be in the range of [0,3]",
        t.getMessage()
    );

    t = Assert.assertThrows(
        DruidException.class,
        () -> Parser.parse("human_readable_binary_byte_format(1024, minLong)", ExprMacroTable.nil())
                    .eval(bestEffortBindings)
    );
    Assert.assertEquals(
        "Function[human_readable_binary_byte_format] given precision[-9223372036854775808] must be in the range of [0,3]",
        t.getMessage()
    );

    t = Assert.assertThrows(
        DruidException.class,
        () -> Parser.parse("human_readable_binary_byte_format(1024, -1)", ExprMacroTable.nil()).eval(bestEffortBindings)
    );
    Assert.assertEquals(
        "Function[human_readable_binary_byte_format] given precision[-1] must be in the range of [0,3]",
        t.getMessage()
    );

    t = Assert.assertThrows(
        DruidException.class,
        () -> Parser.parse("human_readable_binary_byte_format(1024, 4)", ExprMacroTable.nil()).eval(bestEffortBindings)
    );
    Assert.assertEquals(
        "Function[human_readable_binary_byte_format] given precision[4] must be in the range of [0,3]",
        t.getMessage()
    );
  }

  @Test
  public void testSizeFormatInvalidArgumentSize()
  {
    Throwable t = Assert.assertThrows(
        ExpressionValidationException.class,
        () -> Parser.parse(
            "human_readable_binary_byte_format(1024, 2, 3)",
            ExprMacroTable.nil()
        ).eval(bestEffortBindings)
    );

    Assert.assertEquals("Function[human_readable_binary_byte_format] requires 1 or 2 arguments", t.getMessage());
  }

  @Test
  public void testSafeDivide()
  {
    // happy path maths
    assertExpr("safe_divide(3, 1)", 3L);
    assertExpr("safe_divide(4.5, 2)", 2.25);
    assertExpr("safe_divide(3, 0)", null);
    assertExpr("safe_divide(1, 0.0)", null);
    // NaN, Infinity and other weird cases
    assertExpr("safe_divide(NaN, 0.0)", null);
    assertExpr("safe_divide(0, NaN)", 0.0);
    assertExpr("safe_divide(0, maxLong)", 0L);
    assertExpr("safe_divide(maxLong,0)", null);
    assertExpr("safe_divide(0.0, inf)", 0.0);
    assertExpr("safe_divide(0.0, -inf)", -0.0);
    assertExpr("safe_divide(0,0)", null);
  }

  @Test
  public void testBitwise()
  {
    // happy path maths
    assertExpr("bitwiseAnd(3, 1)", 1L);
    assertExpr("bitwiseAnd(2, 1)", 0L);
    assertExpr("bitwiseOr(3, 1)", 3L);
    assertExpr("bitwiseOr(2, 1)", 3L);
    assertExpr("bitwiseXor(3, 1)", 2L);
    assertExpr("bitwiseXor(2, 1)", 3L);
    assertExpr("bitwiseShiftLeft(2, 1)", 4L);
    assertExpr("bitwiseShiftRight(2, 1)", 1L);
    assertExpr("bitwiseAnd(bitwiseComplement(1), 7)", 6L);

    // funny types
    // two strings is sad
    assertExpr("bitwiseAnd('2', '1')", null);
    // but one is ok, druid forgives you
    assertExpr("bitwiseAnd(3, '1')", 1L);
    assertExpr("bitwiseAnd(2, null)", NullHandling.replaceWithDefault() ? 0L : null);

    // unary doesn't accept any slop
    assertExpr("bitwiseComplement('1')", null);
    assertExpr("bitwiseComplement(null)", null);

    // data truncation
    Throwable t = Assert.assertThrows(
        DruidException.class,
        () -> assertExpr("bitwiseComplement(461168601842738800000000000000.000000)", null)
    );
    Assert.assertEquals(
        "Function[bitwiseComplement] Possible data truncation, param [461168601842738800000000000000.000000] is out of LONG value range",
        t.getMessage()
    );

    // doubles are cast
    assertExpr("bitwiseOr(2.345, 1)", 3L);
    assertExpr("bitwiseOr(2, 1.3)", 3L);
    assertExpr("bitwiseAnd(2.345, 2.0)", 2L);

    // but can be converted to be double-like
    assertExpr(
        "bitwiseAnd(bitwiseConvertDoubleToLongBits(2.345), bitwiseConvertDoubleToLongBits(2.0))",
        4611686018427387904L
    );
    assertExpr(
        "bitwiseConvertLongBitsToDouble(bitwiseAnd(bitwiseConvertDoubleToLongBits(2.345), bitwiseConvertDoubleToLongBits(2.0)))",
        2.0
    );
    assertExpr("bitwiseConvertDoubleToLongBits(2.0)", 4611686018427387904L);
    assertExpr("bitwiseConvertDoubleToLongBits(bitwiseConvertDoubleToLongBits(2.0))", 4886405595696988160L);
    assertExpr("bitwiseConvertLongBitsToDouble(4611686018427387904)", 2.0);
    assertExpr("bitwiseConvertLongBitsToDouble(bitwiseConvertLongBitsToDouble(4611686018427387904))", 1.0E-323);

    // conversion returns null if nonsense inputs
    assertExpr("bitwiseConvertLongBitsToDouble('wat')", null);
    assertExpr("bitwiseConvertLongBitsToDouble('1')", null);
    assertExpr("bitwiseConvertLongBitsToDouble(null)", null);
    assertExpr("bitwiseConvertDoubleToLongBits('wat')", null);
    assertExpr("bitwiseConvertDoubleToLongBits('1.0')", null);
    assertExpr("bitwiseConvertDoubleToLongBits(null)", null);
  }

  @Test
  public void testRepeat()
  {
    assertExpr("repeat('hello', 2)", "hellohello");
    assertExpr("repeat('hello', -1)", null);
    assertExpr("repeat(null, 10)", null);
    assertExpr("repeat(nonexistent, 10)", null);
  }

  @Test
  public void testDecodeBase64UTF()
  {
    assertExpr("decode_base64_utf8('aGVsbG8=')", "hello");
    assertExpr(
        "decode_base64_utf8('V2hlbiBhbiBvbmlvbiBpcyBjdXQsIGNlcnRhaW4gKGxhY2hyeW1hdG9yKSBjb21wb3VuZHMgYXJlIHJlbGVhc2VkIGNhdXNpbmcgdGhlIG5lcnZlcyBhcm91bmQgdGhlIGV5ZXMgKGxhY3JpbWFsIGdsYW5kcykgdG8gYmVjb21lIGlycml0YXRlZC4=')",
        "When an onion is cut, certain (lachrymator) compounds are released causing the nerves around the eyes (lacrimal glands) to become irritated."
    );
    assertExpr("decode_base64_utf8('eyJ0ZXN0IjogMX0=')", "{\"test\": 1}");
    assertExpr("decode_base64_utf8('')", NullHandling.sqlCompatible() ? "" : null);
  }

  @Test
  public void testComplexDecode()
  {
    TypeStrategiesTest.NullableLongPair expected = new TypeStrategiesTest.NullableLongPair(1L, 2L);
    TypeStrategy strategy = TypeStrategiesTest.NULLABLE_TEST_PAIR_TYPE.getStrategy();

    final byte[] bytes = new byte[strategy.estimateSizeBytes(expected)];
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    int written = strategy.write(buffer, expected, bytes.length);
    Assert.assertEquals(bytes.length, written);
    assertExpr(
        StringUtils.format(
            "complex_decode_base64('%s', '%s')",
            TypeStrategiesTest.NULLABLE_TEST_PAIR_TYPE.getComplexTypeName(),
            StringUtils.encodeBase64String(bytes)
        ),
        expected
    );
    // test with alias
    assertExpr(
        StringUtils.format(
            "decode_base64_complex('%s', '%s')",
            TypeStrategiesTest.NULLABLE_TEST_PAIR_TYPE.getComplexTypeName(),
            StringUtils.encodeBase64String(bytes)
        ),
        expected
    );
  }

  @Test
  public void testMacrosWithMultipleAliases()
  {
    ExprMacroTable.ExprMacro drinkMacro = new ExprMacroTable.ExprMacro()
    {
      @Override
      public Expr apply(List<Expr> args)
      {
        return new StringExpr("happiness");
      }

      @Override
      public String name()
      {
        return "drink";
      }
    };
    List<ExprMacroTable.ExprMacro> macros = new ArrayList<>();
    macros.add(drinkMacro);
    List<String> aliases = Arrays.asList("tea", "coffee", "chai", "chaha", "kevha", "chay");
    for (String tea : aliases) {
      macros.add(new ExprMacroTable.AliasExprMacro(drinkMacro, tea));
    }
    final ExprMacroTable exprMacroTable = new ExprMacroTable(macros);
    final Expr happiness = new StringExpr("happiness");
    Assert.assertEquals(happiness, Parser.parse("drink(1,2)", exprMacroTable));
    for (String tea : aliases) {
      Assert.assertEquals(happiness, Parser.parse(StringUtils.format("%s(1,2)", tea), exprMacroTable));
    }
  }

  @Test
  public void testComplexDecodeNull()
  {
    assertExpr(
        StringUtils.format(
            "complex_decode_base64('%s', null)",
            TypeStrategiesTest.NULLABLE_TEST_PAIR_TYPE.getComplexTypeName()
        ),
        null
    );
    assertExpr(
        StringUtils.format(
            "decode_base64_complex('%s', null)",
            TypeStrategiesTest.NULLABLE_TEST_PAIR_TYPE.getComplexTypeName()
        ),
        null
    );
  }

  @Test
  public void testComplexDecodeBaseWrongArgCount()
  {
    Throwable t = Assert.assertThrows(
        ExpressionValidationException.class,
        () -> assertExpr("complex_decode_base64(string)", null)
    );
    Assert.assertEquals("Function[complex_decode_base64] requires 2 arguments", t.getMessage());
  }

  @Test
  public void testComplexDecodeBaseArg0Null()
  {
    Throwable t = Assert.assertThrows(
        ExpressionValidationException.class,
        () -> assertExpr("complex_decode_base64(null, string)", null)
    );
    Assert.assertEquals(
        "Function[complex_decode_base64] first argument must be constant STRING expression containing a valid complex type name but got NULL instead",
        t.getMessage()
    );
  }

  @Test
  public void testComplexDecodeBaseArg0BadType()
  {
    Throwable t = Assert.assertThrows(
        ExpressionValidationException.class,
        () -> assertExpr("complex_decode_base64(1, string)", null)
    );
    Assert.assertEquals(
        "Function[complex_decode_base64] first argument must be constant STRING expression containing a valid complex type name but got '1' instead",
        t.getMessage()
    );
  }

  @Test
  public void testComplexDecodeBaseArg0Unknown()
  {
    Throwable t = Assert.assertThrows(
        ExpressionValidationException.class,
        () -> assertExpr("complex_decode_base64('unknown', string)", null)
    );
    Assert.assertEquals(
        "Function[complex_decode_base64] first argument must be a valid COMPLEX type name, got unknown COMPLEX type [COMPLEX<unknown>]",
        t.getMessage()
    );
  }

  @Test
  public void testMultiValueStringToArrayWithValidInputs()
  {
    assertArrayExpr("mv_to_array(x)", new String[]{"foo"});
    assertArrayExpr("mv_to_array(a)", new String[]{"foo", "bar", "baz", "foobar"});
    assertArrayExpr("mv_to_array(b)", new String[]{"1", "2", "3", "4", "5"});
    assertArrayExpr("mv_to_array(c)", new String[]{"3.1", "4.2", "5.3"});
  }

  @Test
  public void testMultiValueStringToArrayWithInvalidInputs()
  {
    Throwable t = Assert.assertThrows(
        ExpressionValidationException.class,
        () -> assertArrayExpr("mv_to_array('1')", null)
    );
    Assert.assertEquals(
        "Function[mv_to_array] argument 1 should be an identifier expression. Use array() instead",
        t.getMessage()
    );

    t = Assert.assertThrows(
        ExpressionValidationException.class,
        () -> assertArrayExpr("mv_to_array(repeat('hello', 2))", null)
    );
    Assert.assertEquals(
        "Function[mv_to_array] argument (repeat [hello, 2]) should be an identifier expression. Use array() instead",
        t.getMessage()
    );

    t = Assert.assertThrows(
        ExpressionValidationException.class,
        () -> assertArrayExpr("mv_to_array(x,y)", null)
    );
    Assert.assertEquals(
        "Function[mv_to_array] requires 1 argument",
        t.getMessage()
    );

    t = Assert.assertThrows(
        ExpressionValidationException.class,
        () -> assertArrayExpr("mv_to_array()", null)
    );
    Assert.assertEquals(
        "Function[mv_to_array] requires 1 argument",
        t.getMessage()
    );
  }

  @Test
  public void testArrayToMultiValueStringWithValidInputs()
  {
    assertArrayExpr("array_to_mv(x)", new String[]{"foo"});
    assertArrayExpr("array_to_mv(a)", new String[]{"foo", "bar", "baz", "foobar"});
    assertArrayExpr("array_to_mv(b)", new String[]{"1", "2", "3", "4", "5"});
    assertArrayExpr("array_to_mv(c)", new String[]{"3.1", "4.2", "5.3"});
    assertArrayExpr("array_to_mv(array(y,z))", new String[]{"2", "3"});
    // array type is determined by the first array type
    assertArrayExpr("array_to_mv(array_concat(b,c))", new String[]{"1", "2", "3", "4", "5", "3", "4", "5"});
    assertArrayExpr(
        "array_to_mv(array_concat(c,b))",
        new String[]{"3.1", "4.2", "5.3", "1.0", "2.0", "3.0", "4.0", "5.0"}
    );
  }

  @Test
  public void testArrayToMultiValueStringWithInvalidInputs()
  {
    Throwable t = Assert.assertThrows(
        ExpressionValidationException.class,
        () -> assertArrayExpr("mv_to_array('1')", null)
    );
    Assert.assertEquals(
        "Function[mv_to_array] argument 1 should be an identifier expression. Use array() instead",
        t.getMessage()
    );
    t = Assert.assertThrows(
        ExpressionValidationException.class,
        () -> assertArrayExpr("mv_to_array(repeat('hello', 2))", null)
    );
    Assert.assertEquals(
        "Function[mv_to_array] argument (repeat [hello, 2]) should be an identifier expression. Use array() instead",
        t.getMessage()
    );
    t = Assert.assertThrows(
        ExpressionValidationException.class,
        () -> assertArrayExpr("mv_to_array(x,y)", null)
    );
    Assert.assertEquals(
        "Function[mv_to_array] requires 1 argument",
        t.getMessage()
    );
    t = Assert.assertThrows(
        ExpressionValidationException.class,
        () -> assertArrayExpr("mv_to_array()", null)
    );
    Assert.assertEquals(
        "Function[mv_to_array] requires 1 argument",
        t.getMessage()
    );
  }

  @Test
  public void testPlusOnString()
  {
    assertExpr("str1 + str2", "v1v2");
  }

  @Test
  public void testMultiplyOnString()
  {
    Throwable t = Assert.assertThrows(
        IAE.class,
        () -> assertExpr("str1 * str2", null)
    );
    Assert.assertEquals(
        "operator '*' in expression (\"str1\" * \"str2\") is not supported on type STRING.",
        t.getMessage()
    );
  }

  @Test
  public void testMinusOnString()
  {
    Throwable t = Assert.assertThrows(
        IAE.class,
        () -> assertExpr("str1 - str2", null)
    );
    Assert.assertEquals(
        "operator '-' in expression (\"str1\" - \"str2\") is not supported on type STRING.",
        t.getMessage()
    );
  }

  @Test
  public void testDivOnString()
  {
    Throwable t = Assert.assertThrows(
        IAE.class,
        () -> assertExpr("str1 / str2", null)
    );
    Assert.assertEquals(
        "operator '/' in expression (\"str1\" / \"str2\") is not supported on type STRING.",
        t.getMessage()
    );
  }

  private void assertExpr(final String expression, @Nullable final Object expectedResult)
  {
    for (Expr.ObjectBinding toUse : allBindings) {
      assertExpr(expression, expectedResult, toUse);
    }
  }

  private void assertExpr(
      final String expression,
      @Nullable final Object expectedResult,
      Expr.ObjectBinding bindings
  )
  {
    final Expr expr = Parser.parse(expression, ExprMacroTable.nil());
    Assert.assertEquals(expression, expectedResult, expr.eval(bindings).value());

    final Expr exprNoFlatten = Parser.parse(expression, ExprMacroTable.nil(), false);
    final Expr roundTrip = Parser.parse(exprNoFlatten.stringify(), ExprMacroTable.nil());
    Assert.assertEquals(expr.stringify(), expectedResult, roundTrip.eval(bindings).value());

    final Expr roundTripFlatten = Parser.parse(expr.stringify(), ExprMacroTable.nil());
    Assert.assertEquals(expr.stringify(), expectedResult, roundTripFlatten.eval(bindings).value());

    Assert.assertEquals(expr.stringify(), roundTrip.stringify());
    Assert.assertEquals(expr.stringify(), roundTripFlatten.stringify());
    Assert.assertArrayEquals(expr.getCacheKey(), roundTrip.getCacheKey());
    Assert.assertArrayEquals(expr.getCacheKey(), roundTripFlatten.getCacheKey());
  }

  private void assertArrayExpr(final String expression, @Nullable final Object[] expectedResult)
  {

    for (Expr.ObjectBinding toUse : allBindings) {
      assertArrayExpr(expression, expectedResult, toUse);
    }
  }

  private void assertArrayExpr(
      final String expression,
      @Nullable final Object[] expectedResult,
      Expr.ObjectBinding bindings
  )
  {
    final Expr expr = Parser.parse(expression, ExprMacroTable.nil());
    Assert.assertArrayEquals(expression, expectedResult, expr.eval(bindings).asArray());

    final Expr exprNoFlatten = Parser.parse(expression, ExprMacroTable.nil(), false);
    final Expr roundTrip = Parser.parse(exprNoFlatten.stringify(), ExprMacroTable.nil());
    Assert.assertArrayEquals(expression, expectedResult, roundTrip.eval(bindings).asArray());

    final Expr roundTripFlatten = Parser.parse(expr.stringify(), ExprMacroTable.nil());
    Assert.assertArrayEquals(expression, expectedResult, roundTripFlatten.eval(bindings).asArray());

    Assert.assertEquals(expr.stringify(), roundTrip.stringify());
    Assert.assertEquals(expr.stringify(), roundTripFlatten.stringify());
    Assert.assertArrayEquals(expr.getCacheKey(), roundTrip.getCacheKey());
    Assert.assertArrayEquals(expr.getCacheKey(), roundTripFlatten.getCacheKey());
  }
}
