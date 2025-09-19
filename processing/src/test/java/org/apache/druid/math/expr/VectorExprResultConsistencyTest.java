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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.druid.guice.DruidGuiceExtensions;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.vector.ExprEvalVector;
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
import org.apache.druid.query.expression.LookupExprMacro;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.lookup.TestMapLookupExtractorFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BooleanSupplier;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * randomize inputs to various vector expressions and make sure the results match nonvectorized expressions
 *
 * this is not a replacement for correctness tests, but will ensure that vectorized and non-vectorized expression
 * evaluation is at least self-consistent...
 */
public class VectorExprResultConsistencyTest extends InitializedNullHandlingTest
{
  private static final Logger log = new Logger(VectorExprResultConsistencyTest.class);
  private static final int NUM_ITERATIONS = 10;
  private static final int VECTOR_SIZE = 4;


  private static final Map<String, String> LOOKUP = Map.of(
      "1", "a",
      "12", "b",
      "33", "c",
      "111", "d",
      "123", "e",
      "124", "f"
  );
  private static final Map<String, String> INJECTIVE_LOOKUP = new HashMap<>()
  {
    @Override
    public String get(Object key)
    {
      return (String) key;
    }
  };

  private static final ExprMacroTable MACRO_TABLE;

  static {
    final Injector injector = Guice.createInjector(
        new DruidGuiceExtensions(),
        new ExpressionModule(),
        binder -> {
          final LookupExtractorFactoryContainerProvider lookupProvider = new LookupExtractorFactoryContainerProvider()
          {
            @Override
            public Set<String> getAllLookupNames()
            {
              return Set.of("test-lookup", "test-lookup-injective");
            }

            @Override
            public Optional<LookupExtractorFactoryContainer> get(String lookupName)
            {
              if ("test-lookup".equals(lookupName)) {
                return Optional.of(
                    new LookupExtractorFactoryContainer(
                        "v0",
                        new TestMapLookupExtractorFactory(LOOKUP, false)
                    )
                );
              } else if ("test-lookup-injective".equals(lookupName)) {
                return Optional.of(
                    new LookupExtractorFactoryContainer(
                        "v0",
                        new TestMapLookupExtractorFactory(INJECTIVE_LOOKUP, true)
                    )
                );
              }
              return Optional.empty();
            }

            @Override
            public String getCanonicalLookupName(String lookupName)
            {
              return "";
            }
          };

          ExpressionModule.addExprMacro(binder, LookupExprMacro.class);
          binder.bind(Key.get(ObjectMapper.class, Json.class)).toInstance(new DefaultObjectMapper());
          binder.bind(LookupExtractorFactoryContainerProvider.class)
                .toInstance(lookupProvider);
        }
    );

    MACRO_TABLE = injector.getInstance(ExprMacroTable.class);
  }

  final Map<String, ExpressionType> types = ImmutableMap.<String, ExpressionType>builder()
                                                        .put("l1", ExpressionType.LONG)
                                                        .put("l2", ExpressionType.LONG)
                                                        .put("l3", ExpressionType.LONG)
                                                        .put("d1", ExpressionType.DOUBLE)
                                                        .put("d2", ExpressionType.DOUBLE)
                                                        .put("d3", ExpressionType.DOUBLE)
                                                        .put("s1", ExpressionType.STRING)
                                                        .put("s2", ExpressionType.STRING)
                                                        .put("s3", ExpressionType.STRING)
                                                        .put("boolString1", ExpressionType.STRING)
                                                        .put("boolString2", ExpressionType.STRING)
                                                        .put("boolString3", ExpressionType.STRING)
                                                        .build();


  @Test
  public void testConstants()
  {
    testExpression("null", types);
    testExpression("1", types);
    testExpression("1.1", types);
    testExpression("NaN", types);
    testExpression("Infinity", types);
    testExpression("-Infinity", types);
    testExpression("'hello'", types);
    testExpression("json_object('a', 1, 'b', 'abc', 'c', 3.3, 'd', array(1,2,3))", types);
  }

  @Test
  public void testIdentifiers()
  {
    final List<String> columns = new ArrayList<>(types.keySet());
    columns.add("unknown");
    final List<String> template = List.of("%s");
    testFunctions(types, template, columns);
  }


  @Test
  public void testCast()
  {
    final Set<String> columns = Set.of("d1", "l1", "s1");
    final Set<String> castTo = Set.of(
        "'STRING'",
        "'LONG'",
        "'DOUBLE'",
        "'ARRAY<STRING>'",
        "'ARRAY<LONG>'",
        "'ARRAY<DOUBLE>'"
    );
    final Set<List<String>> args = Sets.cartesianProduct(columns, castTo);
    final List<String> templates = List.of("cast(%s, %s)");
    testFunctions(types, templates, args);
  }

  @Test
  public void testCastArraysRoundTrip()
  {
    testExpression("cast(cast(s1, 'ARRAY<STRING>'), 'STRING')", types);
    testExpression("cast(cast(d1, 'ARRAY<DOUBLE>'), 'DOUBLE')", types);
    testExpression("cast(cast(d1, 'ARRAY<STRING>'), 'DOUBLE')", types);
    testExpression("cast(cast(l1, 'ARRAY<LONG>'), 'LONG')", types);
    testExpression("cast(cast(l1, 'ARRAY<STRING>'), 'LONG')", types);
  }

  @Test
  public void testUnaryOperators()
  {
    final List<String> functions = List.of("-");
    final List<String> templates = List.of("%sd1", "%sl1");

    testFunctions(types, templates, functions);
  }

  @Test
  public void testBinaryMathOperators()
  {
    final Set<String> columns = Set.of("d1", "d2", "l1", "l2", "1", "1.0", "nonexistent", "null", "s1");
    final Set<String> columns2 = Set.of("d1", "d2", "l1", "l2", "1", "1.0");
    final Set<List<String>> templateInputs = Sets.cartesianProduct(columns, columns2);
    final List<String> templates = new ArrayList<>();
    for (List<String> template : templateInputs) {
      templates.add(StringUtils.format("%s %s %s", template.get(0), "%s", template.get(1)));
    }
    final List<String> args = List.of("+", "-", "*", "/", "^", "%");

    testFunctions(types, templates, args);
  }

  @Test
  public void testBinaryComparisonOperators()
  {
    final Set<String> columns = Set.of("d1", "d2", "l1", "l2", "1", "1.0", "s1", "s2", "nonexistent", "null");
    final Set<String> columns2 = Set.of("d1", "d2", "l1", "l2", "1", "1.0", "s1", "s2", "null");
    final Set<List<String>> templateInputs = Sets.cartesianProduct(columns, columns2);
    final List<String> templates = new ArrayList<>();
    for (List<String> template : templateInputs) {
      templates.add(StringUtils.format("%s %s %s", template.get(0), "%s", template.get(1)));
    }
    final List<String> args = List.of(">", ">=", "<", "<=", "==", "!=");

    testFunctions(types, templates, args);
  }

  @Test
  public void testUnaryLogicOperators()
  {
    final List<String> functions = List.of("!");
    final List<String> templates = List.of("%sd1", "%sl1", "%sboolString1");
    testFunctions(types, templates, functions);
  }

  @Test
  public void testBinaryLogicOperators()
  {
    final List<String> functions = List.of("&&", "||");
    final List<String> templates = List.of(
        "d1 %s d2",
        "l1 %s l2",
        "boolString1 %s boolString2",
        "(d1 == d2) %s (l1 == l2)"
    );
    testFunctions(types, templates, functions);
  }

  @Test
  public void testBinaryOperatorTrees()
  {
    final Set<String> columns = Set.of("d1", "l1", "1", "1.0", "nonexistent", "null");
    final Set<String> columns2 = Set.of("d2", "l2", "2", "2.0");
    final Set<List<String>> templateInputs = Sets.cartesianProduct(columns, columns2, columns2);
    final List<String> templates = new ArrayList<>();
    for (List<String> template : templateInputs) {
      templates.add(StringUtils.format(
          "(%s %s %s) %s %s",
          template.get(0),
          "%s",
          template.get(1),
          "%s",
          template.get(2)
      ));
    }
    final Set<String> ops = Set.of("+", "-", "*", "/");
    final Set<List<String>> args = Sets.cartesianProduct(ops, ops);
    testFunctions(types, templates, args);
  }

  @Test
  public void testUnivariateFunctions()
  {
    final List<String> functions = List.of("parse_long", "isNull", "notNull");
    final List<String> templates = List.of("%s(s1)", "%s(l1)", "%s(d1)", "%s(nonexistent)", "%s(null)");
    testFunctions(types, templates, functions);
  }

  @Test
  public void testLike()
  {
    testExpression("like(s1, '1%')", types);
    testExpression("like(s1, '%1')", types);
    testExpression("like(s1, '%1%')", types);
  }

  @Test
  public void testUnivariateMathFunctions()
  {
    final List<String> functions = List.of(
        "abs",
        "acos",
        "asin",
        "atan",
        "cbrt",
        "ceil",
        "cos",
        "cosh",
        "cot",
        "exp",
        "expm1",
        "floor",
        "getExponent",
        "log",
        "log10",
        "log1p",
        "nextUp",
        "rint",
        "signum",
        "sin",
        "sinh",
        "sqrt",
        "tan",
        "tanh",
        "toDegrees",
        "toRadians",
        "ulp",
        "bitwiseComplement",
        "bitwiseConvertDoubleToLongBits",
        "bitwiseConvertLongBitsToDouble"
    );
    final List<String> templates = List.of("%s(l1)", "%s(d1)", "%s(pi())", "%s(null)", "%s(missing)");
    testFunctions(types, templates, functions);
  }

  @Test
  public void testBivariateMathFunctions()
  {
    final List<String> functions = List.of(
        "atan2",
        "copySign",
        "div",
        "hypot",
        "remainder",
        "max",
        "min",
        "nextAfter",
        "scalb",
        "pow",
        "safe_divide",
        "bitwiseAnd",
        "bitwiseOr",
        "bitwiseXor",
        "bitwiseShiftLeft",
        "bitwiseShiftRight"
    );
    final List<String> templates = List.of(
        "%s(d1, d2)",
        "%s(d1, l1)",
        "%s(l1, d1)",
        "%s(l1, l2)",
        "%s(nonexistent, l1)",
        "%s(nonexistent, d1)"
    );
    testFunctions(types, templates, functions);
  }

  @Test
  public void testSymmetricalBivariateFunctions()
  {
    final List<String> functions = List.of(
        "nvl",
        "coalesce"
    );
    final List<String> templates = List.of(
        "%s(d1, d2)",
        "%s(l1, l2)",
        "%s(s1, s2)",
        "%s(nonexistent, l1)",
        "%s(nonexistent, d1)",
        "%s(nonexistent, s1)",
        "%s(nonexistent, nonexistent2)"
    );
    testFunctions(types, templates, functions);
  }

  @Test
  public void testIfFunction()
  {
    testExpression("if(l1, l1, l2)", types);
    testExpression("if(l1, s1, s2)", types);
    testExpression("if(l1, d1, d2)", types);
    testExpression("if(d1, l1, l2)", types);
    testExpression("if(d1, s1, s2)", types);
    testExpression("if(d1, d1, d2)", types);
    testExpression("if(boolString1, s1, s2)", types);
    testExpression("if(boolString1, l1, l2)", types);
    testExpression("if(boolString1, d1, d2)", types);
    // make sure eval of else is lazy, else this would be divide by zero error
    testExpression("if(l1 % 2 == 0, -1, l2 / (l1 % 2))", types);
    // cannot vectorize mixed types
    Assertions.assertFalse(
        Parser.parse("if(s1, l1, d2)", MACRO_TABLE).canVectorize(InputBindings.inspectorFromTypeMap(types))
    );
    Assertions.assertFalse(
        Parser.parse("if(s1, d1, s2)", MACRO_TABLE).canVectorize(InputBindings.inspectorFromTypeMap(types))
    );
  }

  @Test
  public void testCaseSearchedFunction()
  {
    testExpression("case_searched(boolString1, s1, boolString2, s2, s1)", types);
    testExpression("case_searched(boolString1, s1, boolString2, s2, boolString3, s3, s1)", types);
    testExpression("case_searched(boolString1, l1, boolString2, l2, l2)", types);
    testExpression("case_searched(boolString1, l1, boolString2, l2, boolString3, l3, l2)", types);
    testExpression("case_searched(boolString1, d1, boolString2, d2, d1)", types);
    testExpression("case_searched(boolString1, d1, boolString2, d2, boolString3, d3, d1)", types);
    testExpression("case_searched(l1 % 2 == 0, -1, l1 % 2 == 1, l2 / (l1 % 2))", types);
    Assertions.assertFalse(
        Parser.parse("case_searched(boolString1, d1, boolString2, d2, l1)", MACRO_TABLE)
              .canVectorize(InputBindings.inspectorFromTypeMap(types))
    );
    Assertions.assertFalse(
        Parser.parse("case_searched(boolString1, d1, boolString2, l1, d1)", MACRO_TABLE)
              .canVectorize(InputBindings.inspectorFromTypeMap(types))
    );
  }

  @Test
  public void testCaseSimpleFunction()
  {
    testExpression("case_simple(s1, s2, s2, s1, s2)", types);
    testExpression("case_simple(s1, s2, s2, s1, s2, s1)", types);
    testExpression("case_simple(s1, s2, l1, s1, l2)", types);
    testExpression("case_simple(s1, s2, d1, s1, d2, d1)", types);
    testExpression("case_simple(s1, l1, d1, d1, d2, d1)", types);
    Assertions.assertFalse(
        Parser.parse("case_simple(s1, d1, s1, l1, d1)", MACRO_TABLE)
              .canVectorize(InputBindings.inspectorFromTypeMap(types))
    );
  }

  @Test
  public void testCoalesceFunction()
  {
    final List<String> functions = List.of(
        "coalesce"
    );
    final List<String> templates = List.of(
        "%s(nonexistent, d1, d2, d3)",
        "%s(nonexistent, d1, nonexistent2, d2, nonexistent3, d3)",
        "%s(nonexistent, nonexistent2, l1, l2, nonexistent, l3)",
        "%s(nonexistent, s1, nonexistent2, s2)"
    );
    testFunctions(types, templates, functions);
    // cannot vectorize mixed arg types
    Assertions.assertFalse(
        Parser.parse("coalesce(s1, d1, s1, l1, d1)", MACRO_TABLE)
              .canVectorize(InputBindings.inspectorFromTypeMap(types))
    );
  }

  @Test
  public void testStringFns()
  {
    testExpression("s1 + s2", types);
    testExpression("s1 + '-' + s2", types);
    testExpression("concat(s1, s2)", types);
    testExpression("concat(s1,'-',s2,'-',l1,'-',d1)", types);
  }

  @Test
  public void testStringFunctions()
  {
    testExpression("format('%s-%d-%.2f', s1, l1, d1)", types);
    testExpression("format('%s %s', s1, s2)", types);
    testExpression("regexp_extract(s1, '[0-9]+')", types);
    testExpression("regexp_extract(s1, '([a-z]+)', 1)", types);
    testExpression("regexp_like(s1, '[0-9]+')", types);
    testExpression("regexp_like(s1, '^test.*')", types);
    testExpression("regexp_replace(s1, '[0-9]+', 'NUM')", types);
    testExpression("contains_string(s1, '1')", types);
    testExpression("icontains_string(s1, '1')", types);
    testExpression("replace(s1, 'test', 'TEST')", types);
    testExpression("replace(s1, s2, s3)", types);
    testExpression("substring(s1, 0, 3)", types);
    testExpression("substring(s1, l1, l2)", types);
    testExpression("right(s1, 3)", types);
    testExpression("right(s1, l1)", types);
    testExpression("left(s1, 3)", types);
    testExpression("left(s1, l1)", types);
    testExpression("strlen(s1)", types);
    testExpression("strpos(s1, 'test')", types);
    testExpression("strpos(s1, s2)", types);
    testExpression("strpos(s1, s2, l1)", types);
    testExpression("trim(s1)", types);
    testExpression("trim(s1, 'abc')", types);
    testExpression("ltrim(s1)", types);
    testExpression("ltrim(s1, 'abc')", types);
    testExpression("rtrim(s1)", types);
    testExpression("rtrim(s1, 'abc')", types);
    testExpression("lower(s1)", types);
    testExpression("upper(s1)", types);
    testExpression("reverse(s1)", types);
    testExpression("repeat(s1, 3)", types);
    testExpression("repeat(s1, l1 % 10)", types);
    testExpression("lpad(s1, 10, 'x')", types);
    testExpression("lpad(s1, l1 % 10, s2)", types);
    testExpression("rpad(s1, 10, 'x')", types);
    testExpression("rpad(s1, l1 % 10, s2)", types);
  }

  @Test
  public void testLookup()
  {
    final List<String> columns = new ArrayList<>(types.keySet());
    columns.add("unknown");
    final List<String> templates = List.of(
        "lookup(%s, 'test-lookup')",
        "lookup(%s, 'test-lookup', 'missing')",
        "lookup(%s, 'test-lookup-injective')",
        "lookup(%s, 'test-lookup-injective', 'missing')",
        "lookup(%s, 'nonexistent-lookup')",
        "lookup(%s, 'nonexistent-lookup', 'missing')"
    );
    testFunctions(types, templates, columns);
  }

  @Test
  public void testArrayFns()
  {
    testExpression("array(s1, s2)", types);
    testExpression("array(l1, l2)", types);
    testExpression("array(d1, d2)", types);
    testExpression("array(l1, d2)", types);
    testExpression("array(s1, l2)", types);

    testExpression("map((x) -> x + 1, array(l1, l2))", types);
    testExpression("map((x) -> x * 2.0, array(d1, d2))", types);
    testExpression("map((x) -> concat(x, '_mapped'), array(s1, s2))", types);

    testExpression("cartesian_map((x, y) -> x + y, array(l1, l2), array(d1, d2))", types);
    testExpression("cartesian_map((x, y) -> concat(x, cast(y, 'STRING')), array(s1, s2), array(l1, l2))", types);

    testExpression("fold((x, acc) -> x + acc, array(l1, l2), 0)", types);
    testExpression("fold((x, acc) -> x + acc, array(d1, d2), 0.0)", types);
    testExpression("fold((x, acc) -> concat(acc, x), array(s1, s2), '')", types);

    testExpression("cartesian_fold((x, y, acc) -> acc + x + y, array(l1, l2), array(d1, d2), 0)", types);

    testExpression("filter((x) -> x > 0, array(l1, l2))", types);
    testExpression("filter((x) -> x > 0.0, array(d1, d2))", types);
    testExpression("filter((x) -> strlen(x) > 0, array(s1, s2))", types);

    testExpression("any((x) -> x > 0, array(l1, l2))", types);
    testExpression("any((x) -> x > 0.0, array(d1, d2))", types);
    testExpression("any((x) -> strlen(x) > 0, array(s1, s2))", types);

    testExpression("all((x) -> x != null, array(l1, l2))", types);
    testExpression("all((x) -> x != null, array(d1, d2))", types);
    testExpression("all((x) -> x != null, array(s1, s2))", types);
  }

  @Test
  public void testArrayFunctions()
  {
    testExpression("array_length(array(s1, s2))", types);
    testExpression("array_length(array(l1, l2, l3))", types);
    testExpression("array_offset(array(s1, s2), 0)", types);
    testExpression("array_offset(array(l1, l2), l1)", types);
    testExpression("array_ordinal(array(s1, s2), 1)", types);
    testExpression("array_ordinal(array(l1, l2), l1)", types);
    testExpression("array_contains(array(s1, s2), s1)", types);
    testExpression("array_contains(array(l1, l2), l1)", types);
    testExpression("array_contains(array(s1, s2), array(s1))", types);
    testExpression("array_overlap(array(s1, s2), array(s2, s3))", types);
    testExpression("array_overlap(array(l1, l2), array(l2, l3))", types);
    testExpression("scalar_in_array(s1, array(s1, s2))", types);
    testExpression("scalar_in_array(l1, array(l1, l2))", types);
    testExpression("array_offset_of(array(s1, s2), s1)", types);
    testExpression("array_offset_of(array(l1, l2), l1)", types);
    testExpression("array_ordinal_of(array(s1, s2), s1)", types);
    testExpression("array_ordinal_of(array(l1, l2), l1)", types);
    testExpression("array_prepend(s1, array(s2, s3))", types);
    testExpression("array_prepend(l1, array(l2, l3))", types);
    testExpression("array_append(array(s1, s2), s3)", types);
    testExpression("array_append(array(l1, l2), l3)", types);
    testExpression("array_concat(array(s1, s2), array(s2, s3))", types);
    testExpression("array_concat(array(l1, l2), array(l2, l3))", types);
    testExpression("array_set_add(array(s1, s2), s1)", types);
    testExpression("array_set_add(array(l1, l2), l3)", types);
    testExpression("array_set_add_all(array(s1, s2), array(s2, s3))", types);
    testExpression("array_set_add_all(array(l1, l2), array(l2, l3))", types);
    testExpression("array_slice(array(s1, s2, s3), 1, 2)", types);
    testExpression("array_slice(array(l1, l2, l3), 1, 2)", types);
    testExpression("array_to_string(array(s1, s2), ',')", types);
    testExpression("array_to_string(array(l1, l2), s1)", types);
    testExpression("string_to_array(s1, ',')", types);
    testExpression("string_to_array(s1, s2)", types);
  }

  @Test
  public void testReduceFns()
  {
    testExpression("greatest(s1, s2)", types);
    testExpression("greatest(l1, l2)", types);
    testExpression("greatest(l1, nonexistent)", types);
    testExpression("greatest(d1, d2)", types);
    testExpression("greatest(l1, d2)", types);
    testExpression("greatest(s1, l2)", types);

    testExpression("least(s1, s2)", types);
    testExpression("least(l1, l2)", types);
    testExpression("least(l1, nonexistent)", types);
    testExpression("least(d1, d2)", types);
    testExpression("least(l1, d2)", types);
    testExpression("least(s1, l2)", types);
  }

  @Test
  public void testJsonFns()
  {
    Assume.assumeTrue(ExpressionProcessing.allowVectorizeFallback());
    testExpression("json_object('k1', s1, 'k2', l1)", types);
    testExpression("json_value(json_object('k1', s1, 'k2', l1), '$.k2', 'STRING')", types);
    testExpression("json_query(json_object('k1', s1, 'k2', l1), '$.k1')", types);
    testExpression("json_query_array(json_object('arr', array(s1, s2)), '$.arr')", types);
    testExpression("parse_json(s1)", types);
    testExpression("try_parse_json(s1)", types);
    testExpression("to_json_string(json_object('k1', s1))", types);
    testExpression("json_keys(json_object('k1', s1, 'k2', l1), '$')", types);
    testExpression("json_paths(json_object('k1', s1, 'k2', l1))", types);
    testExpression("json_merge(json_object('k1', s1), json_object('k2', l1))", types);
  }

  @Test
  public void testTimeFunctions()
  {
    testExpression("timestamp_ceil(l1, 'P1D')", types);
    testExpression("timestamp_ceil(l1, 'PT1H')", types);
    testExpression("timestamp_floor(l1, 'P1D')", types);
    testExpression("timestamp_floor(l1, 'PT1H')", types);
    testExpression("timestamp_extract(l1, 'MILLENNIUM')", types);
    testExpression("timestamp_extract(l1, 'CENTURY')", types);
    testExpression("timestamp_extract(l1, 'YEAR')", types);
    testExpression("timestamp_extract(l1, 'MONTH')", types);
    testExpression("timestamp_extract(l1, 'DAY')", types);
    testExpression("timestamp_extract(l1, 'HOUR')", types);
    testExpression("timestamp_extract(l1, 'MINUTE')", types);
    testExpression("timestamp_extract(l1, 'SECOND')", types);
    testExpression("timestamp_extract(l1, 'MILLISECOND')", types);
    testExpression("timestamp_extract(l1, 'EPOCH')", types);
    testExpression("timestamp_shift(l1, 'P1M', 1)", types);
    testExpression("timestamp(s1)", types);
    testExpression("timestamp(s1, 'yyyy-MM-dd')", types);
    testExpression("unix_timestamp(s1)", types);
    testExpression("unix_timestamp(s1, 'yyyy-MM-dd')", types);
    testExpression("timestamp_parse(s1)", types);
    testExpression("timestamp_parse(s1, 'yyyy-MM-dd')", types);
    testExpression("timestamp_parse(s1, 'yyyy-MM-dd', 'UTC')", types);
    testExpression("timestamp_format(l1)", types);
    testExpression("timestamp_format(l1, 'yyyy-MM-dd')", types);
    testExpression("timestamp_format(l1, 'yyyy-MM-dd', 'UTC')", types);
  }

  @Test
  public void testIpAddressFunctions()
  {
    testExpression("ipv4_match('192.168.1.1', '192.168.0.0/16')", types);
    testExpression("ipv4_match(s1, '192.168.0.0/16')", types);
    testExpression("ipv4_parse('192.168.1.1')", types);
    testExpression("ipv4_parse(s1)", types);
    testExpression("ipv4_stringify(3232235777)", types);
    testExpression("ipv4_stringify(l1)", types);
    testExpression("ipv6_match('2001:db8::1', '2001:db8::/32')", types);
    testExpression("ipv6_match(s1, '2001:db8::/32')", types);
  }

  @Test
  public void testOtherFunctions()
  {
    testExpression("human_readable_binary_byte_format(l1)", types);
    testExpression("human_readable_binary_byte_format(l1, 3)", types);
    testExpression("human_readable_decimal_byte_format(l1)", types);
    testExpression("human_readable_decimal_byte_format(l1, 3)", types);
    testExpression("human_readable_decimal_format(l1)", types);
    testExpression("human_readable_decimal_format(l1, 3)", types);
    testExpression("pi()", types);
  }

  /**
   * Applies a list of arguments to a list of templates to fill in each template from the argument to generate
   * expression strings and call {@link #testExpression} for each combination
   */
  static void testFunctions(Map<String, ExpressionType> types, List<String> templates, List<String> args)
  {
    for (String template : templates) {
      for (String arg : args) {
        String expr = StringUtils.format(template, arg);
        testExpression(expr, types);
      }
    }
  }

  /**
   * Applies a set of argument lists to a list of string templates to fill in each template from the argument list to
   * generate expression strings and call {@link #testExpression} for each combination
   */
  static void testFunctions(Map<String, ExpressionType> types, List<String> templates, Set<List<String>> argsArrays)
  {
    for (String template : templates) {
      for (List<String> args : argsArrays) {
        String expr = StringUtils.format(template, args.toArray());
        testExpression(expr, types);
      }
    }
  }

  /**
   * run an expression both vectorized and non-vectorized, comparing results with randomized bindings and sequential
   * bindings
   */
  public static void testExpression(String expr, Map<String, ExpressionType> types)
  {
    testExpression(expr, types, MACRO_TABLE);
  }

  /**
   * run an expression both vectorized and non-vectorized, comparing results with randomized bindings and sequential
   * bindings
   */
  public static void testExpression(String expr, Map<String, ExpressionType> types, ExprMacroTable macroTable)
  {
    log.debug("running expression [%s]", expr);
    Expr parsed = Parser.parse(expr, macroTable);

    testExpressionRandomizedBindings(expr, parsed, types, NUM_ITERATIONS);
    testExpressionSequentialBindings(expr, parsed, types, NUM_ITERATIONS);
  }

  /**
   * runs expressions against bindings generated by increasing a counter
   */
  public static void testExpressionSequentialBindings(
      final String expr,
      final Expr parsed,
      final Map<String, ExpressionType> types,
      final int numIterations
  )
  {
    for (int iter = 0; iter < numIterations; iter++) {
      assertEvalsMatch(
          expr,
          parsed,
          makeSequentialBinding(
              VECTOR_SIZE,
              types,
              -2 + (iter * VECTOR_SIZE) // include negative numbers and zero
          )
      );
    }
  }

  public static void testExpressionRandomizedBindings(
      final String expr,
      final Expr parsed,
      final Map<String, ExpressionType> types,
      final int numIterations
  )
  {
    for (int iterations = 0; iterations < numIterations; iterations++) {
      assertEvalsMatch(expr, parsed, makeRandomizedBindings(VECTOR_SIZE, types));
    }
  }

  public static void assertEvalsMatch(
      String exprString,
      Expr expr,
      NonnullPair<Expr.ObjectBinding[], Expr.VectorInputBinding> bindings
  )
  {
    Assert.assertTrue(StringUtils.format("Cannot vectorize[%s]", expr), expr.canVectorize(bindings.rhs));

    final ExpressionType outputType = expr.getOutputType(bindings.rhs);
    final Either<String, Object[]> vectorEval = evalVector(expr, bindings.rhs, outputType);
    final Either<String, Object[]> nonVectorEval = evalNonVector(expr, bindings.lhs, outputType);

    Assert.assertEquals(
        StringUtils.format("Errors do not match for expr[%s], bindings[%s]", exprString, bindings.lhs),
        nonVectorEval.isError() ? nonVectorEval.error() : "",
        vectorEval.isError() ? vectorEval.error() : ""
    );

    if (vectorEval.isValue() && nonVectorEval.isValue()) {
      for (int i = 0; i < VECTOR_SIZE; i++) {
        final String message = StringUtils.format(
            "Values do not match for row[%s] for expression[%s], bindings[%s]",
            i,
            exprString,
            bindings.lhs[i]
        );
        if (outputType != null && outputType.isArray()) {
          Assert.assertArrayEquals(
              message,
              (Object[]) nonVectorEval.valueOrThrow()[i],
              (Object[]) vectorEval.valueOrThrow()[i]
          );
        } else {
          Assert.assertEquals(
              message,
              nonVectorEval.valueOrThrow()[i],
              vectorEval.valueOrThrow()[i]
          );
        }
      }
    }
  }

  /**
   * Random backed bindings that provide positive longs in the positive integer range, random doubles, and random
   * strings, with a 10% null rate
   */
  public static NonnullPair<Expr.ObjectBinding[], Expr.VectorInputBinding> makeRandomizedBindings(
      int vectorSize,
      Map<String, ExpressionType> types
  )
  {

    final ThreadLocalRandom r = ThreadLocalRandom.current();
    return populateBindings(
        vectorSize,
        types,
        () -> r.nextLong(Integer.MIN_VALUE, Integer.MAX_VALUE),
        () -> r.nextDouble() * (r.nextBoolean() ? 10 : -10),
        () -> r.nextDouble(0, 1.0) > 0.9,
        () -> String.valueOf(r.nextInt())
    );
  }

  /**
   * expression bindings that use a counter to generate double, long, and string values with a random null rate
   */
  public static NonnullPair<Expr.ObjectBinding[], Expr.VectorInputBinding> makeSequentialBinding(
      int vectorSize,
      Map<String, ExpressionType> types,
      int start
  )
  {

    return populateBindings(
        vectorSize,
        types,
        new LongSupplier()
        {
          int counter = start;

          @Override
          public long getAsLong()
          {
            return counter++;
          }
        },
        new DoubleSupplier()
        {
          int counter = start;

          @Override
          public double getAsDouble()
          {
            return counter++;
          }
        },
        () -> ThreadLocalRandom.current().nextBoolean(),
        new Supplier<>()
        {
          int counter = start;

          @Override
          public String get()
          {
            return String.valueOf(counter++);
          }
        }
    );
  }

  /**
   * Populates a vectors worth of expression bindings
   */
  static NonnullPair<Expr.ObjectBinding[], Expr.VectorInputBinding> populateBindings(
      int vectorSize,
      Map<String, ExpressionType> types,
      LongSupplier longsFn,
      DoubleSupplier doublesFn,
      BooleanSupplier nullsFn,
      Supplier<String> stringFn
  )
  {
    SettableVectorInputBinding vectorBinding = new SettableVectorInputBinding(vectorSize);
    SettableObjectBinding[] objectBindings = new SettableObjectBinding[vectorSize];
    Expr.InputBindingInspector inspector = InputBindings.inspectorFromTypeMap(types);

    for (Map.Entry<String, ExpressionType> entry : types.entrySet()) {
      boolean[] nulls = new boolean[vectorSize];

      switch (entry.getValue().getType()) {
        case LONG:
          long[] longs = new long[vectorSize];
          for (int i = 0; i < vectorSize; i++) {
            nulls[i] = nullsFn.getAsBoolean();
            longs[i] = nulls[i] ? 0L : longsFn.getAsLong();
            if (objectBindings[i] == null) {
              objectBindings[i] = new SettableObjectBinding().withInspector(inspector);
            }
            objectBindings[i].withBinding(entry.getKey(), nulls[i] ? null : longs[i]);
          }
          vectorBinding.addLong(entry.getKey(), longs, nulls);
          break;
        case DOUBLE:
          double[] doubles = new double[vectorSize];
          for (int i = 0; i < vectorSize; i++) {
            nulls[i] = nullsFn.getAsBoolean();
            doubles[i] = nulls[i] ? 0.0 : doublesFn.getAsDouble();
            if (objectBindings[i] == null) {
              objectBindings[i] = new SettableObjectBinding().withInspector(inspector);
            }
            objectBindings[i].withBinding(entry.getKey(), nulls[i] ? null : doubles[i]);
          }
          vectorBinding.addDouble(entry.getKey(), doubles, nulls);
          break;
        case STRING:
          String[] strings = new String[vectorSize];
          for (int i = 0; i < vectorSize; i++) {
            nulls[i] = nullsFn.getAsBoolean();
            if (!nulls[i] && entry.getKey().startsWith("boolString")) {
              strings[i] = String.valueOf(nullsFn.getAsBoolean());
            } else {
              strings[i] = nulls[i] ? null : String.valueOf(stringFn.get());
            }
            if (objectBindings[i] == null) {
              objectBindings[i] = new SettableObjectBinding().withInspector(inspector);
            }
            objectBindings[i].withBinding(entry.getKey(), nulls[i] ? null : strings[i]);
          }
          vectorBinding.addString(entry.getKey(), strings);
          break;
      }
    }

    return new NonnullPair<>(objectBindings, vectorBinding);
  }

  private static Either<String, Object[]> evalVector(
      Expr expr,
      Expr.VectorInputBinding bindings,
      @Nullable ExpressionType outputType
  )
  {
    final ExprVectorProcessor<Object> processor = expr.asVectorProcessor(bindings);
    final ExprEvalVector<?> vectorEval;
    try {
      vectorEval = processor.evalVector(bindings);
    }
    catch (ArithmeticException e) {
      // After a few occasions, Java starts throwing ArithmeticException without a message for division by zero.
      return Either.error(e.getClass().getName());
    }
    catch (Exception e) {
      return Either.error(e.toString());
    }

    final Object[] vectorVals = vectorEval.getObjectVector();
    // 'null' expressions can have an output type of null, but still evaluate in default mode, so skip type checks
    if (outputType != null) {
      Assert.assertEquals("vector eval type", outputType, vectorEval.getType());
    }

    return Either.value(vectorVals);
  }

  private static Either<String, Object[]> evalNonVector(
      Expr expr,
      Expr.ObjectBinding[] bindings,
      @Nullable ExpressionType outputType
  )
  {
    final Object[] exprValues = new Object[VECTOR_SIZE];

    for (int i = 0; i < VECTOR_SIZE; i++) {
      ExprEval<?> eval;
      try {
        eval = expr.eval(bindings[i]);
      }
      catch (ArithmeticException e) {
        // After a few occasions, Java starts throwing ArithmeticException without a message for division by zero.
        return Either.error(e.getClass().getName());
      }
      catch (Exception e) {
        return Either.error(e.toString());
      }
      // 'null' expressions can have an output type of null, but still evaluate in default mode, so skip type checks
      if (outputType != null && eval.value() != null) {
        Assert.assertEquals("nonvector eval type", eval.type(), outputType);
      }
      exprValues[i] = eval.value();
    }

    return Either.value(exprValues);
  }
}
