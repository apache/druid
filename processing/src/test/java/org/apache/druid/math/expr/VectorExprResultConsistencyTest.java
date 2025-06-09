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
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.vector.ExprEvalVector;
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
import org.apache.druid.query.expression.LookupExprMacro;
import org.apache.druid.query.expression.NestedDataExpressions;
import org.apache.druid.query.expression.TimestampFloorExprMacro;
import org.apache.druid.query.expression.TimestampShiftExprMacro;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.lookup.TestMapLookupExtractorFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

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
  private static final int VECTOR_SIZE = 512;


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

  private static final ExprMacroTable MACRO_TABLE = new ExprMacroTable(
      ImmutableList.of(
          new TimestampFloorExprMacro(),
          new TimestampShiftExprMacro(),
          new NestedDataExpressions.JsonObjectExprMacro(),
          new LookupExprMacro(
              new LookupExtractorFactoryContainerProvider()
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
              }
          )
      )
  );

  final Map<String, ExpressionType> types = ImmutableMap.<String, ExpressionType>builder()
      .put("l1", ExpressionType.LONG)
      .put("l2", ExpressionType.LONG)
      .put("d1", ExpressionType.DOUBLE)
      .put("d2", ExpressionType.DOUBLE)
      .put("s1", ExpressionType.STRING)
      .put("s2", ExpressionType.STRING)
      .put("boolString1", ExpressionType.STRING)
      .put("boolString2", ExpressionType.STRING)
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
    final Set<String> castTo = Set.of("'STRING'", "'LONG'", "'DOUBLE'", "'ARRAY<STRING>'", "'ARRAY<LONG>'", "'ARRAY<DOUBLE>'");
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
    final List<String> templates = List.of("d1 %s d2", "l1 %s l2", "boolString1 %s boolString2", "(d1 == d2) %s (l1 == l2)");
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
      templates.add(StringUtils.format("(%s %s %s) %s %s", template.get(0), "%s", template.get(1), "%s", template.get(2)));
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
        "nvl"
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
  public void testStringFns()
  {
    testExpression("s1 + s2", types);
    testExpression("s1 + '-' + s2", types);
    testExpression("concat(s1, s2)", types);
    testExpression("concat(s1,'-',s2,'-',l1,'-',d1)", types);
  }

  @Test
  public void testLookup()
  {
    final List<String> columns = new ArrayList<>(types.keySet());
    columns.add("unknown");
    final List<String> templates = List.of(
        "lookup(%s, 'test-lookup')",
        "lookkup(%s, 'test-lookup', 'missing')",
        "lookup(%s, 'test-lookup-injective')",
        "lookup(%s, 'test-lookup-injective', 'missing')"
    );
    testFunctions(types, templates, columns);
  }

  @Test
  public void testArrayFns()
  {
    try {
      ExpressionProcessing.initializeForFallback();
      testExpression("array(s1, s2)", types);
      testExpression("array(l1, l2)", types);
      testExpression("array(d1, d2)", types);
      testExpression("array(l1, d2)", types);
      testExpression("array(s1, l2)", types);
    }
    finally {
      ExpressionProcessing.initializeForTests();
    }
  }

  @Test
  public void testJsonFns()
  {
    Assume.assumeTrue(ExpressionProcessing.allowVectorizeFallback());
    testExpression("json_object('k1', s1, 'k2', l1)", types);
  }

  @Test
  public void testTimeFunctions()
  {
    testExpression("timestamp_floor(l1, 'PT1H')", types);
    testExpression("timestamp_shift(l1, 'P1M', 1)", types);
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
      String expr,
      Expr parsed,
      Map<String, ExpressionType> types,
      int numIterations
  )
  {
    for (int iter = 0; iter < numIterations; iter++) {
      NonnullPair<Expr.ObjectBinding[], Expr.VectorInputBinding> bindings = makeSequentialBinding(
          VECTOR_SIZE,
          types,
          1 + (iter * VECTOR_SIZE) // the plus 1 is because dividing by zero is sad
      );
      Assert.assertTrue(StringUtils.format("Cannot vectorize %s", expr), parsed.canVectorize(bindings.rhs));
      ExpressionType outputType = parsed.getOutputType(bindings.rhs);
      ExprEvalVector<?> vectorEval = parsed.asVectorProcessor(bindings.rhs).evalVector(bindings.rhs);
      // 'null' expressions can have an output type of null, but still evaluate in default mode, so skip type checks
      if (outputType != null) {
        Assert.assertEquals(expr, outputType, vectorEval.getType());
      }
      final Object[] vectorVals = vectorEval.getObjectVector();
      for (int i = 0; i < VECTOR_SIZE; i++) {
        ExprEval<?> eval = parsed.eval(bindings.lhs[i]);
        // 'null' expressions can have an output type of null, but still evaluate in default mode, so skip type checks
        if (outputType != null && !eval.isNumericNull()) {
          Assert.assertEquals(eval.type(), outputType);
        }
        if (outputType != null && outputType.isArray()) {
          Assert.assertArrayEquals(
              StringUtils.format("Values do not match for row %s for expression %s", i, expr),
              (Object[]) eval.valueOrDefault(),
              (Object[]) vectorVals[i]
          );
        } else {
          Assert.assertEquals(
              StringUtils.format("Values do not match for row %s for expression %s", i, expr),
              eval.valueOrDefault(),
              vectorVals[i]
          );
        }
      }
    }
  }

  public static void testExpressionRandomizedBindings(
      String expr,
      Expr parsed,
      Map<String, ExpressionType> types,
      int numIterations
  )
  {
    Expr.InputBindingInspector inspector = InputBindings.inspectorFromTypeMap(types);
    Expr.VectorInputBindingInspector vectorInputBindingInspector = new Expr.VectorInputBindingInspector()
    {
      @Override
      public int getMaxVectorSize()
      {
        return VECTOR_SIZE;
      }

      @Nullable
      @Override
      public ExpressionType getType(String name)
      {
        return inspector.getType(name);
      }
    };
    Assert.assertTrue(StringUtils.format("Cannot vectorize %s", expr), parsed.canVectorize(inspector));
    ExpressionType outputType = parsed.getOutputType(inspector);
    final ExprVectorProcessor processor = parsed.asVectorProcessor(vectorInputBindingInspector);
    // 'null' expressions can have an output type of null, but still evaluate in default mode, so skip type checks
    if (outputType != null) {
      Assert.assertEquals(expr, outputType, processor.getOutputType());
    }
    for (int iterations = 0; iterations < numIterations; iterations++) {
      NonnullPair<Expr.ObjectBinding[], Expr.VectorInputBinding> bindings = makeRandomizedBindings(VECTOR_SIZE, types);
      ExprEvalVector<?> vectorEval = processor.evalVector(bindings.rhs);
      final Object[] vectorVals = vectorEval.getObjectVector();
      for (int i = 0; i < VECTOR_SIZE; i++) {
        ExprEval<?> eval = parsed.eval(bindings.lhs[i]);
        // 'null' expressions can have an output type of null, but still evaluate in default mode, so skip type checks
        if (outputType != null && !eval.isNumericNull()) {
          Assert.assertEquals(eval.type(), outputType);
        }
        if (outputType != null && outputType.isArray()) {
          Assert.assertArrayEquals(
              StringUtils.format("Values do not match for row %s for expression %s", i, expr),
              (Object[]) eval.valueOrDefault(),
              (Object[]) vectorVals[i]
          );
        } else {
          Assert.assertEquals(
              StringUtils.format("Values do not match for row %s for expression %s", i, expr),
              eval.valueOrDefault(),
              vectorVals[i]
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
        () -> r.nextLong(0, Integer.MAX_VALUE - 1),
        r::nextDouble,
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

    for (Map.Entry<String, ExpressionType> entry : types.entrySet()) {
      boolean[] nulls = new boolean[vectorSize];

      switch (entry.getValue().getType()) {
        case LONG:
          long[] longs = new long[vectorSize];
          for (int i = 0; i < vectorSize; i++) {
            nulls[i] = nullsFn.getAsBoolean();
            longs[i] = nulls[i] ? 0L : longsFn.getAsLong();
            if (objectBindings[i] == null) {
              objectBindings[i] = new SettableObjectBinding();
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
              objectBindings[i] = new SettableObjectBinding();
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
              objectBindings[i] = new SettableObjectBinding();
            }
            objectBindings[i].withBinding(entry.getKey(), nulls[i] ? null : strings[i]);
          }
          vectorBinding.addString(entry.getKey(), strings);
          break;
      }
    }

    return new NonnullPair<>(objectBindings, vectorBinding);
  }

}
