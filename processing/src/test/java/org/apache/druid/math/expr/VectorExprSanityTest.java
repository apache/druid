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
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.vector.ExprEvalVector;
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
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
public class VectorExprSanityTest extends InitializedNullHandlingTest
{
  private static final Logger log = new Logger(VectorExprSanityTest.class);
  private static final int NUM_ITERATIONS = 10;
  private static final int VECTOR_SIZE = 512;

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
  public void testUnaryOperators()
  {
    final String[] functions = new String[]{"-"};
    final String[] templates = new String[]{"%sd1", "%sl1"};

    testFunctions(types, templates, functions);
  }

  @Test
  public void testBinaryMathOperators()
  {
    final String[] columns = new String[]{"d1", "d2", "l1", "l2", "1", "1.0", "nonexistent", "null", "s1"};
    final String[] columns2 = new String[]{"d1", "d2", "l1", "l2", "1", "1.0"};
    final String[][] templateInputs = makeTemplateArgs(columns, columns2);
    final String[] templates =
        Arrays.stream(templateInputs)
              .map(i -> StringUtils.format("%s %s %s", i[0], "%s", i[1]))
              .toArray(String[]::new);
    final String[] args = new String[]{"+", "-", "*", "/", "^", "%"};

    testFunctions(types, templates, args);
  }

  @Test
  public void testBinaryComparisonOperators()
  {
    final String[] columns = new String[]{"d1", "d2", "l1", "l2", "1", "1.0", "s1", "s2", "nonexistent", "null"};
    final String[] columns2 = new String[]{"d1", "d2", "l1", "l2", "1", "1.0", "s1", "s2", "null"};
    final String[][] templateInputs = makeTemplateArgs(columns, columns2);
    final String[] templates =
        Arrays.stream(templateInputs)
              .map(i -> StringUtils.format("%s %s %s", i[0], "%s", i[1]))
              .toArray(String[]::new);
    final String[] args = new String[]{">", ">=", "<", "<=", "==", "!="};

    testFunctions(types, templates, args);
  }

  @Test
  public void testUnaryLogicOperators()
  {
    final String[] functions = new String[]{"!"};
    final String[] templates = new String[]{"%sd1", "%sl1", "%sboolString1"};
    testFunctions(types, templates, functions);
  }

  @Test
  public void testBinaryLogicOperators()
  {
    final String[] functions = new String[]{"&&", "||"};
    final String[] templates = new String[]{"d1 %s d2", "l1 %s l2", "boolString1 %s boolString2", "(d1 == d2) %s (l1 == l2)"};
    testFunctions(types, templates, functions);
  }

  @Test
  public void testBinaryOperatorTrees()
  {
    final String[] columns = new String[]{"d1", "l1", "1", "1.0", "nonexistent", "null"};
    final String[] columns2 = new String[]{"d2", "l2", "2", "2.0"};
    final String[][] templateInputs = makeTemplateArgs(columns, columns2, columns2);
    final String[] templates =
        Arrays.stream(templateInputs)
              .map(i -> StringUtils.format("(%s %s %s) %s %s", i[0], "%s", i[1], "%s", i[2]))
              .toArray(String[]::new);
    final String[] ops = new String[]{"+", "-", "*", "/"};
    final String[][] args = makeTemplateArgs(ops, ops);
    testFunctions(types, templates, args);
  }

  @Test
  public void testUnivariateFunctions()
  {
    final String[] functions = new String[]{"parse_long", "isNull", "notNull"};
    final String[] templates = new String[]{"%s(s1)", "%s(l1)", "%s(d1)", "%s(nonexistent)", "%s(null)"};
    testFunctions(types, templates, functions);
  }

  @Test
  public void testUnivariateMathFunctions()
  {
    final String[] functions = new String[]{
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
    };
    final String[] templates = new String[]{"%s(l1)", "%s(d1)", "%s(pi())", "%s(null)"};
    testFunctions(types, templates, functions);
  }

  @Test
  public void testBivariateMathFunctions()
  {
    final String[] functions = new String[]{
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
    };
    final String[] templates = new String[]{
        "%s(d1, d2)",
        "%s(d1, l1)",
        "%s(l1, d1)",
        "%s(l1, l2)",
        "%s(nonexistent, l1)",
        "%s(nonexistent, d1)"
    };
    testFunctions(types, templates, functions);
  }

  @Test
  public void testSymmetricalBivariateFunctions()
  {
    final String[] functions = new String[]{
        "nvl",
    };
    final String[] templates = new String[]{
        "%s(d1, d2)",
        "%s(l1, l2)",
        "%s(s1, s2)",
        "%s(nonexistent, l1)",
        "%s(nonexistent, d1)",
        "%s(nonexistent, s1)"
    };
    testFunctions(types, templates, functions);
  }

  @Test
  public void testCast()
  {
    final String[] columns = new String[]{"d1", "l1", "s1"};
    final String[] castTo = new String[]{"'STRING'", "'LONG'", "'DOUBLE'"};
    final String[][] args = makeTemplateArgs(columns, castTo);
    final String[] templates = new String[]{"cast(%s, %s)"};
    testFunctions(types, templates, args);
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
  public void testConstants()
  {
    testExpression("null", types);
    testExpression("1", types);
    testExpression("1.1", types);
    testExpression("NaN", types);
    testExpression("Infinity", types);
    testExpression("-Infinity", types);
    testExpression("'hello'", types);
  }

  static void testFunctions(Map<String, ExpressionType> types, String[] templates, String[] args)
  {
    for (String template : templates) {
      for (String arg : args) {
        String expr = StringUtils.format(template, arg);
        testExpression(expr, types);
      }
    }
  }

  static void testFunctions(Map<String, ExpressionType> types, String[] templates, String[][] argsArrays)
  {
    for (String template : templates) {
      for (Object[] args : argsArrays) {
        String expr = StringUtils.format(template, args);
        testExpression(expr, types);
      }
    }
  }

  static void testExpression(String expr, Map<String, ExpressionType> types)
  {
    log.debug("[%s]", expr);
    Expr parsed = Parser.parse(expr, ExprMacroTable.nil());

    testExpression(expr, parsed, types, NUM_ITERATIONS);
    testSequentialBinding(expr, parsed, types);
  }

  public static void testSequentialBinding(
      String expr,
      Expr parsed,
      Map<String, ExpressionType> types
  )
  {
    NonnullPair<Expr.ObjectBinding[], Expr.VectorInputBinding> bindings = makeSequentialBinding(VECTOR_SIZE, types);
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
      Assert.assertEquals(
          StringUtils.format("Values do not match for row %s for expression %s", i, expr),
          eval.valueOrDefault(),
          vectorVals[i]
      );
    }
  }

  public static void testExpression(
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
        Assert.assertEquals(
            StringUtils.format("Values do not match for row %s for expression %s", i, expr),
            eval.valueOrDefault(),
            vectorVals[i]
        );
      }
    }
  }

  public static NonnullPair<Expr.ObjectBinding[], Expr.VectorInputBinding> makeRandomizedBindings(
      int vectorSize,
      Map<String, ExpressionType> types
  )
  {

    final ThreadLocalRandom r = ThreadLocalRandom.current();
    return makeBindings(
        vectorSize,
        types,
        () -> r.nextLong(Integer.MAX_VALUE - 1),
        r::nextDouble,
        () -> r.nextDouble(0, 1.0) > 0.9,
        () -> String.valueOf(r.nextInt())
    );
  }

  public static NonnullPair<Expr.ObjectBinding[], Expr.VectorInputBinding> makeSequentialBinding(
      int vectorSize,
      Map<String, ExpressionType> types
  )
  {

    return makeBindings(
        vectorSize,
        types,
        new LongSupplier()
        {
          int counter = 1;

          @Override
          public long getAsLong()
          {
            return counter++;
          }
        },
        new DoubleSupplier()
        {
          int counter = 1;

          @Override
          public double getAsDouble()
          {
            return counter++;
          }
        },
        () -> ThreadLocalRandom.current().nextBoolean(),
        new Supplier<String>()
        {
          int counter = 1;

          @Override
          public String get()
          {
            return String.valueOf(counter++);
          }
        }
    );
  }

  static NonnullPair<Expr.ObjectBinding[], Expr.VectorInputBinding> makeBindings(
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

    final boolean hasNulls = NullHandling.sqlCompatible();
    for (Map.Entry<String, ExpressionType> entry : types.entrySet()) {
      boolean[] nulls = new boolean[vectorSize];

      switch (entry.getValue().getType()) {
        case LONG:
          long[] longs = new long[vectorSize];
          for (int i = 0; i < vectorSize; i++) {
            nulls[i] = hasNulls && nullsFn.getAsBoolean();
            longs[i] = nulls[i] ? 0L : longsFn.getAsLong();
            if (objectBindings[i] == null) {
              objectBindings[i] = new SettableObjectBinding();
            }
            objectBindings[i].withBinding(entry.getKey(), nulls[i] ? null : longs[i]);
          }
          if (hasNulls) {
            vectorBinding.addLong(entry.getKey(), longs, nulls);
          } else {
            vectorBinding.addLong(entry.getKey(), longs);
          }
          break;
        case DOUBLE:
          double[] doubles = new double[vectorSize];
          for (int i = 0; i < vectorSize; i++) {
            nulls[i] = hasNulls && nullsFn.getAsBoolean();
            doubles[i] = nulls[i] ? 0.0 : doublesFn.getAsDouble();
            if (objectBindings[i] == null) {
              objectBindings[i] = new SettableObjectBinding();
            }
            objectBindings[i].withBinding(entry.getKey(), nulls[i] ? null : doubles[i]);
          }
          if (hasNulls) {
            vectorBinding.addDouble(entry.getKey(), doubles, nulls);
          } else {
            vectorBinding.addDouble(entry.getKey(), doubles);
          }
          break;
        case STRING:
          String[] strings = new String[vectorSize];
          for (int i = 0; i < vectorSize; i++) {
            nulls[i] = hasNulls && nullsFn.getAsBoolean();
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

  static String[][] makeTemplateArgs(String[] arg1, String[] arg2)
  {
    return Arrays.stream(arg1)
                 .flatMap(a1 -> Arrays.stream(arg2).map(a2 -> new String[]{a1, a2}))
                 .toArray(String[][]::new);
  }

  static String[][] makeTemplateArgs(String[] arg1, String[] arg2, String[] arg3)
  {
    return Arrays.stream(arg1)
                 .flatMap(a1 ->
                              Arrays.stream(arg2).flatMap(a2 -> Arrays.stream(arg3).map(a3 -> new String[]{a1, a2, a3}))
                 )
                 .toArray(String[][]::new);
  }

  static class SettableVectorInputBinding implements Expr.VectorInputBinding
  {
    private final Map<String, boolean[]> nulls;
    private final Map<String, long[]> longs;
    private final Map<String, double[]> doubles;
    private final Map<String, Object[]> objects;
    private final Map<String, ExpressionType> types;

    private final int vectorSize;

    private int id = 0;

    SettableVectorInputBinding(int vectorSize)
    {
      this.nulls = new HashMap<>();
      this.longs = new HashMap<>();
      this.doubles = new HashMap<>();
      this.objects = new HashMap<>();
      this.types = new HashMap<>();
      this.vectorSize = vectorSize;
    }

    public SettableVectorInputBinding addBinding(String name, ExpressionType type, boolean[] nulls)
    {
      this.nulls.put(name, nulls);
      this.types.put(name, type);
      return this;
    }

    public SettableVectorInputBinding addLong(String name, long[] longs)
    {
      return addLong(name, longs, new boolean[longs.length]);
    }

    public SettableVectorInputBinding addLong(String name, long[] longs, boolean[] nulls)
    {
      assert longs.length == vectorSize;
      this.longs.put(name, longs);
      return addBinding(name, ExpressionType.LONG, nulls);
    }

    public SettableVectorInputBinding addDouble(String name, double[] doubles)
    {
      return addDouble(name, doubles, new boolean[doubles.length]);
    }

    public SettableVectorInputBinding addDouble(String name, double[] doubles, boolean[] nulls)
    {
      assert doubles.length == vectorSize;
      this.doubles.put(name, doubles);
      return addBinding(name, ExpressionType.DOUBLE, nulls);
    }

    public SettableVectorInputBinding addString(String name, String[] strings)
    {
      assert strings.length == vectorSize;
      this.objects.put(name, strings);
      return addBinding(name, ExpressionType.STRING, new boolean[strings.length]);
    }

    @Override
    public <T> T[] getObjectVector(String name)
    {
      return (T[]) objects.getOrDefault(name, new Object[getCurrentVectorSize()]);
    }

    @Override
    public ExpressionType getType(String name)
    {
      return types.get(name);
    }

    @Override
    public long[] getLongVector(String name)
    {
      return longs.getOrDefault(name, new long[getCurrentVectorSize()]);
    }

    @Override
    public double[] getDoubleVector(String name)
    {
      return doubles.getOrDefault(name, new double[getCurrentVectorSize()]);
    }

    @Nullable
    @Override
    public boolean[] getNullVector(String name)
    {
      final boolean[] defaultVector = new boolean[getCurrentVectorSize()];
      Arrays.fill(defaultVector, NullHandling.sqlCompatible());
      return nulls.getOrDefault(name, defaultVector);
    }

    @Override
    public int getMaxVectorSize()
    {
      return vectorSize;
    }

    @Override
    public int getCurrentVectorSize()
    {
      return vectorSize;
    }

    @Override
    public int getCurrentVectorId()
    {
      // never cache, this is just for tests anyway
      return id++;
    }
  }
}
