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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.SettableObjectBinding;
import org.apache.druid.math.expr.vector.ExprEvalVector;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
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
 * <p>
 * this is not a replacement for correctness tests, but will ensure that vectorized and non-vectorized expression
 * evaluation is at least self consistent...
 */
public class VectorExpressionsSanityTest extends InitializedNullHandlingTest
{
  private static final Logger log = new Logger(VectorExpressionsSanityTest.class);
  private static final int NUM_ITERATIONS = 10;
  private static final int VECTOR_SIZE = 512;
  private static final TimestampShiftExprMacro TIMESTAMP_SHIFT_EXPR_MACRO = new TimestampShiftExprMacro();
  private static final DateTime DATE_TIME = DateTimes.of("2020-11-05T04:05:06");

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

  @BeforeClass
  public static void setupTests()
  {
    ExpressionProcessing.initializeForStrictBooleansTests(true);
  }

  @AfterClass
  public static void teardownTests()
  {
    ExpressionProcessing.initializeForTests(null);
  }

  static void testExpression(String expr, Expr parsed, Map<String, ExpressionType> types)
  {
    log.debug("[%s]", expr);
    NonnullPair<Expr.ObjectBinding[], Expr.VectorInputBinding> bindings;
    for (int iterations = 0; iterations < NUM_ITERATIONS; iterations++) {
      bindings = makeRandomizedBindings(VECTOR_SIZE, types);
      testExpressionWithBindings(expr, parsed, bindings);
    }
    bindings = makeSequentialBinding(VECTOR_SIZE, types);
    testExpressionWithBindings(expr, parsed, bindings);
  }

  private static void testExpressionWithBindings(
      String expr,
      Expr parsed,
      NonnullPair<Expr.ObjectBinding[], Expr.VectorInputBinding> bindings
  )
  {
    Assert.assertTrue(StringUtils.format("Cannot vectorize %s", expr), parsed.canVectorize(bindings.rhs));
    ExpressionType outputType = parsed.getOutputType(bindings.rhs);
    ExprEvalVector<?> vectorEval = parsed.buildVectorized(bindings.rhs).evalVector(bindings.rhs);
    // 'null' expressions can have an output type of null, but still evaluate in default mode, so skip type checks
    if (outputType != null) {
      Assert.assertEquals(outputType, vectorEval.getType());
    }
    for (int i = 0; i < VECTOR_SIZE; i++) {
      ExprEval<?> eval = parsed.eval(bindings.lhs[i]);
      // 'null' expressions can have an output type of null, but still evaluate in default mode, so skip type checks
      if (outputType != null && !eval.isNumericNull()) {
        Assert.assertEquals(eval.type(), outputType);
      }
      Assert.assertEquals(
          StringUtils.format("Values do not match for row %s for expression %s", i, expr),
          eval.value(),
          vectorEval.get(i)
      );
    }
  }

  static NonnullPair<Expr.ObjectBinding[], Expr.VectorInputBinding> makeRandomizedBindings(
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
        r::nextBoolean,
        () -> String.valueOf(r.nextInt())
    );
  }

  static NonnullPair<Expr.ObjectBinding[], Expr.VectorInputBinding> makeSequentialBinding(
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

  @Test
  public void testTimeShiftFn()
  {
    int step = 1;
    Expr parsed = TIMESTAMP_SHIFT_EXPR_MACRO.apply(
        ImmutableList.of(
            ExprEval.of(DATE_TIME.getMillis()).toExpr(),
            ExprEval.of("P1M").toExpr(),
            ExprEval.of(step).toExpr()
        ));
    testExpression("time_shift(l1, s2, 3)", parsed, types);
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

    SettableVectorInputBinding addBinding(String name, ExpressionType type, boolean[] nulls)
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

