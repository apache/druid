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

package org.apache.druid.math.expr.vector;

import com.google.common.base.Preconditions;
import org.apache.druid.error.DruidException;
import org.apache.druid.math.expr.Evals;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.Exprs;
import org.apache.druid.segment.column.Types;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.function.Supplier;

public class VectorProcessors
{
  /**
   * Make a 2 argument, symmetrical processor where both argments must be the same input type and produce the same
   * output type
   *    long, long      -> long
   *    double, double  -> double
   *    string, string  -> string
   */
  public static <T> ExprVectorProcessor<T> makeSymmetricalProcessor(
      Expr.VectorInputBindingInspector inspector,
      Expr left,
      Expr right,
      Supplier<ExprVectorProcessor<?>> longProcessor,
      Supplier<ExprVectorProcessor<?>> doubleProcessor,
      Supplier<ExprVectorProcessor<?>> objectProcessor
  )
  {
    final ExpressionType leftType = left.getOutputType(inspector);

    // if type is null, it means the input is all nulls
    if (leftType == null) {
      return right.asVectorProcessor(inspector);
    }

    Preconditions.checkArgument(
        inspector.areSameTypes(left, right),
        "%s and %s are not the same type",
        leftType,
        right.getOutputType(inspector)
    );

    ExprVectorProcessor<?> processor = null;
    if (Types.is(leftType, ExprType.STRING)) {
      processor = objectProcessor.get();
    } else if (Types.is(leftType, ExprType.LONG)) {
      processor = longProcessor.get();
    } else if (Types.is(leftType, ExprType.DOUBLE)) {
      processor = doubleProcessor.get();
    }

    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (ExprVectorProcessor<T>) processor;
  }

  /**
   * Creates an {@link ExprVectorProcessor} that creates a {@link ExprEvalVector} for a constant any non-numeric value.
   * Numeric types should use {@link #constant(Double, int)} or {@link #constant(Long, int)} instead.
   *
   * @see org.apache.druid.math.expr.ConstantExpr
   */
  public static <T> ExprVectorProcessor<T> constant(@Nullable Object constant, int maxVectorSize, ExpressionType type)
  {
    if (type.isNumeric()) {
      throw DruidException.defensive("Type[%s] should use the numeric constant creator instead", type);
    }
    final Object[] objects = new Object[maxVectorSize];
    Arrays.fill(objects, constant);
    final ExprEvalObjectVector eval = new ExprEvalObjectVector(objects, type);
    return (ExprVectorProcessor<T>) new ConstantVectorProcessor<>(eval, maxVectorSize);
  }

  /**
   * Creates an {@link ExprVectorProcessor} that creates a {@link ExprEvalVector} for a constant {@link Double} value.
   *
   * @see org.apache.druid.math.expr.ConstantExpr
   */
  public static <T> ExprVectorProcessor<T> constant(@Nullable Double constant, int maxVectorSize)
  {
    final double[] doubles = new double[maxVectorSize];
    final boolean[] nulls;
    if (constant == null) {
      nulls = new boolean[maxVectorSize];
      Arrays.fill(nulls, true);
    } else {
      nulls = null;
      Arrays.fill(doubles, constant);
    }
    final ExprEvalDoubleVector eval = new ExprEvalDoubleVector(doubles, nulls);
    return (ExprVectorProcessor<T>) new ConstantVectorProcessor<>(eval, maxVectorSize);
  }

  /**
   * Creates an {@link ExprVectorProcessor} that creates a {@link ExprEvalVector} for a constant {@link Long} value.
   *
   * @see org.apache.druid.math.expr.ConstantExpr
   */
  public static <T> ExprVectorProcessor<T> constant(@Nullable Long constant, int maxVectorSize)
  {
    final long[] longs = new long[maxVectorSize];
    final boolean[] nulls;
    if (constant == null) {
      nulls = new boolean[maxVectorSize];
      Arrays.fill(nulls, true);
    } else {
      nulls = null;
      Arrays.fill(longs, constant);
    }
    final ExprEvalLongVector eval = new ExprEvalLongVector(longs, nulls);
    return (ExprVectorProcessor<T>) new ConstantVectorProcessor<>(eval, maxVectorSize);
  }

  /**
   * Creates an {@link ExprVectorProcessor} that creates a {@link ExprEvalVector} for some expression variable input
   * binding, typically for segment column value vectors from a
   * {@link org.apache.druid.segment.vector.VectorValueSelector} or
   * {@link org.apache.druid.segment.vector.VectorObjectSelector}.
   *
   * @see org.apache.druid.math.expr.IdentifierExpr
   */
  public static ExprVectorProcessor<?> identifier(Expr.VectorInputBindingInspector inspector, String binding)
  {
    final ExpressionType inputType = inspector.getType(binding);

    return new IdentifierVectorProcessor<>(
        inputType == null ? ExpressionType.LONG : inputType,
        binding,
        inspector.getMaxVectorSize()
    );
  }

  /**
   * Creates an {@link ExprVectorProcessor} that will parse a string into a long value given a radix.
   *
   * @see org.apache.druid.math.expr.Function.ParseLong
   */
  public static <T> ExprVectorProcessor<T> parseLong(Expr.VectorInputBindingInspector inspector, Expr arg, int radix)
  {
    final ExprVectorProcessor<?> processor = new LongUnivariateObjectFunctionVectorProcessor(
        arg.asVectorProcessor(inspector),
        ExpressionType.STRING
    )
    {
      @Override
      public int maxVectorSize()
      {
        return inspector.getMaxVectorSize();
      }

      @Override
      public void processIndex(Object[] strings, long[] longs, boolean[] outputNulls, int i)
      {
        try {
          final String input = (String) strings[i];
          if (input == null) {
            longs[i] = 0L;
            outputNulls[i] = true;
          } else {
            if (radix == 16 && (input.startsWith("0x") || input.startsWith("0X"))) {
              // Strip leading 0x from hex strings.
              longs[i] = Long.parseLong(input.substring(2), radix);
            } else {
              longs[i] = Long.parseLong(input, radix);
            }
            outputNulls[i] = false;
          }
        }
        catch (NumberFormatException e) {
          longs[i] = 0L;
          outputNulls[i] = true;
        }
      }
    };

    return (ExprVectorProcessor<T>) processor;
  }

  /**
   * Creates an {@link ExprVectorProcessor} for the 'isnull' function, that produces a "boolean" typed output
   * vector (long[]) with values set to 1 if the input value was null or 0 if it was not null.
   *
   * @see org.apache.druid.math.expr.Function.IsNullFunc
   */
  public static <T> ExprVectorProcessor<T> isNull(Expr.VectorInputBindingInspector inspector, Expr expr)
  {

    final ExpressionType type = expr.getOutputType(inspector);

    if (type == null) {
      return constant(1L, inspector.getMaxVectorSize());
    }
    final long[] outputValues = new long[inspector.getMaxVectorSize()];

    final ExprVectorProcessor<?> processor;
    if (Types.is(type, ExprType.LONG)) {
      final ExprVectorProcessor<long[]> input = expr.asVectorProcessor(inspector);
      processor = new ExprVectorProcessor<long[]>()
      {
        @Override
        public ExprEvalVector<long[]> evalVector(Expr.VectorInputBinding bindings)
        {
          final ExprEvalVector<long[]> inputEval = input.evalVector(bindings);

          final int currentSize = bindings.getCurrentVectorSize();
          final boolean[] nulls = inputEval.getNullVector();
          if (nulls == null) {
            Arrays.fill(outputValues, 0L);
          } else {
            for (int i = 0; i < currentSize; i++) {
              if (nulls[i]) {
                outputValues[i] = 1L;
              } else {
                outputValues[i] = 0L;
              }
            }
          }
          return new ExprEvalLongVector(outputValues, null);
        }

        @Override
        public ExpressionType getOutputType()
        {
          return ExpressionType.LONG;
        }

        @Override
        public int maxVectorSize()
        {
          return outputValues.length;
        }
      };
    } else if (Types.is(type, ExprType.DOUBLE)) {
      final ExprVectorProcessor<double[]> input = expr.asVectorProcessor(inspector);
      processor = new ExprVectorProcessor<long[]>()
      {
        @Override
        public ExprEvalVector<long[]> evalVector(Expr.VectorInputBinding bindings)
        {
          final ExprEvalVector<double[]> inputEval = input.evalVector(bindings);

          final int currentSize = bindings.getCurrentVectorSize();
          final boolean[] nulls = inputEval.getNullVector();
          if (nulls == null) {
            Arrays.fill(outputValues, 0L);
          } else {
            for (int i = 0; i < currentSize; i++) {
              if (nulls[i]) {
                outputValues[i] = 1L;
              } else {
                outputValues[i] = 0L;
              }
            }
          }
          return new ExprEvalLongVector(outputValues, null);
        }

        @Override
        public ExpressionType getOutputType()
        {
          return ExpressionType.LONG;
        }

        @Override
        public int maxVectorSize()
        {
          return outputValues.length;
        }
      };
    } else {
      final ExprVectorProcessor<Object[]> input = expr.asVectorProcessor(inspector);
      processor = new ExprVectorProcessor<long[]>()
      {
        @Override
        public ExprEvalVector<long[]> evalVector(Expr.VectorInputBinding bindings)
        {
          final ExprEvalVector<Object[]> inputEval = input.evalVector(bindings);

          final int currentSize = bindings.getCurrentVectorSize();
          final Object[] values = inputEval.values();
          for (int i = 0; i < currentSize; i++) {
            if (values[i] == null) {
              outputValues[i] = 1L;
            } else {
              outputValues[i] = 0L;
            }
          }
          return new ExprEvalLongVector(outputValues, null);
        }

        @Override
        public ExpressionType getOutputType()
        {
          return ExpressionType.LONG;
        }

        @Override
        public int maxVectorSize()
        {
          return outputValues.length;
        }
      };
    }

    return (ExprVectorProcessor<T>) processor;
  }

  /**
   * Creates an {@link ExprVectorProcessor} for the 'isnotnull' function, that produces a "boolean" typed output
   * vector (long[]) with values set to 1 if the input value was not null or 0 if it was null.
   *
   * @see org.apache.druid.math.expr.Function.IsNotNullFunc
   */
  public static <T> ExprVectorProcessor<T> isNotNull(Expr.VectorInputBindingInspector inspector, Expr expr)
  {

    final ExpressionType type = expr.getOutputType(inspector);
    if (type == null) {
      return constant(0L, inspector.getMaxVectorSize());
    }

    final long[] outputValues = new long[inspector.getMaxVectorSize()];

    final ExprVectorProcessor<?> processor;
    if (Types.is(type, ExprType.LONG)) {
      final ExprVectorProcessor<long[]> input = expr.asVectorProcessor(inspector);
      processor = new ExprVectorProcessor<long[]>()
      {
        @Override
        public ExprEvalVector<long[]> evalVector(Expr.VectorInputBinding bindings)
        {
          final ExprEvalVector<long[]> inputEval = input.evalVector(bindings);

          final int currentSize = bindings.getCurrentVectorSize();
          final boolean[] nulls = inputEval.getNullVector();
          if (nulls == null) {
            Arrays.fill(outputValues, 1L);
          } else {
            for (int i = 0; i < currentSize; i++) {
              if (nulls[i]) {
                outputValues[i] = 0L;
              } else {
                outputValues[i] = 1L;
              }
            }
          }
          return new ExprEvalLongVector(outputValues, null);
        }

        @Override
        public ExpressionType getOutputType()
        {
          return ExpressionType.LONG;
        }

        @Override
        public int maxVectorSize()
        {
          return outputValues.length;
        }
      };
    } else if (Types.is(type, ExprType.DOUBLE)) {
      final ExprVectorProcessor<double[]> input = expr.asVectorProcessor(inspector);
      processor = new ExprVectorProcessor<long[]>()
      {
        @Override
        public ExprEvalVector<long[]> evalVector(Expr.VectorInputBinding bindings)
        {
          final ExprEvalVector<double[]> inputEval = input.evalVector(bindings);

          final int currentSize = bindings.getCurrentVectorSize();
          final boolean[] nulls = inputEval.getNullVector();
          if (nulls == null) {
            Arrays.fill(outputValues, 1L);
          } else {
            for (int i = 0; i < currentSize; i++) {
              if (nulls[i]) {
                outputValues[i] = 0L;
              } else {
                outputValues[i] = 1L;
              }
            }
          }
          return new ExprEvalLongVector(outputValues, null);
        }

        @Override
        public ExpressionType getOutputType()
        {
          return ExpressionType.LONG;
        }

        @Override
        public int maxVectorSize()
        {
          return outputValues.length;
        }
      };
    } else {
      final ExprVectorProcessor<Object[]> input = expr.asVectorProcessor(inspector);
      processor = new ExprVectorProcessor<long[]>()
      {
        @Override
        public ExprEvalVector<long[]> evalVector(Expr.VectorInputBinding bindings)
        {
          final ExprEvalVector<Object[]> inputEval = input.evalVector(bindings);

          final int currentSize = bindings.getCurrentVectorSize();
          final Object[] values = inputEval.values();
          for (int i = 0; i < currentSize; i++) {
            if (values[i] == null) {
              outputValues[i] = 0L;
            } else {
              outputValues[i] = 1L;
            }
          }
          return new ExprEvalLongVector(outputValues, null);
        }

        @Override
        public ExpressionType getOutputType()
        {
          return ExpressionType.LONG;
        }

        @Override
        public int maxVectorSize()
        {
          return inspector.getMaxVectorSize();
        }
      };
    }

    return (ExprVectorProcessor<T>) processor;
  }

  /**
   * Creates an {@link ExprVectorProcessor} for the logical 'not' operator, which produces a long typed vector output
   * with values set by the following rules:
   *    false -> true (1)
   *    null -> null
   *    true -> false (0)
   *
   * @see org.apache.druid.math.expr.UnaryNotExpr
   */
  public static <T> ExprVectorProcessor<T> not(Expr.VectorInputBindingInspector inspector, Expr expr)
  {
    final ExpressionType inputType = expr.getOutputType(inspector);
    ExprVectorProcessor<?> processor = null;
    if (Types.is(inputType, ExprType.STRING)) {
      processor = new LongUnivariateObjectFunctionVectorProcessor(expr.asVectorProcessor(inspector), ExpressionType.STRING)
      {
        @Override
        public void processIndex(Object[] strings, long[] longs, boolean[] outputNulls, int i)
        {
          outputNulls[i] = strings[i] == null;
          if (!outputNulls[i]) {
            longs[i] = Evals.asLong(!Evals.asBoolean((String) strings[i]));
          }
        }
      };
    } else if (Types.is(inputType, ExprType.LONG)) {
      processor = new LongUnivariateLongFunctionVectorProcessor(
          expr.asVectorProcessor(inspector),
          (input) -> Evals.asLong(!Evals.asBoolean(input))
      );
    } else if (Types.is(inputType, ExprType.DOUBLE)) {

      processor = new LongUnivariateDoubleFunctionVectorProcessor(
          expr.asVectorProcessor(inspector),
          input -> Evals.asLong(!Evals.asBoolean(input))
      );
    }
    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (ExprVectorProcessor<T>) processor;
  }

  /**
   * Creates an {@link ExprVectorProcessor} for the logical 'or' operator, which produces a long typed vector output
   * with values set by the following rules:
   *    true/null, null/true -> true (1)
   *    false/null, null/false, null/null -> null
   *    false/false -> false (0)
   *
   * @see org.apache.druid.math.expr.BinOrExpr
   */
  public static <T> ExprVectorProcessor<T> or(Expr.VectorInputBindingInspector inspector, Expr left, Expr right)
  {
    final int maxVectorSize = inspector.getMaxVectorSize();
    return makeSymmetricalProcessor(
        inspector,
        left,
        right,
        () -> new SymmetricalBivariateFunctionVectorProcessor<long[]>(
            ExpressionType.LONG,
            left.asVectorProcessor(inspector),
            right.asVectorProcessor(inspector)
        )
        {
          final long[] output = new long[maxVectorSize];
          final boolean[] outputNulls = new boolean[maxVectorSize];

          @Override
          public void processIndex(
              long[] leftInput,
              @Nullable boolean[] leftNulls,
              long[] rightInput,
              @Nullable boolean[] rightNulls,
              int i
          )
          {
            // true/null, null/true -> true
            // false/null, null/false, null/null -> null
            final boolean leftNull = leftNulls != null && leftNulls[i];
            final boolean rightNull = rightNulls != null && rightNulls[i];
            if (leftNull) {
              if (rightNull) {
                output[i] = 0L;
                outputNulls[i] = true;
                return;
              }
              final boolean bool = Evals.asBoolean(rightInput[i]);
              output[i] = Evals.asLong(bool);
              outputNulls[i] = !bool;
              return;
            } else if (rightNull) {
              final boolean bool = Evals.asBoolean(leftInput[i]);
              output[i] = Evals.asLong(bool);
              outputNulls[i] = !bool;
              return;
            }
            output[i] = Evals.asLong(Evals.asBoolean(leftInput[i]) || Evals.asBoolean(rightInput[i]));
            outputNulls[i] = false;
          }

          @Override
          public ExprEvalVector<long[]> asEval()
          {
            return new ExprEvalLongVector(output, outputNulls);
          }
        },
        () -> new BivariateFunctionVectorProcessor<double[], double[], long[]>(
            ExpressionType.LONG,
            left.asVectorProcessor(inspector),
            right.asVectorProcessor(inspector)
        )
        {
          final long[] output = new long[maxVectorSize];
          final boolean[] outputNulls = new boolean[maxVectorSize];

          @Override
          public void processIndex(
              double[] leftInput,
              @Nullable boolean[] leftNulls,
              double[] rightInput,
              @Nullable boolean[] rightNulls,
              int i
          )
          {
            // true/null, null/true -> true
            // false/null, null/false, null/null -> null
            final boolean leftNull = leftNulls != null && leftNulls[i];
            final boolean rightNull = rightNulls != null && rightNulls[i];
            if (leftNull) {
              if (rightNull) {
                output[i] = 0;
                outputNulls[i] = true;
                return;
              }
              final boolean bool = Evals.asBoolean(rightInput[i]);
              output[i] = Evals.asLong(bool);
              outputNulls[i] = !bool;
              return;
            } else if (rightNull) {
              final boolean bool = Evals.asBoolean(leftInput[i]);
              output[i] = Evals.asLong(bool);
              outputNulls[i] = !bool;
              return;
            }
            output[i] = Evals.asLong(Evals.asBoolean(leftInput[i]) || Evals.asBoolean(rightInput[i]));
            outputNulls[i] = false;
          }

          @Override
          public ExprEvalVector<long[]> asEval()
          {
            return new ExprEvalLongVector(output, outputNulls);
          }
        },
        () -> new BivariateFunctionVectorProcessor<Object[], Object[], long[]>(
            ExpressionType.LONG,
            left.asVectorProcessor(inspector),
            right.asVectorProcessor(inspector)
        )
        {
          final long[] output = new long[maxVectorSize];
          final boolean[] outputNulls = new boolean[maxVectorSize];

          @Override
          public void processIndex(
              Object[] leftInput,
              @Nullable boolean[] leftNulls,
              Object[] rightInput,
              @Nullable boolean[] rightNulls,
              int i
          )
          {
            // true/null, null/true -> true
            // false/null, null/false, null/null -> null
            final boolean leftNull = leftInput[i] == null;
            final boolean rightNull = rightInput[i] == null;
            if (leftNull) {
              if (rightNull) {
                outputNulls[i] = true;
                return;
              }
              final boolean bool = Evals.asBoolean((String) rightInput[i]);
              output[i] = Evals.asLong(bool);
              outputNulls[i] = !bool;
              return;
            } else if (rightNull) {
              final boolean bool = Evals.asBoolean((String) leftInput[i]);
              output[i] = Evals.asLong(bool);
              outputNulls[i] = !bool;
              return;
            }
            output[i] = Evals.asLong(Evals.asBoolean((String) leftInput[i]) || Evals.asBoolean((String) rightInput[i]));
            outputNulls[i] = false;
          }

          @Override
          public ExprEvalVector<long[]> asEval()
          {
            return new ExprEvalLongVector(output, outputNulls);
          }
        }
    );
  }

  /**
   * Creates an {@link ExprVectorProcessor} for the logical 'and' operator, which produces a long typed vector output
   * with values set by the following rules:
   *    true/true -> true (1)
   *    true/null, null/true, null/null -> null
   *    false/null, null/false -> false (0)
   *
   * @see org.apache.druid.math.expr.BinAndExpr
   */
  public static <T> ExprVectorProcessor<T> and(Expr.VectorInputBindingInspector inputTypes, Expr left, Expr right)
  {
    final int maxVectorSize = inputTypes.getMaxVectorSize();
    return makeSymmetricalProcessor(
        inputTypes,
        left,
        right,
        () -> new SymmetricalBivariateFunctionVectorProcessor<long[]>(
            ExpressionType.LONG,
            left.asVectorProcessor(inputTypes),
            right.asVectorProcessor(inputTypes)
        )
        {
          final long[] output = new long[maxVectorSize];
          final boolean[] outputNulls = new boolean[maxVectorSize];

          @Override
          public void processIndex(
              long[] leftInput,
              @Nullable boolean[] leftNulls,
              long[] rightInput,
              @Nullable boolean[] rightNulls,
              int i
          )
          {
            // true/null, null/true, null/null -> null
            // false/null, null/false -> false
            final boolean leftNull = leftNulls != null && leftNulls[i];
            final boolean rightNull = rightNulls != null && rightNulls[i];
            if (leftNull) {
              if (rightNull) {
                output[i] = 0L;
                outputNulls[i] = true;
                return;
              }
              final boolean bool = Evals.asBoolean(rightInput[i]);
              output[i] = Evals.asLong(bool);
              outputNulls[i] = bool;
              return;
            } else if (rightNull) {
              final boolean bool = Evals.asBoolean(leftInput[i]);
              output[i] = Evals.asLong(bool);
              outputNulls[i] = bool;
              return;
            }
            output[i] = Evals.asLong(Evals.asBoolean(leftInput[i]) && Evals.asBoolean(rightInput[i]));
            outputNulls[i] = false;
          }

          @Override
          public ExprEvalVector<long[]> asEval()
          {
            return new ExprEvalLongVector(output, outputNulls);
          }
        },
        () -> new BivariateFunctionVectorProcessor<double[], double[], long[]>(
            ExpressionType.LONG,
            left.asVectorProcessor(inputTypes),
            right.asVectorProcessor(inputTypes)
        )
        {
          final long[] output = new long[maxVectorSize];
          final boolean[] outputNulls = new boolean[maxVectorSize];

          @Override
          public void processIndex(
              double[] leftInput,
              @Nullable boolean[] leftNulls,
              double[] rightInput,
              @Nullable boolean[] rightNulls,
              int i
          )
          {
            // true/null, null/true, null/null -> null
            // false/null, null/false -> false
            final boolean leftNull = leftNulls != null && leftNulls[i];
            final boolean rightNull = rightNulls != null && rightNulls[i];
            if (leftNull) {
              if (rightNull) {
                output[i] = 0L;
                outputNulls[i] = true;
                return;
              }
              final boolean bool = Evals.asBoolean(rightInput[i]);
              output[i] = Evals.asLong(bool);
              outputNulls[i] = bool;
              return;
            } else if (rightNull) {
              final boolean bool = Evals.asBoolean(leftInput[i]);
              output[i] = Evals.asLong(bool);
              outputNulls[i] = bool;
              return;
            }
            output[i] = Evals.asLong(Evals.asBoolean(leftInput[i]) && Evals.asBoolean(rightInput[i]));
            outputNulls[i] = false;
          }

          @Override
          public ExprEvalVector<long[]> asEval()
          {
            return new ExprEvalLongVector(output, outputNulls);
          }
        },
        () -> new BivariateFunctionVectorProcessor<Object[], Object[], long[]>(
            ExpressionType.LONG,
            left.asVectorProcessor(inputTypes),
            right.asVectorProcessor(inputTypes)
        )
        {
          final long[] output = new long[maxVectorSize];
          final boolean[] outputNulls = new boolean[maxVectorSize];

          @Override
          public void processIndex(
              Object[] leftInput,
              @Nullable boolean[] leftNulls,
              Object[] rightInput,
              @Nullable boolean[] rightNulls,
              int i
          )
          {
            // true/null, null/true, null/null -> null
            // false/null, null/false -> false
            final boolean leftNull = leftInput[i] == null;
            final boolean rightNull = rightInput[i] == null;
            if (leftNull) {
              if (rightNull) {
                outputNulls[i] = true;
                return;
              }
              final boolean bool = Evals.asBoolean((String) rightInput[i]);
              output[i] = Evals.asLong(bool);
              outputNulls[i] = bool;
              return;
            } else if (rightNull) {
              final boolean bool = Evals.asBoolean((String) leftInput[i]);
              output[i] = Evals.asLong(bool);
              outputNulls[i] = bool;
              return;
            }
            output[i] = Evals.asLong(
                Evals.asBoolean((String) leftInput[i]) && Evals.asBoolean((String) rightInput[i])
            );
            outputNulls[i] = false;
          }

          @Override
          public ExprEvalVector<long[]> asEval()
          {
            return new ExprEvalLongVector(output, outputNulls);
          }
        }
    );
  }

  private VectorProcessors()
  {
    // No instantiation
  }

  static final class ConstantVectorProcessor<T> implements ExprVectorProcessor<T>
  {
    private final ExprEvalVector<T> constantVector;
    private final int maxVectorSize;

    ConstantVectorProcessor(ExprEvalVector<T> constantVector, int maxVectorSize)
    {
      this.constantVector = constantVector;
      this.maxVectorSize = maxVectorSize;
    }

    @Override
    public ExprEvalVector<T> evalVector(Expr.VectorInputBinding bindings)
    {
      return constantVector;
    }

    @Override
    public ExpressionType getOutputType()
    {
      return constantVector.getType();
    }

    @Override
    public int maxVectorSize()
    {
      return maxVectorSize;
    }
  }
  /**
   * Basic scaffolding for an 'identifier' {@link ExprVectorProcessor}
   * 
   * @see #identifier
   */
  static final class IdentifierVectorProcessor<T> implements ExprVectorProcessor<T>
  {
    private final ExpressionType outputType;
    private final String bindingName;
    private final int maxVectorSize;

    public IdentifierVectorProcessor(ExpressionType outputType, String bindingName, int maxVectorSize)
    {
      this.outputType = outputType;
      this.bindingName = bindingName;
      this.maxVectorSize = maxVectorSize;
    }

    @Override
    public ExprEvalVector<T> evalVector(Expr.VectorInputBinding bindings)
    {
      return new ExprEvalBindingVector<>(outputType, bindings, bindingName);
    }

    @Override
    public ExpressionType getOutputType()
    {
      return outputType;
    }

    @Override
    public int maxVectorSize()
    {
      return maxVectorSize;
    }
  }
}
