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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.math.expr.Evals;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.ExpressionProcessing;
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
      Supplier<ExprVectorProcessor<?>> stringProcessor
  )
  {
    final ExpressionType leftType = left.getOutputType(inspector);

    // if type is null, it means the input is all nulls
    if (leftType == null) {
      return right.buildVectorized(inspector);
    }

    Preconditions.checkArgument(
        inspector.areSameTypes(left, right),
        "%s and %s are not the same type",
        leftType,
        right.getOutputType(inspector)
    );

    ExprVectorProcessor<?> processor = null;
    if (Types.is(leftType, ExprType.STRING)) {
      processor = stringProcessor.get();
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

  public static <T> ExprVectorProcessor<T> constant(@Nullable String constant, int maxVectorSize)
  {
    final Object[] strings = new Object[maxVectorSize];
    Arrays.fill(strings, constant);
    final ExprEvalObjectVector eval = new ExprEvalObjectVector(strings);
    return new ExprVectorProcessor<T>()
    {
      @Override
      public ExprEvalVector<T> evalVector(Expr.VectorInputBinding bindings)
      {
        return (ExprEvalVector<T>) eval;
      }

      @Override
      public ExpressionType getOutputType()
      {
        return ExpressionType.STRING;
      }
    };
  }

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
    return new ExprVectorProcessor<T>()
    {
      @Override
      public ExprEvalVector<T> evalVector(Expr.VectorInputBinding bindings)
      {
        return (ExprEvalVector<T>) eval;
      }

      @Override
      public ExpressionType getOutputType()
      {
        return ExpressionType.DOUBLE;
      }
    };
  }

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
    return new ExprVectorProcessor<T>()
    {
      @Override
      public ExprEvalVector<T> evalVector(Expr.VectorInputBinding bindings)
      {
        return (ExprEvalVector<T>) eval;
      }

      @Override
      public ExpressionType getOutputType()
      {
        return ExpressionType.LONG;
      }
    };
  }

  public static <T> ExprVectorProcessor<T> parseLong(Expr.VectorInputBindingInspector inspector, Expr arg, int radix)
  {
    final ExprVectorProcessor<?> processor = new LongOutObjectInFunctionVectorProcessor(
        arg.buildVectorized(inspector),
        inspector.getMaxVectorSize(),
        ExpressionType.STRING
    )
    {
      @Override
      public void processIndex(Object[] strings, long[] longs, boolean[] outputNulls, int i)
      {
        try {
          final String input = (String) strings[i];
          if (input == null) {
            longs[i] = 0L;
            outputNulls[i] = NullHandling.sqlCompatible();
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
          outputNulls[i] = NullHandling.sqlCompatible();
        }
      }
    };

    return (ExprVectorProcessor<T>) processor;
  }

  public static <T> ExprVectorProcessor<T> isNull(Expr.VectorInputBindingInspector inspector, Expr expr)
  {

    final ExpressionType type = expr.getOutputType(inspector);

    if (type == null) {
      return constant(1L, inspector.getMaxVectorSize());
    }
    final long[] outputValues = new long[inspector.getMaxVectorSize()];

    ExprVectorProcessor<?> processor = null;
    if (Types.is(type, ExprType.STRING)) {
      final ExprVectorProcessor<Object[]> input = expr.buildVectorized(inspector);
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
      };
    } else if (Types.is(type, ExprType.LONG)) {
      final ExprVectorProcessor<long[]> input = expr.buildVectorized(inspector);
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
      };
    } else if (Types.is(type, ExprType.DOUBLE)) {
      final ExprVectorProcessor<double[]> input = expr.buildVectorized(inspector);
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
      };
    }

    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (ExprVectorProcessor<T>) processor;
  }

  public static <T> ExprVectorProcessor<T> isNotNull(Expr.VectorInputBindingInspector inspector, Expr expr)
  {

    final ExpressionType type = expr.getOutputType(inspector);
    if (type == null) {
      return constant(0L, inspector.getMaxVectorSize());
    }

    final long[] outputValues = new long[inspector.getMaxVectorSize()];

    ExprVectorProcessor<?> processor = null;
    if (Types.is(type, ExprType.STRING)) {
      final ExprVectorProcessor<Object[]> input = expr.buildVectorized(inspector);
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
      };
    } else if (Types.is(type, ExprType.LONG)) {
      final ExprVectorProcessor<long[]> input = expr.buildVectorized(inspector);
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
      };
    } else if (Types.is(type, ExprType.DOUBLE)) {
      final ExprVectorProcessor<double[]> input = expr.buildVectorized(inspector);
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
      };
    }

    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (ExprVectorProcessor<T>) processor;
  }

  public static <T> ExprVectorProcessor<T> nvl(Expr.VectorInputBindingInspector inspector, Expr left, Expr right)
  {
    final int maxVectorSize = inspector.getMaxVectorSize();

    return makeSymmetricalProcessor(
        inspector,
        left,
        right,
        () -> new SymmetricalBivariateFunctionVectorProcessor<long[]>(
            ExpressionType.LONG,
            left.buildVectorized(inspector),
            right.buildVectorized(inspector)
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
            if (leftNulls != null && leftNulls[i]) {
              if (rightNulls != null) {
                output[i] = rightNulls[i] ? 0L : rightInput[i];
                outputNulls[i] = rightNulls[i];
              } else {
                output[i] = rightInput[i];
              }
            } else {
              output[i] = leftInput[i];
            }
          }

          @Override
          public ExprEvalVector<long[]> asEval()
          {
            return new ExprEvalLongVector(output, outputNulls);
          }
        },
        () -> new SymmetricalBivariateFunctionVectorProcessor<double[]>(
            ExpressionType.DOUBLE,
            left.buildVectorized(inspector),
            right.buildVectorized(inspector)
        )
        {
          final double[] output = new double[maxVectorSize];
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
            if (leftNulls != null && leftNulls[i]) {
              if (rightNulls != null) {
                output[i] = rightNulls[i] ? 0.0 : rightInput[i];
                outputNulls[i] = rightNulls[i];
              } else {
                output[i] = rightInput[i];
              }
            } else {
              output[i] = leftInput[i];
            }
          }

          @Override
          public ExprEvalVector<double[]> asEval()
          {
            return new ExprEvalDoubleVector(output, outputNulls);
          }
        },
        () -> new SymmetricalBivariateFunctionVectorProcessor<Object[]>(
            ExpressionType.STRING,
            left.buildVectorized(inspector),
            right.buildVectorized(inspector)
        )
        {
          final Object[] output = new Object[maxVectorSize];

          @Override
          public void processIndex(
              Object[] leftInput,
              @Nullable boolean[] leftNulls,
              Object[] rightInput,
              @Nullable boolean[] rightNulls,
              int i
          )
          {
            output[i] = leftInput[i] != null ? leftInput[i] : rightInput[i];
          }

          @Override
          public ExprEvalVector<Object[]> asEval()
          {
            return new ExprEvalObjectVector(output);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> not(Expr.VectorInputBindingInspector inspector, Expr expr)
  {
    final ExpressionType inputType = expr.getOutputType(inspector);
    final int maxVectorSize = inspector.getMaxVectorSize();
    ExprVectorProcessor<?> processor = null;
    if (Types.is(inputType, ExprType.STRING)) {
      processor = new LongOutObjectInFunctionVectorProcessor(
          expr.buildVectorized(inspector),
          maxVectorSize,
          ExpressionType.STRING
      )
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
      processor = new LongOutLongInFunctionVectorValueProcessor(expr.buildVectorized(inspector), maxVectorSize)
      {
        @Override
        public long apply(long input)
        {
          return Evals.asLong(!Evals.asBoolean(input));
        }
      };
    } else if (Types.is(inputType, ExprType.DOUBLE)) {
      if (!ExpressionProcessing.useStrictBooleans()) {
        processor = new DoubleOutDoubleInFunctionVectorValueProcessor(expr.buildVectorized(inspector), maxVectorSize)
        {
          @Override
          public double apply(double input)
          {
            return Evals.asDouble(!Evals.asBoolean(input));
          }
        };
      } else {
        processor = new LongOutDoubleInFunctionVectorValueProcessor(expr.buildVectorized(inspector), maxVectorSize)
        {
          @Override
          public long apply(double input)
          {
            return Evals.asLong(!Evals.asBoolean(input));
          }
        };
      }
    }
    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (ExprVectorProcessor<T>) processor;
  }

  public static <T> ExprVectorProcessor<T> or(Expr.VectorInputBindingInspector inspector, Expr left, Expr right)
  {
    final int maxVectorSize = inspector.getMaxVectorSize();
    return makeSymmetricalProcessor(
        inspector,
        left,
        right,
        () -> new SymmetricalBivariateFunctionVectorProcessor<long[]>(
            ExpressionType.LONG,
            left.buildVectorized(inspector),
            right.buildVectorized(inspector)
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
            if (NullHandling.sqlCompatible()) {
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
            }
            output[i] = Evals.asLong(Evals.asBoolean(leftInput[i]) || Evals.asBoolean(rightInput[i]));
          }

          @Override
          public ExprEvalVector<long[]> asEval()
          {
            return new ExprEvalLongVector(output, outputNulls);
          }
        },
        () -> new BivariateFunctionVectorProcessor<double[], double[], long[]>(
            ExpressionType.LONG,
            left.buildVectorized(inspector),
            right.buildVectorized(inspector)
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
            if (NullHandling.sqlCompatible()) {
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
            }
            output[i] = Evals.asLong(Evals.asBoolean(leftInput[i]) || Evals.asBoolean(rightInput[i]));
          }

          @Override
          public ExprEvalVector<long[]> asEval()
          {
            return new ExprEvalLongVector(output, outputNulls);
          }
        },
        () -> new BivariateFunctionVectorProcessor<Object[], Object[], long[]>(
            ExpressionType.LONG,
            left.buildVectorized(inspector),
            right.buildVectorized(inspector)
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
          }

          @Override
          public ExprEvalVector<long[]> asEval()
          {
            return new ExprEvalLongVector(output, outputNulls);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> and(Expr.VectorInputBindingInspector inputTypes, Expr left, Expr right)
  {
    final int maxVectorSize = inputTypes.getMaxVectorSize();
    return makeSymmetricalProcessor(
        inputTypes,
        left,
        right,
        () -> new SymmetricalBivariateFunctionVectorProcessor<long[]>(
            ExpressionType.LONG,
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes)
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
            if (NullHandling.sqlCompatible()) {
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
            }
            output[i] = Evals.asLong(Evals.asBoolean(leftInput[i]) && Evals.asBoolean(rightInput[i]));
          }

          @Override
          public ExprEvalVector<long[]> asEval()
          {
            return new ExprEvalLongVector(output, outputNulls);
          }
        },
        () -> new BivariateFunctionVectorProcessor<double[], double[], long[]>(
            ExpressionType.DOUBLE,
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes)
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
            if (NullHandling.sqlCompatible()) {
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
            }
            output[i] = Evals.asLong(Evals.asBoolean(leftInput[i]) && Evals.asBoolean(rightInput[i]));
          }

          @Override
          public ExprEvalVector<long[]> asEval()
          {
            return new ExprEvalLongVector(output, outputNulls);
          }
        },
        () -> new BivariateFunctionVectorProcessor<Object[], Object[], long[]>(
            ExpressionType.STRING,
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes)
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
}
