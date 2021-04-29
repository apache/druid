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
import org.apache.druid.math.expr.Exprs;

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
      Supplier<ExprVectorProcessor<long[]>> longProcessor,
      Supplier<ExprVectorProcessor<double[]>> doubleProcessor,
      Supplier<ExprVectorProcessor<String[]>> stringProcessor
  )
  {
    final ExprType leftType = left.getOutputType(inspector);

    if (leftType == null) {
      return right.buildVectorized(inspector);
    }

    Preconditions.checkArgument(inspector.areSameTypes(left, right));

    ExprVectorProcessor<?> processor = null;
    if (ExprType.STRING == leftType) {
      processor = stringProcessor.get();
    } else if (ExprType.LONG == leftType) {
      processor = longProcessor.get();
    } else if (ExprType.DOUBLE == leftType) {
      processor = doubleProcessor.get();
    }

    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (ExprVectorProcessor<T>) processor;
  }

  public static <T> ExprVectorProcessor<T> constant(@Nullable String constant, int maxVectorSize)
  {
    final String[] strings = new String[maxVectorSize];
    Arrays.fill(strings, constant);
    final ExprEvalStringVector eval = new ExprEvalStringVector(strings);
    return new ExprVectorProcessor<T>()
    {
      @Override
      public ExprEvalVector<T> evalVector(Expr.VectorInputBinding bindings)
      {
        return (ExprEvalVector<T>) eval;
      }

      @Override
      public ExprType getOutputType()
      {
        return ExprType.STRING;
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
      public ExprType getOutputType()
      {
        return ExprType.DOUBLE;
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
      public ExprType getOutputType()
      {
        return ExprType.LONG;
      }
    };
  }

  public static <T> ExprVectorProcessor<T> parseLong(Expr.VectorInputBindingInspector inspector, Expr arg, int radix)
  {
    final ExprVectorProcessor<?> processor = new LongOutStringInFunctionVectorProcessor(
        CastToTypeVectorProcessor.cast(arg.buildVectorized(inspector), ExprType.STRING),
        inspector.getMaxVectorSize()
    )
    {
      @Override
      public void processIndex(String[] strings, long[] longs, boolean[] outputNulls, int i)
      {
        try {
          final String input = strings[i];
          if (radix == 16 && (input.startsWith("0x") || input.startsWith("0X"))) {
            // Strip leading 0x from hex strings.
            longs[i] = Long.parseLong(input.substring(2), radix);
          } else {
            longs[i] = Long.parseLong(input, radix);
          }
          outputNulls[i] = false;
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

    final ExprType type = expr.getOutputType(inspector);

    if (type == null) {
      return constant(1L, inspector.getMaxVectorSize());
    }
    final long[] outputValues = new long[inspector.getMaxVectorSize()];

    ExprVectorProcessor<?> processor = null;
    if (ExprType.STRING == type) {
      final ExprVectorProcessor<String[]> input = expr.buildVectorized(inspector);
      processor = new ExprVectorProcessor<long[]>()
      {
        @Override
        public ExprEvalVector<long[]> evalVector(Expr.VectorInputBinding bindings)
        {
          final ExprEvalVector<String[]> inputEval = input.evalVector(bindings);

          final int currentSize = bindings.getCurrentVectorSize();
          final String[] values = inputEval.values();
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
        public ExprType getOutputType()
        {
          return ExprType.LONG;
        }
      };
    } else if (ExprType.LONG == type) {
      final ExprVectorProcessor<long[]> input = expr.buildVectorized(inspector);
      processor = new ExprVectorProcessor<long[]>()
      {
        @Override
        public ExprEvalVector<long[]> evalVector(Expr.VectorInputBinding bindings)
        {
          final ExprEvalVector<long[]> inputEval = input.evalVector(bindings);

          final int currentSize = bindings.getCurrentVectorSize();
          final boolean[] nulls = inputEval.getNullVector();
          for (int i = 0; i < currentSize; i++) {
            if (nulls != null && nulls[i]) {
              outputValues[i] = 1L;
            } else {
              outputValues[i] = 0L;
            }
          }
          return new ExprEvalLongVector(outputValues, null);
        }

        @Override
        public ExprType getOutputType()
        {
          return ExprType.LONG;
        }
      };
    } else if (ExprType.DOUBLE == type) {
      final ExprVectorProcessor<double[]> input = expr.buildVectorized(inspector);
      processor = new ExprVectorProcessor<long[]>()
      {
        @Override
        public ExprEvalVector<long[]> evalVector(Expr.VectorInputBinding bindings)
        {
          final ExprEvalVector<double[]> inputEval = input.evalVector(bindings);

          final int currentSize = bindings.getCurrentVectorSize();
          final boolean[] nulls = inputEval.getNullVector();
          for (int i = 0; i < currentSize; i++) {
            if (nulls != null && nulls[i]) {
              outputValues[i] = 1L;
            } else {
              outputValues[i] = 0L;
            }
          }
          return new ExprEvalLongVector(outputValues, null);
        }

        @Override
        public ExprType getOutputType()
        {
          return ExprType.LONG;
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

    final ExprType type = expr.getOutputType(inspector);
    if (type == null) {
      return constant(0L, inspector.getMaxVectorSize());
    }

    final long[] outputValues = new long[inspector.getMaxVectorSize()];

    ExprVectorProcessor<?> processor = null;
    if (ExprType.STRING == type) {
      final ExprVectorProcessor<String[]> input = expr.buildVectorized(inspector);
      processor = new ExprVectorProcessor<long[]>()
      {
        @Override
        public ExprEvalVector<long[]> evalVector(Expr.VectorInputBinding bindings)
        {
          final ExprEvalVector<String[]> inputEval = input.evalVector(bindings);

          final int currentSize = bindings.getCurrentVectorSize();
          final String[] values = inputEval.values();
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
        public ExprType getOutputType()
        {
          return ExprType.LONG;
        }
      };
    } else if (ExprType.LONG == type) {
      final ExprVectorProcessor<long[]> input = expr.buildVectorized(inspector);
      processor = new ExprVectorProcessor<long[]>()
      {
        @Override
        public ExprEvalVector<long[]> evalVector(Expr.VectorInputBinding bindings)
        {
          final ExprEvalVector<long[]> inputEval = input.evalVector(bindings);

          final int currentSize = bindings.getCurrentVectorSize();
          final boolean[] nulls = inputEval.getNullVector();
          for (int i = 0; i < currentSize; i++) {
            if (nulls != null && nulls[i]) {
              outputValues[i] = 0L;
            } else {
              outputValues[i] = 1L;
            }
          }
          return new ExprEvalLongVector(outputValues, null);
        }

        @Override
        public ExprType getOutputType()
        {
          return ExprType.LONG;
        }
      };
    } else if (ExprType.DOUBLE == type) {
      final ExprVectorProcessor<double[]> input = expr.buildVectorized(inspector);
      processor = new ExprVectorProcessor<long[]>()
      {
        @Override
        public ExprEvalVector<long[]> evalVector(Expr.VectorInputBinding bindings)
        {
          final ExprEvalVector<double[]> inputEval = input.evalVector(bindings);

          final int currentSize = bindings.getCurrentVectorSize();
          final boolean[] nulls = inputEval.getNullVector();
          for (int i = 0; i < currentSize; i++) {
            if (nulls != null && nulls[i]) {
              outputValues[i] = 0L;
            } else {
              outputValues[i] = 1L;
            }
          }
          return new ExprEvalLongVector(outputValues, null);
        }

        @Override
        public ExprType getOutputType()
        {
          return ExprType.LONG;
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
            ExprType.LONG,
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
            ExprType.DOUBLE,
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
        () -> new SymmetricalBivariateFunctionVectorProcessor<String[]>(
            ExprType.STRING,
            left.buildVectorized(inspector),
            right.buildVectorized(inspector)
        )
        {
          final String[] output = new String[maxVectorSize];

          @Override
          public void processIndex(
              String[] leftInput,
              @Nullable boolean[] leftNulls,
              String[] rightInput,
              @Nullable boolean[] rightNulls,
              int i
          )
          {
            output[i] = leftInput[i] != null ? leftInput[i] : rightInput[i];
          }

          @Override
          public ExprEvalVector<String[]> asEval()
          {
            return new ExprEvalStringVector(output);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> not(Expr.VectorInputBindingInspector inspector, Expr expr)
  {
    final ExprType inputType = expr.getOutputType(inspector);
    final int maxVectorSize = inspector.getMaxVectorSize();
    ExprVectorProcessor<?> processor = null;
    if (ExprType.STRING.equals(inputType)) {
      processor = new LongOutStringInFunctionVectorProcessor(expr.buildVectorized(inspector), maxVectorSize)
      {
        @Override
        public void processIndex(String[] strings, long[] longs, boolean[] outputNulls, int i)
        {
          outputNulls[i] = strings[i] == null;
          if (!outputNulls[i]) {
            longs[i] = Evals.asLong(!Evals.asBoolean(strings[i]));
          }
        }
      };
    } else if (ExprType.LONG.equals(inputType)) {
      processor = new LongOutLongInFunctionVectorValueProcessor(expr.buildVectorized(inspector), maxVectorSize)
      {
        @Override
        public long apply(long input)
        {
          return Evals.asLong(!Evals.asBoolean(input));
        }
      };
    } else if (ExprType.DOUBLE.equals(inputType)) {
      processor = new DoubleOutDoubleInFunctionVectorValueProcessor(expr.buildVectorized(inspector), maxVectorSize)
      {
        @Override
        public double apply(double input)
        {
          return Evals.asDouble(!Evals.asBoolean(input));
        }
      };
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
            ExprType.LONG,
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
              // true/null, null/true, null/null -> true
              // false/null, null/false -> null
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
        () -> new SymmetricalBivariateFunctionVectorProcessor<double[]>(
            ExprType.DOUBLE,
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
            if (NullHandling.sqlCompatible()) {
              // true/null, null/true, null/null -> true
              // false/null, null/false -> null
              final boolean leftNull = leftNulls != null && leftNulls[i];
              final boolean rightNull = rightNulls != null && rightNulls[i];
              if (leftNull) {
                if (rightNull) {
                  output[i] = 0.0;
                  outputNulls[i] = true;
                  return;
                }
                final boolean bool = Evals.asBoolean(rightInput[i]);
                output[i] = Evals.asDouble(bool);
                outputNulls[i] = !bool;
                return;
              } else if (rightNull) {
                final boolean bool = Evals.asBoolean(leftInput[i]);
                output[i] = Evals.asDouble(bool);
                outputNulls[i] = !bool;
                return;
              }
            }
            output[i] = Evals.asDouble(Evals.asBoolean(leftInput[i]) || Evals.asBoolean(rightInput[i]));
          }

          @Override
          public ExprEvalVector<double[]> asEval()
          {
            return new ExprEvalDoubleVector(output, outputNulls);
          }
        },
        () -> new SymmetricalBivariateFunctionVectorProcessor<String[]>(
            ExprType.STRING,
            left.buildVectorized(inspector),
            right.buildVectorized(inspector)
        )
        {
          final String[] output = new String[maxVectorSize];

          @Override
          public void processIndex(
              String[] leftInput,
              @Nullable boolean[] leftNulls,
              String[] rightInput,
              @Nullable boolean[] rightNulls,
              int i
          )
          {
            // true/null, null/true, null/null -> true
            // false/null, null/false -> null
            final boolean leftNull = leftInput[i] == null;
            final boolean rightNull = rightInput[i] == null;
            if (leftNull) {
              if (rightNull) {
                output[i] = null;
                return;
              }
              final boolean bool = Evals.asBoolean(rightInput[i]);
              output[i] = bool ? Boolean.toString(true) : null;
              return;
            } else if (rightNull) {
              final boolean bool = Evals.asBoolean(leftInput[i]);
              output[i] = bool ? Boolean.toString(true) : null;
              return;
            }
            output[i] = Boolean.toString(Evals.asBoolean(leftInput[i]) || Evals.asBoolean(rightInput[i]));
          }

          @Override
          public ExprEvalVector<String[]> asEval()
          {
            return new ExprEvalStringVector(output);
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
            ExprType.LONG,
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
        () -> new SymmetricalBivariateFunctionVectorProcessor<double[]>(
            ExprType.DOUBLE,
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes)
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
                output[i] = Evals.asDouble(bool);
                outputNulls[i] = bool;
                return;
              } else if (rightNull) {
                final boolean bool = Evals.asBoolean(leftInput[i]);
                output[i] = Evals.asDouble(bool);
                outputNulls[i] = bool;
                return;
              }
            }
            output[i] = Evals.asDouble(Evals.asBoolean(leftInput[i]) && Evals.asBoolean(rightInput[i]));
          }

          @Override
          public ExprEvalVector<double[]> asEval()
          {
            return new ExprEvalDoubleVector(output, outputNulls);
          }
        },
        () -> new SymmetricalBivariateFunctionVectorProcessor<String[]>(
            ExprType.STRING,
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes)
        )
        {
          final String[] output = new String[maxVectorSize];

          @Override
          public void processIndex(
              String[] leftInput,
              @Nullable boolean[] leftNulls,
              String[] rightInput,
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
                output[i] = null;
                return;
              }
              final boolean bool = Evals.asBoolean(rightInput[i]);
              output[i] = !bool ? Boolean.toString(false) : null;
              return;
            } else if (rightNull) {
              final boolean bool = Evals.asBoolean(leftInput[i]);
              output[i] = !bool ? Boolean.toString(false) : null;
              return;
            }
            output[i] = Boolean.toString(Evals.asBoolean(leftInput[i]) && Evals.asBoolean(rightInput[i]));
          }

          @Override
          public ExprEvalVector<String[]> asEval()
          {
            return new ExprEvalStringVector(output);
          }
        }
    );
  }

  private VectorProcessors()
  {
    // No instantiation
  }
}
