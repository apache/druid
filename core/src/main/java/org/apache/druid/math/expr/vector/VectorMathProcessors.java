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

import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.Exprs;

import java.util.function.Supplier;

public class VectorMathProcessors
{
  /**
   * Make a 1 argument math processor with the following type rules
   *    long    -> long
   *    double  -> double
   */
  public static <T> VectorExprProcessor<T> makeMathProcessor(
      Expr.VectorInputBindingTypes inputTypes,
      Expr arg,
      Supplier<LongOutLongInFunctionVectorProcessor> longOutLongInSupplier,
      Supplier<DoubleOutDoubleInFunctionVectorProcessor> doubleOutDoubleInSupplier
  )
  {
    final ExprType inputType = arg.getOutputType(inputTypes);

    VectorExprProcessor<?> processor = null;
    if (ExprType.LONG.equals(inputType)) {
      processor = longOutLongInSupplier.get();
    } else if (ExprType.DOUBLE.equals(inputType)) {
      processor = doubleOutDoubleInSupplier.get();
    }
    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (VectorExprProcessor<T>) processor;
  }

  /**
   * Make a 1 argument math processor with the following type rules
   *    long    -> double
   *    double  -> double
   */
  public static <T> VectorExprProcessor<T> makeDoubleMathProcessor(
      Expr.VectorInputBindingTypes inputTypes,
      Expr arg,
      Supplier<DoubleOutLongInFunctionVectorProcessor> doubleOutLongInSupplier,
      Supplier<DoubleOutDoubleInFunctionVectorProcessor> doubleOutDoubleInSupplier
  )
  {
    final ExprType inputType = arg.getOutputType(inputTypes);

    VectorExprProcessor<?> processor = null;
    if (ExprType.LONG.equals(inputType)) {
      processor = doubleOutLongInSupplier.get();
    } else if (ExprType.DOUBLE.equals(inputType)) {
      processor = doubleOutDoubleInSupplier.get();
    }
    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (VectorExprProcessor<T>) processor;
  }

  /**
   * Make a 2 argument, math processor with the following type rules
   *    long, long      -> long
   *    long, double    -> double
   *    double, long    -> double
   *    double, double  -> double
   */
  public static <T> VectorExprProcessor<T> makeMathProcessor(
      Expr.VectorInputBindingTypes inputTypes,
      Expr left,
      Expr right,
      Supplier<LongOutLongsInFunctionVectorProcessor> longOutLongsInProcessor,
      Supplier<DoubleOutLongDoubleInFunctionVectorProcessor> doubleOutLongDoubleInProcessor,
      Supplier<DoubleOutDoubleLongInFunctionVectorProcessor> doubleOutDoubleLongInProcessor,
      Supplier<DoubleOutDoublesInFunctionVectorProcessor> doubleOutDoublesInProcessor
  )
  {
    final ExprType leftType = left.getOutputType(inputTypes);
    final ExprType rightType = right.getOutputType(inputTypes);
    VectorExprProcessor<?> processor = null;
    if (ExprType.LONG.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = longOutLongsInProcessor.get();
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = doubleOutLongDoubleInProcessor.get();
      }
    } else if (ExprType.DOUBLE.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = doubleOutDoubleLongInProcessor.get();
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = doubleOutDoublesInProcessor.get();
      }
    }
    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (VectorExprProcessor<T>) processor;
  }

  public static <T> VectorExprProcessor<T> plus(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
  {
    return makeMathProcessor(
        inputTypes,
        left,
        right,
        () -> new LongOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return left + right;
          }
        },
        () -> new DoubleOutLongDoubleInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return (double) left + right;
          }
        },
        () -> new DoubleOutDoubleLongInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return left + (double) right;
          }
        },
        () -> new DoubleOutDoublesInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return left + right;
          }
        }
    );
  }

  public static <T> VectorExprProcessor<T> minus(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
  {
    return makeMathProcessor(
        inputTypes,
        left,
        right,
        () -> new LongOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return left - right;
          }
        },
        () -> new DoubleOutLongDoubleInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return (double) left - right;
          }
        },
        () -> new DoubleOutDoubleLongInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return left - (double) right;
          }
        },
        () -> new DoubleOutDoublesInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return left - right;
          }
        }
    );
  }

  public static <T> VectorExprProcessor<T> multiply(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
  {
    return makeMathProcessor(
        inputTypes,
        left,
        right,
        () -> new LongOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return left * right;
          }
        },
        () -> new DoubleOutLongDoubleInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return (double) left * right;
          }
        },
        () -> new DoubleOutDoubleLongInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return left * (double) right;
          }
        },
        () -> new DoubleOutDoublesInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return left * right;
          }
        }
    );
  }

  public static <T> VectorExprProcessor<T> divide(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
  {
    return makeMathProcessor(
        inputTypes,
        left,
        right,
        () -> new LongOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return left / right;
          }
        },
        () -> new DoubleOutLongDoubleInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return (double) left / right;
          }
        },
        () -> new DoubleOutDoubleLongInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return left / (double) right;
          }
        },
        () -> new DoubleOutDoublesInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return left / right;
          }
        }
    );
  }

  public static <T> VectorExprProcessor<T> modulo(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
  {
    return makeMathProcessor(
        inputTypes,
        left,
        right,
        () -> new LongOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return left % right;
          }
        },
        () -> new DoubleOutLongDoubleInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return (double) left % right;
          }
        },
        () -> new DoubleOutDoubleLongInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return left % (double) right;
          }
        },
        () -> new DoubleOutDoublesInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return left % right;
          }
        }
    );
  }

  public static <T> VectorExprProcessor<T> negate(Expr.VectorInputBindingTypes inputTypes, Expr arg)
  {
    return makeMathProcessor(
        inputTypes,
        arg,
        () -> new LongOutLongInFunctionVectorProcessor(
            arg.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long input)
          {
            return -input;
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorProcessor(
            arg.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double input)
          {
            return -input;
          }
        }
    );
  }

  public static <T> VectorExprProcessor<T> power(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
  {
    return makeMathProcessor(
        inputTypes,
        left,
        right,
        () -> new LongOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return LongMath.pow(left, Ints.checkedCast(right));
          }
        },
        () -> new DoubleOutLongDoubleInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return Math.pow(left, right);
          }
        },
        () -> new DoubleOutDoubleLongInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return Math.pow(left, right);
          }
        },
        () -> new DoubleOutDoublesInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return Math.pow(left, right);
          }
        }
    );
  }

  public static <T> VectorExprProcessor<T> doublePower(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
  {
    BivariateFunctionVectorProcessor<?, ?, ?> processor = null;
    if (ExprType.LONG.equals(left.getOutputType(inputTypes))) {
      if (ExprType.LONG.equals(right.getOutputType(inputTypes))) {
        processor = new DoubleOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, long right)
          {
            return Math.pow(left, right);
          }
        };
      }
    }

    if (processor != null) {
      return (VectorExprProcessor<T>) processor;
    }
    return power(inputTypes, left, right);
  }

  public static <T> VectorExprProcessor<T> max(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
  {
    return makeMathProcessor(
        inputTypes,
        left,
        right,
        () -> new LongOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return Math.max(left, right);
          }
        },
        () -> new DoubleOutLongDoubleInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return Math.max(left, right);
          }
        },
        () -> new DoubleOutDoubleLongInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return Math.max(left, right);
          }
        },
        () -> new DoubleOutDoublesInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return Math.max(left, right);
          }
        }
    );
  }

  public static <T> VectorExprProcessor<T> min(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
  {
    return makeMathProcessor(
        inputTypes,
        left,
        right,
        () -> new LongOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return Math.min(left, right);
          }
        },
        () -> new DoubleOutLongDoubleInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return Math.min(left, right);
          }
        },
        () -> new DoubleOutDoubleLongInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return Math.min(left, right);
          }
        },
        () -> new DoubleOutDoublesInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return Math.min(left, right);
          }
        }
    );
  }

  public static <T> VectorExprProcessor<T> atan(Expr.VectorInputBindingTypes inputTypes, Expr arg)
  {
    return makeDoubleMathProcessor(
        inputTypes,
        arg,
        () -> new DoubleOutLongInFunctionVectorProcessor(
            arg.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.atan(input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorProcessor(
            arg.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double input)
          {
            return Math.atan(input);
          }
        }
    );
  }

  public static <T> VectorExprProcessor<T> cos(Expr.VectorInputBindingTypes inputTypes, Expr arg)
  {
    return makeDoubleMathProcessor(
        inputTypes,
        arg,
        () -> new DoubleOutLongInFunctionVectorProcessor(
            arg.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.cos(input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorProcessor(
            arg.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double input)
          {
            return Math.cos(input);
          }
        }
    );
  }

  public static <T> VectorExprProcessor<T> cosh(Expr.VectorInputBindingTypes inputTypes, Expr arg)
  {
    return makeDoubleMathProcessor(
        inputTypes,
        arg,
        () -> new DoubleOutLongInFunctionVectorProcessor(
            arg.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.cosh(input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorProcessor(
            arg.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double input)
          {
            return Math.cosh(input);
          }
        }
    );
  }

  public static <T> VectorExprProcessor<T> cot(Expr.VectorInputBindingTypes inputTypes, Expr arg)
  {
    return makeDoubleMathProcessor(
        inputTypes,
        arg,
        () -> new DoubleOutLongInFunctionVectorProcessor(
            arg.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.cos(input) / Math.sin(input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorProcessor(
            arg.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double input)
          {
            return Math.cos(input) / Math.sin(input);
          }
        }
    );
  }

  public static <T> VectorExprProcessor<T> sin(Expr.VectorInputBindingTypes inputTypes, Expr arg)
  {
    return makeDoubleMathProcessor(
        inputTypes,
        arg,
        () -> new DoubleOutLongInFunctionVectorProcessor(
            arg.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.sin(input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorProcessor(
            arg.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double input)
          {
            return Math.sin(input);
          }
        }
    );
  }

  public static <T> VectorExprProcessor<T> sinh(Expr.VectorInputBindingTypes inputTypes, Expr arg)
  {
    return makeDoubleMathProcessor(
        inputTypes,
        arg,
        () -> new DoubleOutLongInFunctionVectorProcessor(
            arg.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.sinh(input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorProcessor(
            arg.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double input)
          {
            return Math.sinh(input);
          }
        }
    );
  }

  public static <T> VectorExprProcessor<T> tan(Expr.VectorInputBindingTypes inputTypes, Expr arg)
  {
    return makeDoubleMathProcessor(
        inputTypes,
        arg,
        () -> new DoubleOutLongInFunctionVectorProcessor(
            arg.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.tan(input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorProcessor(
            arg.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double input)
          {
            return Math.tan(input);
          }
        }
    );
  }

  public static <T> VectorExprProcessor<T> tanh(Expr.VectorInputBindingTypes inputTypes, Expr arg)
  {
    return makeDoubleMathProcessor(
        inputTypes,
        arg,
        () -> new DoubleOutLongInFunctionVectorProcessor(
            arg.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.tanh(input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorProcessor(
            arg.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double input)
          {
            return Math.tanh(input);
          }
        }
    );
  }

  private VectorMathProcessors()
  {
    // No instantiation
  }
}
