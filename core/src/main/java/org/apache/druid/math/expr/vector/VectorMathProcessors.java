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
  public static <T> ExprVectorProcessor<T> makeMathProcessor(
      Expr.VectorInputBindingTypes inputTypes,
      Expr arg,
      Supplier<LongOutLongInFunctionVectorProcessor> longOutLongInSupplier,
      Supplier<DoubleOutDoubleInFunctionVectorProcessor> doubleOutDoubleInSupplier
  )
  {
    final ExprType inputType = arg.getOutputType(inputTypes);

    ExprVectorProcessor<?> processor = null;
    if (ExprType.LONG.equals(inputType)) {
      processor = longOutLongInSupplier.get();
    } else if (ExprType.DOUBLE.equals(inputType)) {
      processor = doubleOutDoubleInSupplier.get();
    }
    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (ExprVectorProcessor<T>) processor;
  }

  /**
   * Make a 1 argument math processor with the following type rules
   *    long    -> double
   *    double  -> double
   */
  public static <T> ExprVectorProcessor<T> makeDoubleMathProcessor(
      Expr.VectorInputBindingTypes inputTypes,
      Expr arg,
      Supplier<DoubleOutLongInFunctionVectorProcessor> doubleOutLongInSupplier,
      Supplier<DoubleOutDoubleInFunctionVectorProcessor> doubleOutDoubleInSupplier
  )
  {
    final ExprType inputType = arg.getOutputType(inputTypes);

    ExprVectorProcessor<?> processor = null;
    if (ExprType.LONG.equals(inputType)) {
      processor = doubleOutLongInSupplier.get();
    } else if (ExprType.DOUBLE.equals(inputType)) {
      processor = doubleOutDoubleInSupplier.get();
    }
    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (ExprVectorProcessor<T>) processor;
  }

  /**
   * Make a 1 argument math processor with the following type rules
   *    long    -> long
   *    double  -> long
   */
  public static <T> ExprVectorProcessor<T> makeLongMathProcessor(
      Expr.VectorInputBindingTypes inputTypes,
      Expr arg,
      Supplier<LongOutLongInFunctionVectorProcessor> longOutLongInSupplier,
      Supplier<LongOutDoubleInFunctionVectorProcessor> longOutDoubleInSupplier
  )
  {
    final ExprType inputType = arg.getOutputType(inputTypes);

    ExprVectorProcessor<?> processor = null;
    if (ExprType.LONG.equals(inputType)) {
      processor = longOutLongInSupplier.get();
    } else if (ExprType.DOUBLE.equals(inputType)) {
      processor = longOutDoubleInSupplier.get();
    }
    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (ExprVectorProcessor<T>) processor;
  }

  /**
   * Make a 2 argument, math processor with the following type rules
   *    long, long      -> long
   *    long, double    -> double
   *    double, long    -> double
   *    double, double  -> double
   */
  public static <T> ExprVectorProcessor<T> makeMathProcessor(
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
    ExprVectorProcessor<?> processor = null;
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
    return (ExprVectorProcessor<T>) processor;
  }

  /**
   * Make a 2 argument, math processor with the following type rules
   *    long, long      -> double
   *    long, double    -> double
   *    double, long    -> double
   *    double, double  -> double
   */
  public static <T> ExprVectorProcessor<T> makeDoubleMathProcessor(
      Expr.VectorInputBindingTypes inputTypes,
      Expr left,
      Expr right,
      Supplier<DoubleOutLongsInFunctionVectorProcessor> doubleOutLongsInProcessor,
      Supplier<DoubleOutLongDoubleInFunctionVectorProcessor> doubleOutLongDoubleInProcessor,
      Supplier<DoubleOutDoubleLongInFunctionVectorProcessor> doubleOutDoubleLongInProcessor,
      Supplier<DoubleOutDoublesInFunctionVectorProcessor> doubleOutDoublesInProcessor
  )
  {
    final ExprType leftType = left.getOutputType(inputTypes);
    final ExprType rightType = right.getOutputType(inputTypes);
    ExprVectorProcessor<?> processor = null;
    if (ExprType.LONG.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = doubleOutLongsInProcessor.get();
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
    return (ExprVectorProcessor<T>) processor;
  }

  /**
   * Make a 2 argument, math processor with the following type rules
   *    long, long      -> long
   *    long, double    -> long
   *    double, long    -> long
   *    double, double  -> long
   */
  public static <T> ExprVectorProcessor<T> makeLongMathProcessor(
      Expr.VectorInputBindingTypes inputTypes,
      Expr left,
      Expr right,
      Supplier<LongOutLongsInFunctionVectorProcessor> longOutLongsInProcessor,
      Supplier<LongOutLongDoubleInFunctionVectorProcessor> longOutLongDoubleInProcessor,
      Supplier<LongOutDoubleLongInFunctionVectorProcessor> longOutDoubleLongInProcessor,
      Supplier<LongOutDoublesInFunctionVectorProcessor> longOutDoublesInProcessor
  )
  {
    final ExprType leftType = left.getOutputType(inputTypes);
    final ExprType rightType = right.getOutputType(inputTypes);
    ExprVectorProcessor<?> processor = null;
    if (ExprType.LONG.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = longOutLongsInProcessor.get();
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = longOutLongDoubleInProcessor.get();
      }
    } else if (ExprType.DOUBLE.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = longOutDoubleLongInProcessor.get();
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = longOutDoublesInProcessor.get();
      }
    }
    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (ExprVectorProcessor<T>) processor;
  }

  public static <T> ExprVectorProcessor<T> plus(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
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

  public static <T> ExprVectorProcessor<T> minus(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
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

  public static <T> ExprVectorProcessor<T> multiply(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
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

  public static <T> ExprVectorProcessor<T> divide(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
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

  public static <T> ExprVectorProcessor<T> longDivide(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
  {
    return makeLongMathProcessor(
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
        () -> new LongOutLongDoubleInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, double right)
          {
            return (long) (left / right);
          }
        },
        () -> new LongOutDoubleLongInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public long apply(double left, long right)
          {
            return (long) (left / right);
          }
        },
        () -> new LongOutDoublesInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public long apply(double left, double right)
          {
            return (long) (left / right);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> modulo(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
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

  public static <T> ExprVectorProcessor<T> negate(Expr.VectorInputBindingTypes inputTypes, Expr arg)
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

  public static <T> ExprVectorProcessor<T> power(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
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

  public static <T> ExprVectorProcessor<T> doublePower(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
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
      return (ExprVectorProcessor<T>) processor;
    }
    return power(inputTypes, left, right);
  }

  public static <T> ExprVectorProcessor<T> max(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
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

  public static <T> ExprVectorProcessor<T> min(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
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

  public static <T> ExprVectorProcessor<T> atan2(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
  {
    return makeDoubleMathProcessor(
        inputTypes,
        left,
        right,
        () -> new DoubleOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, long right)
          {
            return Math.atan2(left, right);
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
            return Math.atan2(left, right);
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
            return Math.atan2(left, right);
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
            return Math.atan2(left, right);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> copySign(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
  {
    return makeDoubleMathProcessor(
        inputTypes,
        left,
        right,
        () -> new DoubleOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, long right)
          {
            return Math.copySign((double) left, (double) right);
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
            return Math.copySign((double) left, right);
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
            return Math.copySign(left, (double) right);
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
            return Math.copySign(left, right);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> hypot(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
  {
    return makeDoubleMathProcessor(
        inputTypes,
        left,
        right,
        () -> new DoubleOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, long right)
          {
            return Math.hypot(left, right);
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
            return Math.hypot(left, right);
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
            return Math.hypot(left, right);
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
            return Math.hypot(left, right);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> remainder(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
  {
    return makeDoubleMathProcessor(
        inputTypes,
        left,
        right,
        () -> new DoubleOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, long right)
          {
            return Math.IEEEremainder(left, right);
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
            return Math.IEEEremainder(left, right);
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
            return Math.IEEEremainder(left, right);
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
            return Math.IEEEremainder(left, right);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> nextAfter(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
  {
    return makeDoubleMathProcessor(
        inputTypes,
        left,
        right,
        () -> new DoubleOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, long right)
          {
            return Math.nextAfter((double) left, (double) right);
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
            return Math.nextAfter((double) left, right);
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
            return Math.nextAfter(left, (double) right);
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
            return Math.nextAfter(left, right);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> scalb(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
  {
    return makeDoubleMathProcessor(
        inputTypes,
        left,
        right,
        () -> new DoubleOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, long right)
          {
            return Math.scalb((double) left, (int) right);
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
            return Math.scalb((double) left, (int) right);
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
            return Math.scalb(left, (int) right);
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
            return Math.scalb(left, (int) right);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> acos(Expr.VectorInputBindingTypes inputTypes, Expr arg)
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
            return Math.acos(input);
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
            return Math.acos(input);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> asin(Expr.VectorInputBindingTypes inputTypes, Expr arg)
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
            return Math.asin(input);
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
            return Math.asin(input);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> atan(Expr.VectorInputBindingTypes inputTypes, Expr arg)
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

  public static <T> ExprVectorProcessor<T> cos(Expr.VectorInputBindingTypes inputTypes, Expr arg)
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

  public static <T> ExprVectorProcessor<T> cosh(Expr.VectorInputBindingTypes inputTypes, Expr arg)
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

  public static <T> ExprVectorProcessor<T> cot(Expr.VectorInputBindingTypes inputTypes, Expr arg)
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

  public static <T> ExprVectorProcessor<T> sin(Expr.VectorInputBindingTypes inputTypes, Expr arg)
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

  public static <T> ExprVectorProcessor<T> sinh(Expr.VectorInputBindingTypes inputTypes, Expr arg)
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

  public static <T> ExprVectorProcessor<T> tan(Expr.VectorInputBindingTypes inputTypes, Expr arg)
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

  public static <T> ExprVectorProcessor<T> tanh(Expr.VectorInputBindingTypes inputTypes, Expr arg)
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

  public static <T> ExprVectorProcessor<T> abs(Expr.VectorInputBindingTypes inputTypes, Expr arg)
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
            return Math.abs(input);
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
            return Math.abs(input);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> cbrt(Expr.VectorInputBindingTypes inputTypes, Expr arg)
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
            return Math.cbrt(input);
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
            return Math.cbrt(input);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> ceil(Expr.VectorInputBindingTypes inputTypes, Expr arg)
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
            return Math.ceil(input);
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
            return Math.ceil(input);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> floor(Expr.VectorInputBindingTypes inputTypes, Expr arg)
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
            return Math.floor(input);
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
            return Math.floor(input);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> exp(Expr.VectorInputBindingTypes inputTypes, Expr arg)
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
            return Math.exp(input);
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
            return Math.exp(input);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> expm1(Expr.VectorInputBindingTypes inputTypes, Expr arg)
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
            return Math.expm1(input);
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
            return Math.expm1(input);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> getExponent(Expr.VectorInputBindingTypes inputTypes, Expr arg)
  {
    return makeLongMathProcessor(
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
            return Math.getExponent((double) input);
          }
        },
        () -> new LongOutDoubleInFunctionVectorProcessor(
            arg.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public long apply(double input)
          {
            return Math.getExponent(input);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> log(Expr.VectorInputBindingTypes inputTypes, Expr arg)
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
            return Math.log(input);
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
            return Math.log(input);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> log10(Expr.VectorInputBindingTypes inputTypes, Expr arg)
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
            return Math.log10(input);
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
            return Math.log10(input);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> log1p(Expr.VectorInputBindingTypes inputTypes, Expr arg)
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
            return Math.log1p(input);
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
            return Math.log1p(input);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> nextUp(Expr.VectorInputBindingTypes inputTypes, Expr arg)
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
            return Math.nextUp((double) input);
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
            return Math.nextUp(input);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> rint(Expr.VectorInputBindingTypes inputTypes, Expr arg)
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
            return Math.rint(input);
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
            return Math.rint(input);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> signum(Expr.VectorInputBindingTypes inputTypes, Expr arg)
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
            return Math.signum(input);
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
            return Math.signum(input);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> sqrt(Expr.VectorInputBindingTypes inputTypes, Expr arg)
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
            return Math.sqrt(input);
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
            return Math.sqrt(input);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> toDegrees(Expr.VectorInputBindingTypes inputTypes, Expr arg)
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
            return Math.toDegrees(input);
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
            return Math.toDegrees(input);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> toRadians(Expr.VectorInputBindingTypes inputTypes, Expr arg)
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
            return Math.toRadians(input);
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
            return Math.toRadians(input);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> ulp(Expr.VectorInputBindingTypes inputTypes, Expr arg)
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
            return Math.ulp((double) input);
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
            return Math.ulp(input);
          }
        }
    );
  }



  private VectorMathProcessors()
  {
    // No instantiation
  }
}
