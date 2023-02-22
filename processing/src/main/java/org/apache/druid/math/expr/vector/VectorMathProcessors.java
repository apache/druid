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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.Exprs;
import org.apache.druid.segment.column.Types;

import java.util.function.Supplier;

public class VectorMathProcessors
{
  /**
   * Make a 1 argument math processor with the following type rules
   *    long    -> long
   *    double  -> double
   */
  public static <T> ExprVectorProcessor<T> makeMathProcessor(
      Expr.VectorInputBindingInspector inspector,
      Expr arg,
      Supplier<LongOutLongInFunctionVectorValueProcessor> longOutLongInSupplier,
      Supplier<DoubleOutDoubleInFunctionVectorValueProcessor> doubleOutDoubleInSupplier
  )
  {
    final ExpressionType inputType = arg.getOutputType(inspector);

    ExprVectorProcessor<?> processor = null;
    if (inputType != null) {
      if (inputType.is(ExprType.LONG)) {
        processor = longOutLongInSupplier.get();
      } else if (inputType.is(ExprType.DOUBLE)) {
        processor = doubleOutDoubleInSupplier.get();
      }
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
      Expr.VectorInputBindingInspector inspector,
      Expr arg,
      Supplier<DoubleOutLongInFunctionVectorValueProcessor> doubleOutLongInSupplier,
      Supplier<DoubleOutDoubleInFunctionVectorValueProcessor> doubleOutDoubleInSupplier
  )
  {
    final ExpressionType inputType = arg.getOutputType(inspector);

    ExprVectorProcessor<?> processor = null;
    if (inputType != null) {
      if (inputType.is(ExprType.LONG)) {
        processor = doubleOutLongInSupplier.get();
      } else if (inputType.is(ExprType.DOUBLE)) {
        processor = doubleOutDoubleInSupplier.get();
      }
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
      Expr.VectorInputBindingInspector inspector,
      Expr arg,
      Supplier<LongOutLongInFunctionVectorValueProcessor> longOutLongInSupplier,
      Supplier<LongOutDoubleInFunctionVectorValueProcessor> longOutDoubleInSupplier
  )
  {
    final ExpressionType inputType = arg.getOutputType(inspector);

    ExprVectorProcessor<?> processor = null;
    if (inputType != null) {
      if (inputType.is(ExprType.LONG)) {
        processor = longOutLongInSupplier.get();
      } else if (inputType.is(ExprType.DOUBLE)) {
        processor = longOutDoubleInSupplier.get();
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
   *    long, double    -> double
   *    double, long    -> double
   *    double, double  -> double
   */
  public static <T> ExprVectorProcessor<T> makeMathProcessor(
      Expr.VectorInputBindingInspector inspector,
      Expr left,
      Expr right,
      Supplier<LongOutLongsInFunctionVectorValueProcessor> longOutLongsInProcessor,
      Supplier<DoubleOutLongDoubleInFunctionVectorValueProcessor> doubleOutLongDoubleInProcessor,
      Supplier<DoubleOutDoubleLongInFunctionVectorValueProcessor> doubleOutDoubleLongInProcessor,
      Supplier<DoubleOutDoublesInFunctionVectorValueProcessor> doubleOutDoublesInProcessor
  )
  {
    final ExpressionType leftType = left.getOutputType(inspector);
    final ExpressionType rightType = right.getOutputType(inspector);
    ExprVectorProcessor<?> processor = null;
    if (Types.is(leftType, ExprType.LONG)) {
      if (Types.isNullOr(rightType, ExprType.LONG)) {
        processor = longOutLongsInProcessor.get();
      } else if (rightType.anyOf(ExprType.STRING, ExprType.DOUBLE)) {
        processor = doubleOutLongDoubleInProcessor.get();
      }
    } else if (Types.is(leftType, ExprType.DOUBLE)) {
      if (Types.is(rightType, ExprType.LONG)) {
        processor = doubleOutDoubleLongInProcessor.get();
      } else if (Types.isNullOrAnyOf(rightType, ExprType.STRING, ExprType.DOUBLE)) {
        processor = doubleOutDoublesInProcessor.get();
      }
    } else if (leftType == null) {
      if (Types.is(rightType, ExprType.LONG)) {
        processor = longOutLongsInProcessor.get();
      } else if (Types.is(rightType, ExprType.DOUBLE)) {
        processor = doubleOutLongDoubleInProcessor.get();
      } else if (rightType == null) {
        processor = longOutLongsInProcessor.get();
      }
    } else if (leftType.is(ExprType.STRING)) {
      if (Types.is(rightType, ExprType.LONG)) {
        processor = doubleOutDoubleLongInProcessor.get();
      } else if (Types.is(rightType, ExprType.DOUBLE)) {
        processor = doubleOutDoublesInProcessor.get();
      }
    }
    if (processor == null) {
      throw Exprs.cannotVectorize(StringUtils.format("%s %s", leftType, rightType));
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
      Expr.VectorInputBindingInspector inspector,
      Expr left,
      Expr right,
      Supplier<DoubleOutLongsInFunctionVectorValueProcessor> doubleOutLongsInProcessor,
      Supplier<DoubleOutLongDoubleInFunctionVectorValueProcessor> doubleOutLongDoubleInProcessor,
      Supplier<DoubleOutDoubleLongInFunctionVectorValueProcessor> doubleOutDoubleLongInProcessor,
      Supplier<DoubleOutDoublesInFunctionVectorValueProcessor> doubleOutDoublesInProcessor
  )
  {
    final ExpressionType leftType = left.getOutputType(inspector);
    final ExpressionType rightType = right.getOutputType(inspector);
    ExprVectorProcessor<?> processor = null;
    if (Types.is(leftType, ExprType.LONG)) {
      if (Types.is(rightType, ExprType.LONG)) {
        processor = doubleOutLongsInProcessor.get();
      } else if (Types.isNullOr(rightType, ExprType.DOUBLE)) {
        processor = doubleOutLongDoubleInProcessor.get();
      }
    } else if (Types.is(leftType, ExprType.DOUBLE)) {
      if (Types.is(rightType, ExprType.LONG)) {
        processor = doubleOutDoubleLongInProcessor.get();
      } else if (Types.isNullOr(rightType, ExprType.DOUBLE)) {
        processor = doubleOutDoublesInProcessor.get();
      }
    } else if (leftType == null) {
      if (Types.is(rightType, ExprType.LONG)) {
        processor = doubleOutDoubleLongInProcessor.get();
      } else if (Types.is(rightType, ExprType.DOUBLE)) {
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
      Expr.VectorInputBindingInspector inspector,
      Expr left,
      Expr right,
      Supplier<LongOutLongsInFunctionVectorValueProcessor> longOutLongsInProcessor,
      Supplier<LongOutLongDoubleInFunctionVectorValueProcessor> longOutLongDoubleInProcessor,
      Supplier<LongOutDoubleLongInFunctionVectorValueProcessor> longOutDoubleLongInProcessor,
      Supplier<LongOutDoublesInFunctionVectorValueProcessor> longOutDoublesInProcessor
  )
  {
    final ExpressionType leftType = left.getOutputType(inspector);
    final ExpressionType rightType = right.getOutputType(inspector);
    ExprVectorProcessor<?> processor = null;
    if (Types.is(leftType, ExprType.LONG)) {
      if (Types.isNullOr(rightType, ExprType.LONG)) {
        processor = longOutLongsInProcessor.get();
      } else if (rightType.is(ExprType.DOUBLE)) {
        processor = longOutLongDoubleInProcessor.get();
      }
    } else if (Types.is(leftType, ExprType.DOUBLE)) {
      if (Types.is(rightType, ExprType.LONG)) {
        processor = longOutDoubleLongInProcessor.get();
      } else if (Types.isNullOr(rightType, ExprType.DOUBLE)) {
        processor = longOutDoublesInProcessor.get();
      }
    } else if (leftType == null) {
      if (Types.is(rightType, ExprType.LONG)) {
        processor = longOutLongsInProcessor.get();
      } else if (Types.is(rightType, ExprType.DOUBLE)) {
        processor = longOutDoublesInProcessor.get();
      }
    }
    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (ExprVectorProcessor<T>) processor;
  }

  public static <T> ExprVectorProcessor<T> plus(Expr.VectorInputBindingInspector inspector, Expr left, Expr right)
  {
    return makeMathProcessor(
        inspector,
        left,
        right,
        () -> new LongOutLongsInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return left + right;
          }
        },
        () -> new DoubleOutLongDoubleInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return (double) left + right;
          }
        },
        () -> new DoubleOutDoubleLongInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return left + (double) right;
          }
        },
        () -> new DoubleOutDoublesInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> minus(Expr.VectorInputBindingInspector inspector, Expr left, Expr right)
  {
    return makeMathProcessor(
        inspector,
        left,
        right,
        () -> new LongOutLongsInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return left - right;
          }
        },
        () -> new DoubleOutLongDoubleInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return (double) left - right;
          }
        },
        () -> new DoubleOutDoubleLongInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return left - (double) right;
          }
        },
        () -> new DoubleOutDoublesInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> multiply(Expr.VectorInputBindingInspector inspector, Expr left, Expr right)
  {
    return makeMathProcessor(
        inspector,
        left,
        right,
        () -> new LongOutLongsInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return left * right;
          }
        },
        () -> new DoubleOutLongDoubleInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return (double) left * right;
          }
        },
        () -> new DoubleOutDoubleLongInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return left * (double) right;
          }
        },
        () -> new DoubleOutDoublesInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> divide(Expr.VectorInputBindingInspector inspector, Expr left, Expr right)
  {
    return makeMathProcessor(
        inspector,
        left,
        right,
        () -> new LongOutLongsInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return left / right;
          }
        },
        () -> new DoubleOutLongDoubleInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return (double) left / right;
          }
        },
        () -> new DoubleOutDoubleLongInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return left / (double) right;
          }
        },
        () -> new DoubleOutDoublesInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> longDivide(Expr.VectorInputBindingInspector inspector, Expr left, Expr right)
  {
    return makeLongMathProcessor(
        inspector,
        left,
        right,
        () -> new LongOutLongsInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return left / right;
          }
        },
        () -> new LongOutLongDoubleInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, double right)
          {
            return (long) (left / right);
          }
        },
        () -> new LongOutDoubleLongInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(double left, long right)
          {
            return (long) (left / right);
          }
        },
        () -> new LongOutDoublesInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> modulo(Expr.VectorInputBindingInspector inspector, Expr left, Expr right)
  {
    return makeMathProcessor(
        inspector,
        left,
        right,
        () -> new LongOutLongsInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return left % right;
          }
        },
        () -> new DoubleOutLongDoubleInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return (double) left % right;
          }
        },
        () -> new DoubleOutDoubleLongInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return left % (double) right;
          }
        },
        () -> new DoubleOutDoublesInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> negate(Expr.VectorInputBindingInspector inspector, Expr arg)
  {
    return makeMathProcessor(
        inspector,
        arg,
        () -> new LongOutLongInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long input)
          {
            return -input;
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> power(Expr.VectorInputBindingInspector inspector, Expr left, Expr right)
  {
    return makeMathProcessor(
        inspector,
        left,
        right,
        () -> new LongOutLongsInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return LongMath.pow(left, Ints.checkedCast(right));
          }
        },
        () -> new DoubleOutLongDoubleInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return Math.pow(left, right);
          }
        },
        () -> new DoubleOutDoubleLongInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return Math.pow(left, right);
          }
        },
        () -> new DoubleOutDoublesInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> doublePower(
      Expr.VectorInputBindingInspector inspector,
      Expr left,
      Expr right
  )
  {
    final ExpressionType leftType = left.getOutputType(inspector);
    final ExpressionType rightType = right.getOutputType(inspector);
    ExprVectorProcessor<?> processor = null;
    if ((Types.is(leftType, ExprType.LONG) && Types.isNullOr(rightType, ExprType.LONG)) ||
        (leftType == null && Types.is(rightType, ExprType.LONG))) {
      processor = new DoubleOutLongsInFunctionVectorValueProcessor(
          left.buildVectorized(inspector),
          right.buildVectorized(inspector),
          inspector.getMaxVectorSize()
      )
      {
        @Override
        public double apply(long left, long right)
        {
          return Math.pow(left, right);
        }
      };
    }

    if (processor != null) {
      return (ExprVectorProcessor<T>) processor;
    }
    return power(inspector, left, right);
  }

  public static <T> ExprVectorProcessor<T> max(Expr.VectorInputBindingInspector inspector, Expr left, Expr right)
  {
    return makeMathProcessor(
        inspector,
        left,
        right,
        () -> new LongOutLongsInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return Math.max(left, right);
          }
        },
        () -> new DoubleOutLongDoubleInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return Math.max(left, right);
          }
        },
        () -> new DoubleOutDoubleLongInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return Math.max(left, right);
          }
        },
        () -> new DoubleOutDoublesInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> min(Expr.VectorInputBindingInspector inspector, Expr left, Expr right)
  {
    return makeMathProcessor(
        inspector,
        left,
        right,
        () -> new LongOutLongsInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return Math.min(left, right);
          }
        },
        () -> new DoubleOutLongDoubleInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return Math.min(left, right);
          }
        },
        () -> new DoubleOutDoubleLongInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return Math.min(left, right);
          }
        },
        () -> new DoubleOutDoublesInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> atan2(Expr.VectorInputBindingInspector inspector, Expr left, Expr right)
  {
    return makeDoubleMathProcessor(
        inspector,
        left,
        right,
        () -> new DoubleOutLongsInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, long right)
          {
            return Math.atan2(left, right);
          }
        },
        () -> new DoubleOutLongDoubleInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return Math.atan2(left, right);
          }
        },
        () -> new DoubleOutDoubleLongInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return Math.atan2(left, right);
          }
        },
        () -> new DoubleOutDoublesInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> copySign(Expr.VectorInputBindingInspector inspector, Expr left, Expr right)
  {
    return makeDoubleMathProcessor(
        inspector,
        left,
        right,
        () -> new DoubleOutLongsInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, long right)
          {
            return Math.copySign((double) left, (double) right);
          }
        },
        () -> new DoubleOutLongDoubleInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return Math.copySign((double) left, right);
          }
        },
        () -> new DoubleOutDoubleLongInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return Math.copySign(left, (double) right);
          }
        },
        () -> new DoubleOutDoublesInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> hypot(Expr.VectorInputBindingInspector inspector, Expr left, Expr right)
  {
    return makeDoubleMathProcessor(
        inspector,
        left,
        right,
        () -> new DoubleOutLongsInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, long right)
          {
            return Math.hypot(left, right);
          }
        },
        () -> new DoubleOutLongDoubleInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return Math.hypot(left, right);
          }
        },
        () -> new DoubleOutDoubleLongInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return Math.hypot(left, right);
          }
        },
        () -> new DoubleOutDoublesInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> remainder(Expr.VectorInputBindingInspector inspector, Expr left, Expr right)
  {
    return makeDoubleMathProcessor(
        inspector,
        left,
        right,
        () -> new DoubleOutLongsInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, long right)
          {
            return Math.IEEEremainder(left, right);
          }
        },
        () -> new DoubleOutLongDoubleInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return Math.IEEEremainder(left, right);
          }
        },
        () -> new DoubleOutDoubleLongInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return Math.IEEEremainder(left, right);
          }
        },
        () -> new DoubleOutDoublesInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> nextAfter(Expr.VectorInputBindingInspector inspector, Expr left, Expr right)
  {
    return makeDoubleMathProcessor(
        inspector,
        left,
        right,
        () -> new DoubleOutLongsInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, long right)
          {
            return Math.nextAfter((double) left, (double) right);
          }
        },
        () -> new DoubleOutLongDoubleInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return Math.nextAfter((double) left, right);
          }
        },
        () -> new DoubleOutDoubleLongInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return Math.nextAfter(left, (double) right);
          }
        },
        () -> new DoubleOutDoublesInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> scalb(Expr.VectorInputBindingInspector inspector, Expr left, Expr right)
  {
    return makeDoubleMathProcessor(
        inspector,
        left,
        right,
        () -> new DoubleOutLongsInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, long right)
          {
            return Math.scalb((double) left, (int) right);
          }
        },
        () -> new DoubleOutLongDoubleInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return Math.scalb((double) left, (int) right);
          }
        },
        () -> new DoubleOutDoubleLongInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return Math.scalb(left, (int) right);
          }
        },
        () -> new DoubleOutDoublesInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> acos(Expr.VectorInputBindingInspector inspector, Expr arg)
  {
    return makeDoubleMathProcessor(
        inspector,
        arg,
        () -> new DoubleOutLongInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.acos(input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> asin(Expr.VectorInputBindingInspector inspector, Expr arg)
  {
    return makeDoubleMathProcessor(
        inspector,
        arg,
        () -> new DoubleOutLongInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.asin(input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> atan(Expr.VectorInputBindingInspector inspector, Expr arg)
  {
    return makeDoubleMathProcessor(
        inspector,
        arg,
        () -> new DoubleOutLongInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.atan(input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> cos(Expr.VectorInputBindingInspector inspector, Expr arg)
  {
    return makeDoubleMathProcessor(
        inspector,
        arg,
        () -> new DoubleOutLongInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.cos(input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> cosh(Expr.VectorInputBindingInspector inspector, Expr arg)
  {
    return makeDoubleMathProcessor(
        inspector,
        arg,
        () -> new DoubleOutLongInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.cosh(input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> cot(Expr.VectorInputBindingInspector inspector, Expr arg)
  {
    return makeDoubleMathProcessor(
        inspector,
        arg,
        () -> new DoubleOutLongInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.cos(input) / Math.sin(input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> sin(Expr.VectorInputBindingInspector inspector, Expr arg)
  {
    return makeDoubleMathProcessor(
        inspector,
        arg,
        () -> new DoubleOutLongInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.sin(input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> sinh(Expr.VectorInputBindingInspector inspector, Expr arg)
  {
    return makeDoubleMathProcessor(
        inspector,
        arg,
        () -> new DoubleOutLongInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.sinh(input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> tan(Expr.VectorInputBindingInspector inspector, Expr arg)
  {
    return makeDoubleMathProcessor(
        inspector,
        arg,
        () -> new DoubleOutLongInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.tan(input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> tanh(Expr.VectorInputBindingInspector inspector, Expr arg)
  {
    return makeDoubleMathProcessor(
        inspector,
        arg,
        () -> new DoubleOutLongInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.tanh(input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> abs(Expr.VectorInputBindingInspector inspector, Expr arg)
  {
    return makeMathProcessor(
        inspector,
        arg,
        () -> new LongOutLongInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long input)
          {
            return Math.abs(input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> cbrt(Expr.VectorInputBindingInspector inspector, Expr arg)
  {
    return makeDoubleMathProcessor(
        inspector,
        arg,
        () -> new DoubleOutLongInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.cbrt(input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> ceil(Expr.VectorInputBindingInspector inspector, Expr arg)
  {
    return makeDoubleMathProcessor(
        inspector,
        arg,
        () -> new DoubleOutLongInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.ceil(input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> floor(Expr.VectorInputBindingInspector inspector, Expr arg)
  {
    return makeDoubleMathProcessor(
        inspector,
        arg,
        () -> new DoubleOutLongInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.floor(input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> exp(Expr.VectorInputBindingInspector inspector, Expr arg)
  {
    return makeDoubleMathProcessor(
        inspector,
        arg,
        () -> new DoubleOutLongInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.exp(input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> expm1(Expr.VectorInputBindingInspector inspector, Expr arg)
  {
    return makeDoubleMathProcessor(
        inspector,
        arg,
        () -> new DoubleOutLongInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.expm1(input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> getExponent(Expr.VectorInputBindingInspector inspector, Expr arg)
  {
    return makeLongMathProcessor(
        inspector,
        arg,
        () -> new LongOutLongInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long input)
          {
            return Math.getExponent((double) input);
          }
        },
        () -> new LongOutDoubleInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> log(Expr.VectorInputBindingInspector inspector, Expr arg)
  {
    return makeDoubleMathProcessor(
        inspector,
        arg,
        () -> new DoubleOutLongInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.log(input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> log10(Expr.VectorInputBindingInspector inspector, Expr arg)
  {
    return makeDoubleMathProcessor(
        inspector,
        arg,
        () -> new DoubleOutLongInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.log10(input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> log1p(Expr.VectorInputBindingInspector inspector, Expr arg)
  {
    return makeDoubleMathProcessor(
        inspector,
        arg,
        () -> new DoubleOutLongInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.log1p(input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> nextUp(Expr.VectorInputBindingInspector inspector, Expr arg)
  {
    return makeDoubleMathProcessor(
        inspector,
        arg,
        () -> new DoubleOutLongInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.nextUp((double) input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> rint(Expr.VectorInputBindingInspector inspector, Expr arg)
  {
    return makeDoubleMathProcessor(
        inspector,
        arg,
        () -> new DoubleOutLongInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.rint(input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> signum(Expr.VectorInputBindingInspector inspector, Expr arg)
  {
    return makeDoubleMathProcessor(
        inspector,
        arg,
        () -> new DoubleOutLongInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.signum(input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> sqrt(Expr.VectorInputBindingInspector inspector, Expr arg)
  {
    return makeDoubleMathProcessor(
        inspector,
        arg,
        () -> new DoubleOutLongInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.sqrt(input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> toDegrees(Expr.VectorInputBindingInspector inspector, Expr arg)
  {
    return makeDoubleMathProcessor(
        inspector,
        arg,
        () -> new DoubleOutLongInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.toDegrees(input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> toRadians(Expr.VectorInputBindingInspector inspector, Expr arg)
  {
    return makeDoubleMathProcessor(
        inspector,
        arg,
        () -> new DoubleOutLongInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.toRadians(input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> ulp(Expr.VectorInputBindingInspector inspector, Expr arg)
  {
    return makeDoubleMathProcessor(
        inspector,
        arg,
        () -> new DoubleOutLongInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long input)
          {
            return Math.ulp((double) input);
          }
        },
        () -> new DoubleOutDoubleInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
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

  public static <T> ExprVectorProcessor<T> bitwiseComplement(Expr.VectorInputBindingInspector inspector, Expr arg)
  {
    return makeLongMathProcessor(
        inspector,
        arg,
        () -> new LongOutLongInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long input)
          {
            return ~input;
          }
        },
        () -> new LongOutDoubleInFunctionVectorValueProcessor(
            arg.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(double input)
          {
            return ~((long) input);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> bitwiseConvertDoubleToLongBits(
      Expr.VectorInputBindingInspector inspector,
      Expr arg
  )
  {
    final ExpressionType inputType = arg.getOutputType(inspector);

    ExprVectorProcessor<?> processor = null;
    if (Types.is(inputType, ExprType.LONG)) {
      processor = new LongOutLongInFunctionVectorValueProcessor(
          arg.buildVectorized(inspector),
          inspector.getMaxVectorSize()
      )
      {
        @Override
        public long apply(long input)
        {
          return Double.doubleToLongBits(input);
        }
      };
    } else if (Types.is(inputType, ExprType.DOUBLE)) {
      processor = new LongOutDoubleInFunctionVectorValueProcessor(
          arg.buildVectorized(inspector),
          inspector.getMaxVectorSize()
      )
      {
        @Override
        public long apply(double input)
        {
          return Double.doubleToLongBits(input);
        }
      };
    }
    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (ExprVectorProcessor<T>) processor;
  }

  public static <T> ExprVectorProcessor<T> bitwiseConvertLongBitsToDouble(
      Expr.VectorInputBindingInspector inspector,
      Expr arg
  )
  {
    final ExpressionType inputType = arg.getOutputType(inspector);

    ExprVectorProcessor<?> processor = null;
    if (Types.is(inputType, ExprType.LONG)) {
      processor = new DoubleOutLongInFunctionVectorValueProcessor(
          arg.buildVectorized(inspector),
          inspector.getMaxVectorSize()
      )
      {
        @Override
        public double apply(long input)
        {
          return Double.longBitsToDouble(input);
        }
      };
    } else if (Types.is(inputType, ExprType.DOUBLE)) {
      processor = new DoubleOutDoubleInFunctionVectorValueProcessor(
          arg.buildVectorized(inspector),
          inspector.getMaxVectorSize()
      )
      {
        @Override
        public double apply(double input)
        {
          return Double.longBitsToDouble((long) input);
        }
      };
    }
    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (ExprVectorProcessor<T>) processor;
  }

  public static <T> ExprVectorProcessor<T> bitwiseAnd(Expr.VectorInputBindingInspector inspector, Expr left, Expr right)
  {
    return makeLongMathProcessor(
        inspector,
        left,
        right,
        () -> new LongOutLongsInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return left & right;
          }
        },
        () -> new LongOutLongDoubleInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, double right)
          {
            return left & (long) right;
          }
        },
        () -> new LongOutDoubleLongInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(double left, long right)
          {
            return (long) left & right;
          }
        },
        () -> new LongOutDoublesInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(double left, double right)
          {
            return (long) left & (long) right;
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> bitwiseOr(Expr.VectorInputBindingInspector inspector, Expr left, Expr right)
  {
    return makeLongMathProcessor(
        inspector,
        left,
        right,
        () -> new LongOutLongsInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return left | right;
          }
        },
        () -> new LongOutLongDoubleInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, double right)
          {
            return left | (long) right;
          }
        },
        () -> new LongOutDoubleLongInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(double left, long right)
          {
            return (long) left | right;
          }
        },
        () -> new LongOutDoublesInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(double left, double right)
          {
            return (long) left | (long) right;
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> bitwiseXor(Expr.VectorInputBindingInspector inspector, Expr left, Expr right)
  {
    return makeLongMathProcessor(
        inspector,
        left,
        right,
        () -> new LongOutLongsInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return left ^ right;
          }
        },
        () -> new LongOutLongDoubleInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, double right)
          {
            return left ^ (long) right;
          }
        },
        () -> new LongOutDoubleLongInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(double left, long right)
          {
            return (long) left ^ right;
          }
        },
        () -> new LongOutDoublesInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(double left, double right)
          {
            return (long) left ^ (long) right;
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> bitwiseShiftLeft(
      Expr.VectorInputBindingInspector inspector,
      Expr left,
      Expr right
  )
  {
    return makeLongMathProcessor(
        inspector,
        left,
        right,
        () -> new LongOutLongsInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return left << right;
          }
        },
        () -> new LongOutLongDoubleInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, double right)
          {
            return left << (long) right;
          }
        },
        () -> new LongOutDoubleLongInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(double left, long right)
          {
            return (long) left << right;
          }
        },
        () -> new LongOutDoublesInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(double left, double right)
          {
            return (long) left << (long) right;
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> bitwiseShiftRight(
      Expr.VectorInputBindingInspector inspector,
      Expr left,
      Expr right
  )
  {
    return makeLongMathProcessor(
        inspector,
        left,
        right,
        () -> new LongOutLongsInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return left >> right;
          }
        },
        () -> new LongOutLongDoubleInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, double right)
          {
            return left >> (long) right;
          }
        },
        () -> new LongOutDoubleLongInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(double left, long right)
          {
            return (long) left >> right;
          }
        },
        () -> new LongOutDoublesInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(double left, double right)
          {
            return (long) left >> (long) right;
          }
        }
    );
  }

  private VectorMathProcessors()
  {
    // No instantiation
  }
}
