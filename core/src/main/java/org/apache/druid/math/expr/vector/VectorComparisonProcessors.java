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

import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.math.expr.Evals;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprType;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.function.Supplier;

public class VectorComparisonProcessors
{
  public static <T> ExprVectorProcessor<T> makeComparisonProcessor(
      Expr.VectorInputBindingInspector inspector,
      Expr left,
      Expr right,
      Supplier<LongOutStringsInFunctionVectorProcessor> longOutStringsInFunctionVectorProcessor,
      Supplier<LongOutLongsInFunctionVectorProcessor> longOutLongsInProcessor,
      Supplier<DoubleOutLongDoubleInFunctionVectorProcessor> doubleOutLongDoubleInProcessor,
      Supplier<DoubleOutDoubleLongInFunctionVectorProcessor> doubleOutDoubleLongInProcessor,
      Supplier<DoubleOutDoublesInFunctionVectorProcessor> doubleOutDoublesInProcessor
  )
  {
    final ExprType leftType = left.getOutputType(inspector);
    final ExprType rightType = right.getOutputType(inspector);
    ExprVectorProcessor<?> processor = null;
    if (leftType == ExprType.STRING) {
      if (rightType == null || rightType == ExprType.STRING) {
        processor = longOutStringsInFunctionVectorProcessor.get();
      } else {
        processor = doubleOutDoublesInProcessor.get();
      }
    } else if (leftType == null) {
      if (rightType == ExprType.STRING || rightType == null) {
        processor = longOutStringsInFunctionVectorProcessor.get();
      }
    } else if (leftType == ExprType.DOUBLE || rightType == ExprType.DOUBLE) {
      processor = doubleOutDoublesInProcessor.get();
    }
    if (processor != null) {
      return (ExprVectorProcessor<T>) processor;
    }
    // fall through to normal math processor logic
    return VectorMathProcessors.makeMathProcessor(
        inspector,
        left,
        right,
        longOutLongsInProcessor,
        doubleOutLongDoubleInProcessor,
        doubleOutDoubleLongInProcessor,
        doubleOutDoublesInProcessor
    );
  }

  public static <T> ExprVectorProcessor<T> equal(
      Expr.VectorInputBindingInspector inspector,
      Expr left,
      Expr right
  )
  {
    return makeComparisonProcessor(
        inspector,
        left,
        right,
        () -> new LongOutStringsInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Nullable
          @Override
          Long processValue(@Nullable String leftVal, @Nullable String rightVal)
          {
            return Evals.asLong(Objects.equals(leftVal, rightVal));
          }
        },
        () -> new LongOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return Evals.asLong(left == right);
          }
        },
        () -> new DoubleOutLongDoubleInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return Evals.asDouble(left == right);
          }
        },
        () -> new DoubleOutDoubleLongInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return Evals.asDouble(left == right);
          }
        },
        () -> new DoubleOutDoublesInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return Evals.asDouble(left == right);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> notEqual(
      Expr.VectorInputBindingInspector inspector,
      Expr left,
      Expr right
  )
  {
    return makeComparisonProcessor(
        inspector,
        left,
        right,
        () -> new LongOutStringsInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Nullable
          @Override
          Long processValue(@Nullable String leftVal, @Nullable String rightVal)
          {
            return Evals.asLong(!Objects.equals(leftVal, rightVal));
          }
        },
        () -> new LongOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return Evals.asLong(left != right);
          }
        },
        () -> new DoubleOutLongDoubleInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return Evals.asDouble(left != right);
          }
        },
        () -> new DoubleOutDoubleLongInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return Evals.asDouble(left != right);
          }
        },
        () -> new DoubleOutDoublesInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return Evals.asDouble(left != right);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> greaterThanOrEqual(
      Expr.VectorInputBindingInspector inspector,
      Expr left,
      Expr right
  )
  {
    return makeComparisonProcessor(
        inspector,
        left,
        right,
        () -> new LongOutStringsInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Nullable
          @Override
          Long processValue(@Nullable String leftVal, @Nullable String rightVal)
          {
            return Evals.asLong(Comparators.<String>naturalNullsFirst().compare(leftVal, rightVal) >= 0);
          }
        },
        () -> new LongOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return Evals.asLong(left >= right);
          }
        },
        () -> new DoubleOutLongDoubleInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return Evals.asDouble(Double.compare(left, right) >= 0);
          }
        },
        () -> new DoubleOutDoubleLongInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return Evals.asDouble(Double.compare(left, right) >= 0);
          }
        },
        () -> new DoubleOutDoublesInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return Evals.asDouble(Double.compare(left, right) >= 0);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> greaterThan(
      Expr.VectorInputBindingInspector inspector,
      Expr left,
      Expr right
  )
  {
    return makeComparisonProcessor(
        inspector,
        left,
        right,
        () -> new LongOutStringsInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Nullable
          @Override
          Long processValue(@Nullable String leftVal, @Nullable String rightVal)
          {
            return Evals.asLong(Comparators.<String>naturalNullsFirst().compare(leftVal, rightVal) > 0);
          }
        },
        () -> new LongOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return Evals.asLong(left > right);
          }
        },
        () -> new DoubleOutLongDoubleInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return Evals.asDouble(Double.compare(left, right) > 0);
          }
        },
        () -> new DoubleOutDoubleLongInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return Evals.asDouble(Double.compare(left, right) > 0);
          }
        },
        () -> new DoubleOutDoublesInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return Evals.asDouble(Double.compare(left, right) > 0);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> lessThanOrEqual(
      Expr.VectorInputBindingInspector inspector,
      Expr left,
      Expr right
  )
  {
    return makeComparisonProcessor(
        inspector,
        left,
        right,
        () -> new LongOutStringsInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Nullable
          @Override
          Long processValue(@Nullable String leftVal, @Nullable String rightVal)
          {
            return Evals.asLong(Comparators.<String>naturalNullsFirst().compare(leftVal, rightVal) <= 0);
          }
        },
        () -> new LongOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return Evals.asLong(left <= right);
          }
        },
        () -> new DoubleOutLongDoubleInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return Evals.asDouble(Double.compare(left, right) <= 0);
          }
        },
        () -> new DoubleOutDoubleLongInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return Evals.asDouble(Double.compare(left, right) <= 0);
          }
        },
        () -> new DoubleOutDoublesInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return Evals.asDouble(Double.compare(left, right) <= 0);
          }
        }
    );
  }

  public static <T> ExprVectorProcessor<T> lessThan(
      Expr.VectorInputBindingInspector inspector,
      Expr left,
      Expr right
  )
  {
    return makeComparisonProcessor(
        inspector,
        left,
        right,
        () -> new LongOutStringsInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Nullable
          @Override
          Long processValue(@Nullable String leftVal, @Nullable String rightVal)
          {
            return Evals.asLong(Comparators.<String>naturalNullsFirst().compare(leftVal, rightVal) < 0);
          }
        },
        () -> new LongOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return Evals.asLong(left < right);
          }
        },
        () -> new DoubleOutLongDoubleInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return Evals.asDouble(Double.compare(left, right) < 0);
          }
        },
        () -> new DoubleOutDoubleLongInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return Evals.asDouble(Double.compare(left, right) < 0);
          }
        },
        () -> new DoubleOutDoublesInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return Evals.asDouble(Double.compare(left, right) < 0);
          }
        }
    );
  }

  private VectorComparisonProcessors()
  {
    // No instantiation
  }
}
