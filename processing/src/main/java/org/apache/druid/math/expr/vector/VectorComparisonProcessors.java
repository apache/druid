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
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.segment.column.Types;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.function.Supplier;

public class VectorComparisonProcessors
{
  @Deprecated
  public static <T> ExprVectorProcessor<T> makeComparisonProcessor(
      Expr.VectorInputBindingInspector inspector,
      Expr left,
      Expr right,
      Supplier<LongOutObjectsInFunctionVectorProcessor> longOutStringsInFunctionVectorProcessor,
      Supplier<LongOutLongsInFunctionVectorValueProcessor> longOutLongsInProcessor,
      Supplier<DoubleOutLongDoubleInFunctionVectorValueProcessor> doubleOutLongDoubleInProcessor,
      Supplier<DoubleOutDoubleLongInFunctionVectorValueProcessor> doubleOutDoubleLongInProcessor,
      Supplier<DoubleOutDoublesInFunctionVectorValueProcessor> doubleOutDoublesInProcessor
  )
  {
    assert !ExpressionProcessing.useStrictBooleans();
    final ExpressionType leftType = left.getOutputType(inspector);
    final ExpressionType rightType = right.getOutputType(inspector);
    ExprVectorProcessor<?> processor = null;
    if (Types.is(leftType, ExprType.STRING)) {
      if (Types.isNullOr(rightType, ExprType.STRING)) {
        processor = longOutStringsInFunctionVectorProcessor.get();
      } else {
        processor = doubleOutDoublesInProcessor.get();
      }
    } else if (leftType == null) {
      if (Types.isNullOr(rightType, ExprType.STRING)) {
        processor = longOutStringsInFunctionVectorProcessor.get();
      }
    } else if (leftType.is(ExprType.DOUBLE) || Types.is(rightType, ExprType.DOUBLE)) {
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

  public static <T> ExprVectorProcessor<T> makeBooleanProcessor(
      Expr.VectorInputBindingInspector inspector,
      Expr left,
      Expr right,
      Supplier<LongOutObjectsInFunctionVectorProcessor> longOutStringsInFunctionVectorProcessor,
      Supplier<LongOutLongsInFunctionVectorValueProcessor> longOutLongsInProcessor,
      Supplier<LongOutLongDoubleInFunctionVectorValueProcessor> longOutLongDoubleInProcessor,
      Supplier<LongOutDoubleLongInFunctionVectorValueProcessor> longOutDoubleLongInProcessor,
      Supplier<LongOutDoublesInFunctionVectorValueProcessor> longOutDoublesInProcessor
  )
  {
    final ExpressionType leftType = left.getOutputType(inspector);
    final ExpressionType rightType = right.getOutputType(inspector);
    ExprVectorProcessor<?> processor = null;
    if (Types.is(leftType, ExprType.STRING)) {
      if (Types.isNullOr(rightType, ExprType.STRING)) {
        processor = longOutStringsInFunctionVectorProcessor.get();
      } else {
        processor = longOutDoublesInProcessor.get();
      }
    } else if (Types.is(rightType, ExprType.STRING)) {
      if (leftType == null) {
        processor = longOutStringsInFunctionVectorProcessor.get();
      } else {
        processor = longOutDoublesInProcessor.get();
      }
    } else if (leftType == null) {
      if (Types.isNullOr(rightType, ExprType.STRING)) {
        processor = longOutStringsInFunctionVectorProcessor.get();
      }
    } else if (leftType.is(ExprType.DOUBLE) || Types.is(rightType, ExprType.DOUBLE)) {
      processor = longOutDoublesInProcessor.get();
    }
    if (processor != null) {
      return (ExprVectorProcessor<T>) processor;
    }
    // fall through to normal math processor logic
    return VectorMathProcessors.makeLongMathProcessor(
        inspector,
        left,
        right,
        longOutLongsInProcessor,
        longOutLongDoubleInProcessor,
        longOutDoubleLongInProcessor,
        longOutDoublesInProcessor
    );
  }

  public static <T> ExprVectorProcessor<T> equal(
      Expr.VectorInputBindingInspector inspector,
      Expr left,
      Expr right
  )
  {
    if (!ExpressionProcessing.useStrictBooleans()) {
      return makeComparisonProcessor(
          inspector,
          left,
          right,
          () -> new LongOutObjectsInFunctionVectorProcessor(
              left.buildVectorized(inspector),
              right.buildVectorized(inspector),
              inspector.getMaxVectorSize(),
              ExpressionType.STRING
          )
          {
            @Nullable
            @Override
            Long processValue(@Nullable Object leftVal, @Nullable Object rightVal)
            {
              return Evals.asLong(Objects.equals(leftVal, rightVal));
            }
          },
          () -> new LongOutLongsInFunctionVectorValueProcessor(
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
          () -> new DoubleOutLongDoubleInFunctionVectorValueProcessor(
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
          () -> new DoubleOutDoubleLongInFunctionVectorValueProcessor(
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
          () -> new DoubleOutDoublesInFunctionVectorValueProcessor(
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
    return makeBooleanProcessor(
        inspector,
        left,
        right,
        () -> new LongOutObjectsInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize(),
            ExpressionType.STRING
        )
        {
          @Nullable
          @Override
          Long processValue(@Nullable Object leftVal, @Nullable Object rightVal)
          {
            return Evals.asLong(Objects.equals(leftVal, rightVal));
          }
        },
        () -> new LongOutLongsInFunctionVectorValueProcessor(
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
        () -> new LongOutLongDoubleInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, double right)
          {
            return Evals.asLong(left == right);
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
            return Evals.asLong(left == right);
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
            return Evals.asLong(left == right);
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
    if (!ExpressionProcessing.useStrictBooleans()) {
      return makeComparisonProcessor(
          inspector,
          left,
          right,
          () -> new LongOutObjectsInFunctionVectorProcessor(
              left.buildVectorized(inspector),
              right.buildVectorized(inspector),
              inspector.getMaxVectorSize(),
              ExpressionType.STRING
          )
          {
            @Nullable
            @Override
            Long processValue(@Nullable Object leftVal, @Nullable Object rightVal)
            {
              return Evals.asLong(!Objects.equals(leftVal, rightVal));
            }
          },
          () -> new LongOutLongsInFunctionVectorValueProcessor(
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
          () -> new DoubleOutLongDoubleInFunctionVectorValueProcessor(
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
          () -> new DoubleOutDoubleLongInFunctionVectorValueProcessor(
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
          () -> new DoubleOutDoublesInFunctionVectorValueProcessor(
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
    return makeBooleanProcessor(
        inspector,
        left,
        right,
        () -> new LongOutObjectsInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize(),
            ExpressionType.STRING
        )
        {
          @Nullable
          @Override
          Long processValue(@Nullable Object leftVal, @Nullable Object rightVal)
          {
            return Evals.asLong(!Objects.equals(leftVal, rightVal));
          }
        },
        () -> new LongOutLongsInFunctionVectorValueProcessor(
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
        () -> new LongOutLongDoubleInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, double right)
          {
            return Evals.asLong(left != right);
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
            return Evals.asLong(left != right);
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
            return Evals.asLong(left != right);
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
    if (!ExpressionProcessing.useStrictBooleans()) {
      return makeComparisonProcessor(
          inspector,
          left,
          right,
          () -> new LongOutObjectsInFunctionVectorProcessor(
              left.buildVectorized(inspector),
              right.buildVectorized(inspector),
              inspector.getMaxVectorSize(),
              ExpressionType.STRING
          )
          {
            @Nullable
            @Override
            Long processValue(@Nullable Object leftVal, @Nullable Object rightVal)
            {
              return Evals.asLong(
                  Comparators.<String>naturalNullsFirst().compare((String) leftVal, (String) rightVal) >= 0
              );
            }
          },
          () -> new LongOutLongsInFunctionVectorValueProcessor(
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
          () -> new DoubleOutLongDoubleInFunctionVectorValueProcessor(
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
          () -> new DoubleOutDoubleLongInFunctionVectorValueProcessor(
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
          () -> new DoubleOutDoublesInFunctionVectorValueProcessor(
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
    return makeBooleanProcessor(
        inspector,
        left,
        right,
        () -> new LongOutObjectsInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize(),
            ExpressionType.STRING
        )
        {
          @Nullable
          @Override
          Long processValue(@Nullable Object leftVal, @Nullable Object rightVal)
          {
            return Evals.asLong(
                Comparators.<String>naturalNullsFirst().compare((String) leftVal, (String) rightVal) >= 0
            );
          }
        },
        () -> new LongOutLongsInFunctionVectorValueProcessor(
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
        () -> new LongOutLongDoubleInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, double right)
          {
            return Evals.asLong(Double.compare(left, right) >= 0);
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
            return Evals.asLong(Double.compare(left, right) >= 0);
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
            return Evals.asLong(Double.compare(left, right) >= 0);
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
    if (!ExpressionProcessing.useStrictBooleans()) {
      return makeComparisonProcessor(
          inspector,
          left,
          right,
          () -> new LongOutObjectsInFunctionVectorProcessor(
              left.buildVectorized(inspector),
              right.buildVectorized(inspector),
              inspector.getMaxVectorSize(),
              ExpressionType.STRING
          )
          {
            @Nullable
            @Override
            Long processValue(@Nullable Object leftVal, @Nullable Object rightVal)
            {
              return Evals.asLong(
                  Comparators.<String>naturalNullsFirst().compare((String) leftVal, (String) rightVal) > 0
              );
            }
          },
          () -> new LongOutLongsInFunctionVectorValueProcessor(
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
          () -> new DoubleOutLongDoubleInFunctionVectorValueProcessor(
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
          () -> new DoubleOutDoubleLongInFunctionVectorValueProcessor(
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
          () -> new DoubleOutDoublesInFunctionVectorValueProcessor(
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
    return makeBooleanProcessor(
        inspector,
        left,
        right,
        () -> new LongOutObjectsInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize(),
            ExpressionType.STRING
        )
        {
          @Nullable
          @Override
          Long processValue(@Nullable Object leftVal, @Nullable Object rightVal)
          {
            return Evals.asLong(
                Comparators.<String>naturalNullsFirst().compare((String) leftVal, (String) rightVal) > 0
            );
          }
        },
        () -> new LongOutLongsInFunctionVectorValueProcessor(
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
        () -> new LongOutLongDoubleInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, double right)
          {
            return Evals.asLong(Double.compare(left, right) > 0);
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
            return Evals.asLong(Double.compare(left, right) > 0);
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
            return Evals.asLong(Double.compare(left, right) > 0);
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
    if (!ExpressionProcessing.useStrictBooleans()) {
      return makeComparisonProcessor(
          inspector,
          left,
          right,
          () -> new LongOutObjectsInFunctionVectorProcessor(
              left.buildVectorized(inspector),
              right.buildVectorized(inspector),
              inspector.getMaxVectorSize(),
              ExpressionType.STRING
          )
          {
            @Nullable
            @Override
            Long processValue(@Nullable Object leftVal, @Nullable Object rightVal)
            {
              return Evals.asLong(
                  Comparators.<String>naturalNullsFirst().compare((String) leftVal, (String) rightVal) <= 0
              );
            }
          },
          () -> new LongOutLongsInFunctionVectorValueProcessor(
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
          () -> new DoubleOutLongDoubleInFunctionVectorValueProcessor(
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
          () -> new DoubleOutDoubleLongInFunctionVectorValueProcessor(
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
          () -> new DoubleOutDoublesInFunctionVectorValueProcessor(
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
    return makeBooleanProcessor(
        inspector,
        left,
        right,
        () -> new LongOutObjectsInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize(),
            ExpressionType.STRING
        )
        {
          @Nullable
          @Override
          Long processValue(@Nullable Object leftVal, @Nullable Object rightVal)
          {
            return Evals.asLong(
                Comparators.<String>naturalNullsFirst().compare((String) leftVal, (String) rightVal) <= 0
            );
          }
        },
        () -> new LongOutLongsInFunctionVectorValueProcessor(
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
        () -> new LongOutLongDoubleInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, double right)
          {
            return Evals.asLong(Double.compare(left, right) <= 0);
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
            return Evals.asLong(Double.compare(left, right) <= 0);
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
            return Evals.asLong(Double.compare(left, right) <= 0);
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
    if (!ExpressionProcessing.useStrictBooleans()) {
      return makeComparisonProcessor(
          inspector,
          left,
          right,
          () -> new LongOutObjectsInFunctionVectorProcessor(
              left.buildVectorized(inspector),
              right.buildVectorized(inspector),
              inspector.getMaxVectorSize(),
              ExpressionType.STRING
          )
          {
            @Nullable
            @Override
            Long processValue(@Nullable Object leftVal, @Nullable Object rightVal)
            {
              return Evals.asLong(
                  Comparators.<String>naturalNullsFirst().compare((String) leftVal, (String) rightVal) < 0
              );
            }
          },
          () -> new LongOutLongsInFunctionVectorValueProcessor(
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
          () -> new DoubleOutLongDoubleInFunctionVectorValueProcessor(
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
          () -> new DoubleOutDoubleLongInFunctionVectorValueProcessor(
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
          () -> new DoubleOutDoublesInFunctionVectorValueProcessor(
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
    return makeBooleanProcessor(
        inspector,
        left,
        right,
        () -> new LongOutObjectsInFunctionVectorProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize(),
            ExpressionType.STRING
        )
        {
          @Nullable
          @Override
          Long processValue(@Nullable Object leftVal, @Nullable Object rightVal)
          {
            return Evals.asLong(
                Comparators.<String>naturalNullsFirst().compare((String) leftVal, (String) rightVal) < 0
            );
          }
        },
        () -> new LongOutLongsInFunctionVectorValueProcessor(
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
        () -> new LongOutLongDoubleInFunctionVectorValueProcessor(
            left.buildVectorized(inspector),
            right.buildVectorized(inspector),
            inspector.getMaxVectorSize()
        )
        {
          @Override
          public long apply(long left, double right)
          {
            return Evals.asLong(Double.compare(left, right) < 0);
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
            return Evals.asLong(Double.compare(left, right) < 0);
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
            return Evals.asLong(Double.compare(left, right) < 0);
          }
        }
    );
  }

  private VectorComparisonProcessors()
  {
    // No instantiation
  }
}
