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

import org.apache.druid.math.expr.Evals;
import org.apache.druid.math.expr.Expr;

public class VectorComparisonProcessors
{
  public static <T> ExprVectorProcessor<T> equal(
      Expr.VectorInputBindingInspector inspector,
      Expr left,
      Expr right
  )
  {
    return VectorMathProcessors.makeMathProcessor(
        inspector,
        left,
        right,
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
    return VectorMathProcessors.makeMathProcessor(
        inspector,
        left,
        right,
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
    return VectorMathProcessors.makeMathProcessor(
        inspector,
        left,
        right,
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
    return VectorMathProcessors.makeMathProcessor(
        inspector,
        left,
        right,
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
    return VectorMathProcessors.makeMathProcessor(
        inspector,
        left,
        right,
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
    return VectorMathProcessors.makeMathProcessor(
        inspector,
        left,
        right,
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
