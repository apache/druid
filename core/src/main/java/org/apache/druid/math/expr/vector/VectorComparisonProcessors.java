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
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.Exprs;

public class VectorComparisonProcessors
{
  private VectorComparisonProcessors()
  {
    // No instantiation
  }

  public static <T> VectorExprProcessor<T> equal(
      Expr.VectorInputBindingTypes inputTypes,
      Expr left,
      Expr right
  )
  {
    final ExprType leftType = left.getOutputType(inputTypes);
    final ExprType rightType = right.getOutputType(inputTypes);

    final int maxVectorSize = inputTypes.getMaxVectorSize();
    BivariateFunctionVectorProcessor<?, ?, ?> processor = null;
    if (ExprType.LONG.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new LongOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return Evals.asLong(left == right);
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleOutLongDoubleInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return Evals.asDouble(left == right);
          }
        };
      }
    } else if (ExprType.DOUBLE.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new DoubleOutDoubleLongInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return Evals.asDouble(left == right);
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleOutDoublesInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return Evals.asDouble(left == right);
          }
        };
      }
    }
    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (VectorExprProcessor<T>) processor;
  }

  public static <T> VectorExprProcessor<T> notEqual(
      Expr.VectorInputBindingTypes inputTypes,
      Expr left,
      Expr right
  )
  {
    final ExprType leftType = left.getOutputType(inputTypes);
    final ExprType rightType = right.getOutputType(inputTypes);

    final int maxVectorSize = inputTypes.getMaxVectorSize();
    BivariateFunctionVectorProcessor<?, ?, ?> processor = null;
    if (ExprType.LONG.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new LongOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return Evals.asLong(left != right);
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleOutLongDoubleInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return Evals.asDouble(left != right);
          }
        };
      }
    } else if (ExprType.DOUBLE.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new DoubleOutDoubleLongInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return Evals.asDouble(left != right);
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleOutDoublesInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return Evals.asDouble(left != right);
          }
        };
      }
    }
    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (VectorExprProcessor<T>) processor;
  }

  public static <T> VectorExprProcessor<T> greaterThanOrEqual(
      Expr.VectorInputBindingTypes inputTypes,
      Expr left,
      Expr right
  )
  {
    final ExprType leftType = left.getOutputType(inputTypes);
    final ExprType rightType = right.getOutputType(inputTypes);

    final int maxVectorSize = inputTypes.getMaxVectorSize();
    BivariateFunctionVectorProcessor<?, ?, ?> processor = null;
    if (ExprType.LONG.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new LongOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return Evals.asLong(left >= right);
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleOutLongDoubleInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return Evals.asDouble(Double.compare(left, right) >= 0);
          }
        };
      }
    } else if (ExprType.DOUBLE.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new DoubleOutDoubleLongInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return Evals.asDouble(Double.compare(left, right) >= 0);
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleOutDoublesInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return Evals.asDouble(Double.compare(left, right) >= 0);
          }
        };
      }
    }
    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (VectorExprProcessor<T>) processor;
  }

  public static <T> VectorExprProcessor<T> greaterThan(
      Expr.VectorInputBindingTypes inputTypes,
      Expr left,
      Expr right
  )
  {
    final ExprType leftType = left.getOutputType(inputTypes);
    final ExprType rightType = right.getOutputType(inputTypes);

    final int maxVectorSize = inputTypes.getMaxVectorSize();
    BivariateFunctionVectorProcessor<?, ?, ?> processor = null;
    if (ExprType.LONG.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new LongOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return Evals.asLong(left > right);
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleOutLongDoubleInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return Evals.asDouble(Double.compare(left, right) > 0);
          }
        };
      }
    } else if (ExprType.DOUBLE.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new DoubleOutDoubleLongInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return Evals.asDouble(Double.compare(left, right) > 0);
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleOutDoublesInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return Evals.asDouble(Double.compare(left, right) > 0);
          }
        };
      }
    }
    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (VectorExprProcessor<T>) processor;
  }

  public static <T> VectorExprProcessor<T> lessThanOrEqual(
      Expr.VectorInputBindingTypes inputTypes,
      Expr left,
      Expr right
  )
  {
    final ExprType leftType = left.getOutputType(inputTypes);
    final ExprType rightType = right.getOutputType(inputTypes);

    final int maxVectorSize = inputTypes.getMaxVectorSize();
    BivariateFunctionVectorProcessor<?, ?, ?> processor = null;
    if (ExprType.LONG.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new LongOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return Evals.asLong(left <= right);
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleOutLongDoubleInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return Evals.asDouble(Double.compare(left, right) <= 0);
          }
        };
      }
    } else if (ExprType.DOUBLE.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new DoubleOutDoubleLongInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return Evals.asDouble(Double.compare(left, right) <= 0);
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleOutDoublesInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return Evals.asDouble(Double.compare(left, right) <= 0);
          }
        };
      }
    }
    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (VectorExprProcessor<T>) processor;
  }

  public static <T> VectorExprProcessor<T> lessThan(
      Expr.VectorInputBindingTypes inputTypes,
      Expr left,
      Expr right
  )
  {
    final ExprType leftType = left.getOutputType(inputTypes);
    final ExprType rightType = right.getOutputType(inputTypes);

    final int maxVectorSize = inputTypes.getMaxVectorSize();
    BivariateFunctionVectorProcessor<?, ?, ?> processor = null;
    if (ExprType.LONG.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new LongOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return Evals.asLong(left < right);
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleOutLongDoubleInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return Evals.asDouble(Double.compare(left, right) < 0);
          }
        };
      }
    } else if (ExprType.DOUBLE.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new DoubleOutDoubleLongInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return Evals.asDouble(Double.compare(left, right) < 0);
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleOutDoublesInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return Evals.asDouble(Double.compare(left, right) < 0);
          }
        };
      }
    }
    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (VectorExprProcessor<T>) processor;
  }
}
