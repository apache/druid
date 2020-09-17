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

package org.apache.druid.math.expr;

import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.math.expr.vector.BivariateFunctionVectorProcessor;
import org.apache.druid.math.expr.vector.DoubleOutDoubleLongInFunctionVectorProcessor;
import org.apache.druid.math.expr.vector.DoubleOutDoublesInFunctionVectorProcessor;
import org.apache.druid.math.expr.vector.DoubleOutLongDoubleInFunctionVectorProcessor;
import org.apache.druid.math.expr.vector.LongOutLongsInFunctionVectorProcessor;
import org.apache.druid.math.expr.vector.VectorExprProcessor;

import javax.annotation.Nullable;
import java.util.Objects;

// logical operators live here

class BinLtExpr extends BinaryEvalOpExprBase
{
  BinLtExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinLtExpr(op, left, right);
  }

  @Override
  protected ExprEval evalString(@Nullable String left, @Nullable String right)
  {
    return ExprEval.ofLongBoolean(Comparators.<String>naturalNullsFirst().compare(left, right) < 0);
  }

  @Override
  protected final long evalLong(long left, long right)
  {
    return Evals.asLong(left < right);
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    // Use Double.compare for more consistent NaN handling.
    return Evals.asDouble(Double.compare(left, right) < 0);
  }

  @Nullable
  @Override
  public ExprType getOutputType(InputBindingTypes inputTypes)
  {
    ExprType implicitCast = super.getOutputType(inputTypes);
    if (ExprType.STRING.equals(implicitCast)) {
      return ExprType.LONG;
    }
    return implicitCast;
  }
  @Override
  public boolean canVectorize(InputBindingTypes inputTypes)
  {
    return inputTypes.areNumeric(left, right) && inputTypes.canVectorize(left, right);
  }

  @Override
  public <T> VectorExprProcessor<T> buildVectorized(VectorInputBindingTypes inputTypes)
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
      throw Exprs.cannotVectorize(this);
    }
    return (VectorExprProcessor<T>) processor;
  }
}

class BinLeqExpr extends BinaryEvalOpExprBase
{
  BinLeqExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinLeqExpr(op, left, right);
  }

  @Override
  protected ExprEval evalString(@Nullable String left, @Nullable String right)
  {
    return ExprEval.ofLongBoolean(Comparators.<String>naturalNullsFirst().compare(left, right) <= 0);
  }

  @Override
  protected final long evalLong(long left, long right)
  {
    return Evals.asLong(left <= right);
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    // Use Double.compare for more consistent NaN handling.
    return Evals.asDouble(Double.compare(left, right) <= 0);
  }

  @Nullable
  @Override
  public ExprType getOutputType(InputBindingTypes inputTypes)
  {
    ExprType implicitCast = super.getOutputType(inputTypes);
    if (ExprType.STRING.equals(implicitCast)) {
      return ExprType.LONG;
    }
    return implicitCast;
  }

  @Override
  public boolean canVectorize(InputBindingTypes inputTypes)
  {
    return inputTypes.areNumeric(left, right) && inputTypes.canVectorize(left, right);
  }

  @Override
  public <T> VectorExprProcessor<T> buildVectorized(VectorInputBindingTypes inputTypes)
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
      throw Exprs.cannotVectorize(this);
    }
    return (VectorExprProcessor<T>) processor;
  }
}

class BinGtExpr extends BinaryEvalOpExprBase
{
  BinGtExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinGtExpr(op, left, right);
  }

  @Override
  protected ExprEval evalString(@Nullable String left, @Nullable String right)
  {
    return ExprEval.ofLongBoolean(Comparators.<String>naturalNullsFirst().compare(left, right) > 0);
  }

  @Override
  protected final long evalLong(long left, long right)
  {
    return Evals.asLong(left > right);
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    // Use Double.compare for more consistent NaN handling.
    return Evals.asDouble(Double.compare(left, right) > 0);
  }

  @Nullable
  @Override
  public ExprType getOutputType(InputBindingTypes inputTypes)
  {
    ExprType implicitCast = super.getOutputType(inputTypes);
    if (ExprType.STRING.equals(implicitCast)) {
      return ExprType.LONG;
    }
    return implicitCast;
  }
  @Override
  public boolean canVectorize(InputBindingTypes inputTypes)
  {
    return inputTypes.areNumeric(left, right) && inputTypes.canVectorize(left, right);
  }

  @Override
  public <T> VectorExprProcessor<T> buildVectorized(VectorInputBindingTypes inputTypes)
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
      throw Exprs.cannotVectorize(this);
    }
    return (VectorExprProcessor<T>) processor;
  }
}

class BinGeqExpr extends BinaryEvalOpExprBase
{
  BinGeqExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinGeqExpr(op, left, right);
  }

  @Override
  protected ExprEval evalString(@Nullable String left, @Nullable String right)
  {
    return ExprEval.ofLongBoolean(Comparators.<String>naturalNullsFirst().compare(left, right) >= 0);
  }

  @Override
  protected final long evalLong(long left, long right)
  {
    return Evals.asLong(left >= right);
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    // Use Double.compare for more consistent NaN handling.
    return Evals.asDouble(Double.compare(left, right) >= 0);
  }

  @Nullable
  @Override
  public ExprType getOutputType(InputBindingTypes inputTypes)
  {
    ExprType implicitCast = super.getOutputType(inputTypes);
    if (ExprType.STRING.equals(implicitCast)) {
      return ExprType.LONG;
    }
    return implicitCast;
  }

  @Override
  public boolean canVectorize(InputBindingTypes inputTypes)
  {
    return inputTypes.areNumeric(left, right) && inputTypes.canVectorize(left, right);
  }

  @Override
  public <T> VectorExprProcessor<T> buildVectorized(VectorInputBindingTypes inputTypes)
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
      throw Exprs.cannotVectorize(this);
    }
    return (VectorExprProcessor<T>) processor;
  }
}

class BinEqExpr extends BinaryEvalOpExprBase
{
  BinEqExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinEqExpr(op, left, right);
  }

  @Override
  protected ExprEval evalString(@Nullable String left, @Nullable String right)
  {
    return ExprEval.ofLongBoolean(Objects.equals(left, right));
  }

  @Override
  protected final long evalLong(long left, long right)
  {
    return Evals.asLong(left == right);
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return Evals.asDouble(left == right);
  }

  @Nullable
  @Override
  public ExprType getOutputType(InputBindingTypes inputTypes)
  {
    ExprType implicitCast = super.getOutputType(inputTypes);
    if (ExprType.STRING.equals(implicitCast)) {
      return ExprType.LONG;
    }
    return implicitCast;
  }

  @Override
  public boolean canVectorize(InputBindingTypes inputTypes)
  {
    return inputTypes.areNumeric(left, right) && inputTypes.canVectorize(left, right);
  }

  @Override
  public <T> VectorExprProcessor<T> buildVectorized(VectorInputBindingTypes inputTypes)
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
      throw Exprs.cannotVectorize(this);
    }
    return (VectorExprProcessor<T>) processor;
  }
}

class BinNeqExpr extends BinaryEvalOpExprBase
{
  BinNeqExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinNeqExpr(op, left, right);
  }

  @Override
  protected ExprEval evalString(@Nullable String left, @Nullable String right)
  {
    return ExprEval.ofLongBoolean(!Objects.equals(left, right));
  }

  @Override
  protected final long evalLong(long left, long right)
  {
    return Evals.asLong(left != right);
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return Evals.asDouble(left != right);
  }

  @Nullable
  @Override
  public ExprType getOutputType(InputBindingTypes inputTypes)
  {
    ExprType implicitCast = super.getOutputType(inputTypes);
    if (ExprType.STRING.equals(implicitCast)) {
      return ExprType.LONG;
    }
    return implicitCast;
  }

  @Override
  public boolean canVectorize(InputBindingTypes inputTypes)
  {
    return inputTypes.areNumeric(left, right) && inputTypes.canVectorize(left, right);
  }

  @Override
  public <T> VectorExprProcessor<T> buildVectorized(VectorInputBindingTypes inputTypes)
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
      throw Exprs.cannotVectorize(this);
    }
    return (VectorExprProcessor<T>) processor;
  }
}

class BinAndExpr extends BinaryOpExprBase
{
  BinAndExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinAndExpr(op, left, right);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    ExprEval leftVal = left.eval(bindings);
    return leftVal.asBoolean() ? right.eval(bindings) : leftVal;
  }
}

class BinOrExpr extends BinaryOpExprBase
{
  BinOrExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinOrExpr(op, left, right);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    ExprEval leftVal = left.eval(bindings);
    return leftVal.asBoolean() ? leftVal : right.eval(bindings);
  }
}
