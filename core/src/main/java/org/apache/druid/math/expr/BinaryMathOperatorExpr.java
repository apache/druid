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

import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.math.expr.vector.BivariateFunctionVectorProcessor;
import org.apache.druid.math.expr.vector.DoubleDoubleLongBivariateFunctionVectorProcessor;
import org.apache.druid.math.expr.vector.DoubleLongDoubleBivariateFunctionVectorProcessor;
import org.apache.druid.math.expr.vector.DoublesBivariateFunctionVectorProcessor;
import org.apache.druid.math.expr.vector.LongsBivariateFunctionVectorProcessor;
import org.apache.druid.math.expr.vector.VectorExprProcessor;

import javax.annotation.Nullable;

// math operators live here

final class BinPlusExpr extends BinaryEvalOpExprBase
{
  BinPlusExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinPlusExpr(op, left, right);
  }

  @Override
  protected ExprEval evalString(@Nullable String left, @Nullable String right)
  {
    return ExprEval.of(NullHandling.nullToEmptyIfNeeded(left) + NullHandling.nullToEmptyIfNeeded(right));
  }

  @Override
  protected long evalLong(long left, long right)
  {
    return left + right;
  }

  @Override
  protected double evalDouble(double left, double right)
  {
    return left + right;
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
        processor = new LongsBivariateFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return left + right;
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleLongDoubleBivariateFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return (double) left + right;
          }
        };
      }
    } else if (ExprType.DOUBLE.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new DoubleDoubleLongBivariateFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return left + (double) right;
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoublesBivariateFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return left + right;
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

final class BinMinusExpr extends BinaryEvalOpExprBase
{
  BinMinusExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinMinusExpr(op, left, right);
  }

  @Override
  protected long evalLong(long left, long right)
  {
    return left - right;
  }

  @Override
  protected double evalDouble(double left, double right)
  {
    return left - right;
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
        processor = new LongsBivariateFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return left - right;
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleLongDoubleBivariateFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return (double) left - right;
          }
        };
      }
    } else if (ExprType.DOUBLE.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new DoubleDoubleLongBivariateFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return left - (double) right;
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoublesBivariateFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return left - right;
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

final class BinMulExpr extends BinaryEvalOpExprBase
{
  BinMulExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinMulExpr(op, left, right);
  }

  @Override
  protected long evalLong(long left, long right)
  {
    return left * right;
  }

  @Override
  protected double evalDouble(double left, double right)
  {
    return left * right;
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
        processor = new LongsBivariateFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return left * right;
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleLongDoubleBivariateFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return (double) left * right;
          }
        };
      }
    } else if (ExprType.DOUBLE.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new DoubleDoubleLongBivariateFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return left * (double) right;
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoublesBivariateFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return left * right;
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

final class BinDivExpr extends BinaryEvalOpExprBase
{
  BinDivExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinDivExpr(op, left, right);
  }

  @Override
  protected long evalLong(long left, long right)
  {
    return left / right;
  }

  @Override
  protected double evalDouble(double left, double right)
  {
    return left / right;
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
        processor = new LongsBivariateFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return left / right;
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleLongDoubleBivariateFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return (double) left / right;
          }
        };
      }
    } else if (ExprType.DOUBLE.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new DoubleDoubleLongBivariateFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return left / (double) right;
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoublesBivariateFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return left / right;
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

class BinPowExpr extends BinaryEvalOpExprBase
{
  BinPowExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinPowExpr(op, left, right);
  }

  @Override
  protected long evalLong(long left, long right)
  {
    return LongMath.pow(left, Ints.checkedCast(right));
  }

  @Override
  protected double evalDouble(double left, double right)
  {
    return Math.pow(left, right);
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
        processor = new LongsBivariateFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return LongMath.pow(left, Ints.checkedCast(right));
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleLongDoubleBivariateFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return Math.pow(left, right);
          }
        };
      }
    } else if (ExprType.DOUBLE.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new DoubleDoubleLongBivariateFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return Math.pow(left, right);
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoublesBivariateFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return Math.pow(left, right);
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

class BinModuloExpr extends BinaryEvalOpExprBase
{
  BinModuloExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinModuloExpr(op, left, right);
  }

  @Override
  protected long evalLong(long left, long right)
  {
    return left % right;
  }

  @Override
  protected double evalDouble(double left, double right)
  {
    return left % right;
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
        processor = new LongsBivariateFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return left % right;
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleLongDoubleBivariateFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return (double) left % right;
          }
        };
      }
    } else if (ExprType.DOUBLE.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new DoubleDoubleLongBivariateFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return left % (double) right;
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoublesBivariateFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return left % right;
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
