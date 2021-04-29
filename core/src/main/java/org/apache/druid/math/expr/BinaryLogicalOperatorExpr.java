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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
import org.apache.druid.math.expr.vector.VectorComparisonProcessors;
import org.apache.druid.math.expr.vector.VectorProcessors;

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
  public ExprType getOutputType(InputBindingInspector inspector)
  {
    ExprType implicitCast = super.getOutputType(inspector);
    if (ExprType.STRING.equals(implicitCast)) {
      return ExprType.LONG;
    }
    return implicitCast;
  }

  @Override
  public boolean canVectorize(InputBindingInspector inspector)
  {
    return inspector.canVectorize(left, right);
  }

  @Override
  public <T> ExprVectorProcessor<T> buildVectorized(VectorInputBindingInspector inspector)
  {
    return VectorComparisonProcessors.lessThan(inspector, left, right);
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
  public ExprType getOutputType(InputBindingInspector inspector)
  {
    ExprType implicitCast = super.getOutputType(inspector);
    if (ExprType.STRING.equals(implicitCast)) {
      return ExprType.LONG;
    }
    return implicitCast;
  }

  @Override
  public boolean canVectorize(InputBindingInspector inspector)
  {
    return inspector.canVectorize(left, right);
  }

  @Override
  public <T> ExprVectorProcessor<T> buildVectorized(VectorInputBindingInspector inspector)
  {
    return VectorComparisonProcessors.lessThanOrEqual(inspector, left, right);
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
  public ExprType getOutputType(InputBindingInspector inspector)
  {
    ExprType implicitCast = super.getOutputType(inspector);
    if (ExprType.STRING.equals(implicitCast)) {
      return ExprType.LONG;
    }
    return implicitCast;
  }
  @Override
  public boolean canVectorize(InputBindingInspector inspector)
  {
    return inspector.canVectorize(left, right);
  }

  @Override
  public <T> ExprVectorProcessor<T> buildVectorized(VectorInputBindingInspector inspector)
  {
    return VectorComparisonProcessors.greaterThan(inspector, left, right);
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
  public ExprType getOutputType(InputBindingInspector inspector)
  {
    ExprType implicitCast = super.getOutputType(inspector);
    if (ExprType.STRING.equals(implicitCast)) {
      return ExprType.LONG;
    }
    return implicitCast;
  }

  @Override
  public boolean canVectorize(InputBindingInspector inspector)
  {
    return inspector.canVectorize(left, right);
  }

  @Override
  public <T> ExprVectorProcessor<T> buildVectorized(VectorInputBindingInspector inspector)
  {
    return VectorComparisonProcessors.greaterThanOrEqual(inspector, left, right);
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
  public ExprType getOutputType(InputBindingInspector inspector)
  {
    ExprType implicitCast = super.getOutputType(inspector);
    if (ExprType.STRING.equals(implicitCast)) {
      return ExprType.LONG;
    }
    return implicitCast;
  }

  @Override
  public boolean canVectorize(InputBindingInspector inspector)
  {
    return inspector.canVectorize(left, right);
  }

  @Override
  public <T> ExprVectorProcessor<T> buildVectorized(VectorInputBindingInspector inspector)
  {
    return VectorComparisonProcessors.equal(inspector, left, right);
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
  public ExprType getOutputType(InputBindingInspector inspector)
  {
    ExprType implicitCast = super.getOutputType(inspector);
    if (ExprType.STRING.equals(implicitCast)) {
      return ExprType.LONG;
    }
    return implicitCast;
  }

  @Override
  public boolean canVectorize(InputBindingInspector inspector)
  {
    return inspector.canVectorize(left, right);
  }

  @Override
  public <T> ExprVectorProcessor<T> buildVectorized(VectorInputBindingInspector inspector)
  {
    return VectorComparisonProcessors.notEqual(inspector, left, right);
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
    if (NullHandling.useLegacyLogicalOperators()) {
      return leftVal.asBoolean() ? right.eval(bindings) : leftVal;
    }

    // if left is false, always false
    if (leftVal.value() != null && !leftVal.asBoolean()) {
      return ExprEval.of(false, leftVal.type());
    }
    ExprEval rightVal;
    if (NullHandling.sqlCompatible() || leftVal.type() == ExprType.STRING) {
      // true/null, null/true, null/null -> null
      // false/null, null/false -> false
      if (leftVal.value() == null) {
        rightVal = right.eval(bindings);
        if (rightVal.value() == null || rightVal.asBoolean()) {
          return ExprEval.ofNull(rightVal.type());
        }
        return ExprEval.of(false, rightVal.type());
      } else {
        // left value must be true
        rightVal = right.eval(bindings);
        if (rightVal.value() == null) {
          return ExprEval.ofNull(leftVal.type());
        }
      }
    } else {
      rightVal = right.eval(bindings);
    }
    ExprType type = ExprTypeConversion.autoDetect(leftVal, rightVal);
    return ExprEval.of(leftVal.asBoolean() && rightVal.asBoolean(), type);
  }

  @Override
  public boolean canVectorize(InputBindingInspector inspector)
  {
    return !NullHandling.useLegacyLogicalOperators() &&
           inspector.areSameTypes(left, right) &&
           inspector.canVectorize(left, right);
  }

  @Override
  public <T> ExprVectorProcessor<T> buildVectorized(VectorInputBindingInspector inspector)
  {
    return VectorProcessors.and(inspector, left, right);
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
    if (NullHandling.useLegacyLogicalOperators()) {
      return leftVal.asBoolean() ? leftVal : right.eval(bindings);
    }

    // if left is true, always true
    if (leftVal.value() != null && leftVal.asBoolean()) {
      return ExprEval.of(true, leftVal.type());
    }

    final ExprEval rightVal;
    if (NullHandling.sqlCompatible() || leftVal.type() == ExprType.STRING) {
      // true/null, null/true, null/null -> true
      // false/null, null/false -> null
      if (leftVal.value() == null) {
        rightVal = right.eval(bindings);
        if (rightVal.value() == null || !rightVal.asBoolean()) {
          return ExprEval.ofNull(rightVal.type());
        }
        return ExprEval.of(true, rightVal.type());
      } else {
        // leftval is false
        rightVal = right.eval(bindings);
        if (rightVal.value() == null) {
          return ExprEval.ofNull(leftVal.type());
        }
      }
    } else {
      rightVal = right.eval(bindings);
    }
    ExprType type = ExprTypeConversion.autoDetect(leftVal, rightVal);
    return ExprEval.of(leftVal.asBoolean() || rightVal.asBoolean(), type);
  }


  @Override
  public boolean canVectorize(InputBindingInspector inspector)
  {

    return !NullHandling.useLegacyLogicalOperators() &&
           inspector.areSameTypes(left, right) &&
           inspector.canVectorize(left, right);
  }

  @Override
  public <T> ExprVectorProcessor<T> buildVectorized(VectorInputBindingInspector inspector)
  {
    return VectorProcessors.or(inspector, left, right);
  }
}
