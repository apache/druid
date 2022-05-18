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
import org.apache.druid.segment.column.Types;

import javax.annotation.Nullable;
import java.util.Objects;

// logical operators live here
@SuppressWarnings("ClassName")
class BinLtExpr extends BinaryBooleanOpExprBase
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
  protected boolean evalString(@Nullable String left, @Nullable String right)
  {
    return Comparators.<String>naturalNullsFirst().compare(left, right) < 0;
  }

  @Override
  protected final boolean evalLong(long left, long right)
  {
    return left < right;
  }

  @Override
  protected final boolean evalDouble(double left, double right)
  {
    // Use Double.compare for more consistent NaN handling.
    return Double.compare(left, right) < 0;
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

@SuppressWarnings("ClassName")
class BinLeqExpr extends BinaryBooleanOpExprBase
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
  protected boolean evalString(@Nullable String left, @Nullable String right)
  {
    return Comparators.<String>naturalNullsFirst().compare(left, right) <= 0;
  }

  @Override
  protected final boolean evalLong(long left, long right)
  {
    return left <= right;
  }

  @Override
  protected final boolean evalDouble(double left, double right)
  {
    // Use Double.compare for more consistent NaN handling.
    return Double.compare(left, right) <= 0;
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

@SuppressWarnings("ClassName")
class BinGtExpr extends BinaryBooleanOpExprBase
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
  protected boolean evalString(@Nullable String left, @Nullable String right)
  {
    return Comparators.<String>naturalNullsFirst().compare(left, right) > 0;
  }

  @Override
  protected final boolean evalLong(long left, long right)
  {
    return left > right;
  }

  @Override
  protected final boolean evalDouble(double left, double right)
  {
    // Use Double.compare for more consistent NaN handling.
    return Double.compare(left, right) > 0;
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

@SuppressWarnings("ClassName")
class BinGeqExpr extends BinaryBooleanOpExprBase
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
  protected boolean evalString(@Nullable String left, @Nullable String right)
  {
    return Comparators.<String>naturalNullsFirst().compare(left, right) >= 0;
  }

  @Override
  protected final boolean evalLong(long left, long right)
  {
    return left >= right;
  }

  @Override
  protected final boolean evalDouble(double left, double right)
  {
    // Use Double.compare for more consistent NaN handling.
    return Double.compare(left, right) >= 0;
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

@SuppressWarnings("ClassName")
class BinEqExpr extends BinaryBooleanOpExprBase
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
  protected boolean evalString(@Nullable String left, @Nullable String right)
  {
    return Objects.equals(left, right);
  }

  @Override
  protected final boolean evalLong(long left, long right)
  {
    return left == right;
  }

  @Override
  protected final boolean evalDouble(double left, double right)
  {
    return left == right;
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

@SuppressWarnings("ClassName")
class BinNeqExpr extends BinaryBooleanOpExprBase
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
  protected boolean evalString(@Nullable String left, @Nullable String right)
  {
    return !Objects.equals(left, right);
  }

  @Override
  protected final boolean evalLong(long left, long right)
  {
    return left != right;
  }

  @Override
  protected final boolean evalDouble(double left, double right)
  {
    return left != right;
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

@SuppressWarnings("ClassName")
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
    if (!ExpressionProcessing.useStrictBooleans()) {
      return leftVal.asBoolean() ? right.eval(bindings) : leftVal;
    }

    // if left is false, always false
    if (leftVal.value() != null && !leftVal.asBoolean()) {
      return ExprEval.ofLongBoolean(false);
    }
    ExprEval rightVal;
    if (NullHandling.sqlCompatible() || Types.is(leftVal.type(), ExprType.STRING)) {
      // true/null, null/true, null/null -> null
      // false/null, null/false -> false
      if (leftVal.value() == null) {
        rightVal = right.eval(bindings);
        if (rightVal.value() == null || rightVal.asBoolean()) {
          return ExprEval.ofLong(null);
        }
        return ExprEval.ofLongBoolean(false);
      } else {
        // left value must be true
        rightVal = right.eval(bindings);
        if (rightVal.value() == null) {
          return ExprEval.ofLong(null);
        }
      }
    } else {
      rightVal = right.eval(bindings);
    }
    return ExprEval.ofLongBoolean(leftVal.asBoolean() && rightVal.asBoolean());
  }

  @Override
  public boolean canVectorize(InputBindingInspector inspector)
  {
    return ExpressionProcessing.useStrictBooleans() &&
           inspector.areSameTypes(left, right) &&
           inspector.canVectorize(left, right);
  }

  @Override
  public <T> ExprVectorProcessor<T> buildVectorized(VectorInputBindingInspector inspector)
  {
    return VectorProcessors.and(inspector, left, right);
  }

  @Nullable
  @Override
  public ExpressionType getOutputType(InputBindingInspector inspector)
  {
    if (!ExpressionProcessing.useStrictBooleans()) {
      return super.getOutputType(inspector);
    }
    return ExpressionType.LONG;
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
    if (!ExpressionProcessing.useStrictBooleans()) {
      return leftVal.asBoolean() ? leftVal : right.eval(bindings);
    }

    // if left is true, always true
    if (leftVal.value() != null && leftVal.asBoolean()) {
      return ExprEval.ofLongBoolean(true);
    }

    final ExprEval rightVal;
    if (NullHandling.sqlCompatible() || Types.is(leftVal.type(), ExprType.STRING)) {
      // true/null, null/true -> true
      // false/null, null/false, null/null -> null
      if (leftVal.value() == null) {
        rightVal = right.eval(bindings);
        if (rightVal.value() == null || !rightVal.asBoolean()) {
          return ExprEval.ofLong(null);
        }
        return ExprEval.ofLongBoolean(true);
      } else {
        // leftval is false
        rightVal = right.eval(bindings);
        if (rightVal.value() == null) {
          return ExprEval.ofLong(null);
        }
      }
    } else {
      rightVal = right.eval(bindings);
    }
    return ExprEval.ofLongBoolean(leftVal.asBoolean() || rightVal.asBoolean());
  }


  @Override
  public boolean canVectorize(InputBindingInspector inspector)
  {

    return ExpressionProcessing.useStrictBooleans() &&
           inspector.areSameTypes(left, right) &&
           inspector.canVectorize(left, right);
  }

  @Override
  public <T> ExprVectorProcessor<T> buildVectorized(VectorInputBindingInspector inspector)
  {
    return VectorProcessors.or(inspector, left, right);
  }

  @Nullable
  @Override
  public ExpressionType getOutputType(InputBindingInspector inspector)
  {
    if (!ExpressionProcessing.useStrictBooleans()) {
      return super.getOutputType(inspector);
    }
    return ExpressionType.LONG;
  }
}
