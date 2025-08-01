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
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
import org.apache.druid.math.expr.vector.VectorComparisonProcessors;
import org.apache.druid.math.expr.vector.VectorProcessors;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.index.AllUnknownBitmapColumnIndex;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.index.semantic.ValueIndexes;

import javax.annotation.Nullable;
import java.util.Objects;

@SuppressWarnings("unused")
final class BinaryLogicalOperatorExpr
{
  // phony class to enable maven to track the compilation of this class
}

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
  protected boolean evalArray(ExprEval left, ExprEval right)
  {
    ExpressionType type = ExpressionTypeConversion.leastRestrictiveType(left.type(), right.type());
    // type cannot be null here because ExprEval type is not nullable
    return type.getNullableStrategy().compare(left.castTo(type).asArray(), right.castTo(type).asArray()) < 0;
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
  public <T> ExprVectorProcessor<T> asVectorProcessor(VectorInputBindingInspector inspector)
  {
    return VectorComparisonProcessors.lessThan().asProcessor(inspector, left, right);
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
  protected boolean evalArray(ExprEval left, ExprEval right)
  {
    ExpressionType type = ExpressionTypeConversion.leastRestrictiveType(left.type(), right.type());
    // type cannot be null here because ExprEval type is not nullable
    return type.getNullableStrategy().compare(left.castTo(type).asArray(), right.castTo(type).asArray()) <= 0;
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
  public <T> ExprVectorProcessor<T> asVectorProcessor(VectorInputBindingInspector inspector)
  {
    return VectorComparisonProcessors.lessThanOrEquals().asProcessor(inspector, left, right);
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
  protected boolean evalArray(ExprEval left, ExprEval right)
  {
    ExpressionType type = ExpressionTypeConversion.leastRestrictiveType(left.type(), right.type());
    // type cannot be null here because ExprEval type is not nullable
    return type.getNullableStrategy().compare(left.castTo(type).asArray(), right.castTo(type).asArray()) > 0;
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
  public <T> ExprVectorProcessor<T> asVectorProcessor(VectorInputBindingInspector inspector)
  {
    return VectorComparisonProcessors.greaterThan().asProcessor(inspector, left, right);
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
  protected boolean evalArray(ExprEval left, ExprEval right)
  {
    ExpressionType type = ExpressionTypeConversion.leastRestrictiveType(left.type(), right.type());
    // type cannot be null here because ExprEval type is not nullable
    return type.getNullableStrategy().compare(left.castTo(type).asArray(), right.castTo(type).asArray()) >= 0;
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
  public <T> ExprVectorProcessor<T> asVectorProcessor(VectorInputBindingInspector inspector)
  {
    return VectorComparisonProcessors.greaterThanOrEquals().asProcessor(inspector, left, right);
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
  protected boolean evalArray(ExprEval left, ExprEval right)
  {
    ExpressionType type = ExpressionTypeConversion.leastRestrictiveType(left.type(), right.type());
    // type cannot be null here because ExprEval type is not nullable
    return type.getNullableStrategy().compare(left.castTo(type).asArray(), right.castTo(type).asArray()) == 0;
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
  public <T> ExprVectorProcessor<T> asVectorProcessor(VectorInputBindingInspector inspector)
  {
    return VectorComparisonProcessors.equals().asProcessor(inspector, left, right);
  }

  @Nullable
  @Override
  public BitmapColumnIndex asBitmapColumnIndex(ColumnIndexSelector selector)
  {
    final ColumnIndexSupplier indexSupplier;
    final ColumnType matchType;
    final Object matchValue;
    final ColumnType sourceType;
    if (right.isLiteral()) {
      final ExpressionType matchExprType = right.getOutputType(InputBindings.nilBindings());
      matchType = matchExprType != null ? ExpressionType.toColumnType(matchExprType) : ColumnType.STRING;
      matchValue = right.getLiteralValue();
      indexSupplier = left.asColumnIndexSupplier(selector, matchType);
      final ExpressionType sourceExprType = left.getOutputType(selector);
      sourceType = sourceExprType != null ? ExpressionType.toColumnType(sourceExprType) : null;
    } else if (left.isLiteral()) {
      final ExpressionType matchExprType = left.getOutputType(InputBindings.nilBindings());
      matchType = matchExprType != null ? ExpressionType.toColumnType(matchExprType) : ColumnType.STRING;
      matchValue = left.getLiteralValue();
      indexSupplier = right.asColumnIndexSupplier(selector, matchType);
      final ExpressionType sourceExprType = right.getOutputType(selector);
      sourceType = sourceExprType != null ? ExpressionType.toColumnType(sourceExprType) : null;
    } else {
      indexSupplier = null;
      matchValue = null;
      matchType = null;
      sourceType = null;
    }
    if (indexSupplier == null) {
      return null;
    }
    // if the source type is string, we have to use a predicate index instead of value index so just fall through to
    // default implementation
    if (matchType.isNumeric() && (sourceType == null || sourceType.is(ValueType.STRING))) {
      return null;
    }

    final ValueIndexes valueIndexes = indexSupplier.as(ValueIndexes.class);
    if (valueIndexes == null) {
      return null;
    }
    if (matchValue == null) {
      return new AllUnknownBitmapColumnIndex(selector);
    }
    return valueIndexes.forValue(matchValue, matchType);
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
  protected boolean evalArray(ExprEval left, ExprEval right)
  {
    ExpressionType type = ExpressionTypeConversion.leastRestrictiveType(left.type(), right.type());
    // type cannot be null here because ExprEval type is not nullable
    return type.getNullableStrategy().compare(left.castTo(type).asArray(), right.castTo(type).asArray()) != 0;
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
  public <T> ExprVectorProcessor<T> asVectorProcessor(VectorInputBindingInspector inspector)
  {
    return VectorComparisonProcessors.notEquals().asProcessor(inspector, left, right);
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

    // if left is false, always false
    if (leftVal.value() != null && !leftVal.asBoolean()) {
      return ExprEval.ofLongBoolean(false);
    }
    ExprEval rightVal;
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
    return ExprEval.ofLongBoolean(leftVal.asBoolean() && rightVal.asBoolean());
  }

  @Override
  public boolean canVectorize(InputBindingInspector inspector)
  {
    return inspector.areSameTypes(left, right) && inspector.canVectorize(left, right);
  }

  @Override
  public <T> ExprVectorProcessor<T> asVectorProcessor(VectorInputBindingInspector inspector)
  {
    return VectorProcessors.and(inspector, left, right);
  }

  @Nullable
  @Override
  public ExpressionType getOutputType(InputBindingInspector inspector)
  {
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

    // if left is true, always true
    if (leftVal.value() != null && leftVal.asBoolean()) {
      return ExprEval.ofLongBoolean(true);
    }

    final ExprEval rightVal;
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
    return ExprEval.ofLongBoolean(leftVal.asBoolean() || rightVal.asBoolean());
  }


  @Override
  public boolean canVectorize(InputBindingInspector inspector)
  {

    return inspector.areSameTypes(left, right) && inspector.canVectorize(left, right);
  }

  @Override
  public <T> ExprVectorProcessor<T> asVectorProcessor(VectorInputBindingInspector inspector)
  {
    return VectorProcessors.or(inspector, left, right);
  }

  @Nullable
  @Override
  public ExpressionType getOutputType(InputBindingInspector inspector)
  {
    return ExpressionType.LONG;
  }
}
