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

import org.apache.druid.com.google.common.collect.ImmutableSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Base type for all binary operators, this {@link Expr} has two children {@link Expr} for the left and right side
 * operands.
 *
 * Note: all concrete subclass of this should have constructor with the form of <init>(String, Expr, Expr)
 * if it's not possible, just be sure Evals.binaryOp() can handle that
 */
abstract class BinaryOpExprBase implements Expr
{
  protected final String op;
  protected final Expr left;
  protected final Expr right;

  BinaryOpExprBase(String op, Expr left, Expr right)
  {
    this.op = op;
    this.left = left;
    this.right = right;
  }

  @Override
  public Expr visit(Shuttle shuttle)
  {
    Expr newLeft = left.visit(shuttle);
    Expr newRight = right.visit(shuttle);
    //noinspection ObjectEquality (checking for object equality here is intentional)
    if (left != newLeft || right != newRight) {
      return shuttle.visit(copy(newLeft, newRight));
    }
    return shuttle.visit(this);
  }

  @Override
  public String toString()
  {
    return StringUtils.format("(%s %s %s)", op, left, right);
  }

  @Override
  public String stringify()
  {
    return StringUtils.format("(%s %s %s)", left.stringify(), op, right.stringify());
  }

  protected abstract BinaryOpExprBase copy(Expr left, Expr right);

  @Override
  public BindingAnalysis analyzeInputs()
  {
    // currently all binary operators operate on scalar inputs
    return left.analyzeInputs().with(right).withScalarArguments(ImmutableSet.of(left, right));
  }

  @Nullable
  @Override
  public ExprType getOutputType(InputBindingInspector inspector)
  {
    if (left.isNullLiteral()) {
      return right.getOutputType(inspector);
    }
    if (right.isNullLiteral()) {
      return left.getOutputType(inspector);
    }
    return ExprTypeConversion.operator(left.getOutputType(inspector), right.getOutputType(inspector));
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BinaryOpExprBase that = (BinaryOpExprBase) o;
    return Objects.equals(op, that.op) &&
           Objects.equals(left, that.left) &&
           Objects.equals(right, that.right);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(op, left, right);
  }
}

/**
 * Base class for numerical binary operators, with additional methods defined to evaluate primitive values directly
 * instead of wrapped with {@link ExprEval}
 */
abstract class BinaryEvalOpExprBase extends BinaryOpExprBase
{
  BinaryEvalOpExprBase(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    ExprEval leftVal = left.eval(bindings);
    ExprEval rightVal = right.eval(bindings);

    // Result of any Binary expressions is null if any of the argument is null.
    // e.g "select null * 2 as c;" or "select null + 1 as c;" will return null as per Standard SQL spec.
    if (NullHandling.sqlCompatible() && (leftVal.value() == null || rightVal.value() == null)) {
      return ExprEval.of(null);
    }

    ExprType type = ExprTypeConversion.autoDetect(leftVal, rightVal);
    switch (type) {
      case STRING:
        return evalString(leftVal.asString(), rightVal.asString());
      case LONG:
        return ExprEval.of(evalLong(leftVal.asLong(), rightVal.asLong()));
      case DOUBLE:
      default:
        if (NullHandling.sqlCompatible() && (leftVal.isNumericNull() || rightVal.isNumericNull())) {
          return ExprEval.of(null);
        }
        return ExprEval.of(evalDouble(leftVal.asDouble(), rightVal.asDouble()));
    }
  }

  protected ExprEval evalString(@Nullable String left, @Nullable String right)
  {
    throw new IllegalArgumentException("unsupported type " + ExprType.STRING);
  }

  protected abstract long evalLong(long left, long right);

  protected abstract double evalDouble(double left, double right);
}
