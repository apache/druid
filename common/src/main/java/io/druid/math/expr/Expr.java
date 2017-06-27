/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.math.expr;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Comparators;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 */
public interface Expr
{
  default boolean isLiteral()
  {
    // Overridden by things that are literals.
    return false;
  }

  /**
   * Returns the value of expr if expr is a literal, or throws an exception otherwise.
   *
   * @return expr's literal value
   *
   * @throws IllegalStateException if expr is not a literal
   */
  @Nullable
  default Object getLiteralValue()
  {
    // Overridden by things that are literals.
    throw new ISE("Not a literal");
  }

  @Nonnull
  ExprEval eval(ObjectBinding bindings);

  interface ObjectBinding
  {
    @Nullable
    Object get(String name);
  }

  void visit(Visitor visitor);

  interface Visitor
  {
    void visit(Expr expr);
  }
}

abstract class ConstantExpr implements Expr
{
  @Override
  public boolean isLiteral()
  {
    return true;
  }

  @Override
  public void visit(Visitor visitor)
  {
    visitor.visit(this);
  }
}

class LongExpr extends ConstantExpr
{
  private final Long value;

  public LongExpr(Long value)
  {
    this.value = Preconditions.checkNotNull(value, "value");
  }

  @Nonnull
  @Override
  public Object getLiteralValue()
  {
    return value;
  }

  @Override
  public String toString()
  {
    return String.valueOf(value);
  }

  @Nonnull
  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return ExprEval.ofLong(value);
  }
}

class StringExpr extends ConstantExpr
{
  private final String value;

  public StringExpr(String value)
  {
    this.value = Strings.emptyToNull(value);
  }

  @Nullable
  @Override
  public Object getLiteralValue()
  {
    return value;
  }

  @Override
  public String toString()
  {
    return value;
  }

  @Nonnull
  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return ExprEval.of(value);
  }
}

class DoubleExpr extends ConstantExpr
{
  private final Double value;

  public DoubleExpr(Double value)
  {
    this.value = Preconditions.checkNotNull(value, "value");
  }

  @Nonnull
  @Override
  public Object getLiteralValue()
  {
    return value;
  }

  @Override
  public String toString()
  {
    return String.valueOf(value);
  }

  @Nonnull
  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return ExprEval.ofDouble(value);
  }
}

class IdentifierExpr implements Expr
{
  private final String value;

  public IdentifierExpr(String value)
  {
    this.value = value;
  }

  @Override
  public String toString()
  {
    return value;
  }

  @Nonnull
  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return ExprEval.bestEffortOf(bindings.get(value));
  }

  @Override
  public void visit(Visitor visitor)
  {
    visitor.visit(this);
  }
}

class FunctionExpr implements Expr
{
  final Function function;
  final String name;
  final List<Expr> args;

  public FunctionExpr(Function function, String name, List<Expr> args)
  {
    this.function = function;
    this.name = name;
    this.args = args;
  }

  @Override
  public String toString()
  {
    return "(" + name + " " + args + ")";
  }

  @Nonnull
  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return function.apply(args, bindings);
  }

  @Override
  public void visit(Visitor visitor)
  {
    for (Expr child : args) {
      child.visit(visitor);
    }
    visitor.visit(this);
  }
}

abstract class UnaryExpr implements Expr
{
  final Expr expr;

  UnaryExpr(Expr expr)
  {
    this.expr = expr;
  }

  @Override
  public void visit(Visitor visitor)
  {
    expr.visit(visitor);
    visitor.visit(this);
  }
}

class UnaryMinusExpr extends UnaryExpr
{
  UnaryMinusExpr(Expr expr)
  {
    super(expr);
  }

  @Nonnull
  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    ExprEval ret = expr.eval(bindings);
    if (ret.type() == ExprType.LONG) {
      return ExprEval.of(-ret.asLong());
    }
    if (ret.type() == ExprType.DOUBLE) {
      return ExprEval.of(-ret.asDouble());
    }
    throw new IAE("unsupported type " + ret.type());
  }

  @Override
  public void visit(Visitor visitor)
  {
    expr.visit(visitor);
    visitor.visit(this);
  }

  @Override
  public String toString()
  {
    return "-" + expr.toString();
  }
}

class UnaryNotExpr extends UnaryExpr
{
  UnaryNotExpr(Expr expr)
  {
    super(expr);
  }

  @Nonnull
  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    ExprEval ret = expr.eval(bindings);
    // conforming to other boolean-returning binary operators
    ExprType retType = ret.type() == ExprType.DOUBLE ? ExprType.DOUBLE : ExprType.LONG;
    return ExprEval.of(!ret.asBoolean(), retType);
  }

  @Override
  public String toString()
  {
    return "!" + expr.toString();
  }
}

// all concrete subclass of this should have constructor with the form of <init>(String, Expr, Expr)
// if it's not possible, just be sure Evals.binaryOp() can handle that
abstract class BinaryOpExprBase implements Expr
{
  protected final String op;
  protected final Expr left;
  protected final Expr right;

  public BinaryOpExprBase(String op, Expr left, Expr right)
  {
    this.op = op;
    this.left = left;
    this.right = right;
  }

  @Override
  public void visit(Visitor visitor)
  {
    left.visit(visitor);
    right.visit(visitor);
    visitor.visit(this);
  }

  @Override
  public String toString()
  {
    return "(" + op + " " + left + " " + right + ")";
  }
}

abstract class BinaryEvalOpExprBase extends BinaryOpExprBase
{
  public BinaryEvalOpExprBase(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Nonnull
  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    ExprEval leftVal = left.eval(bindings);
    ExprEval rightVal = right.eval(bindings);
    if (leftVal.type() == ExprType.STRING && rightVal.type() == ExprType.STRING) {
      return evalString(leftVal.asString(), rightVal.asString());
    } else if (leftVal.type() == ExprType.LONG && rightVal.type() == ExprType.LONG) {
      return ExprEval.of(evalLong(leftVal.asLong(), rightVal.asLong()));
    } else {
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

class BinMinusExpr extends BinaryEvalOpExprBase
{
  BinMinusExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected final long evalLong(long left, long right)
  {
    return left - right;
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return left - right;
  }
}

class BinPowExpr extends BinaryEvalOpExprBase
{
  BinPowExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected final long evalLong(long left, long right)
  {
    return LongMath.pow(left, Ints.checkedCast(right));
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return Math.pow(left, right);
  }
}

class BinMulExpr extends BinaryEvalOpExprBase
{
  BinMulExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected final long evalLong(long left, long right)
  {
    return left * right;
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return left * right;
  }
}

class BinDivExpr extends BinaryEvalOpExprBase
{
  BinDivExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected final long evalLong(long left, long right)
  {
    return left / right;
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return left / right;
  }
}

class BinModuloExpr extends BinaryEvalOpExprBase
{
  BinModuloExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected final long evalLong(long left, long right)
  {
    return left % right;
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return left % right;
  }
}

class BinPlusExpr extends BinaryEvalOpExprBase
{
  BinPlusExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected ExprEval evalString(@Nullable String left, @Nullable String right)
  {
    return ExprEval.of(Strings.nullToEmpty(left) + Strings.nullToEmpty(right));
  }

  @Override
  protected final long evalLong(long left, long right)
  {
    return left + right;
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return left + right;
  }
}

class BinLtExpr extends BinaryEvalOpExprBase
{
  BinLtExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected ExprEval evalString(@Nullable String left, @Nullable String right)
  {
    return ExprEval.of(Comparators.<String>naturalNullsFirst().compare(left, right) < 0, ExprType.LONG);
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
}

class BinLeqExpr extends BinaryEvalOpExprBase
{
  BinLeqExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected ExprEval evalString(@Nullable String left, @Nullable String right)
  {
    return ExprEval.of(Comparators.<String>naturalNullsFirst().compare(left, right) <= 0, ExprType.LONG);
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
}

class BinGtExpr extends BinaryEvalOpExprBase
{
  BinGtExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected ExprEval evalString(@Nullable String left, @Nullable String right)
  {
    return ExprEval.of(Comparators.<String>naturalNullsFirst().compare(left, right) > 0, ExprType.LONG);
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
}

class BinGeqExpr extends BinaryEvalOpExprBase
{
  BinGeqExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected ExprEval evalString(@Nullable String left, @Nullable String right)
  {
    return ExprEval.of(Comparators.<String>naturalNullsFirst().compare(left, right) >= 0, ExprType.LONG);
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
}

class BinEqExpr extends BinaryEvalOpExprBase
{
  BinEqExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected ExprEval evalString(@Nullable String left, @Nullable String right)
  {
    return ExprEval.of(Objects.equals(left, right), ExprType.LONG);
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
}

class BinNeqExpr extends BinaryEvalOpExprBase
{
  BinNeqExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected ExprEval evalString(@Nullable String left, @Nullable String right)
  {
    return ExprEval.of(!Objects.equals(left, right), ExprType.LONG);
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
}

class BinAndExpr extends BinaryOpExprBase
{
  BinAndExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Nonnull
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

  @Nonnull
  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    ExprEval leftVal = left.eval(bindings);
    return leftVal.asBoolean() ? leftVal : right.eval(bindings);
  }
}
