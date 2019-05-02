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

import com.google.common.base.Preconditions;
import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Comparators;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Base interface of Druid expression language abstract syntax tree
 */
public interface Expr
{
  default boolean isLiteral()
  {
    // Overridden by things that are literals.
    return false;
  }

  default boolean isArray()
  {
    // Overridden by things that are arrays.
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

  /**
   * Evaluate the {@link Expr} with the bindings which supply {@link IdentifierExpr} with their values, producing an
   * {@link ExprEval} with the result.
   */
  @Nonnull
  ExprEval eval(ObjectBinding bindings);

  /**
   * Mechanism to supply values to back {@link IdentifierExpr} during expression evaluation
   */
  interface ObjectBinding
  {
    /**
     * Get value binding for string identifier of {@link IdentifierExpr}
     */
    @Nullable
    Object get(String name);
  }

  /**
   * Programmatically inspect the {@link Expr} tree with a {@link Visitor}. Each {@link Expr} is responsible for
   * ensuring the {@link Visitor} can reach all of it's {@link Expr} children.
   */
  void visit(Visitor visitor);

  /**
   * Programatically rewrite the {@link Expr} tree with a {@link Shuttle}.Each {@link Expr} is responsible for
   * ensuring the {@link Shuttle} can reach all of it's {@link Expr} children, as well as updating it's children
   * {@link Expr} with the results from the {@link Shuttle}.
   */
  Expr visit(Shuttle shuttle);

  /**
   * Mechanism to inspect an {@link Expr}, implementing a {@link Visitor} allows visiting all children of an
   * {@link Expr}
   */
  interface Visitor
  {
    /**
     * Provide the {@link Visitor} with an {@link Expr} to inspect
     */
    void visit(Expr expr);
  }

  /**
   * Mechanism to rewrite an {@link Expr}, implementing a {@link Shuttle} allows visiting all children of an
   * {@link Expr}, and replacing them as desired.
   */
  interface Shuttle
  {
    /**
     * Provide the {@link Shuttle} with an {@link Expr} to inspect and potentially rewrite.
     */
    Expr visit(Expr expr);
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

  @Override
  public Expr visit(Shuttle shuttle)
  {
    return shuttle.visit(this);
  }
}

abstract class ConstantArrayExpr extends ConstantExpr
{
  @Override
  public boolean isArray()
  {
    return true;
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

class LongArrayExpr extends ConstantArrayExpr
{
  private final Long[] value;

  public LongArrayExpr(Long[] value)
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
    return Arrays.stream(value).map(String::valueOf).collect(Collectors.joining(", "));
  }

  @Nonnull
  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return ExprEval.ofLongArray(value);
  }
}

class StringExpr extends ConstantExpr
{
  private final String value;

  public StringExpr(String value)
  {
    this.value = NullHandling.emptyToNullIfNeeded(value);
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

class StringArrayExpr extends ConstantArrayExpr
{
  private final String[] value;

  public StringArrayExpr(String[] value)
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
    return String.join(", ", value);
  }

  @Nonnull
  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return ExprEval.ofStringArray(value);
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

class DoubleArrayExpr extends ConstantArrayExpr
{
  private final Double[] value;

  public DoubleArrayExpr(Double[] value)
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
    return Arrays.stream(value).map(String::valueOf).collect(Collectors.joining(", "));
  }

  @Nonnull
  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return ExprEval.ofDoubleArray(value);
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

  @Override
  public Expr visit(Shuttle shuttle)
  {
    return shuttle.visit(this);
  }
}

class LambdaExpr implements Expr
{
  private final List<IdentifierExpr> args;
  private final Expr expr;

  public LambdaExpr(List<IdentifierExpr> args, Expr expr)
  {
    this.args = args;
    this.expr = expr;
  }

  @Override
  public String toString()
  {
    return "(" + args + " " + expr + ")";
  }

  public String getIdentifier()
  {
    Preconditions.checkState(args.size() == 1, "LambdaExpr has no or multiple arguments");
    return args.get(0).toString();
  }

  public List<String> getIdentifiers()
  {
    return args.stream().map(IdentifierExpr::toString).collect(Collectors.toList());
  }

  public List<IdentifierExpr> getIdentifierExprs()
  {
    return args;
  }

  public Expr getExpr()
  {
    return expr;
  }

  @Nonnull
  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return expr.eval(bindings);
  }

  @Override
  public void visit(Visitor visitor)
  {
    // return free variables only
    expr.visit(
        _expr -> {
          if (_expr instanceof IdentifierExpr) {
            if (args.stream().noneMatch(x -> _expr.toString().equals(x.toString()))) {
              visitor.visit(_expr);
            }
          }
        }
    );
    visitor.visit(this);
  }

  @Override
  public Expr visit(Shuttle shuttle)
  {
    List<IdentifierExpr> newArgs =
        args.stream().map(arg -> (IdentifierExpr) shuttle.visit(arg)).collect(Collectors.toList());
    Expr newBody = expr.visit(shuttle);
    return shuttle.visit(new LambdaExpr(newArgs, newBody));
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

  @Override
  public Expr visit(Shuttle shuttle)
  {
    List<Expr> newArgs = args.stream().map(shuttle::visit).collect(Collectors.toList());
    return shuttle.visit(new FunctionExpr(function, name, newArgs));
  }
}

class ApplyFunctionExpr implements Expr
{
  final ApplyFunction function;
  final String name;
  final LambdaExpr lambdaExpr;
  final List<Expr> argsExpr;

  public ApplyFunctionExpr(ApplyFunction function, String name, LambdaExpr expr, List<Expr> args)
  {
    this.function = function;
    this.name = name;
    this.argsExpr = args;
    this.lambdaExpr = expr;
  }

  @Override
  public String toString()
  {
    return "(" + name + " " + lambdaExpr + " " + argsExpr + ")";
  }

  @Nonnull
  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return function.apply(lambdaExpr, argsExpr, bindings);
  }

  @Override
  public void visit(Visitor visitor)
  {
    lambdaExpr.visit(visitor);
    for (Expr arg : argsExpr) {
      arg.visit(visitor);
    }
    visitor.visit(this);
  }

  @Override
  public Expr visit(Shuttle shuttle)
  {
    LambdaExpr newLambda = (LambdaExpr) lambdaExpr.visit(shuttle);
    List<Expr> newArgs = argsExpr.stream().map(shuttle::visit).collect(Collectors.toList());
    return shuttle.visit(new ApplyFunctionExpr(function, name, newLambda, newArgs));
  }
}

abstract class UnaryExpr implements Expr
{
  final Expr expr;

  UnaryExpr(Expr expr)
  {
    this.expr = expr;
  }

  abstract UnaryExpr copy(Expr expr);

  @Override
  public void visit(Visitor visitor)
  {
    expr.visit(visitor);
    visitor.visit(this);
  }

  @Override
  public Expr visit(Shuttle shuttle)
  {
    Expr newExpr = expr.visit(shuttle);
    if (newExpr != expr) {
      return shuttle.visit(copy(newExpr));
    }
    return shuttle.visit(this);
  }
}

class UnaryMinusExpr extends UnaryExpr
{
  UnaryMinusExpr(Expr expr)
  {
    super(expr);
  }

  @Override
  UnaryExpr copy(Expr expr)
  {
    return new UnaryMinusExpr(expr);
  }

  @Nonnull
  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    ExprEval ret = expr.eval(bindings);
    if (NullHandling.sqlCompatible() && (ret.value() == null)) {
      return ExprEval.of(null);
    }
    if (ret.type() == ExprType.LONG) {
      return ExprEval.of(-ret.asLong());
    }
    if (ret.type() == ExprType.DOUBLE) {
      return ExprEval.of(-ret.asDouble());
    }
    throw new IAE("unsupported type " + ret.type());
  }

  @Override
  public String toString()
  {
    return "-" + expr;
  }
}

class UnaryNotExpr extends UnaryExpr
{
  UnaryNotExpr(Expr expr)
  {
    super(expr);
  }

  @Override
  UnaryExpr copy(Expr expr)
  {
    return new UnaryNotExpr(expr);
  }

  @Nonnull
  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    ExprEval ret = expr.eval(bindings);
    if (NullHandling.sqlCompatible() && (ret.value() == null)) {
      return ExprEval.of(null);
    }
    // conforming to other boolean-returning binary operators
    ExprType retType = ret.type() == ExprType.DOUBLE ? ExprType.DOUBLE : ExprType.LONG;
    return ExprEval.of(!ret.asBoolean(), retType);
  }

  @Override
  public String toString()
  {
    return "!" + expr;
  }
}

// all concrete subclass of this should have constructor with the form of <init>(String, Expr, Expr)
// if it's not possible, just be sure Evals.binaryOp() can handle that
abstract class BinaryOpExprBase implements Expr
{
  protected final String op;
  protected Expr left;
  protected Expr right;

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
  public Expr visit(Shuttle shuttle)
  {
    Expr newLeft = left.visit(shuttle);
    Expr newRight = right.visit(shuttle);
    if (left != newLeft || right != newRight) {
      return shuttle.visit(copy(newLeft, newRight));
    }
    return shuttle.visit(this);
  }

  @Override
  public String toString()
  {
    return "(" + op + " " + left + " " + right + ")";
  }

  protected abstract BinaryOpExprBase copy(Expr left, Expr right);

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

    // Result of any Binary expressions is null if any of the argument is null.
    // e.g "select null * 2 as c;" or "select null + 1 as c;" will return null as per Standard SQL spec.
    if (NullHandling.sqlCompatible() && (leftVal.value() == null || rightVal.value() == null)) {
      return ExprEval.of(null);
    }


    if (leftVal.type() == ExprType.STRING && rightVal.type() == ExprType.STRING) {
      return evalString(leftVal.asString(), rightVal.asString());
    } else if (leftVal.type() == ExprType.LONG && rightVal.type() == ExprType.LONG) {
      if (NullHandling.sqlCompatible() && (leftVal.isNumericNull() || rightVal.isNumericNull())) {
        return ExprEval.of(null);
      }
      return ExprEval.of(evalLong(leftVal.asLong(), rightVal.asLong()));
    } else {
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

class BinMinusExpr extends BinaryEvalOpExprBase
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
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinPowExpr(op, left, right);
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
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinMulExpr(op, left, right);
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
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinDivExpr(op, left, right);
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
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinModuloExpr(op, left, right);
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
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinPlusExpr(op, left, right);
  }

  @Override
  protected ExprEval evalString(@Nullable String left, @Nullable String right)
  {
    return ExprEval.of(NullHandling.nullToEmptyIfNeeded(left)
                       + NullHandling.nullToEmptyIfNeeded(right));
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
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinLtExpr(op, left, right);
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
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinLeqExpr(op, left, right);
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
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinGtExpr(op, left, right);
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
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinGeqExpr(op, left, right);
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
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinEqExpr(op, left, right);
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
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinNeqExpr(op, left, right);
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

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinAndExpr(op, left, right);
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

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinOrExpr(op, left, right);
  }

  @Nonnull
  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    ExprEval leftVal = left.eval(bindings);
    return leftVal.asBoolean() ? leftVal : right.eval(bindings);
  }
}
