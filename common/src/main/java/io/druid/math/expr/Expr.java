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

import com.google.common.math.LongMath;

import java.util.List;

/**
 */
public interface Expr
{
  Number eval(ObjectBinding bindings);

  interface ObjectBinding
  {
    Number get(String name);
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
  public void visit(Visitor visitor)
  {
    visitor.visit(this);
  }
}

class LongExpr extends ConstantExpr
{
  private final long value;

  public LongExpr(long value)
  {
    this.value = value;
  }

  @Override
  public String toString()
  {
    return String.valueOf(value);
  }

  @Override
  public Number eval(ObjectBinding bindings)
  {
    return value;
  }
}

class DoubleExpr extends ConstantExpr
{
  private final double value;

  public DoubleExpr(double value)
  {
    this.value = value;
  }

  @Override
  public String toString()
  {
    return String.valueOf(value);
  }

  @Override
  public Number eval(ObjectBinding bindings)
  {
    return value;
  }
}

class IdentifierExpr extends ConstantExpr
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

  @Override
  public Number eval(ObjectBinding bindings)
  {
    Number val = bindings.get(value);
    if (val == null) {
      throw new RuntimeException("No binding found for " + value);
    } else {
      return val instanceof Long ? val : val.doubleValue();
    }
  }
}

class FunctionExpr implements Expr
{
  final String name;
  final List<Expr> args;

  public FunctionExpr(String name, List<Expr> args)
  {
    this.name = name;
    this.args = args;
  }

  @Override
  public String toString()
  {
    return "(" + name + " " + args + ")";
  }

  @Override
  public Number eval(ObjectBinding bindings)
  {
    return Parser.func.get(name.toLowerCase()).apply(args, bindings);
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

  @Override
  public Number eval(ObjectBinding bindings)
  {
    Number valObj = expr.eval(bindings);
    if (valObj instanceof Long) {
      return -1 * valObj.longValue();
    } else {
      return -1 * valObj.doubleValue();
    }
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

  @Override
  public Number eval(ObjectBinding bindings)
  {
    Number valObj = expr.eval(bindings);
    return valObj.doubleValue() > 0 ? 0.0d : 1.0d;
  }

  @Override
  public String toString()
  {
    return "!" + expr.toString();
  }
}

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

  protected boolean isLong(Number left, Number right)
  {
    return left instanceof Long && right instanceof Long;
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

class BinMinusExpr extends BinaryOpExprBase
{

  BinMinusExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public Number eval(ObjectBinding bindings)
  {
    Number leftVal = left.eval(bindings);
    Number rightVal = right.eval(bindings);
    if (isLong(leftVal, rightVal)) {
      return leftVal.longValue() - rightVal.longValue();
    } else {
      return leftVal.doubleValue() - rightVal.doubleValue();
    }
  }
}

class BinPowExpr extends BinaryOpExprBase
{

  BinPowExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public Number eval(ObjectBinding bindings)
  {
    Number leftVal = left.eval(bindings);
    Number rightVal = right.eval(bindings);
    if (isLong(leftVal, rightVal)) {
      return LongMath.pow(leftVal.longValue(), rightVal.intValue());
    } else {
      return Math.pow(leftVal.doubleValue(), rightVal.doubleValue());
    }
  }
}

class BinMulExpr extends BinaryOpExprBase
{

  BinMulExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public Number eval(ObjectBinding bindings)
  {
    Number leftVal = left.eval(bindings);
    Number rightVal = right.eval(bindings);
    if (isLong(leftVal, rightVal)) {
      return leftVal.longValue() * rightVal.longValue();
    } else {
      return leftVal.doubleValue() * rightVal.doubleValue();
    }
  }
}

class BinDivExpr extends BinaryOpExprBase
{

  BinDivExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public Number eval(ObjectBinding bindings)
  {
    Number leftVal = left.eval(bindings);
    Number rightVal = right.eval(bindings);
    if (isLong(leftVal, rightVal)) {
      return leftVal.longValue() / rightVal.longValue();
    } else {
      return leftVal.doubleValue() / rightVal.doubleValue();
    }
  }
}

class BinModuloExpr extends BinaryOpExprBase
{

  BinModuloExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public Number eval(ObjectBinding bindings)
  {
    Number leftVal = left.eval(bindings);
    Number rightVal = right.eval(bindings);
    if (isLong(leftVal, rightVal)) {
      return leftVal.longValue() % rightVal.longValue();
    } else {
      return leftVal.doubleValue() % rightVal.doubleValue();
    }
  }
}

class BinPlusExpr extends BinaryOpExprBase
{

  BinPlusExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public Number eval(ObjectBinding bindings)
  {
    Number leftVal = left.eval(bindings);
    Number rightVal = right.eval(bindings);
    if (isLong(leftVal, rightVal)) {
      return leftVal.longValue() + rightVal.longValue();
    } else {
      return leftVal.doubleValue() + rightVal.doubleValue();
    }
  }
}

class BinLtExpr extends BinaryOpExprBase
{

  BinLtExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public Number eval(ObjectBinding bindings)
  {
    Number leftVal = left.eval(bindings);
    Number rightVal = right.eval(bindings);
    if (isLong(leftVal, rightVal)) {
      return leftVal.longValue() < rightVal.longValue() ? 1 : 0;
    } else {
      return leftVal.doubleValue() < rightVal.doubleValue() ? 1.0d : 0.0d;
    }
  }
}

class BinLeqExpr extends BinaryOpExprBase
{

  BinLeqExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public Number eval(ObjectBinding bindings)
  {
    Number leftVal = left.eval(bindings);
    Number rightVal = right.eval(bindings);
    if (isLong(leftVal, rightVal)) {
      return leftVal.longValue() <= rightVal.longValue() ? 1 : 0;
    } else {
      return leftVal.doubleValue() <= rightVal.doubleValue() ? 1.0d : 0.0d;
    }
  }
}

class BinGtExpr extends BinaryOpExprBase
{

  BinGtExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public Number eval(ObjectBinding bindings)
  {
    Number leftVal = left.eval(bindings);
    Number rightVal = right.eval(bindings);
    if (isLong(leftVal, rightVal)) {
      return leftVal.longValue() > rightVal.longValue() ? 1 : 0;
    } else {
      return leftVal.doubleValue() > rightVal.doubleValue() ? 1.0d : 0.0d;
    }
  }
}

class BinGeqExpr extends BinaryOpExprBase
{

  BinGeqExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public Number eval(ObjectBinding bindings)
  {
    Number leftVal = left.eval(bindings);
    Number rightVal = right.eval(bindings);
    if (isLong(leftVal, rightVal)) {
      return leftVal.longValue() >= rightVal.longValue() ? 1 : 0;
    } else {
      return leftVal.doubleValue() >= rightVal.doubleValue() ? 1.0d : 0.0d;
    }
  }
}

class BinEqExpr extends BinaryOpExprBase
{

  BinEqExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public Number eval(ObjectBinding bindings)
  {
    Number leftVal = left.eval(bindings);
    Number rightVal = right.eval(bindings);
    if (isLong(leftVal, rightVal)) {
      return leftVal.longValue() == rightVal.longValue() ? 1 : 0;
    } else {
      return leftVal.doubleValue() == rightVal.doubleValue() ? 1.0d : 0.0d;
    }
  }
}

class BinNeqExpr extends BinaryOpExprBase
{

  BinNeqExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public Number eval(ObjectBinding bindings)
  {
    Number leftVal = left.eval(bindings);
    Number rightVal = right.eval(bindings);
    if (isLong(leftVal, rightVal)) {
      return leftVal.longValue() != rightVal.longValue() ? 1 : 0;
    } else {
      return leftVal.doubleValue() != rightVal.doubleValue() ? 1.0d : 0.0d;
    }
  }
}

class BinAndExpr extends BinaryOpExprBase
{

  BinAndExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public Number eval(ObjectBinding bindings)
  {
    Number leftVal = left.eval(bindings);
    Number rightVal = right.eval(bindings);
    if (isLong(leftVal, rightVal)) {
      long lval = leftVal.longValue();
      if (lval > 0) {
        long rval = rightVal.longValue();
        return rval > 0 ? 1 : 0;
      } else {
        return 0;
      }
    } else {
      double lval = leftVal.doubleValue();
      if (lval > 0) {
        double rval = rightVal.doubleValue();
        return rval > 0 ? 1.0d : 0.0d;
      } else {
        return 0.0d;
      }
    }
  }
}

class BinOrExpr extends BinaryOpExprBase
{

  BinOrExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public Number eval(ObjectBinding bindings)
  {
    Number leftVal = left.eval(bindings);
    Number rightVal = right.eval(bindings);
    if (isLong(leftVal, rightVal)) {
      long lval = leftVal.longValue();
      if (lval > 0) {
        return 1;
      } else {
        long rval = rightVal.longValue();
        return rval > 0 ? 1 : 0;
      }
    } else {
      double lval = leftVal.doubleValue();
      if (lval > 0) {
        return 1.0d;
      } else {
        double rval = rightVal.doubleValue();
        return rval > 0 ? 1.0d : 0.0d;
      }
    }
  }
}
