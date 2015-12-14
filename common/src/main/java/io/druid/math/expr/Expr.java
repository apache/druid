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
import java.util.Map;

/**
 */
public interface Expr
{
  Number eval(Map<String, Number> bindings);
}

class SimpleExpr implements Expr
{
  private final Atom atom;

  public SimpleExpr(Atom atom)
  {
    this.atom = atom;
  }

  @Override
  public String toString()
  {
    return atom.toString();
  }

  @Override
  public Number eval(Map<String, Number> bindings)
  {
    return atom.eval(bindings);
  }
}

class BinExpr implements Expr
{
  private final Token opToken;
  private final Expr left;
  private final Expr right;

  public BinExpr(Token opToken, Expr left, Expr right)
  {
    this.opToken = opToken;
    this.left = left;
    this.right = right;
  }

  public Number eval(Map<String, Number> bindings)
  {
    switch(opToken.getType()) {
      case Token.AND:
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
      case Token.OR:
        leftVal = left.eval(bindings);
        rightVal = right.eval(bindings);
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
      case Token.LT:
        leftVal = left.eval(bindings);
        rightVal = right.eval(bindings);
        if (isLong(leftVal, rightVal)) {
          return leftVal.longValue() < rightVal.longValue() ? 1 : 0;
        } else {
          return leftVal.doubleValue() < rightVal.doubleValue() ? 1.0d : 0.0d;
        }
      case Token.LEQ:
        leftVal = left.eval(bindings);
        rightVal = right.eval(bindings);
        if (isLong(leftVal, rightVal)) {
          return leftVal.longValue() <= rightVal.longValue() ? 1 : 0;
        } else {
          return leftVal.doubleValue() <= rightVal.doubleValue() ? 1.0d : 0.0d;
        }
      case Token.GT:
        leftVal = left.eval(bindings);
        rightVal = right.eval(bindings);
        if (isLong(leftVal, rightVal)) {
          return leftVal.longValue() > rightVal.longValue() ? 1 : 0;
        } else {
          return leftVal.doubleValue() > rightVal.doubleValue() ? 1.0d : 0.0d;
        }
      case Token.GEQ:
        leftVal = left.eval(bindings);
        rightVal = right.eval(bindings);
        if (isLong(leftVal, rightVal)) {
          return leftVal.longValue() >= rightVal.longValue() ? 1 : 0;
        } else {
          return leftVal.doubleValue() >= rightVal.doubleValue() ? 1.0d : 0.0d;
        }
      case Token.EQ:
        leftVal = left.eval(bindings);
        rightVal = right.eval(bindings);
        if (isLong(leftVal, rightVal)) {
          return leftVal.longValue() == rightVal.longValue() ? 1 : 0;
        } else {
          return leftVal.doubleValue() == rightVal.doubleValue() ? 1.0d : 0.0d;
        }
      case Token.NEQ:
        leftVal = left.eval(bindings);
        rightVal = right.eval(bindings);
        if (isLong(leftVal, rightVal)) {
          return leftVal.longValue() != rightVal.longValue() ? 1 : 0;
        } else {
          return leftVal.doubleValue() != rightVal.doubleValue() ? 1.0d : 0.0d;
        }
      case Token.PLUS:
        leftVal = left.eval(bindings);
        rightVal = right.eval(bindings);
        if (isLong(leftVal, rightVal)) {
          return leftVal.longValue() + rightVal.longValue();
        } else {
          return leftVal.doubleValue() + rightVal.doubleValue();
        }
      case Token.MINUS:
        leftVal = left.eval(bindings);
        rightVal = right.eval(bindings);
        if (isLong(leftVal, rightVal)) {
          return leftVal.longValue() - rightVal.longValue();
        } else {
          return leftVal.doubleValue() - rightVal.doubleValue();
        }
      case Token.MUL:
        leftVal = left.eval(bindings);
        rightVal = right.eval(bindings);
        if (isLong(leftVal, rightVal)) {
          return leftVal.longValue() * rightVal.longValue();
        } else {
          return leftVal.doubleValue() * rightVal.doubleValue();
        }
      case Token.DIV:
        leftVal = left.eval(bindings);
        rightVal = right.eval(bindings);
        if (isLong(leftVal, rightVal)) {
          return leftVal.longValue() / rightVal.longValue();
        } else {
          return leftVal.doubleValue() / rightVal.doubleValue();
        }
      case Token.MODULO:
        leftVal = left.eval(bindings);
        rightVal = right.eval(bindings);
        if (isLong(leftVal, rightVal)) {
          return leftVal.longValue() % rightVal.longValue();
        } else {
          return leftVal.doubleValue() % rightVal.doubleValue();
        }
      case Token.CARROT:
        leftVal = left.eval(bindings);
        rightVal = right.eval(bindings);
        if (isLong(leftVal, rightVal)) {
          return LongMath.pow(leftVal.longValue(), rightVal.intValue());
        } else {
          return Math.pow(leftVal.doubleValue(), rightVal.doubleValue());
        }
      default:
        throw new RuntimeException("Unknown operator " + opToken.getMatch());
    }
  }

  private boolean isLong(Number left, Number right)
  {
    return left instanceof Long && right instanceof Long;
  }

  @Override
  public String toString()
  {
    return "(" + opToken.getMatch() + " " + left + " " + right + ")";
  }
}



interface Atom
{
  Number eval(Map<String, Number> bindings);
}

class LongValueAtom implements Atom
{
  private final long value;

  public LongValueAtom(long value)
  {
    this.value = value;
  }

  @Override
  public String toString()
  {
    return String.valueOf(value);
  }

  @Override
  public Number eval(Map<String, Number> bindings)
  {
    return value;
  }
}

class DoubleValueAtom implements Atom
{
  private final double value;

  public DoubleValueAtom(double value)
  {
    this.value = value;
  }

  @Override
  public String toString()
  {
    return String.valueOf(value);
  }

  @Override
  public Number eval(Map<String, Number> bindings)
  {
    return value;
  }
}

class IdentifierAtom implements Atom
{
  private final Token value;

  public IdentifierAtom(Token value)
  {
    this.value = value;
  }

  @Override
  public String toString()
  {
    return value.getMatch();
  }

  @Override
  public Number eval(Map<String, Number> bindings)
  {
    Number val = bindings.get(value.getMatch());
    if (val == null) {
      throw new RuntimeException("No binding found for " + value.getMatch());
    } else {
      return val;
    }
  }
}

class UnaryNotExprAtom implements Atom
{
  private final Expr expr;

  public UnaryNotExprAtom(Expr expr)
  {
    this.expr = expr;
  }

  @Override
  public String toString()
  {
    return "!" + expr.toString();
  }

  @Override
  public Number eval(Map<String, Number> bindings)
  {
    Number valObj = expr.eval(bindings);
    return valObj.doubleValue() > 0 ? 0.0d : 1.0d;
  }
}

class UnaryMinusExprAtom implements Atom
{
  private final Expr expr;

  public UnaryMinusExprAtom(Expr expr)
  {
    this.expr = expr;
  }

  @Override
  public String toString()
  {
    return "-" + expr.toString();
  }

  @Override
  public Number eval(Map<String, Number> bindings)
  {
    Number valObj = expr.eval(bindings);
    if (valObj instanceof Long) {
      return -1 * valObj.longValue();
    } else {
      return -1 * valObj.doubleValue();
    }
  }
}


class NestedExprAtom implements Atom
{
  private final Expr expr;

  public NestedExprAtom(Expr expr)
  {
    this.expr = expr;
  }

  @Override
  public String toString()
  {
    return expr.toString();
  }

  @Override
  public Number eval(Map<String, Number> bindings)
  {
    return expr.eval(bindings);
  }
}

class FunctionAtom implements Atom
{
  private final String name;
  private final List<Expr> args;

  public FunctionAtom(String name, List<Expr> args)
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
  public Number eval(Map<String, Number> bindings)
  {
    return Parser.func.get(name).apply(args, bindings);
  }
}
