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

import com.google.common.collect.ImmutableSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;

import java.util.Objects;

/**
 * Base type for all single argument operators, with a single {@link Expr} child for the operand.
 */
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
    //noinspection ObjectEquality (checking for object equality here is intentional)
    if (newExpr != expr) {
      return shuttle.visit(copy(newExpr));
    }
    return shuttle.visit(this);
  }

  @Override
  public BindingDetails analyzeInputs()
  {
    // currently all unary operators only operate on scalar inputs
    return expr.analyzeInputs().withScalarArguments(ImmutableSet.of(expr));
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
    UnaryExpr unaryExpr = (UnaryExpr) o;
    return Objects.equals(expr, unaryExpr.expr);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(expr);
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
  public String stringify()
  {
    return StringUtils.format("-%s", expr.stringify());
  }

  @Override
  public String toString()
  {
    return StringUtils.format("-%s", expr);
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
  public String stringify()
  {
    return StringUtils.format("!%s", expr.stringify());
  }

  @Override
  public String toString()
  {
    return StringUtils.format("!%s", expr);
  }
}
