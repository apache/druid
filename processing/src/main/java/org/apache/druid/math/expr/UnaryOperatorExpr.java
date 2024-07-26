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
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
import org.apache.druid.math.expr.vector.VectorMathProcessors;
import org.apache.druid.math.expr.vector.VectorProcessors;
import org.apache.druid.segment.column.Types;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.Objects;


@SuppressWarnings("unused")
final class UnaryOperatorExpr
{
  // phony class to enable maven to track the compilation of this class
}

/**
 * Base type for all single argument operators, with a single {@link Expr} child for the operand.
 */
@SuppressWarnings("ClassName")
abstract class UnaryExpr implements Expr
{
  final String op;
  final Expr expr;

  UnaryExpr(String op, Expr expr)
  {
    this.op = op;
    this.expr = expr;
  }

  abstract UnaryExpr copy(Expr expr);

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
  public BindingAnalysis analyzeInputs()
  {
    // currently all unary operators only operate on scalar inputs
    return expr.analyzeInputs().withScalarArguments(ImmutableSet.of(expr));
  }

  @Nullable
  @Override
  public ExpressionType getOutputType(InputBindingInspector inspector)
  {
    return expr.getOutputType(inspector);
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

  @Override
  public String stringify()
  {
    return StringUtils.format("%s%s", op, expr.stringify());
  }

  @Override
  public String toString()
  {
    return StringUtils.format("%s%s", op, expr);
  }
}

@SuppressWarnings("ClassName")
class UnaryMinusExpr extends UnaryExpr
{
  UnaryMinusExpr(String op, Expr expr)
  {
    super(op, expr);
  }

  @Override
  UnaryExpr copy(Expr expr)
  {
    return new UnaryMinusExpr(op, expr);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    if (expr instanceof BigIntegerExpr) {
      // Special case to handle unary minus for Long.MIN_VALUE: converting the literal to long directly is impossible
      return ExprEval.of(((BigInteger) expr.getLiteralValue()).multiply(BigInteger.valueOf(-1)).longValueExact());
    }
    ExprEval ret = expr.eval(bindings);
    if (NullHandling.sqlCompatible() && (ret.value() == null)) {
      return ExprEval.of(null);
    }
    if (ret.type().is(ExprType.LONG)) {
      return ExprEval.of(-ret.asLong());
    }
    if (ret.type().is(ExprType.DOUBLE)) {
      return ExprEval.of(-ret.asDouble());
    }
    throw new IAE("unsupported type " + ret.type());
  }

  @Override
  public boolean canVectorize(InputBindingInspector inspector)
  {
    return inspector.areNumeric(expr) && expr.canVectorize(inspector);
  }

  @Override
  public <T> ExprVectorProcessor<T> asVectorProcessor(VectorInputBindingInspector inspector)
  {
    return VectorMathProcessors.negate(inspector, expr);
  }
}

@SuppressWarnings("ClassName")
class UnaryNotExpr extends UnaryExpr
{
  UnaryNotExpr(String op, Expr expr)
  {
    super(op, expr);
  }

  @Override
  UnaryExpr copy(Expr expr)
  {
    return new UnaryNotExpr(op, expr);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    ExprEval ret = expr.eval(bindings);
    if (NullHandling.sqlCompatible() && (ret.value() == null)) {
      return ExprEval.of(null);
    }
    if (!ExpressionProcessing.useStrictBooleans()) {
      // conforming to other boolean-returning binary operators
      ExpressionType retType = ret.type().is(ExprType.DOUBLE) ? ExpressionType.DOUBLE : ExpressionType.LONG;
      return ExprEval.ofBoolean(!ret.asBoolean(), retType);
    }
    return ExprEval.ofLongBoolean(!ret.asBoolean());
  }

  @Nullable
  @Override
  public ExpressionType getOutputType(InputBindingInspector inspector)
  {
    if (!ExpressionProcessing.useStrictBooleans()) {
      ExpressionType implicitCast = super.getOutputType(inspector);
      if (Types.is(implicitCast, ExprType.STRING)) {
        return ExpressionType.LONG;
      }
      return implicitCast;
    }
    return ExpressionType.LONG;
  }

  @Override
  public boolean canVectorize(InputBindingInspector inspector)
  {
    return expr.canVectorize(inspector);
  }

  @Override
  public <T> ExprVectorProcessor<T> asVectorProcessor(VectorInputBindingInspector inspector)
  {
    return VectorProcessors.not(inspector, expr);
  }
}
