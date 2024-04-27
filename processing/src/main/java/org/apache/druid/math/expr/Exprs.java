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

import org.apache.druid.java.util.common.UOE;
import org.apache.druid.segment.join.Equality;
import org.apache.druid.segment.join.JoinPrefixUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Stack;

public class Exprs
{
  public static final byte EXPR_CACHE_KEY = 0x00;

  public static UnsupportedOperationException cannotVectorize(Expr expr)
  {
    return new UOE("Unable to vectorize expression:[%s]", expr.stringify());
  }

  public static UnsupportedOperationException cannotVectorize(Function function)
  {
    return new UOE("Unable to vectorize function:[%s]", function.name());
  }

  public static UnsupportedOperationException cannotVectorize()
  {
    return new UOE("Unable to vectorize expression");
  }

  public static UnsupportedOperationException cannotVectorize(String msg)
  {
    return new UOE("Unable to vectorize expression: %s", msg);
  }

  /**
   * Return a {@link Expr.BindingAnalysis} that represents an analysis of all provided args.
   */
  public static Expr.BindingAnalysis analyzeBindings(final List<Expr> args)
  {
    Expr.BindingAnalysis accumulator = new Expr.BindingAnalysis();
    for (final Expr arg : args) {
      accumulator = accumulator.with(arg);
    }
    return accumulator;
  }

  /**
   * Decomposes any expr into a list of exprs that, if ANDed together, are equivalent to the input expr.
   *
   * @param expr any expr
   *
   * @return list of exprs that, if ANDed together, are equivalent to the input expr
   */
  public static List<Expr> decomposeAnd(final Expr expr)
  {
    final List<Expr> retVal = new ArrayList<>();
    final Stack<Expr> stack = new Stack<>();
    stack.push(expr);

    while (!stack.empty()) {
      final Expr current = stack.pop();

      if (current instanceof BinAndExpr) {
        stack.push(((BinAndExpr) current).right);
        stack.push(((BinAndExpr) current).left);
      } else {
        retVal.add(current);
      }
    }

    return retVal;
  }

  /**
   * Decomposes an equality expr into an {@link Equality}. Used by join-related code to identify equi-joins.
   *
   * @return decomposed equality, or empty if the input expr was not an equality expr
   */
  public static Optional<Equality> decomposeEquals(final Expr expr, final String rightPrefix)
  {
    final Expr lhs;
    final Expr rhs;
    final boolean includeNull;

    if (expr instanceof BinEqExpr) {
      lhs = ((BinEqExpr) expr).left;
      rhs = ((BinEqExpr) expr).right;
      includeNull = false;
    } else if (expr instanceof FunctionExpr
               && ((FunctionExpr) expr).function instanceof Function.IsNotDistinctFromFunc) {
      final List<Expr> args = ((FunctionExpr) expr).args;
      lhs = args.get(0);
      rhs = args.get(1);
      includeNull = true;
    } else {
      return Optional.empty();
    }

    if (isLeftExprAndRightColumn(lhs, rhs, rightPrefix)) {
      // rhs is a right-hand column; lhs is an expression solely of the left-hand side.
      return Optional.of(
          new Equality(
              lhs,
              Objects.requireNonNull(rhs.getBindingIfIdentifier()).substring(rightPrefix.length()),
              includeNull
          )
      );
    } else if (isLeftExprAndRightColumn(rhs, lhs, rightPrefix)) {
      return Optional.of(
          new Equality(
              rhs,
              Objects.requireNonNull(lhs.getBindingIfIdentifier()).substring(rightPrefix.length()),
              includeNull
          )
      );
    } else {
      return Optional.empty();
    }
  }

  private static boolean isLeftExprAndRightColumn(final Expr a, final Expr b, final String rightPrefix)
  {
    return a.analyzeInputs().getRequiredBindings().stream().noneMatch(c -> JoinPrefixUtils.isPrefixedBy(c, rightPrefix))
           && b.getBindingIfIdentifier() != null
           && JoinPrefixUtils.isPrefixedBy(b.getBindingIfIdentifier(), rightPrefix);
  }
}
