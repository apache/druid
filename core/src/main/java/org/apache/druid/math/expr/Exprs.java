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

import org.apache.druid.java.util.common.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Stack;

public class Exprs
{
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
   * Decomposes an equality expr into the left- and right-hand side.
   *
   * @return decomposed equality, or empty if the input expr was not an equality expr
   */
  public static Optional<Pair<Expr, Expr>> decomposeEquals(final Expr expr)
  {
    if (expr instanceof BinEqExpr) {
      return Optional.of(Pair.of(((BinEqExpr) expr).left, ((BinEqExpr) expr).right));
    } else {
      return Optional.empty();
    }
  }
}
