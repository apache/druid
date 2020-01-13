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

package org.apache.druid.segment.join;

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.Exprs;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.expression.ExprUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents analysis of a join condition.
 *
 * Each condition is decomposed into "equiConditions" and "nonEquiConditions".
 *
 * 1) The equiConditions are of the form ExpressionOfLeft = ColumnFromRight. The right-hand part cannot be an expression
 * because we use this analysis to determine if we can perform the join using hashtables built off right-hand-side
 * columns.
 *
 * 2) The nonEquiConditions are other conditions that should also be ANDed together
 *
 * All of these conditions are ANDed together to get the overall condition.
 */
public class JoinConditionAnalysis
{
  private final String originalExpression;
  private final List<Equality> equiConditions;
  private final List<Expr> nonEquiConditions;

  private JoinConditionAnalysis(
      final String originalExpression,
      final List<Equality> equiConditions,
      final List<Expr> nonEquiConditions
  )
  {
    this.originalExpression = Preconditions.checkNotNull(originalExpression, "originalExpression");
    this.equiConditions = equiConditions;
    this.nonEquiConditions = nonEquiConditions;
  }

  public static JoinConditionAnalysis forExpression(
      final String condition,
      final String rightPrefix,
      final ExprMacroTable macroTable
  )
  {
    final Expr conditionExpr = Parser.parse(condition, macroTable);
    final List<Equality> equiConditions = new ArrayList<>();
    final List<Expr> nonEquiConditions = new ArrayList<>();

    final List<Expr> exprs = Exprs.decomposeAnd(conditionExpr);
    for (Expr childExpr : exprs) {
      final Optional<Pair<Expr, Expr>> maybeDecomposed = Exprs.decomposeEquals(childExpr);

      if (!maybeDecomposed.isPresent()) {
        nonEquiConditions.add(childExpr);
      } else {
        final Pair<Expr, Expr> decomposed = maybeDecomposed.get();
        final Expr lhs = decomposed.lhs;
        final Expr rhs = decomposed.rhs;

        if (isLeftExprAndRightColumn(lhs, rhs, rightPrefix)) {
          // rhs is a right-hand column; lhs is an expression solely of the left-hand side.
          equiConditions.add(new Equality(lhs, rhs.getIdentifierIfIdentifier().substring(rightPrefix.length())));
        } else if (isLeftExprAndRightColumn(rhs, lhs, rightPrefix)) {
          equiConditions.add(new Equality(rhs, lhs.getIdentifierIfIdentifier().substring(rightPrefix.length())));
        } else {
          nonEquiConditions.add(childExpr);
        }
      }
    }

    return new JoinConditionAnalysis(condition, equiConditions, nonEquiConditions);
  }

  private static boolean isLeftExprAndRightColumn(final Expr a, final Expr b, final String rightPrefix)
  {
    return a.analyzeInputs().getRequiredBindings().stream().noneMatch(c -> c.startsWith(rightPrefix))
           && b.getIdentifierIfIdentifier() != null
           && b.getIdentifierIfIdentifier().startsWith(rightPrefix);
  }

  /**
   * Return the condition expression.
   */
  public String getOriginalExpression()
  {
    return originalExpression;
  }

  /**
   * Return a list of equi-conditions (see class-level javadoc).
   */
  public List<Equality> getEquiConditions()
  {
    return equiConditions;
  }

  /**
   * Return a list of non-equi-conditions (see class-level javadoc).
   */
  public List<Expr> getNonEquiConditions()
  {
    return nonEquiConditions;
  }

  /**
   * Return whether this condition is a constant that is always false.
   */
  public boolean isAlwaysFalse()
  {
    return nonEquiConditions.stream()
                            .anyMatch(expr -> expr.isLiteral() && !expr.eval(ExprUtils.nilBindings()).asBoolean());
  }

  /**
   * Return whether this condition is a constant that is always true.
   */
  public boolean isAlwaysTrue()
  {
    return equiConditions.isEmpty() &&
           nonEquiConditions.stream()
                            .allMatch(expr -> expr.isLiteral() && expr.eval(ExprUtils.nilBindings()).asBoolean());
  }

  /**
   * Returns whether this condition can be satisfied using a hashtable made from the right-hand side.
   */
  public boolean canHashJoin()
  {
    return nonEquiConditions.stream().allMatch(Expr::isLiteral);
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
    JoinConditionAnalysis that = (JoinConditionAnalysis) o;
    return Objects.equals(originalExpression, that.originalExpression);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(originalExpression);
  }

  @Override
  public String toString()
  {
    return originalExpression;
  }
}
