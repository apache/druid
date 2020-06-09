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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
  private final String rightPrefix;
  private final List<Equality> equiConditions;
  private final List<Expr> nonEquiConditions;
  private final boolean isAlwaysFalse;
  private final boolean isAlwaysTrue;
  private final boolean canHashJoin;
  private final Set<String> rightKeyColumns;

  private JoinConditionAnalysis(
      final String originalExpression,
      final String rightPrefix,
      final List<Equality> equiConditions,
      final List<Expr> nonEquiConditions
  )
  {
    this.originalExpression = Preconditions.checkNotNull(originalExpression, "originalExpression");
    this.rightPrefix = Preconditions.checkNotNull(rightPrefix, "rightPrefix");
    this.equiConditions = Collections.unmodifiableList(equiConditions);
    this.nonEquiConditions = Collections.unmodifiableList(nonEquiConditions);
    // if any nonEquiCondition is an expression and it evaluates to false
    isAlwaysFalse = nonEquiConditions.stream()
                                     .anyMatch(expr -> expr.isLiteral() && !expr.eval(ExprUtils.nilBindings())
                                                                                .asBoolean());
    // if there are no equiConditions and all nonEquiConditions are literals and the evaluate to true
    isAlwaysTrue = equiConditions.isEmpty() && nonEquiConditions.stream()
                                                                .allMatch(expr -> expr.isLiteral() && expr.eval(
                                                                    ExprUtils.nilBindings()).asBoolean());
    canHashJoin = nonEquiConditions.stream().allMatch(Expr::isLiteral);
    rightKeyColumns = getEquiConditions().stream().map(Equality::getRightColumn).collect(Collectors.toSet());
  }

  /**
   * Analyze a join condition.
   *
   * @param condition   the condition expression
   * @param rightPrefix prefix for the right-hand side of the join; will be used to determine which identifiers in
   *                    the condition come from the right-hand side and which come from the left-hand side
   * @param macroTable  macro table for parsing the condition expression
   */
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
        final Expr lhs = Objects.requireNonNull(decomposed.lhs);
        final Expr rhs = Objects.requireNonNull(decomposed.rhs);

        if (isLeftExprAndRightColumn(lhs, rhs, rightPrefix)) {
          // rhs is a right-hand column; lhs is an expression solely of the left-hand side.
          equiConditions.add(
              new Equality(lhs, Objects.requireNonNull(rhs.getBindingIfIdentifier()).substring(rightPrefix.length()))
          );
        } else if (isLeftExprAndRightColumn(rhs, lhs, rightPrefix)) {
          equiConditions.add(
              new Equality(rhs, Objects.requireNonNull(lhs.getBindingIfIdentifier()).substring(rightPrefix.length()))
          );
        } else {
          nonEquiConditions.add(childExpr);
        }
      }
    }

    return new JoinConditionAnalysis(condition, rightPrefix, equiConditions, nonEquiConditions);
  }

  private static boolean isLeftExprAndRightColumn(final Expr a, final Expr b, final String rightPrefix)
  {
    return a.analyzeInputs().getRequiredBindings().stream().noneMatch(c -> Joinables.isPrefixedBy(c, rightPrefix))
           && b.getBindingIfIdentifier() != null
           && Joinables.isPrefixedBy(b.getBindingIfIdentifier(), rightPrefix);
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
    return isAlwaysFalse;
  }

  /**
   * Return whether this condition is a constant that is always true.
   */
  public boolean isAlwaysTrue()
  {
    return isAlwaysTrue;
  }

  /**
   * Returns whether this condition can be satisfied using a hashtable made from the right-hand side.
   */
  public boolean canHashJoin()
  {
    return canHashJoin;
  }

  /**
   * Returns the distinct column keys from the RHS required to evaluate the equi conditions.
   */
  public Set<String> getRightEquiConditionKeys()
  {
    return rightKeyColumns;
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
    return Objects.equals(originalExpression, that.originalExpression) &&
           Objects.equals(rightPrefix, that.rightPrefix);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(originalExpression, rightPrefix);
  }

  @Override
  public String toString()
  {
    return originalExpression;
  }
}
