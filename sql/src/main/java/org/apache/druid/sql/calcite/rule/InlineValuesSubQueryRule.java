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

package org.apache.druid.sql.calcite.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.rules.SubQueryRemoveRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.Program;
import org.apache.calcite.util.Sarg;
import org.apache.druid.error.DruidException;
import org.apache.druid.sql.calcite.planner.DruidRexExecutor;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Rewrites {@code <expr> [NOT] IN (SELECT col FROM (VALUES ...))} into {@code SEARCH(<expr>, Sarg[...])}
 * before Calcite's {@link SubQueryRemoveRule#rewriteIn} runs. When that rule applies along with an UNNEST,
 * the subquery ends up being too complex for us to plan. See {@code qaUnnest/mv_sql_subquery_with_where.09.all.iq}
 * for a test case that would fail without this rule.
 */
public class InlineValuesSubQueryRule implements Program
{
  private final PlannerContext plannerContext;

  public InlineValuesSubQueryRule(final PlannerContext plannerContext)
  {
    this.plannerContext = plannerContext;
  }

  @Override
  public RelNode run(
      final RelOptPlanner planner,
      final RelNode rel,
      final RelTraitSet requiredOutputTraits,
      final List<RelOptMaterialization> materializations,
      final List<RelOptLattice> lattices
  )
  {
    return rel.accept(new InlineValuesRelShuttle(plannerContext));
  }

  private static class InlineValuesRelShuttle extends RelHomogeneousShuttle
  {
    private final PlannerContext plannerContext;

    InlineValuesRelShuttle(final PlannerContext plannerContext)
    {
      this.plannerContext = plannerContext;
    }

    @Override
    public RelNode visit(RelNode other)
    {
      final RelNode visited = super.visit(other);
      if (visited instanceof Filter filter && RexUtil.SubQueryFinder.containsSubQuery(filter)) {
        final RexNode condition = filter.getCondition();
        final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
        final RexNode newCondition = condition.accept(new InlineValuesRexShuttle(rexBuilder, plannerContext));
        if (newCondition != condition) {
          return filter.copy(filter.getTraitSet(), filter.getInput(), newCondition);
        }
      }
      return visited;
    }
  }

  private static class InlineValuesRexShuttle extends RexShuttle
  {
    private final RexBuilder rexBuilder;
    private final int inSubQueryThreshold;
    private final DruidRexExecutor executor;

    InlineValuesRexShuttle(final RexBuilder rexBuilder, final PlannerContext plannerContext)
    {
      this.rexBuilder = rexBuilder;
      this.inSubQueryThreshold = plannerContext.queryContext().getInSubQueryThreshold();
      this.executor = new DruidRexExecutor(plannerContext);
    }

    @Override
    public RexNode visitSubQuery(final RexSubQuery subQuery)
    {
      if (subQuery.getKind() == SqlKind.IN) {
        final RexNode inlined = tryInlineIn(subQuery);
        if (inlined != null) {
          return inlined;
        }
      }
      return super.visitSubQuery(subQuery);
    }

    @Override
    public RexNode visitCall(final RexCall call)
    {
      // NOT IN appears as NOT(IN(...))
      if (call.getKind() == SqlKind.NOT && call.getOperands().size() == 1) {
        final RexNode operand = call.getOperands().get(0);
        if (operand instanceof RexSubQuery && operand.getKind() == SqlKind.IN) {
          final RexNode inlined = tryInlineIn((RexSubQuery) operand);
          if (inlined != null) {
            return rexBuilder.makeCall(SqlStdOperatorTable.NOT, inlined);
          }
        }
      }
      return super.visitCall(call);
    }

    @Nullable
    private RexNode tryInlineIn(final RexSubQuery subQuery)
    {
      if (subQuery.getOperands().size() != 1) {
        return null;
      }

      // Accept a bare Values, or a Project-over-Values. For Project, we evaluate the projection
      // expression on each Values tuple via the executor.
      final Values valuesRel;
      final RexNode projExpr;
      if (subQuery.rel instanceof Values) {
        valuesRel = (Values) subQuery.rel;
        projExpr = null;
      } else if (subQuery.rel instanceof Project
                 && ((Project) subQuery.rel).getProjects().size() == 1
                 && ((Project) subQuery.rel).getInput() instanceof Values) {
        valuesRel = (Values) ((Project) subQuery.rel).getInput();
        projExpr = CollectionUtils.getOnlyElement(
            ((Project) subQuery.rel).getProjects(),
            xs -> DruidException.defensive("Expected 1 project, got[%s]", xs)
        );
      } else {
        return null;
      }

      if (valuesRel.getRowType().getFieldCount() != 1) {
        return null;
      }

      final ImmutableList<ImmutableList<RexLiteral>> tuples = valuesRel.getTuples();
      if (tuples.isEmpty() || tuples.size() >= inSubQueryThreshold) {
        return null;
      }

      final RelDataType targetType =
          projExpr != null ? projExpr.getType() : valuesRel.getRowType().getFieldList().getFirst().getType();

      final List<RexLiteral> literals = new ArrayList<>(tuples.size());
      for (final ImmutableList<RexLiteral> tuple : tuples) {
        if (tuple.size() != 1) {
          return null;
        }
        final RexLiteral raw = tuple.getFirst();
        final RexLiteral literal;
        if (projExpr == null) {
          literal = raw;
        } else {
          // Substitute the row's literal for $0 in the projection, then evaluate.
          final RexNode substituted = projExpr.accept(new RexShuttle()
          {
            @Override
            public RexNode visitInputRef(final RexInputRef ref)
            {
              return ref.getIndex() == 0 ? raw : ref;
            }
          });
          final List<RexNode> reducedValues = new ArrayList<>(1);
          executor.reduce(rexBuilder, ImmutableList.of(substituted), reducedValues);
          final RexNode reduced = CollectionUtils.getOnlyElement(
              reducedValues,
              xs -> DruidException.defensive("Expected 1 value, got[%s]", xs)
          );
          if (!(reduced instanceof RexLiteral)) {
            return null;
          }
          literal = (RexLiteral) reduced;
        }
        if (literal.isNull()) {
          // Bounce out if we have an IN/NOT IN with NULL in the list. Let other rules try to handle it.
          return null;
        }
        literals.add(literal);
      }

      final RexNode lhs = CollectionUtils.getOnlyElement(
          subQuery.getOperands(),
          xs -> DruidException.defensive("Expected 1 operator, got[%s]", xs)
      );

      return buildSearch(lhs, literals, targetType);
    }

    /**
     * Build a SEARCH(lhs, Sarg[v1, v2, ...]) expression from the extracted literals. Literals
     * must be non-null; the caller is responsible for bailing out if any NULL is present in
     * the VALUES list.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private RexNode buildSearch(
        final RexNode lhs,
        final List<RexLiteral> literals,
        final RelDataType targetType
    )
    {
      final TreeRangeSet rangeSet = TreeRangeSet.create();
      for (final RexLiteral literal : literals) {
        rangeSet.add(Range.singleton(literal.getValueAs(Comparable.class)));
      }

      final Sarg sarg = Sarg.of(RexUnknownAs.UNKNOWN, ImmutableRangeSet.copyOf(rangeSet));
      return RexUtil.sargRef(rexBuilder, lhs, sarg, targetType, RexUnknownAs.UNKNOWN);
    }
  }
}
