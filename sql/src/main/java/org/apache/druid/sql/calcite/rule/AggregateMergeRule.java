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
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSplittableAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Modified version of Calcite's {@link org.apache.calcite.rel.rules.AggregateMergeRule}, with two changes:
 *
 * (1) Includes a workaround for https://issues.apache.org/jira/browse/CALCITE-7162
 * (2) Includes the ability to merge two {@link Aggregate} with a {@link Project} between them, by pushing the
 * {@link Project} below the new merged {@link Aggregate}. This is done by {@link WithProject}.
 */
public abstract class AggregateMergeRule extends RelOptRule
{
  protected AggregateMergeRule(final RelOptRuleOperand operand, final String description)
  {
    super(operand, description);
  }

  /**
   * Logic borrowed from Calcite's {@link org.apache.calcite.rel.rules.AggregateMergeRule} to merge two
   * {@link Aggregate}, with modification to work around https://issues.apache.org/jira/browse/CALCITE-7162.
   */
  @Nullable
  protected RelNode apply(Aggregate topAgg, Aggregate bottomAgg)
  {
    if (topAgg.getGroupCount() > bottomAgg.getGroupCount()) {
      return null;
    }

    final ImmutableBitSet bottomGroupSet = bottomAgg.getGroupSet();
    final Map<Integer, Integer> map = new HashMap<>(); // group index => underlying field number
    bottomGroupSet.forEachInt(v -> map.put(map.size(), v));
    for (int k : topAgg.getGroupSet()) {
      if (!map.containsKey(k)) {
        return null;
      }
    }

    // top aggregate keys must be function of lower aggregate keys
    final ImmutableBitSet topGroupSet = topAgg.getGroupSet().permute(map);
    if (!bottomGroupSet.contains(topGroupSet)) {
      return null;
    }

    boolean hasEmptyGroup = topAgg.getGroupSets()
                                  .stream().anyMatch(ImmutableBitSet::isEmpty);

    final List<AggregateCall> finalCalls = new ArrayList<>();
    for (AggregateCall topCall : topAgg.getAggCallList()) {
      if (!isAggregateSupported(topCall)
          || topCall.getArgList().isEmpty()) {
        return null;
      }
      // Make sure top aggregate argument refers to one of the aggregate
      int bottomIndex = topCall.getArgList().get(0) - bottomGroupSet.cardinality();
      if (bottomIndex >= bottomAgg.getAggCallList().size()
          || bottomIndex < 0) {
        return null;
      }
      AggregateCall bottomCall = bottomAgg.getAggCallList().get(bottomIndex);
      // Should not merge if top agg with empty group keys and the lower agg
      // function is COUNT, because in case of empty input for lower agg,
      // the result is empty, if we merge them, we end up with 1 result with
      // 0, which is wrong.
      if (!isAggregateSupported(bottomCall)
          || (bottomCall.getAggregation() == SqlStdOperatorTable.COUNT
              && topCall.getAggregation().getKind() != SqlKind.SUM0
              && hasEmptyGroup)) {
        return null;
      }
      SqlSplittableAggFunction splitter =
          bottomCall.getAggregation()
                    .unwrapOrThrow(SqlSplittableAggFunction.class);
      AggregateCall finalCall = splitter.merge(topCall, bottomCall);
      // fail to merge the aggregate call, bail out
      if (finalCall == null) {
        return null;
      }
      if (!finalCall.getType().equals(topCall.getType())) {
        // Fix up type to work around https://issues.apache.org/jira/browse/CALCITE-7162.
        finalCall = AggregateCall.create(
            finalCall.getAggregation(),
            finalCall.isDistinct(),
            finalCall.isApproximate(),
            finalCall.ignoreNulls(),
            finalCall.rexList,
            finalCall.getArgList(),
            finalCall.filterArg,
            finalCall.distinctKeys,
            finalCall.collation,
            topCall.getType(),
            finalCall.getName()
        );
      }
      finalCalls.add(finalCall);
    }

    // re-map grouping sets
    ImmutableList<ImmutableBitSet> newGroupingSets = null;
    if (topAgg.getGroupType() != Aggregate.Group.SIMPLE) {
      newGroupingSets =
          ImmutableBitSet.ORDERING.immutableSortedCopy(ImmutableBitSet.permute(topAgg.getGroupSets(), map));
    }

    return topAgg.copy(
        topAgg.getTraitSet(),
        bottomAgg.getInput(),
        topGroupSet,
        newGroupingSets,
        finalCalls
    );
  }

  private static boolean isAggregateSupported(AggregateCall aggCall)
  {
    if (aggCall.isDistinct()
        || aggCall.hasFilter()
        || aggCall.isApproximate()
        || aggCall.getArgList().size() > 1) {
      return false;
    }
    return aggCall.getAggregation()
                  .maybeUnwrap(SqlSplittableAggFunction.class).isPresent();
  }

  /**
   * Helper for {@link WithProject}. Rewrites a {@link Project} on an {@link Aggregate} into an {@link Aggregate}
   * on a {@link Project}. Only accepts scenarios where the project refers to functions of grouping keys and/or
   * individual aggregators.
   *
   * Changes semantics of the aggregation: by rewriting the GROUP BY of the {@link Aggregate}, a different (smaller)
   * number of rows may emerge. This is OK because the eventual goal is to merge it with a higher-level
   * {@link Aggregate} anyway.
   */
  @Nullable
  private static Aggregate pushProjectThroughAggregate(final Project project, final Aggregate aggregate)
  {
    if (aggregate.getGroupSets().size() != 1) {
      // Multiple GROUPING SETS not supported.
      return null;
    }

    if (project.containsOver()) {
      // Project containing window functions is not supported.
      return null;
    }

    final ImmutableBitSet groupSet = aggregate.getGroupSet();

    // group-by index => underlying field number
    final Mapping groupSetMapping =
        Mappings.source(groupSet.toList(), aggregate.getInput().getRowType().getFieldCount());

    final List<RexNode> newProjects = new ArrayList<>();
    final IntList newGroupSet = new IntArrayList();
    final IntList newAggCallPositions = new IntArrayList();
    final List<AggregateCall> newAggCalls = new ArrayList<>();
    final Int2IntMap aggCallArgMap = new Int2IntOpenHashMap();

    for (final RexNode projectExpr : project.getProjects()) {
      final ImmutableBitSet inputBits = RelOptUtil.InputFinder.bits(projectExpr);
      if (ImmutableBitSet.range(0, aggregate.getGroupCount()).contains(inputBits)) {
        // projectExpr is a function of grouping keys. New Aggregate will group by this expr.
        if (!newAggCallPositions.isEmpty()) {
          // For the Project to be replaceable with an Aggregate, all grouping fields must occur before all
          // aggregate calls. For some reason, this is true even when the order varies in the SQL.
          // See CalciteQueryTest#testCollapsibleNestedGroupBy2 for an example.
          return null;
        }

        newGroupSet.add(newProjects.size());
        newProjects.add(RexUtil.apply(groupSetMapping, projectExpr));
      } else if (projectExpr.isA(SqlKind.INPUT_REF)
                 && ((RexInputRef) projectExpr).getIndex() >= aggregate.getGroupCount()) {
        // projectExpr is simple reference to an aggregator. New Aggregate will include the same aggregator.
        final int inputBit = inputBits.asList().get(0);
        final int aggCallPosition = inputBit - aggregate.getGroupCount();
        final AggregateCall aggCall = aggregate.getAggCallList().get(aggCallPosition);
        if (!isAggregateSupported(aggCall)) {
          return null;
        }

        newAggCallPositions.add(aggCallPosition);

        // Add aggregator args to the new Project.
        for (int argNum : aggCall.getArgList()) {
          aggCallArgMap.put(argNum, newProjects.size());
          newProjects.add(RexInputRef.of(argNum, aggregate.getInput().getRowType()));
        }
      } else {
        // Cannot handle projectExpr.
        return null;
      }
    }

    // Create the new Project.
    final Project newProject = project.copy(
        project.getTraitSet(),
        aggregate.getInput(),
        newProjects,
        RexUtil.createStructType(
            aggregate.getInput().getCluster().getTypeFactory(),
            newProjects,
            null,
            null
        )
    );

    // Create the new AggregateCalls.
    final Mappings.TargetMapping aggCallArgMapping = Mappings.target(
        aggCallArgMap,
        aggregate.getInput().getRowType().getFieldCount(),
        newProjects.size()
    );

    for (int i : newAggCallPositions) {
      final AggregateCall aggCall = aggregate.getAggCallList().get(i);
      newAggCalls.add(
          AggregateCall.create(
              aggCall.getAggregation(),
              aggCall.isDistinct(),
              aggCall.isApproximate(),
              aggCall.ignoreNulls(),
              aggCall.rexList,
              Mappings.apply2((Mapping) aggCallArgMapping, aggCall.getArgList()),
              aggCall.hasFilter() ? Mappings.apply(aggCallArgMapping, aggCall.filterArg) : -1,
              aggCall.distinctKeys == null ? null : aggCall.distinctKeys.permute(aggCallArgMapping),
              RelCollations.permute(aggCall.getCollation(), aggCallArgMapping),
              newGroupSet.size(),
              newProject,
              null,
              aggCall.getName()
          )
      );
    }

    // Create the new Aggregate.
    return aggregate.copy(
        aggregate.getTraitSet(),
        newProject,
        ImmutableBitSet.of(newGroupSet),
        null,
        newAggCalls
    );
  }

  /**
   * Implementation matching an Aggregate on top of an Aggregate with an intervening Project.
   */
  public static class WithProject extends AggregateMergeRule
  {
    public WithProject()
    {
      super(
          operand(Aggregate.class, operand(Project.class, operand(Aggregate.class, any()))),
          "AggregateMergeRule[WithProject]"
      );
    }

    @Override
    public void onMatch(RelOptRuleCall call)
    {
      final Aggregate topAggregate = call.rel(0);
      final Project project = call.rel(1);
      final Aggregate bottomAggregate = call.rel(2);

      final Aggregate newBottomAggregate = pushProjectThroughAggregate(project, bottomAggregate);
      if (newBottomAggregate != null) {
        final RelNode rel = apply(topAggregate, newBottomAggregate);
        if (rel != null) {
          // Don't prune original, since it is possible that the Aggregate-Project-Aggregate sandwich would
          // be more efficient than a merged Aggregate-Project. (If the bottom Aggregate reduces the row count
          // greatly, and if the Project is expensive to apply.) Let the planner consider both possible options.
          call.transformTo(rel);
        }
      }
    }
  }

  /**
   * Implementation matching an Aggregate on top of an Aggregate without any intervening Project.
   */
  public static class WithoutProject extends AggregateMergeRule
  {
    public WithoutProject()
    {
      super(
          operand(Aggregate.class, operand(Aggregate.class, any())),
          "AggregateMergeRule[WithoutProject]"
      );
    }

    @Override
    public void onMatch(RelOptRuleCall call)
    {
      final Aggregate topAggregate = call.rel(0);
      final Aggregate bottomAggregate = call.rel(1);
      final RelNode rel = apply(topAggregate, bottomAggregate);
      if (rel != null) {
        // Prune original, one aggregation is always better than two.
        call.transformTo(rel);
        call.getPlanner().prune(topAggregate);
      }
    }
  }
}
