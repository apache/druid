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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.druid.java.util.common.ISE;

import java.util.ArrayList;
import java.util.List;

/**
 * Rule that prunes unused aggregators after a projection.
 */
public class ProjectAggregatePruneUnusedCallRule extends RelOptRule
{
  private static final ProjectAggregatePruneUnusedCallRule INSTANCE = new ProjectAggregatePruneUnusedCallRule();

  private ProjectAggregatePruneUnusedCallRule()
  {
    super(operand(Project.class, operand(Aggregate.class, any())));
  }

  public static ProjectAggregatePruneUnusedCallRule instance()
  {
    return INSTANCE;
  }

  @Override
  public boolean matches(final RelOptRuleCall call)
  {
    final Aggregate aggregate = call.rel(1);
    return !aggregate.indicator && aggregate.getGroupSets().size() == 1;
  }

  @Override
  public void onMatch(final RelOptRuleCall call)
  {
    final Project project = call.rel(0);
    final Aggregate aggregate = call.rel(1);

    final ImmutableBitSet projectBits = RelOptUtil.InputFinder.bits(project.getChildExps(), null);

    final int fieldCount = aggregate.getGroupCount() + aggregate.getAggCallList().size();
    if (fieldCount != aggregate.getRowType().getFieldCount()) {
      throw new ISE(
          "WTF, expected[%s] to have[%s] fields but it had[%s]",
          aggregate,
          fieldCount,
          aggregate.getRowType().getFieldCount()
      );
    }

    final ImmutableBitSet callsToKeep = projectBits.intersect(
        ImmutableBitSet.range(aggregate.getGroupCount(), fieldCount)
    );

    if (callsToKeep.cardinality() < aggregate.getAggCallList().size()) {
      // There are some aggregate calls to prune.
      final List<AggregateCall> newAggregateCalls = new ArrayList<>();

      for (int i : callsToKeep) {
        newAggregateCalls.add(aggregate.getAggCallList().get(i - aggregate.getGroupCount()));
      }

      final Aggregate newAggregate = aggregate.copy(
          aggregate.getTraitSet(),
          aggregate.getInput(),
          aggregate.indicator,
          aggregate.getGroupSet(),
          aggregate.getGroupSets(),
          newAggregateCalls
      );

      // Project that will match the old Aggregate in its row type, so we can layer the original "project" on top.
      final List<RexNode> fixUpProjects = new ArrayList<>();
      final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();

      // Project the group unchanged.
      for (int i = 0; i < aggregate.getGroupCount(); i++) {
        fixUpProjects.add(rexBuilder.makeInputRef(newAggregate, i));
      }

      // Replace pruned-out aggregators with NULLs.
      int j = aggregate.getGroupCount();
      for (int i = aggregate.getGroupCount(); i < fieldCount; i++) {
        if (callsToKeep.get(i)) {
          fixUpProjects.add(rexBuilder.makeInputRef(newAggregate, j++));
        } else {
          fixUpProjects.add(rexBuilder.makeNullLiteral(aggregate.getRowType().getFieldList().get(i).getType()));
        }
      }

      call.transformTo(
          call.builder()
              .push(newAggregate)
              .project(fixUpProjects)
              .project(project.getChildExps())
              .build()
      );

      call.getPlanner().setImportance(project, 0.0);
    }
  }
}
