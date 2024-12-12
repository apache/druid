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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.sql.calcite.expression.builtin.QueryLookupOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Rule that pulls {@link QueryLookupOperatorConversion#SQL_FUNCTION} up through an {@link Aggregate}.
 */
public class AggregatePullUpLookupRule extends RelOptRule
{
  private final PlannerContext plannerContext;

  public AggregatePullUpLookupRule(final PlannerContext plannerContext)
  {
    super(operand(Aggregate.class, operand(Project.class, any())));
    this.plannerContext = plannerContext;
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final Aggregate aggregate = call.rel(0);
    final Project project = call.rel(1);
    final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    final Set<Integer> aggCallInputs = RelOptUtil.getAllFields2(ImmutableBitSet.of(), aggregate.getAggCallList());

    RexNode[] topProjects = null; // Projects pulled up on top of the Aggregate
    RexNode[] bottomProjects = null; // Projects that stay on the bottom of the Aggregate
    boolean matched = false; // Whether we found a LOOKUP call to pull up

    int dimensionIndex = 0;
    for (Iterator<Integer> iterator = aggregate.getGroupSet().iterator(); iterator.hasNext(); dimensionIndex++) {
      int projectIndex = iterator.next();
      final RexNode projectExpr = project.getProjects().get(projectIndex);

      if (ReverseLookupRule.isLookupCall(projectExpr) && !aggCallInputs.contains(projectIndex)) {
        final RexCall lookupCall = (RexCall) projectExpr;
        final String lookupName = RexLiteral.stringValue(lookupCall.getOperands().get(1));
        final LookupExtractor lookup = plannerContext.getLookup(lookupName);

        if (lookup != null && lookup.isOneToOne()) {
          if (!matched) {
            matched = true;
            bottomProjects = new RexNode[project.getProjects().size()];
            topProjects = new RexNode[aggregate.getRowType().getFieldCount()];
          }

          // Rewrite LOOKUP("x", 'lookupName) => "x" underneath the Aggregate.
          bottomProjects[projectIndex] = lookupCall.getOperands().get(0);

          // Pull LOOKUP above the Aggregate.
          final List<RexNode> newLookupOperands = new ArrayList<>(lookupCall.getOperands());
          final RelDataType dimensionType = aggregate.getRowType().getFieldList().get(dimensionIndex).getType();
          newLookupOperands.set(0, rexBuilder.makeInputRef(dimensionType, dimensionIndex));
          topProjects[dimensionIndex] = lookupCall.clone(dimensionType, newLookupOperands);
        }
      }
    }

    if (matched) {
      // Fill in any missing bottomProjects.
      for (int i = 0; i < bottomProjects.length; i++) {
        if (bottomProjects[i] == null) {
          bottomProjects[i] = project.getProjects().get(i);
        }
      }

      // Fill in any missing topProjects.
      for (int i = 0; i < topProjects.length; i++) {
        if (topProjects[i] == null) {
          topProjects[i] = rexBuilder.makeInputRef(aggregate.getRowType().getFieldList().get(i).getType(), i);
        }
      }

      final RelBuilder relBuilder = call.builder();
      call.transformTo(relBuilder
                           .push(project.getInput())
                           .project(bottomProjects)
                           .aggregate(
                               relBuilder.groupKey(aggregate.getGroupSet(), aggregate.getGroupSets()),
                               aggregate.getAggCallList()
                           )
                           .project(topProjects)
                           .build());
      call.getPlanner().prune(aggregate);
    }
  }
}
