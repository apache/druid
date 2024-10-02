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

package org.apache.druid.sql.calcite.rule.logical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil.InputFinder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.druid.error.DruidException;
import java.util.ArrayList;
import java.util.List;

/**
 * Makes tweaks to LogicalUnnest input.
 *
 * Removes any MV_TO_ARRAY call if its present for the input of the
 * {@link LogicalUnnest}.
 *
 */
public class UnnestInputCleanupRule extends RelOptRule implements SubstitutionRule
{
  public UnnestInputCleanupRule()
  {
    super(
        operand(
            LogicalUnnest.class,
            operand(Project.class, any())
        )
    );
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    LogicalUnnest unnest = call.rel(0);
    Project project = call.rel(1);

    ImmutableBitSet input = InputFinder.analyze(unnest.unnestExpr).build();
    if (input.isEmpty()) {
      throw DruidException.defensive("Found an unbound unnest expression.");
    }

    if (!(unnest.unnestExpr instanceof RexInputRef)) {
      // could be supported; but is there a need?
      return;
    }
    if (input.cardinality() != 1) {
      return;
    }

    int inputIndex = input.nextSetBit(0);

    List<RexNode> projects = new ArrayList<>(project.getProjects());
    RexNode unnestInput = projects.get(inputIndex);

    projects.set(
        inputIndex,
        call.builder().getRexBuilder().makeInputRef(project.getInput(), 0)
    );

    RexNode newUnnestExpr = unnestInput.accept(new ExpressionPullerRexShuttle(projects, inputIndex));

    if (projects.size() != project.getProjects().size()) {
      // lets leave this for later
      return;
    }


    RelNode newInputRel = call.builder()
        .push(project.getInput())
        .project(projects)
        .build();


    RelNode newUnnest = new LogicalUnnest(
        unnest.getCluster(), unnest.getTraitSet(), newInputRel, newUnnestExpr,
        unnest.getRowType(), unnest.filter
    );
    call.transformTo(newUnnest);
    call.getPlanner().prune(unnest);
  }

  /**
   * Pulls an expression thru a {@link Project}.
   *
   * May add new projections to the passed mutable list.
   */
  private static class ExpressionPullerRexShuttle extends RexShuttle
  {
    private final List<RexNode> projects;
    private int replaceableIndex;

    private ExpressionPullerRexShuttle(List<RexNode> projects, int replaceableIndex)
    {
      this.projects = projects;
      this.replaceableIndex = replaceableIndex;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef)
    {
      int newIndex = projects.indexOf(inputRef);
      if (newIndex < 0) {
        if (replaceableIndex >= 0) {
          newIndex = replaceableIndex;
          projects.set(replaceableIndex, inputRef);
          replaceableIndex = -1;
        } else {
          newIndex = projects.size();
          projects.add(inputRef);
        }
      }
      if (newIndex == inputRef.getIndex()) {
        return inputRef;
      } else {
        return new RexInputRef(newIndex, inputRef.getType());
      }
    }
  }
}
