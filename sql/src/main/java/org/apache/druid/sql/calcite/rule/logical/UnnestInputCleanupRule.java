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

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil.InputFinder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilder;
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
    Project oldProject = call.rel(1);

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

    RelBuilder builder = call.builder();
    RexBuilder rexBuilder = builder.getRexBuilder();

    int inputIndex = input.nextSetBit(0);
    List<RexNode> newProjects = new ArrayList<>(oldProject.getProjects());
    RexNode unnestInput = newProjects.get(inputIndex);

    newProjects.set(inputIndex, null);

    RexNode newUnnestExpr = unnestInput.accept(new ExpressionPullerRexShuttle(newProjects, inputIndex));

    if (newUnnestExpr instanceof RexInputRef) {
      // this won't make it simpler
      return;
    }

    if (newProjects.get(inputIndex) == null) {
      newProjects.set(
          inputIndex,
          rexBuilder.makeInputRef(oldProject.getInput(), 0)
      );
    }

    RelNode newInputRel = builder
        .push(oldProject.getInput())
        .project(newProjects)
        .build();


    RelNode newUnnest = new LogicalUnnest(
        unnest.getCluster(),
        unnest.getTraitSet(),
        newInputRel,
        newUnnestExpr,
        unnest.unnestFieldType,
        unnest.filter
    );

    builder.push(newUnnest);
    // Erase any extra fields created during the above transformation to be seen outside
    // this could happen in case the pulled out expression referenced
    // not-anymore referenced input columns beneath oldProject
    List<RexNode> projectFields = new ArrayList<>(builder.fields());
    int hideCount = newProjects.size() - oldProject.getProjects().size();
    for (int i = 0; i < hideCount; i++) {
      projectFields.remove(unnest.getRowType().getFieldCount() - 2);
    }

    projectFields.set(
        inputIndex,
        newUnnestExpr
    );
    builder.project(projectFields, ImmutableSet.of(), true);

    RelNode build = builder.build();
    call.transformTo(build);
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

    private ExpressionPullerRexShuttle(List<RexNode> projects, int replaceableIndex)
    {
      this.projects = projects;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef)
    {
      int newIndex = projects.indexOf(inputRef);
      if (newIndex < 0) {
        newIndex = projects.size();
        projects.add(inputRef);
      }
      if (newIndex == inputRef.getIndex()) {
        return inputRef;
      } else {
        return new RexInputRef(newIndex, inputRef.getType());
      }
    }
  }
}
