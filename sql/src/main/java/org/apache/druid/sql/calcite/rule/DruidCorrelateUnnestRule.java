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

import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.DruidCorrelateUnnestRel;
import org.apache.druid.sql.calcite.rel.DruidJoinQueryRel;
import org.apache.druid.sql.calcite.rel.DruidJoinUnnestRel;
import org.apache.druid.sql.calcite.rel.DruidRel;
import org.apache.druid.sql.calcite.rel.DruidUnnestRel;
import org.apache.druid.sql.calcite.rel.PartialDruidQuery;

import java.util.ArrayList;
import java.util.List;

/**
 * This class creates the rule to abide by for creating correlations during unnest.
 * Typically, Calcite plans the unnest query such as
 * SELECT * from numFoo, unnest(dim3) in the following way:
 *
 * <pre>
 * 80:LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{3}])
 *   6:LogicalTableScan(subset=[rel#74:Subset#0.NONE.[]], table=[[druid, numfoo]])
 *   78:Uncollect(subset=[rel#79:Subset#3.NONE.[]])
 *     76:LogicalProject(subset=[rel#77:Subset#2.NONE.[]], EXPR$0=[MV_TO_ARRAY($cor0.dim3)])
 *       7:LogicalValues(subset=[rel#75:Subset#1.NONE.[0]], tuples=[[{ 0 }]])
 * </pre>
 * <p>
 * {@link DruidUnnestRule} takes care of the Uncollect(last 3 lines) to generate a {@link DruidUnnestRel}
 * thereby reducing the logical plan to:
 * <pre>
 *        LogicalCorrelate
 *           /       \
 *      DruidRel    DruidRel
 * </pre>
 * This forms the premise of this rule. The goal is to transform the above-mentioned structure in the tree
 * with a new rel {@link DruidCorrelateUnnestRel} which shall be created here.
 *
 * Note that the right can be a special rel {@link DruidJoinUnnestRel} where there is a join with unnest on the left
 * So the inital tree in such a case was
 * <pre/
 *          LogicalCorrelate
 *            /           \
 *      DruidRel1        LogicalJoin
 *                        /       \
 *             DruidUnnestRel    DruidRel2
 * </pre>
 * And this rule helps transpose the Correlate and Join thereby creating the Tree
 * <pre/
 *               LogicalJoin
 *               /        \
 *   LogicalCorrelate    DruidRel2
 *       /       \
 *   DruidRel1    DruidUnnestRel
 * </pre>
 */
public class DruidCorrelateUnnestRule extends RelOptRule
{
  private final PlannerContext plannerContext;

  public DruidCorrelateUnnestRule(final PlannerContext plannerContext)
  {
    super(
        operand(
            Correlate.class,
            operand(DruidRel.class, any()),
            operand(DruidRel.class, any())
        )
    );

    this.plannerContext = plannerContext;
  }

  @Override
  public boolean matches(RelOptRuleCall call)
  {
    final DruidRel<?> left = call.rel(1);
    return left.getPartialDruidQuery() != null;
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final Correlate correlate = call.rel(0);
    final DruidRel<?> left = call.rel(1);
    final DruidUnnestRel right;
    final DruidRel<?> rightRel = call.rel(2);
    // Update right if it follows the special pattern of unnest to the left
    if (rightRel instanceof DruidJoinUnnestRel) {
      right = ((DruidJoinUnnestRel) rightRel).getUnnestRel();
    } else {
      right = (DruidUnnestRel) rightRel;
    }
    final RexBuilder rexBuilder = correlate.getCluster().getRexBuilder();
    final DruidRel<?> newLeft;
    final List<RexNode> pulledUpProjects = new ArrayList<>();
    final Filter leftFilter;
    final CorrelationId newCorrelationId;
    final RexNode newUnnestRexNode;
    final ImmutableBitSet requiredCols;

    // the partial query should in a SELECT_PROJECT stage
    // the right no expressions and just a reference (ask G/C)
    if (left.getPartialDruidQuery().stage() == PartialDruidQuery.Stage.SELECT_PROJECT
      //&& RelOptUtil.InputFinder.bits(right.getInputRexNode()).isEmpty()
    ) {
      // Swap the left-side projection above the Correlate, so the left side is a simple scan or mapping. This helps us
      // avoid subqueries.
      final RelNode leftScan = left.getPartialDruidQuery().getScan();
      final Project leftProject = left.getPartialDruidQuery().getSelectProject();
      pulledUpProjects.addAll(leftProject.getProjects());
      leftFilter = left.getPartialDruidQuery().getWhereFilter();
      newLeft = left.withPartialQuery(PartialDruidQuery.create(leftScan).withWhereFilter(leftFilter));

      // push the correlation past the project
      newCorrelationId = correlate.getCluster().createCorrel();
      final PushCorrelatedFieldAccessPastProject correlatedFieldRewriteShuttle =
          new PushCorrelatedFieldAccessPastProject(correlate.getCorrelationId(), newCorrelationId, leftProject);
      newUnnestRexNode = correlatedFieldRewriteShuttle.apply(right.getInputRexNode());
      requiredCols = ImmutableBitSet.of(correlatedFieldRewriteShuttle.getRequiredColumns());
    } else {
      for (int i = 0; i < left.getRowType().getFieldCount(); i++) {
        pulledUpProjects.add(rexBuilder.makeInputRef(correlate.getRowType().getFieldList().get(i).getType(), i));
      }
      newLeft = left;
      newUnnestRexNode = right.getInputRexNode();
      requiredCols = correlate.getRequiredColumns();
      newCorrelationId = correlate.getCorrelationId();
    }

    // process right
    // Leave as-is. Write input refs that do nothing.
    for (int i = 0; i < right.getRowType().getFieldCount(); i++) {
      pulledUpProjects.add(
          rexBuilder.makeInputRef(
              correlate.getRowType().getFieldList().get(left.getRowType().getFieldCount() + i).getType(),
              newLeft.getRowType().getFieldCount() + i
          )
      );
    }

    // Build the new Correlate rel and a DruidCorrelateUnnestRel wrapper.
    final DruidCorrelateUnnestRel druidCorrelateUnnest = DruidCorrelateUnnestRel.create(
        correlate.copy(
            correlate.getTraitSet(),
            newLeft,
            // Right side: use rewritten newUnnestRexNode, pushed past the left Project.
            right.withUnnestRexNode(newUnnestRexNode),
            newCorrelationId,
            requiredCols,
            correlate.getJoinType()
        ),
        plannerContext
    );
    // Move the join out if the right follows the special pattern
    if (rightRel instanceof DruidJoinUnnestRel) {
      // create a join rel
      // with left as druidCorrelateUnnest
      // and right as rightRel.getRight()
      // Rewrite the join condition as the join pointed to offsets
      // of the join between unnest and DruidRel
      // But now should point to the offsets of the
      // join between Correlate and DruidRel
      final Join j = ((DruidJoinUnnestRel) rightRel).getJoin();
      final RexNode condition = j.getCondition();
      // Last index of left is fieldCount-1
      // Since unnest is always the 0th index
      // we need the left to shift the join condition
      // by adding the index in the shuttle
      UpdateJoinConditionShuttle jctShuttle = new UpdateJoinConditionShuttle(
          druidCorrelateUnnest.getRowType().getFieldCount() - 1);
      final RexNode out = jctShuttle.apply(condition);
      Join updatedJoin = j.copy(
          correlate.getTraitSet(),
          out,
          druidCorrelateUnnest,
          ((DruidJoinUnnestRel) rightRel).getRightRel(),
          j.getJoinType(),
          j.isSemiJoinDone()
      );
      final DruidJoinQueryRel druidJoin = DruidJoinQueryRel.create(
          updatedJoin,
          null,
          left.getPlannerContext()
      );
      final RelBuilder relBuilder =
          call.builder()
              .push(druidJoin)
              .project(
                  RexUtil.fixUp(
                      rexBuilder,
                      pulledUpProjects,
                      RelOptUtil.getFieldTypeList(druidCorrelateUnnest.getRowType())
                  )
              );
      RelNode r = relBuilder.build();
      call.transformTo(r);
      return;
    }
    // Now push the Project back on top of the Correlate.
    final RelBuilder relBuilder =
        call.builder()
            .push(druidCorrelateUnnest)
            .project(
                RexUtil.fixUp(
                    rexBuilder,
                    pulledUpProjects,
                    RelOptUtil.getFieldTypeList(druidCorrelateUnnest.getRowType())
                )
            );

    relBuilder.convert(correlate.getRowType(), false);
    final RelNode build = relBuilder.build();
    call.transformTo(build);
  }

  private static class UpdateJoinConditionShuttle extends RexShuttle
  {
    private final int leftSize;


    private UpdateJoinConditionShuttle(int leftSize)
    {
      this.leftSize = leftSize;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef)
    {
      return new RexInputRef(inputRef.getIndex() + leftSize, inputRef.getType());
    }
  }

  /**
   * Shuttle that pushes correlating variable accesses past a Project.
   */
  private static class PushCorrelatedFieldAccessPastProject extends RexShuttle
  {
    private final CorrelationId correlationId;
    private final CorrelationId newCorrelationId;
    private final Project project;

    // "Sidecar" return value: computed along with the shuttling.
    private final IntSet requiredColumns = new IntAVLTreeSet();

    public PushCorrelatedFieldAccessPastProject(
        final CorrelationId correlationId,
        final CorrelationId newCorrelationId,
        final Project project
    )
    {
      this.correlationId = correlationId;
      this.newCorrelationId = newCorrelationId;
      this.project = project;
    }

    public IntSet getRequiredColumns()
    {
      return requiredColumns;
    }

    @Override
    public RexNode visitFieldAccess(final RexFieldAccess fieldAccess)
    {
      if (fieldAccess.getReferenceExpr() instanceof RexCorrelVariable) {
        final RexCorrelVariable encounteredCorrelVariable = (RexCorrelVariable) fieldAccess.getReferenceExpr();
        if (encounteredCorrelVariable.id.equals(correlationId)) {
          final RexNode projectExpr = project.getProjects().get(fieldAccess.getField().getIndex());

          // Rewrite RexInputRefs as correlation variable accesses.
          final RexBuilder rexBuilder = project.getCluster().getRexBuilder();
          final RexNode newCorrel = rexBuilder.makeCorrel(project.getInput().getRowType(), newCorrelationId);
          return new RexShuttle()
          {
            @Override
            public RexNode visitInputRef(RexInputRef inputRef)
            {
              requiredColumns.add(inputRef.getIndex());
              return project.getCluster().getRexBuilder().makeFieldAccess(newCorrel, inputRef.getIndex());
            }
          }.apply(projectExpr);
        }
      }

      return super.visitFieldAccess(fieldAccess);
    }
  }
}
