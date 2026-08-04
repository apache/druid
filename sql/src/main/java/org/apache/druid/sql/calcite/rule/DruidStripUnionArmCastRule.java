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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Strips numeric-to-numeric {@code CAST} expressions so {@link DruidUnionDataSourceRule} is able
 * to generate unions in more plans.
 *
 * <p>Example: for {@code SELECT dim1, dim2, m1 FROM foo2 UNION ALL SELECT dim1, dim2, m1 FROM foo}
 * where {@code foo2.m1} is {@code BIGINT} and {@code foo.m1} is {@code FLOAT}, Calcite produces:
 * <pre>
 *   LogicalUnion(all=[true])
 *     LogicalProject(dim1=[$1], dim2=[$2], m1=[CAST($5):FLOAT])
 *       LogicalTableScan(table=[[druid, foo2]])
 *     LogicalProject(dim1=[$1], dim2=[$2], m1=[$5])
 *       LogicalTableScan(table=[[druid, foo]])
 * </pre>
 * This rule rewrites it to:
 * <pre>
 *   LogicalUnion(all=[true])
 *     LogicalProject(dim1=[$1], dim2=[$2], m1=[$5])  // CAST stripped
 *       LogicalTableScan(table=[[druid, foo2]])
 *     LogicalProject(dim1=[$1], dim2=[$2], m1=[$5])
 *       LogicalTableScan(table=[[druid, foo]])
 * </pre>
 *
 * <p>See {@code union_datasource.iq} for test cases that fail without this rule.
 */
public class DruidStripUnionArmCastRule extends RelOptRule implements SubstitutionRule
{
  private static final DruidStripUnionArmCastRule INSTANCE = new DruidStripUnionArmCastRule();

  private DruidStripUnionArmCastRule()
  {
    super(operand(Union.class, any()));
  }

  public static DruidStripUnionArmCastRule instance()
  {
    return INSTANCE;
  }

  @Override
  public void onMatch(final RelOptRuleCall call)
  {
    final Union union = call.rel(0);
    final List<RelNode> newInputs = new ArrayList<>(union.getInputs().size());
    boolean anyChanged = false;
    for (final RelNode input : union.getInputs()) {
      final RelNode arm = input.stripped();
      if (armHasStrippableCast(arm)) {
        newInputs.add(stripArmCasts((Project) arm));
        anyChanged = true;
      } else {
        newInputs.add(input);
      }
    }
    if (anyChanged) {
      final RelNode transformed = union.copy(union.getTraitSet(), newInputs, union.all);
      if (!transformed.getRowType().equals(union.getRowType())) {
        return;
      }
      call.transformTo(transformed);
    }
  }

  /**
   * Returns true when {@code arm} is a {@link Project} directly atop a {@link TableScan} and at
   * least one of its projections is a Druid-absorbable numeric {@code CAST}.
   */
  private static boolean armHasStrippableCast(final RelNode arm)
  {
    if (!(arm instanceof Project project)) {
      return false;
    }
    if (!(project.getInput().stripped() instanceof TableScan)) {
      return false;
    }
    for (final RexNode expr : project.getProjects()) {
      if (isStrippableCast(expr)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns a new {@link LogicalProject} with strippable CAST replaced by their underlying {@link RexInputRef}.
   */
  private static Project stripArmCasts(final Project project)
  {
    final List<RexNode> newExprs = new ArrayList<>(project.getProjects().size());
    for (final RexNode expr : project.getProjects()) {
      if (isStrippableCast(expr)) {
        newExprs.add(((RexCall) expr).getOperands().get(0));
      } else {
        newExprs.add(expr);
      }
    }
    // Using LogicalProject.create so Calcite re-derives the row type from the new expressions.
    return LogicalProject.create(
        project.getInput(),
        project.getHints(),
        newExprs,
        project.getRowType().getFieldNames(),
        project.getVariablesSet()
    );
  }

  /**
   * Returns true when {@code expr} is a {@code CAST} of a {@link RexInputRef} between numeric types.
   */
  private static boolean isStrippableCast(final RexNode expr)
  {
    if (!expr.isA(SqlKind.CAST)) {
      return false;
    }
    final RexCall castCall = (RexCall) expr;
    if (castCall.getOperands().size() != 1 || !(castCall.getOperands().get(0) instanceof RexInputRef)) {
      return false;
    }
    final RelDataType fromType = castCall.getOperands().get(0).getType();
    final RelDataType toType = castCall.getType();
    return SqlTypeUtil.isNumeric(fromType) && SqlTypeUtil.isNumeric(toType);
  }
}
