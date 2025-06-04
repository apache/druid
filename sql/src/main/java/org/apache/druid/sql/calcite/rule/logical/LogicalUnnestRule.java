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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.druid.error.DruidException;
import org.apache.druid.sql.calcite.rel.DruidCorrelateUnnestRel;

/**
 * Recognizes a LogicalUnnest operation in the plan.
 *
 * Matches on the layout:
 *
 * <pre>
 *   LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{4}])
 *     RelNodeSubtree
 *     Uncollect
 *       LogicalProject(arrayLongNulls=[$cor0.arrayLongNulls])
 *         LogicalValues(tuples=[[{ 0 }]])
 * </pre>
 *
 * Translates it to use a {@link LogicalUnnest} like:
 *
 * <pre>
 *   LogicalUnnest(unnestExpr=[$cor0.arrayLongNulls])
 *     RelNodeSubtree
 * </pre>
 *
 * It raises an error for cases when {@link LogicalCorrelate} can't be
 * translated as those are currently unsupported in Druid.
 */
public class LogicalUnnestRule extends RelOptRule implements SubstitutionRule
{
  public LogicalUnnestRule()
  {
    super(operand(LogicalCorrelate.class, any()));
  }

  @Override
  public boolean autoPruneOld()
  {
    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    LogicalCorrelate cor = call.rel(0);
    UnnestConfiguration unnestConfig = unwrapUnnestConfigurationExpression(cor.getRight().stripped());

    if (unnestConfig == null) {
      throw DruidException.defensive("Couldn't process possible unnest for reltree: \n%s", RelOptUtil.toString(cor));
    }

    unnestConfig = unnestConfig.withExpr(
        new DruidCorrelateUnnestRel.CorrelatedFieldAccessToInputRef(cor.getCorrelationId())
            .apply(unnestConfig.expr)
    );

    RelDataTypeField unnestFieldType = Iterables.getLast(cor.getRowType().getFieldList());

    RelBuilder builder = call.builder();
    builder.push(cor.getLeft());
    builder.push(
        new LogicalUnnest(
            cor.getCluster(),
            cor.getTraitSet(),
            builder.build(),
            unnestConfig.expr,
            unnestFieldType,
            unnestConfig.condition
        )
    );
    if (unnestConfig.discard) {
      // drop unnested output column
      ImmutableList<RexNode> fields = builder.fields();
      builder.project(fields.subList(0, fields.size() - 1));
    }
    call.transformTo(builder.build());
  }

  private static class UnnestConfiguration
  {
    protected final RexNode expr;
    protected final RexNode condition;
    protected final boolean discard;


    public UnnestConfiguration(RexNode unnestExpression, RexNode condition, boolean discard)
    {
      this.expr = unnestExpression;
      this.condition = condition;
      this.discard = discard;
    }

    public UnnestConfiguration withExpr(RexNode expr)
    {
      return new UnnestConfiguration(expr, condition, discard);
    }

    public static UnnestConfiguration ofExpression(RexNode unnestExpression)
    {
      return new UnnestConfiguration(unnestExpression, null, false);
    }

    public UnnestConfiguration withFilter(RexNode condition)
    {
      return new UnnestConfiguration(expr, condition, discard);
    }

    public UnnestConfiguration withDiscard()
    {
      return new UnnestConfiguration(expr, condition, true);
    }
  }

  private UnnestConfiguration unwrapUnnestConfigurationExpression(RelNode rel)
  {
    rel = rel.stripped();
    if (rel instanceof Project) {
      Project project = (Project) rel;
      if (project.getProjects().size() == 0) {
        return unwrapUnnestConfigurationExpression(project.getInput()).withDiscard();
      }
    }
    if (rel instanceof Filter) {
      Filter filter = (Filter) rel;
      UnnestConfiguration conf = unwrapUnnestConfigurationExpression(filter.getInput());
      if (conf != null) {
        return conf.withFilter(filter.getCondition());
      }
    }
    if (rel instanceof Uncollect) {
      Uncollect uncollect = (Uncollect) rel;
      if (!uncollect.withOrdinality) {
        return unwrapProjectExpression(uncollect.getInput());
      }
    }
    return null;
  }

  private UnnestConfiguration unwrapProjectExpression(RelNode rel)
  {
    rel = rel.stripped();
    if (rel instanceof Project) {
      Project project = (Project) rel;
      if (isValues(project.getInput().stripped())) {
        return UnnestConfiguration.ofExpression(Iterables.getOnlyElement(project.getProjects()));
      }
    }
    return null;
  }

  private boolean isValues(RelNode rel)
  {
    rel = rel.stripped();
    return (rel instanceof LogicalValues);
  }
}
