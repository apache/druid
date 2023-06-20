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

package org.apache.druid.sql.calcite.rel.logical;

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.sql.calcite.rel.CostEstimates;

import java.util.List;

/**
 * {@link DruidLogicalNode} convention node for {@link Project} plan node.
 */
public class DruidProject extends Project implements DruidLogicalNode
{
  public DruidProject(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      List<? extends RexNode> projects,
      RelDataType rowType
  )
  {
    super(cluster, traitSet, input, projects, rowType);
    assert getConvention() instanceof DruidLogicalConvention;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq)
  {
    double cost = 0;
    double rowCount = mq.getRowCount(getInput());
    for (final RexNode rexNode : getProjects()) {
      if (rexNode.isA(SqlKind.INPUT_REF)) {
        cost += 0;
      }
      if (rexNode.getType().getSqlTypeName() == SqlTypeName.BOOLEAN || rexNode.isA(SqlKind.CAST)) {
        cost += 0;
      } else if (!rexNode.isA(ImmutableSet.of(SqlKind.INPUT_REF, SqlKind.LITERAL))) {
        cost += CostEstimates.COST_EXPRESSION;
      }
    }
    // adding atleast 1e-6 cost since zero cost is converted to a tiny cost by the planner which is (1 row, 1 cpu, 0 io)
    // that becomes a significant cost in some cases.
    return planner.getCostFactory().makeCost(0, Math.max(cost * rowCount, 1e-6), 0);
  }

  @Override
  public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType)
  {
    return new DruidProject(getCluster(), traitSet, input, exps, rowType);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw)
  {
    return super.explainTerms(pw).item("druid", "logical");
  }

  public static DruidProject create(final RelNode input, final List<? extends RexNode> projects, RelDataType rowType)
  {
    final RelOptCluster cluster = input.getCluster();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final RelTraitSet traitSet =
        input.getTraitSet().replaceIfs(
            RelCollationTraitDef.INSTANCE,
            () -> RelMdCollation.project(mq, input, projects)
        );
    return new DruidProject(cluster, traitSet, input, projects, rowType);
  }
}
