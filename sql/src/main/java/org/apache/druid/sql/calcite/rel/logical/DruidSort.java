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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.druid.sql.calcite.planner.OffsetLimit;
import org.apache.druid.sql.calcite.rel.CostEstimates;

/**
 * {@link DruidLogicalNode} convention node for {@link Sort} plan node.
 */
public class DruidSort extends Sort implements DruidLogicalNode
{
  private DruidSort(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      RelCollation collation,
      RexNode offset,
      RexNode fetch
  )
  {
    super(cluster, traits, input, collation, offset, fetch);
    assert getConvention() instanceof DruidLogicalConvention;
  }

  public static DruidSort create(RelNode input, RelCollation collation, RexNode offset, RexNode fetch)
  {
    RelOptCluster cluster = input.getCluster();
    collation = RelCollationTraitDef.INSTANCE.canonize(collation);
    RelTraitSet traitSet =
        input.getTraitSet().replace(DruidLogicalConvention.instance()).replace(collation);
    return new DruidSort(cluster, traitSet, input, collation, offset, fetch);
  }

  @Override
  public Sort copy(
      RelTraitSet traitSet,
      RelNode newInput,
      RelCollation newCollation,
      RexNode offset,
      RexNode fetch
  )
  {
    return new DruidSort(getCluster(), traitSet, newInput, newCollation, offset, fetch);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq)
  {
    double cost = 0;
    double rowCount = mq.getRowCount(this);

    if (fetch != null) {
      OffsetLimit offsetLimit = OffsetLimit.fromSort(this);
      rowCount = Math.min(rowCount, offsetLimit.getLimit() - offsetLimit.getOffset());
    }

    if (!getCollation().getFieldCollations().isEmpty() && fetch == null) {
      cost = rowCount * CostEstimates.MULTIPLIER_ORDER_BY;
    }
    return planner.getCostFactory().makeCost(rowCount, cost, 0);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw)
  {
    return super.explainTerms(pw).item("druid", "logical");
  }
}
