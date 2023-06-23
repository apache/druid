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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

/**
 * {@link DruidLogicalNode} convention node for {@link Filter} plan node.
 */
public class DruidFilter extends Filter implements DruidLogicalNode
{

  public DruidFilter(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition)
  {
    super(cluster, traits, child, condition);
    assert getConvention() instanceof DruidLogicalConvention;
  }

  @Override
  public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition)
  {
    return new DruidFilter(getCluster(), getTraitSet(), input, condition);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq)
  {
    return planner.getCostFactory().makeCost(mq.getRowCount(this), 0, 0);
  }
}
