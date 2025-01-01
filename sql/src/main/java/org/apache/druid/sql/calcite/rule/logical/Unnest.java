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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public abstract class Unnest extends SingleRel
{
  protected final RexNode unnestExpr;
  protected final RexNode filter;

  protected Unnest(RelOptCluster cluster, RelTraitSet traits, RelNode input, RexNode unnestExpr,
      RelDataType rowType, RexNode condition)
  {
    super(cluster, traits, input);
    this.unnestExpr = unnestExpr;
    this.rowType = rowType;
    this.filter = condition;
  }

  public final RexNode getUnnestExpr()
  {
    return unnestExpr;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw)
  {
    return super.explainTerms(pw)
        .item("unnestExpr", unnestExpr)
        .itemIf("filter", filter, filter != null);
  }

  @Override
  public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs)
  {
    return copy(traitSet, inputs.get(0));
  }

  protected abstract RelNode copy(RelTraitSet traitSet, RelNode input);
}
