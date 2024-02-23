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
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.querygen.SourceDescProducer;
import java.util.ArrayList;
import java.util.List;

public class DruidUnion extends Union implements DruidLogicalNode, SourceDescProducer
{
  public DruidUnion(
      RelOptCluster cluster,
      RelTraitSet traits,
      List<RelHint> hints,
      List<RelNode> inputs,
      boolean all)
  {
    super(cluster, traits, hints, inputs, all);
  }

  @Override
  public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all)
  {
    return new DruidUnion(getCluster(), traitSet, hints, inputs, all);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq)
  {
    return planner.getCostFactory().makeCost(mq.getRowCount(this), 0, 0);
  }

  @Override
  public SourceDesc getSourceDesc(PlannerContext plannerContext, List<SourceDesc> sources)
  {
    List<DataSource> dataSources = new ArrayList<>();
    RowSignature signature = null;
    for (SourceDesc sourceDesc : sources) {
      checkDataSourceSupported(sourceDesc.dataSource);
      dataSources.add(sourceDesc.dataSource);
      if (signature == null) {
        signature = sourceDesc.rowSignature;
      } else {
        if (!signature.equals(sourceDesc.rowSignature)) {
          throw DruidException.defensive(
              "Row signature mismatch in Union inputs [%s] and [%s]",
              signature,
              sourceDesc.rowSignature
          );
        }
      }
    }
    return new SourceDesc(new UnionDataSource(dataSources), signature);
  }

  private void checkDataSourceSupported(DataSource dataSource)
  {
    if (dataSource instanceof TableDataSource || dataSource instanceof InlineDataSource) {
      return;
    }
    throw DruidException.defensive("Only Table and Values are supported as inputs for Union [%s]", dataSource);
  }
}
