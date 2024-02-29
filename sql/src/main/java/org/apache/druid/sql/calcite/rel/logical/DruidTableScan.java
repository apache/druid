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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Table;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.querygen.InputDescProducer;
import org.apache.druid.sql.calcite.table.DruidTable;

import java.util.List;

/**
 * {@link DruidLogicalNode} convention node for {@link TableScan} plan node.
 */
public class DruidTableScan extends TableScan implements DruidLogicalNode, InputDescProducer
{
  public DruidTableScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table
  )
  {
    super(cluster, traitSet, table);
    assert getConvention() instanceof DruidLogicalConvention;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs)
  {
    return new DruidTableScan(getCluster(), traitSet, table);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq)
  {
    return planner.getCostFactory().makeTinyCost();
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq)
  {
    return 1_000;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw)
  {
    return super.explainTerms(pw).item("druid", "logical");
  }

  @Override
  public RelDataType deriveRowType()
  {
    return super.deriveRowType();
  }

  public static DruidTableScan create(RelOptCluster cluster, final RelOptTable relOptTable)
  {
    final Table table = relOptTable.unwrap(Table.class);
    final RelTraitSet traitSet =
        cluster.traitSet().replaceIfs(RelCollationTraitDef.INSTANCE, () -> {
          if (table != null) {
            return table.getStatistic().getCollations();
          }
          return ImmutableList.of();
        });
    return new DruidTableScan(cluster, traitSet, relOptTable);
  }

  @Override
  public InputDesc getInputDesc(PlannerContext plannerContext, List<InputDesc> inputs)
  {
    final DruidTable druidTable = getDruidTable();
    return new InputDesc(druidTable.getDataSource(), druidTable.getRowSignature());
  }

  private DruidTable getDruidTable()
  {
    final RelOptTable table = getTable();
    final DruidTable druidTable = table.unwrap(DruidTable.class);
    Preconditions.checkNotNull(druidTable, "DruidTable may not be null");
    return druidTable;
  }
}
