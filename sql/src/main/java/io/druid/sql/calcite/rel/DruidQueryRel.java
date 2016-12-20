/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.rel;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.druid.sql.calcite.filtration.Filtration;
import io.druid.sql.calcite.table.DruidTable;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.Row;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

public class DruidQueryRel extends DruidRel<DruidQueryRel>
{
  private final RelOptTable table;
  private final DruidTable druidTable;
  private final DruidQueryBuilder queryBuilder;

  private DruidQueryRel(
      final RelOptCluster cluster,
      final RelTraitSet traitSet,
      final RelOptTable table,
      final DruidTable druidTable,
      final DruidQueryBuilder queryBuilder
  )
  {
    super(cluster, traitSet);
    this.table = Preconditions.checkNotNull(table, "table");
    this.druidTable = Preconditions.checkNotNull(druidTable, "druidTable");
    this.queryBuilder = Preconditions.checkNotNull(queryBuilder, "queryBuilder");
  }

  /**
   * Create a DruidQueryRel representing a full scan.
   */
  public static DruidQueryRel fullScan(
      final RelOptCluster cluster,
      final RelTraitSet traitSet,
      final RelOptTable table,
      final DruidTable druidTable
  )
  {
    return new DruidQueryRel(
        cluster,
        traitSet,
        table,
        druidTable,
        DruidQueryBuilder.fullScan(druidTable, cluster.getTypeFactory())
    );
  }

  public DruidQueryRel asBindable()
  {
    return new DruidQueryRel(
        getCluster(),
        getTraitSet().plus(BindableConvention.INSTANCE),
        table,
        druidTable,
        queryBuilder
    );
  }

  public DruidTable getDruidTable()
  {
    return druidTable;
  }

  public DruidQueryBuilder getQueryBuilder()
  {
    return queryBuilder;
  }

  public DruidQueryRel withQueryBuilder(final DruidQueryBuilder newQueryBuilder)
  {
    return new DruidQueryRel(
        getCluster(),
        getTraitSet().plusAll(newQueryBuilder.getRelTraits()),
        table,
        druidTable,
        newQueryBuilder
    );
  }

  @Override
  public void accumulate(final Function<Row, Void> sink)
  {
    queryBuilder.accumulate(druidTable, sink);
  }

  @Override
  public RelOptTable getTable()
  {
    return table;
  }

  @Override
  public Class<Object[]> getElementType()
  {
    return Object[].class;
  }

  @Override
  protected RelDataType deriveRowType()
  {
    return queryBuilder.getRowType();
  }

  @Override
  public RelWriter explainTerms(final RelWriter pw)
  {
    pw.item("dataSource", druidTable.getDataSource());
    if (queryBuilder != null) {
      final Filtration filtration = Filtration.create(queryBuilder.getFilter()).optimize(druidTable);
      if (!filtration.getIntervals().equals(ImmutableList.of(Filtration.eternity()))) {
        pw.item("intervals", filtration.getIntervals());
      }
      if (filtration.getDimFilter() != null) {
        pw.item("filter", filtration.getDimFilter());
      }
      if (queryBuilder.getSelectProjection() != null) {
        pw.item("selectDimensions", queryBuilder.getSelectProjection().getDimensions());
        pw.item("selectMetrics", queryBuilder.getSelectProjection().getMetrics());
      }
      if (queryBuilder.getGrouping() != null) {
        pw.item("dimensions", queryBuilder.getGrouping().getDimensions());
        pw.item("aggregations", queryBuilder.getGrouping().getAggregations());
      }
      if (queryBuilder.getHaving() != null) {
        pw.item("having", queryBuilder.getHaving());
      }
      if (queryBuilder.getLimitSpec() != null) {
        pw.item("limitSpec", queryBuilder.getLimitSpec());
      }
    }
    return pw;
  }

  @Override
  public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq)
  {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }
}
