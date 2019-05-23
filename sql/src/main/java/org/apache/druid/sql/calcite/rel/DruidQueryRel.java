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

package org.apache.druid.sql.calcite.rel;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.sql.calcite.table.DruidTable;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * DruidRel that uses a "table" dataSource.
 */
public class DruidQueryRel extends DruidRel<DruidQueryRel>
{
  // Factors used for computing cost (see computeSelfCost). These are intended to encourage pushing down filters
  // and limits through stacks of nested queries when possible.
  private static final double COST_BASE = 1.0;
  private static final double COST_PER_COLUMN = 0.001;
  private static final double COST_FILTER_MULTIPLIER = 0.1;
  private static final double COST_GROUPING_MULTIPLIER = 0.5;
  private static final double COST_LIMIT_MULTIPLIER = 0.5;
  private static final double COST_HAVING_MULTIPLIER = 5.0;

  private final RelOptTable table;
  private final DruidTable druidTable;
  private final PartialDruidQuery partialQuery;

  private DruidQueryRel(
      final RelOptCluster cluster,
      final RelTraitSet traitSet,
      final RelOptTable table,
      final DruidTable druidTable,
      final QueryMaker queryMaker,
      final PartialDruidQuery partialQuery
  )
  {
    super(cluster, traitSet, queryMaker);
    this.table = Preconditions.checkNotNull(table, "table");
    this.druidTable = Preconditions.checkNotNull(druidTable, "druidTable");
    this.partialQuery = Preconditions.checkNotNull(partialQuery, "partialQuery");
  }

  /**
   * Create a DruidQueryRel representing a full scan.
   */
  public static DruidQueryRel fullScan(
      final LogicalTableScan scanRel,
      final RelOptTable table,
      final DruidTable druidTable,
      final QueryMaker queryMaker
  )
  {
    return new DruidQueryRel(
        scanRel.getCluster(),
        scanRel.getCluster().traitSetOf(Convention.NONE),
        table,
        druidTable,
        queryMaker,
        PartialDruidQuery.create(scanRel)
    );
  }

  @Override
  @Nonnull
  public DruidQuery toDruidQuery(final boolean finalizeAggregations)
  {
    return partialQuery.build(
        druidTable.getDataSource(),
        druidTable.getRowSignature(),
        getPlannerContext(),
        getCluster().getRexBuilder(),
        finalizeAggregations
    );
  }

  @Override
  public DruidQuery toDruidQueryForExplaining()
  {
    return toDruidQuery(false);
  }

  @Override
  public DruidQueryRel asDruidConvention()
  {
    return new DruidQueryRel(
        getCluster(),
        getTraitSet().replace(DruidConvention.instance()),
        table,
        druidTable,
        getQueryMaker(),
        partialQuery
    );
  }

  @Override
  public List<String> getDataSourceNames()
  {
    return druidTable.getDataSource().getNames();
  }

  @Override
  public PartialDruidQuery getPartialDruidQuery()
  {
    return partialQuery;
  }

  @Override
  public DruidQueryRel withPartialQuery(final PartialDruidQuery newQueryBuilder)
  {
    return new DruidQueryRel(
        getCluster(),
        getTraitSet().plusAll(newQueryBuilder.getRelTraits()),
        table,
        druidTable,
        getQueryMaker(),
        newQueryBuilder
    );
  }

  @Override
  public int getQueryCount()
  {
    return 1;
  }

  @Override
  public Sequence<Object[]> runQuery()
  {
    // runQuery doesn't need to finalize aggregations, because the fact that runQuery is happening suggests this
    // is the outermost query and it will actually get run as a native query. Druid's native query layer will
    // finalize aggregations for the outermost query even if we don't explicitly ask it to.

    return getQueryMaker().runQuery(toDruidQuery(false));
  }

  @Override
  public RelOptTable getTable()
  {
    return table;
  }

  @Override
  protected RelDataType deriveRowType()
  {
    return partialQuery.getRowType();
  }

  @Override
  public RelWriter explainTerms(final RelWriter pw)
  {
    final String queryString;
    final DruidQuery druidQuery = toDruidQueryForExplaining();

    try {
      queryString = getQueryMaker().getJsonMapper().writeValueAsString(druidQuery.getQuery());
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    return pw.item("query", queryString)
             .item("signature", druidQuery.getOutputRowSignature());
  }

  @Override
  public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq)
  {
    double cost = COST_BASE;

    if (partialQuery.getSelectProject() != null) {
      cost += COST_PER_COLUMN * partialQuery.getSelectProject().getChildExps().size();
    }

    if (partialQuery.getWhereFilter() != null) {
      cost *= COST_FILTER_MULTIPLIER;
    }

    if (partialQuery.getAggregate() != null) {
      cost *= COST_GROUPING_MULTIPLIER;
      cost += COST_PER_COLUMN * partialQuery.getAggregate().getGroupSet().size();
      cost += COST_PER_COLUMN * partialQuery.getAggregate().getAggCallList().size();
    }

    if (partialQuery.getAggregateProject() != null) {
      cost += COST_PER_COLUMN * partialQuery.getAggregateProject().getChildExps().size();
    }

    if (partialQuery.getSort() != null && partialQuery.getSort().fetch != null) {
      cost *= COST_LIMIT_MULTIPLIER;
    }

    if (partialQuery.getSortProject() != null) {
      cost += COST_PER_COLUMN * partialQuery.getSortProject().getChildExps().size();
    }

    if (partialQuery.getHavingFilter() != null) {
      cost *= COST_HAVING_MULTIPLIER;
    }

    return planner.getCostFactory().makeCost(cost, 0, 0);
  }
}
