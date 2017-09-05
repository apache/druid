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

import com.google.common.base.Preconditions;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.QueryDataSource;
import io.druid.query.groupby.GroupByQuery;
import io.druid.sql.calcite.table.DruidTable;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;
import javax.annotation.Nullable;
import java.io.IOException;

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
  private final DruidQueryBuilder queryBuilder;

  private DruidQueryRel(
      final RelOptCluster cluster,
      final RelTraitSet traitSet,
      final RelOptTable table,
      final DruidTable druidTable,
      final QueryMaker queryMaker,
      final DruidQueryBuilder queryBuilder
  )
  {
    super(cluster, traitSet, queryMaker);
    this.table = Preconditions.checkNotNull(table, "table");
    this.druidTable = Preconditions.checkNotNull(druidTable, "druidTable");
    this.queryBuilder = Preconditions.checkNotNull(queryBuilder, "queryBuilder");
  }

  /**
   * Create a DruidQueryRel representing a full scan.
   */
  public static DruidQueryRel fullScan(
      final RelOptCluster cluster,
      final RelOptTable table,
      final DruidTable druidTable,
      final QueryMaker queryMaker
  )
  {
    return new DruidQueryRel(
        cluster,
        cluster.traitSetOf(Convention.NONE),
        table,
        druidTable,
        queryMaker,
        DruidQueryBuilder.fullScan(druidTable.getRowSignature(), cluster.getTypeFactory())
    );
  }

  @Nullable
  @Override
  public QueryDataSource asDataSource()
  {
    final GroupByQuery groupByQuery = getQueryBuilder().toGroupByQuery(druidTable.getDataSource(), getPlannerContext());

    if (groupByQuery == null) {
      // QueryDataSources must currently embody groupBy queries. This will thrown an exception if the query
      // cannot be converted to a groupBy, but that's OK because we really shouldn't get into that situation anyway.
      // That would be a bug in our planner rules.
      throw new IllegalStateException("WTF?! Tried to convert query to QueryDataSource but couldn't make a groupBy?");
    }

    return new QueryDataSource(groupByQuery);
  }

  @Override
  public DruidQueryRel asBindable()
  {
    return new DruidQueryRel(
        getCluster(),
        getTraitSet().plus(BindableConvention.INSTANCE),
        table,
        druidTable,
        getQueryMaker(),
        queryBuilder
    );
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
        queryBuilder
    );
  }

  @Override
  public List<String> getDatasourceNames()
  {
    return druidTable.getDataSource().getNames();
  }

  @Override
  public RowSignature getSourceRowSignature()
  {
    return druidTable.getRowSignature();
  }

  @Override
  public DruidQueryBuilder getQueryBuilder()
  {
    return queryBuilder;
  }

  @Override
  public DruidQueryRel withQueryBuilder(final DruidQueryBuilder newQueryBuilder)
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
    return getQueryMaker().runQuery(druidTable.getDataSource(), queryBuilder);
  }

  @Override
  public RelOptTable getTable()
  {
    return table;
  }

  @Override
  protected RelDataType deriveRowType()
  {
    return queryBuilder.getRowType();
  }

  @Override
  public RelWriter explainTerms(final RelWriter pw)
  {
    final String queryString;

    try {
      queryString = getQueryMaker()
          .getJsonMapper()
          .writeValueAsString(queryBuilder.toQuery(druidTable.getDataSource(), getPlannerContext()));
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    return pw.item("query", queryString);
  }

  @Override
  public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq)
  {
    double cost = COST_BASE;

    if (queryBuilder.getSelectProjection() != null) {
      cost += COST_PER_COLUMN * queryBuilder.getSelectProjection().getVirtualColumns().size();
      cost += COST_PER_COLUMN * queryBuilder.getSelectProjection().getDirectColumns().size();
    }

    if (queryBuilder.getFilter() != null) {
      cost *= COST_FILTER_MULTIPLIER;
    }

    if (queryBuilder.getGrouping() != null) {
      cost *= COST_GROUPING_MULTIPLIER;
      cost += COST_PER_COLUMN * queryBuilder.getGrouping().getAggregatorFactories().size();
      cost += COST_PER_COLUMN * queryBuilder.getGrouping().getPostAggregators().size();
    }

    if (queryBuilder.getLimitSpec() != null && queryBuilder.getLimitSpec().getLimit() < Integer.MAX_VALUE) {
      cost *= COST_LIMIT_MULTIPLIER;
    }

    if (queryBuilder.getHaving() != null) {
      cost *= COST_HAVING_MULTIPLIER;
    }

    return planner.getCostFactory().makeCost(cost, 0, 0);
  }
}
