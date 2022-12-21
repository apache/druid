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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;
import java.util.Set;
import java.util.stream.Collectors;

public class DruidCorrelateUnnestRel extends DruidRel<DruidCorrelateUnnestRel>
{
  // This may be needed for the explain plan later
  // private static final TableDataSource DUMMY_DATA_SOURCE = new TableDataSource("__unnest__");
  private final PartialDruidQuery partialQuery;
  private final PlannerConfig plannerConfig;
  private final LogicalCorrelate logicalCorrelate;
  private final DataSource baseDataSource;
  private final DruidQueryRel druidQueryRel;
  private final Filter baseFilter;
  private DruidUnnestDatasourceRel unnestDatasourceRel;

  public DruidCorrelateUnnestRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      LogicalCorrelate logicalCorrelateRel,
      PartialDruidQuery partialQuery,
      DruidQueryRel druidQueryRel,
      DruidUnnestDatasourceRel unnestDatasourceRel,
      Filter baseFilter,
      PlannerContext plannerContext
  )
  {
    super(cluster, traitSet, plannerContext);
    this.logicalCorrelate = logicalCorrelateRel;
    this.partialQuery = partialQuery;
    this.plannerConfig = plannerContext.getPlannerConfig();
    this.druidQueryRel = druidQueryRel;
    this.baseDataSource = druidQueryRel.getDruidTable().getDataSource();
    this.unnestDatasourceRel = unnestDatasourceRel;
    this.baseFilter = baseFilter;
  }


  @Nullable
  @Override
  public PartialDruidQuery getPartialDruidQuery()
  {
    return partialQuery;
  }

  @Override
  public DruidCorrelateUnnestRel withPartialQuery(PartialDruidQuery newQueryBuilder)
  {
    return new DruidCorrelateUnnestRel(
        getCluster(),
        getTraitSet().plusAll(newQueryBuilder.getRelTraits()),
        logicalCorrelate,
        newQueryBuilder,
        druidQueryRel,
        unnestDatasourceRel,
        baseFilter,
        getPlannerContext()
    );
  }

  @Override
  public DruidQuery toDruidQuery(boolean finalizeAggregations)
  {
    final RowSignature rowSignature = RowSignatures.fromRelDataType(
        logicalCorrelate.getRowType().getFieldNames(),
        logicalCorrelate.getRowType()
    );

    final DruidQuery leftQuery = Preconditions.checkNotNull((druidQueryRel).toDruidQuery(false), "leftQuery");
    final DataSource leftDataSource;

    if (DruidJoinQueryRel.computeLeftRequiresSubquery(druidQueryRel)) {
      leftDataSource = new QueryDataSource(leftQuery.getQuery());
    } else {
      leftDataSource = leftQuery.getDataSource();
    }

    final DruidExpression expression = Expressions.toDruidExpression(
        getPlannerContext(),
        rowSignature,
        unnestDatasourceRel.getUnnestProject().getProjects().get(0)
    );

    LogicalProject unnestProject = LogicalProject.create(
        this,
        ImmutableList.of(unnestDatasourceRel.getUnnestProject()
                                            .getProjects()
                                            .get(0)),
        unnestDatasourceRel.getUnnestProject().getRowType()
    );

    String dimensionToUnnest;

    final VirtualColumnRegistry virtualColumnRegistry = VirtualColumnRegistry.create(
        rowSignature,
        getPlannerContext().getExprMacroTable(),
        getPlannerContext().getPlannerConfig().isForceExpressionVirtualColumns()
    );
    getPlannerContext().setJoinExpressionVirtualColumnRegistry(virtualColumnRegistry);

    if (expression.isDirectColumnAccess()) {
      dimensionToUnnest = expression.getDirectColumn();
    } else {
      // buckle up time to create virtual columns on expressions
      dimensionToUnnest = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
          expression,
          expression.getDruidType()
      );
    }

    // create the unnest data source to use in the partial query
    UnnestDataSource unnestDataSource =
        UnnestDataSource.create(
            leftDataSource,
            dimensionToUnnest,
            unnestDatasourceRel.getUnnestProject().getRowType().getFieldNames().get(0),
            null
        );

    // add the unnest project to the partial query
    // This is necessary to handle the virtual columns on the unnestProject
    return partialQuery.withUnnest(unnestProject).build(
        unnestDataSource,
        rowSignature,
        getPlannerContext(),
        getCluster().getRexBuilder(),
        finalizeAggregations,
        virtualColumnRegistry
    );
  }

  @Override
  protected RelDataType deriveRowType()
  {
    return partialQuery.getRowType();
  }

  @Override
  public DruidQuery toDruidQueryForExplaining()
  {
    return toDruidQuery(false);
  }

  @Override
  public DruidCorrelateUnnestRel asDruidConvention()
  {
    return new DruidCorrelateUnnestRel(
        getCluster(),
        getTraitSet().replace(DruidConvention.instance()),
        (LogicalCorrelate) logicalCorrelate.copy(
            logicalCorrelate.getTraitSet(),
            logicalCorrelate.getInputs()
                            .stream()
                            .map(input -> RelOptRule.convert(input, DruidConvention.instance()))
                            .collect(Collectors.toList())
        ),
        partialQuery,
        druidQueryRel,
        unnestDatasourceRel,
        baseFilter,
        getPlannerContext()
    );
  }


  @Override
  public Set<String> getDataSourceNames()
  {
    return baseDataSource.getTableNames();
  }
}
