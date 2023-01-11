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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.druid.java.util.common.StringUtils;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DruidCorrelateUnnestRel extends DruidRel<DruidCorrelateUnnestRel>
{
  private final PartialDruidQuery partialQuery;
  private final PlannerConfig plannerConfig;
  private final LogicalCorrelate logicalCorrelate;
  private final RelNode left;
  private final RelNode right;
  private final Filter baseFilter;

  public DruidCorrelateUnnestRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      LogicalCorrelate logicalCorrelateRel,
      PartialDruidQuery partialQuery,
      Filter baseFilter,
      PlannerContext plannerContext
  )
  {
    super(cluster, traitSet, plannerContext);
    this.logicalCorrelate = logicalCorrelateRel;
    this.partialQuery = partialQuery;
    this.plannerConfig = plannerContext.getPlannerConfig();
    this.left = logicalCorrelateRel.getLeft();
    this.right = logicalCorrelateRel.getRight();
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

    final DruidRel<?> druidQueryRel = (DruidRel<?>) left;
    final DruidQuery leftQuery = Preconditions.checkNotNull((druidQueryRel).toDruidQuery(false), "leftQuery");
    final DataSource leftDataSource;

    if (DruidJoinQueryRel.computeLeftRequiresSubquery(druidQueryRel)) {
      leftDataSource = new QueryDataSource(leftQuery.getQuery());
    } else {
      leftDataSource = leftQuery.getDataSource();
    }

    final DruidUnnestDatasourceRel unnestDatasourceRel = (DruidUnnestDatasourceRel) right;
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

    // placeholder for dimension or expression to be unnested
    final String dimOrExpToUnnest;
    final VirtualColumnRegistry virtualColumnRegistry = VirtualColumnRegistry.create(
        rowSignature,
        getPlannerContext().getExprMacroTable(),
        getPlannerContext().getPlannerConfig().isForceExpressionVirtualColumns()
    );

    boolean unnestProjectNeeded = false;
    getPlannerContext().setJoinExpressionVirtualColumnRegistry(virtualColumnRegistry);

    // handling for case when mv_to_array is used
    // No need to use virtual column in such a case
    if (StringUtils.toLowerCase(expression.getExpression()).startsWith("mv_to_array")) {
      dimOrExpToUnnest = expression.getArguments().get(0).getSimpleExtraction().getColumn();
    } else {
      if (expression.isDirectColumnAccess()) {
        dimOrExpToUnnest = expression.getDirectColumn();
      } else {
        // buckle up time to create virtual columns on expressions
        unnestProjectNeeded = true;
        dimOrExpToUnnest = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
            expression,
            expression.getDruidType()
        );
      }
    }

    // create the unnest data source to use in the partial query
    UnnestDataSource unnestDataSource =
        UnnestDataSource.create(
            leftDataSource,
            dimOrExpToUnnest,
            unnestDatasourceRel.getUnnestProject().getRowType().getFieldNames().get(0),
            null
        );

    // add the unnest project to the partial query
    // This is necessary to handle the virtual columns on the unnestProject
    PartialDruidQuery partialDruidQuery = unnestProjectNeeded ? partialQuery.withUnnest(unnestProject) : partialQuery;
    return partialDruidQuery.build(
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
        logicalCorrelate.copy(
            logicalCorrelate.getTraitSet(),
            left,
            right,
            logicalCorrelate.getCorrelationId(),
            logicalCorrelate.getRequiredColumns(),
            logicalCorrelate.getJoinType()
        ),
        partialQuery,
        baseFilter,
        getPlannerContext()
    );
  }

  @Override
  public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs)
  {
    return new DruidCorrelateUnnestRel(
        getCluster(),
        traitSet,
        (LogicalCorrelate) logicalCorrelate.copy(logicalCorrelate.getTraitSet(), inputs),
        getPartialDruidQuery(),
        baseFilter,
        getPlannerContext()
    );
  }

  @Override
  public Set<String> getDataSourceNames()
  {
    final Set<String> retVal = new HashSet<>(((DruidRel<?>) left).getDataSourceNames());
    return retVal;
  }
}
