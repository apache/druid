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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class DruidCorrelateUnnestRel extends DruidRel<DruidCorrelateUnnestRel>
{
  private static final TableDataSource DUMMY_DATA_SOURCE = new TableDataSource("__unnest__");

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


    final DruidExpression expression = Expressions.toDruidExpression(
        getPlannerContext(),
        rowSignature,
        unnestDatasourceRel.getUnnestProject().getProjects().get(0)
    );

    String dimensionToUnnest;
    /*if (expression.getArguments().get(0).isDirectColumnAccess()) {
      dimensionToUnnest = expression.getArguments().get(0).getDirectColumn();
    } else {
      // to be checked later
      dimensionToUnnest = "dummy";
    }*/

    final VirtualColumnRegistry virtualColumnRegistry = VirtualColumnRegistry.create(
        rowSignature,
        getPlannerContext().getExprMacroTable(),
        getPlannerContext().getPlannerConfig().isForceExpressionVirtualColumns()
    );
    getPlannerContext().setJoinExpressionVirtualColumnRegistry(virtualColumnRegistry);

    if(expression.isDirectColumnAccess()) {
      dimensionToUnnest = expression.getDirectColumn();
    } else {
      // buckle up time to create virtual columns on expressions
      dimensionToUnnest = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(expression, expression.getDruidType());
    }

    DataSource unnestDataSource =
        UnnestDataSource.create(
            baseDataSource,
            dimensionToUnnest,
            // check how this would come from the as alias
            unnestDatasourceRel.getUnnestProject().getRowType().getFieldNames().get(0),
            null
        );



    return partialQuery.build(
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
    /*return partialQuery.build(
        DUMMY_DATA_SOURCE,
        RowSignatures.fromRelDataType(
            logicalCorrelate.getRowType().getFieldNames(),
            logicalCorrelate.getRowType()
        ),
        getPlannerContext(),
        getCluster().getRexBuilder(),
        false
    );*/
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
